#!/usr/bin/env python3
"""
Advanced Search Engine for Gitea-Kimai Integration

This module provides advanced search and filtering capabilities for sync data,
enabling users to quickly find and filter issues, timesheets, and sync operations.
"""

import re
import json
import sqlite3
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import operator
from functools import reduce

logger = logging.getLogger(__name__)

class SearchOperator(Enum):
    """Search operators for filtering."""
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    GREATER_THAN = "gt"
    GREATER_EQUAL = "gte"
    LESS_THAN = "lt"
    LESS_EQUAL = "lte"
    IN = "in"
    NOT_IN = "not_in"
    BETWEEN = "between"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    REGEX = "regex"

class SortOrder(Enum):
    """Sort order options."""
    ASC = "asc"
    DESC = "desc"

@dataclass
class SearchFilter:
    """Search filter definition."""
    field: str
    operator: SearchOperator
    value: Any
    case_sensitive: bool = False

@dataclass
class SortCriteria:
    """Sort criteria definition."""
    field: str
    order: SortOrder = SortOrder.ASC

@dataclass
class SearchQuery:
    """Complete search query definition."""
    filters: List[SearchFilter] = field(default_factory=list)
    sort_criteria: List[SortCriteria] = field(default_factory=list)
    limit: Optional[int] = None
    offset: int = 0
    search_text: Optional[str] = None
    date_range: Optional[Tuple[datetime, datetime]] = None

@dataclass
class SearchResult:
    """Search result container."""
    data: List[Dict[str, Any]]
    total_count: int
    filtered_count: int
    query_time: float
    page: int
    per_page: int
    has_more: bool

class SearchIndexer:
    """Manages search indexes for fast text search."""

    def __init__(self, database_path: str):
        self.database_path = database_path
        self._init_search_indexes()

    def _init_search_indexes(self):
        """Initialize search indexes."""
        with sqlite3.connect(self.database_path) as conn:
            # Create full-text search table for sync items
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS sync_items_fts
                USING fts5(
                    id,
                    title,
                    repository,
                    content=sync_items,
                    content_rowid=rowid
                )
            """)

            # Create trigram index for fuzzy search
            conn.execute("""
                CREATE TABLE IF NOT EXISTS search_trigrams (
                    trigram TEXT,
                    item_id TEXT,
                    field TEXT,
                    frequency INTEGER,
                    PRIMARY KEY (trigram, item_id, field)
                )
            """)

            # Create search statistics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS search_stats (
                    query_hash TEXT PRIMARY KEY,
                    query_text TEXT,
                    result_count INTEGER,
                    execution_time REAL,
                    last_executed DATETIME,
                    frequency INTEGER DEFAULT 1
                )
            """)

            conn.commit()

    def index_item(self, item: Dict[str, Any]):
        """Index a single item for search."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                # Insert into FTS index
                conn.execute("""
                    INSERT OR REPLACE INTO sync_items_fts (id, title, repository)
                    VALUES (?, ?, ?)
                """, (
                    item.get('id'),
                    item.get('title', ''),
                    item.get('repository', '')
                ))

                # Generate and store trigrams
                self._generate_trigrams(conn, item)
                conn.commit()

        except Exception as e:
            logger.error(f"Failed to index item {item.get('id')}: {e}")

    def _generate_trigrams(self, conn: sqlite3.Connection, item: Dict[str, Any]):
        """Generate trigrams for fuzzy search."""
        item_id = item.get('id')
        text_fields = ['title', 'repository', 'description']

        for field in text_fields:
            text = str(item.get(field, '')).lower()
            if len(text) < 3:
                continue

            # Generate trigrams
            trigrams = {}
            for i in range(len(text) - 2):
                trigram = text[i:i+3]
                trigrams[trigram] = trigrams.get(trigram, 0) + 1

            # Store trigrams
            for trigram, frequency in trigrams.items():
                conn.execute("""
                    INSERT OR REPLACE INTO search_trigrams
                    (trigram, item_id, field, frequency)
                    VALUES (?, ?, ?, ?)
                """, (trigram, item_id, field, frequency))

    def rebuild_index(self):
        """Rebuild search indexes."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                # Clear existing indexes
                conn.execute("DELETE FROM sync_items_fts")
                conn.execute("DELETE FROM search_trigrams")

                # Rebuild from sync_items
                cursor = conn.execute("SELECT * FROM sync_items")
                for row in cursor.fetchall():
                    item = dict(zip([col[0] for col in cursor.description], row))
                    self.index_item(item)

                logger.info("Search index rebuilt successfully")

        except Exception as e:
            logger.error(f"Failed to rebuild search index: {e}")

class QueryBuilder:
    """Builds SQL queries from search criteria."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.field_mappings = {
            'sync_items': {
                'id': 'id',
                'title': 'title',
                'repository': 'repository',
                'status': 'sync_status',
                'updated': 'last_updated',
                'gitea_id': 'gitea_id',
                'kimai_id': 'kimai_id'
            }
        }

    def build_query(self, search_query: SearchQuery) -> Tuple[str, List[Any]]:
        """Build SQL query from search criteria."""
        base_query = f"SELECT * FROM {self.table_name}"
        where_clauses = []
        params = []

        # Add filters
        for filter_item in search_query.filters:
            clause, filter_params = self._build_filter_clause(filter_item)
            if clause:
                where_clauses.append(clause)
                params.extend(filter_params)

        # Add text search
        if search_query.search_text:
            text_clause, text_params = self._build_text_search(search_query.search_text)
            where_clauses.append(text_clause)
            params.extend(text_params)

        # Add date range
        if search_query.date_range:
            date_clause, date_params = self._build_date_range(search_query.date_range)
            where_clauses.append(date_clause)
            params.extend(date_params)

        # Build WHERE clause
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)

        # Add sorting
        if search_query.sort_criteria:
            order_clauses = []
            for sort_item in search_query.sort_criteria:
                field_name = self._get_field_name(sort_item.field)
                order_clauses.append(f"{field_name} {sort_item.order.value.upper()}")
            base_query += " ORDER BY " + ", ".join(order_clauses)

        # Add pagination
        if search_query.limit:
            base_query += f" LIMIT {search_query.limit}"
            if search_query.offset:
                base_query += f" OFFSET {search_query.offset}"

        return base_query, params

    def _build_filter_clause(self, filter_item: SearchFilter) -> Tuple[str, List[Any]]:
        """Build WHERE clause for a single filter."""
        field_name = self._get_field_name(filter_item.field)
        operator = filter_item.operator
        value = filter_item.value

        if operator == SearchOperator.EQUALS:
            return f"{field_name} = ?", [value]
        elif operator == SearchOperator.NOT_EQUALS:
            return f"{field_name} != ?", [value]
        elif operator == SearchOperator.CONTAINS:
            if filter_item.case_sensitive:
                return f"{field_name} LIKE ?", [f"%{value}%"]
            else:
                return f"LOWER({field_name}) LIKE LOWER(?)", [f"%{value}%"]
        elif operator == SearchOperator.NOT_CONTAINS:
            if filter_item.case_sensitive:
                return f"{field_name} NOT LIKE ?", [f"%{value}%"]
            else:
                return f"LOWER({field_name}) NOT LIKE LOWER(?)", [f"%{value}%"]
        elif operator == SearchOperator.STARTS_WITH:
            if filter_item.case_sensitive:
                return f"{field_name} LIKE ?", [f"{value}%"]
            else:
                return f"LOWER({field_name}) LIKE LOWER(?)", [f"{value}%"]
        elif operator == SearchOperator.ENDS_WITH:
            if filter_item.case_sensitive:
                return f"{field_name} LIKE ?", [f"%{value}"]
            else:
                return f"LOWER({field_name}) LIKE LOWER(?)", [f"%{value}"]
        elif operator == SearchOperator.GREATER_THAN:
            return f"{field_name} > ?", [value]
        elif operator == SearchOperator.GREATER_EQUAL:
            return f"{field_name} >= ?", [value]
        elif operator == SearchOperator.LESS_THAN:
            return f"{field_name} < ?", [value]
        elif operator == SearchOperator.LESS_EQUAL:
            return f"{field_name} <= ?", [value]
        elif operator == SearchOperator.IN:
            placeholders = ",".join(["?" for _ in value])
            return f"{field_name} IN ({placeholders})", value
        elif operator == SearchOperator.NOT_IN:
            placeholders = ",".join(["?" for _ in value])
            return f"{field_name} NOT IN ({placeholders})", value
        elif operator == SearchOperator.BETWEEN:
            return f"{field_name} BETWEEN ? AND ?", [value[0], value[1]]
        elif operator == SearchOperator.IS_NULL:
            return f"{field_name} IS NULL", []
        elif operator == SearchOperator.IS_NOT_NULL:
            return f"{field_name} IS NOT NULL", []
        elif operator == SearchOperator.REGEX:
            return f"{field_name} REGEXP ?", [value]
        else:
            return "", []

    def _build_text_search(self, search_text: str) -> Tuple[str, List[Any]]:
        """Build full-text search clause."""
        # Use FTS if available, otherwise use LIKE
        return """
            id IN (
                SELECT id FROM sync_items_fts
                WHERE sync_items_fts MATCH ?
            )
        """, [search_text]

    def _build_date_range(self, date_range: Tuple[datetime, datetime]) -> Tuple[str, List[Any]]:
        """Build date range clause."""
        start_date, end_date = date_range
        return "last_updated BETWEEN ? AND ?", [
            start_date.isoformat(),
            end_date.isoformat()
        ]

    def _get_field_name(self, field: str) -> str:
        """Get actual database field name from search field."""
        mappings = self.field_mappings.get(self.table_name, {})
        return mappings.get(field, field)

class AdvancedSearchEngine:
    """Main search engine with advanced capabilities."""

    def __init__(self, database_path: str = "sync.db"):
        self.database_path = database_path
        self.indexer = SearchIndexer(database_path)
        self.query_builders = {
            'sync_items': QueryBuilder('sync_items'),
            'sync_operations': QueryBuilder('sync_operations'),
            'metrics': QueryBuilder('sync_metrics')
        }

    def search(self, table: str, search_query: SearchQuery) -> SearchResult:
        """Execute search query."""
        import time
        start_time = time.time()

        try:
            # Build and execute query
            query_builder = self.query_builders.get(table)
            if not query_builder:
                raise ValueError(f"Table '{table}' not supported for search")

            sql_query, params = query_builder.build_query(search_query)

            with sqlite3.connect(self.database_path) as conn:
                conn.row_factory = sqlite3.Row

                # Get total count (without pagination)
                count_query = self._build_count_query(sql_query)
                total_count = conn.execute(count_query, params).fetchone()[0]

                # Execute main query
                cursor = conn.execute(sql_query, params)
                rows = cursor.fetchall()

                data = [dict(row) for row in rows]

                query_time = time.time() - start_time

                # Record search statistics
                self._record_search_stats(search_query, len(data), query_time)

                # Calculate pagination info
                per_page = search_query.limit or len(data)
                page = (search_query.offset // per_page) + 1 if per_page > 0 else 1
                has_more = (search_query.offset + len(data)) < total_count

                return SearchResult(
                    data=data,
                    total_count=total_count,
                    filtered_count=len(data),
                    query_time=query_time,
                    page=page,
                    per_page=per_page,
                    has_more=has_more
                )

        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise

    def fuzzy_search(self, table: str, search_text: str, threshold: float = 0.6) -> List[Dict[str, Any]]:
        """Perform fuzzy search using trigrams."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                conn.row_factory = sqlite3.Row

                # Generate trigrams for search text
                search_trigrams = set()
                search_text = search_text.lower()
                for i in range(len(search_text) - 2):
                    search_trigrams.add(search_text[i:i+3])

                if not search_trigrams:
                    return []

                # Find matching items using trigram similarity
                placeholders = ",".join(["?" for _ in search_trigrams])
                query = f"""
                    SELECT
                        item_id,
                        COUNT(*) as matches,
                        COUNT(*) * 1.0 / ? as similarity_score
                    FROM search_trigrams
                    WHERE trigram IN ({placeholders})
                    GROUP BY item_id
                    HAVING similarity_score >= ?
                    ORDER BY similarity_score DESC
                """

                params = [len(search_trigrams)] + list(search_trigrams) + [threshold]
                matches = conn.execute(query, params).fetchall()

                # Get full item data
                if matches:
                    item_ids = [match['item_id'] for match in matches]
                    placeholders = ",".join(["?" for _ in item_ids])
                    items_query = f"SELECT * FROM {table} WHERE id IN ({placeholders})"
                    items = conn.execute(items_query, item_ids).fetchall()

                    # Add similarity scores
                    score_map = {match['item_id']: match['similarity_score'] for match in matches}
                    result = []
                    for item in items:
                        item_dict = dict(item)
                        item_dict['similarity_score'] = score_map.get(item['id'], 0)
                        result.append(item_dict)

                    return result

                return []

        except Exception as e:
            logger.error(f"Fuzzy search failed: {e}")
            return []

    def suggest_search_terms(self, partial_text: str, limit: int = 10) -> List[str]:
        """Suggest search terms based on partial input."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                # Search in titles and repositories
                query = """
                    SELECT DISTINCT title as term FROM sync_items
                    WHERE LOWER(title) LIKE LOWER(?)
                    UNION
                    SELECT DISTINCT repository as term FROM sync_items
                    WHERE LOWER(repository) LIKE LOWER(?)
                    ORDER BY term
                    LIMIT ?
                """

                params = [f"%{partial_text}%", f"%{partial_text}%", limit]
                cursor = conn.execute(query, params)

                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Search suggestion failed: {e}")
            return []

    def get_search_facets(self, table: str, field: str) -> Dict[str, int]:
        """Get facet counts for a field."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                query_builder = self.query_builders.get(table)
                field_name = query_builder._get_field_name(field)

                query = f"""
                    SELECT {field_name} as value, COUNT(*) as count
                    FROM {table}
                    WHERE {field_name} IS NOT NULL
                    GROUP BY {field_name}
                    ORDER BY count DESC
                """

                cursor = conn.execute(query)
                return {row[0]: row[1] for row in cursor.fetchall()}

        except Exception as e:
            logger.error(f"Failed to get facets for {field}: {e}")
            return {}

    def get_popular_searches(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get most popular search queries."""
        try:
            with sqlite3.connect(self.database_path) as conn:
                conn.row_factory = sqlite3.Row

                query = """
                    SELECT query_text, frequency, last_executed
                    FROM search_stats
                    ORDER BY frequency DESC
                    LIMIT ?
                """

                cursor = conn.execute(query, [limit])
                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get popular searches: {e}")
            return []

    def _build_count_query(self, original_query: str) -> str:
        """Build count query from original query."""
        # Replace SELECT clause with COUNT(*)
        select_pattern = r"SELECT\s+.*?\s+FROM"
        count_query = re.sub(select_pattern, "SELECT COUNT(*) FROM", original_query, flags=re.IGNORECASE)

        # Remove ORDER BY clause
        order_pattern = r"\s+ORDER\s+BY\s+.*?(?=\s+LIMIT|\s*$)"
        count_query = re.sub(order_pattern, "", count_query, flags=re.IGNORECASE)

        # Remove LIMIT and OFFSET
        limit_pattern = r"\s+LIMIT\s+\d+(?:\s+OFFSET\s+\d+)?"
        count_query = re.sub(limit_pattern, "", count_query, flags=re.IGNORECASE)

        return count_query

    def _record_search_stats(self, search_query: SearchQuery, result_count: int, execution_time: float):
        """Record search statistics."""
        try:
            # Create a hash of the query for tracking
            query_text = json.dumps({
                'filters': [(f.field, f.operator.value, f.value) for f in search_query.filters],
                'search_text': search_query.search_text,
                'sort': [(s.field, s.order.value) for s in search_query.sort_criteria]
            }, sort_keys=True)

            query_hash = str(hash(query_text))

            with sqlite3.connect(self.database_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO search_stats
                    (query_hash, query_text, result_count, execution_time, last_executed, frequency)
                    VALUES (?, ?, ?, ?, ?,
                        COALESCE((SELECT frequency FROM search_stats WHERE query_hash = ?), 0) + 1)
                """, (
                    query_hash, query_text, result_count, execution_time,
                    datetime.now().isoformat(), query_hash
                ))
                conn.commit()

        except Exception as e:
            logger.error(f"Failed to record search stats: {e}")

# Convenience functions for common search patterns
def search_by_repository(repository: str, status: str = None) -> SearchQuery:
    """Create search query for repository."""
    filters = [SearchFilter("repository", SearchOperator.EQUALS, repository)]

    if status:
        filters.append(SearchFilter("status", SearchOperator.EQUALS, status))

    return SearchQuery(filters=filters)

def search_by_date_range(start_date: datetime, end_date: datetime) -> SearchQuery:
    """Create search query for date range."""
    return SearchQuery(date_range=(start_date, end_date))

def search_by_text(text: str, case_sensitive: bool = False) -> SearchQuery:
    """Create search query for text search."""
    return SearchQuery(search_text=text)

def create_advanced_filter(field: str, operator: str, value: Any) -> SearchFilter:
    """Create advanced search filter."""
    return SearchFilter(
        field=field,
        operator=SearchOperator(operator),
        value=value
    )

# Global search engine instance
_global_search_engine = None

def get_search_engine() -> AdvancedSearchEngine:
    """Get global search engine instance."""
    global _global_search_engine

    if _global_search_engine is None:
        _global_search_engine = AdvancedSearchEngine()

    return _global_search_engine

if __name__ == "__main__":
    # Example usage
    search_engine = AdvancedSearchEngine()

    # Simple text search
    query = SearchQuery(search_text="bug fix")
    results = search_engine.search("sync_items", query)
    print(f"Found {results.total_count} results in {results.query_time:.3f}s")

    # Advanced filtered search
    filters = [
        SearchFilter("repository", SearchOperator.EQUALS, "my-repo"),
        SearchFilter("status", SearchOperator.IN, ["completed", "pending"]),
        SearchFilter("updated", SearchOperator.GREATER_THAN, datetime.now() - timedelta(days=7))
    ]

    sort_criteria = [
        SortCriteria("updated", SortOrder.DESC)
    ]

    advanced_query = SearchQuery(
        filters=filters,
        sort_criteria=sort_criteria,
        limit=50
    )

    results = search_engine.search("sync_items", advanced_query)
    print(f"Advanced search found {results.filtered_count} results")
