#!/usr/bin/env python3
"""
Data Indexing Utility

Provides utilities for creating and managing indexes for efficient
data retrieval and search operations.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict
import time

logger = logging.getLogger(__name__)

class IndexType(Enum):
    """Types of indexes."""
    HASH = "hash"
    BTREE = "btree"
    FULLTEXT = "fulltext"
    COMPOSITE = "composite"

@dataclass
class IndexEntry:
    """An index entry."""
    key: Any
    value: Any
    metadata: Dict[str, Any] = None

@dataclass
class IndexResult:
    """Result of an index operation."""
    found: bool
    values: List[Any]
    count: int
    search_time: float

class DataIndex:
    """Main data indexing utility."""
    
    def __init__(self, index_type: IndexType = IndexType.HASH):
        self.index_type = index_type
        self.indexes: Dict[str, Any] = {}
        self.data: Dict[str, Any] = {}
        self._setup_index()
    
    def _setup_index(self):
        """Setup the index based on type."""
        if self.index_type == IndexType.HASH:
            self.indexes = defaultdict(list)
        elif self.index_type == IndexType.BTREE:
            self.indexes = {}
        elif self.index_type == IndexType.FULLTEXT:
            self.indexes = defaultdict(set)
        elif self.index_type == IndexType.COMPOSITE:
            self.indexes = defaultdict(dict)
    
    def add_data(self, key: str, value: Any, metadata: Dict[str, Any] = None):
        """Add data to the index."""
        self.data[key] = value
        
        # Create index entries based on index type
        if self.index_type == IndexType.HASH:
            self._add_hash_index(key, value, metadata)
        elif self.index_type == IndexType.BTREE:
            self._add_btree_index(key, value, metadata)
        elif self.index_type == IndexType.FULLTEXT:
            self._add_fulltext_index(key, value, metadata)
        elif self.index_type == IndexType.COMPOSITE:
            self._add_composite_index(key, value, metadata)
        
        logger.debug(f"Added data with key: {key}")
    
    def _add_hash_index(self, key: str, value: Any, metadata: Dict[str, Any] = None):
        """Add entry to hash index."""
        if isinstance(value, dict):
            for field, field_value in value.items():
                index_key = f"{field}:{field_value}"
                self.indexes[index_key].append(IndexEntry(key, value, metadata))
        else:
            index_key = str(value)
            self.indexes[index_key].append(IndexEntry(key, value, metadata))
    
    def _add_btree_index(self, key: str, value: Any, metadata: Dict[str, Any] = None):
        """Add entry to B-tree index."""
        if isinstance(value, dict):
            for field, field_value in value.items():
                if field not in self.indexes:
                    self.indexes[field] = {}
                self.indexes[field][field_value] = IndexEntry(key, value, metadata)
        else:
            self.indexes[key] = IndexEntry(key, value, metadata)
    
    def _add_fulltext_index(self, key: str, value: Any, metadata: Dict[str, Any] = None):
        """Add entry to fulltext index."""
        if isinstance(value, str):
            words = value.lower().split()
            for word in words:
                self.indexes[word].add(key)
        elif isinstance(value, dict):
            for field, field_value in value.items():
                if isinstance(field_value, str):
                    words = field_value.lower().split()
                    for word in words:
                        self.indexes[f"{field}:{word}"].add(key)
    
    def _add_composite_index(self, key: str, value: Any, metadata: Dict[str, Any] = None):
        """Add entry to composite index."""
        if isinstance(value, dict):
            # Create composite keys from multiple fields
            fields = list(value.keys())
            for i in range(len(fields)):
                for j in range(i + 1, len(fields)):
                    field1, field2 = fields[i], fields[j]
                    composite_key = f"{field1}:{value[field1]}_{field2}:{value[field2]}"
                    self.indexes[composite_key][key] = IndexEntry(key, value, metadata)
    
    def search(self, query: Union[str, Dict[str, Any]], 
              search_type: str = "exact") -> IndexResult:
        """Search the index."""
        start_time = time.time()
        
        if self.index_type == IndexType.HASH:
            result = self._search_hash(query, search_type)
        elif self.index_type == IndexType.BTREE:
            result = self._search_btree(query, search_type)
        elif self.index_type == IndexType.FULLTEXT:
            result = self._search_fulltext(query, search_type)
        elif self.index_type == IndexType.COMPOSITE:
            result = self._search_composite(query, search_type)
        else:
            result = IndexResult(found=False, values=[], count=0, search_time=0)
        
        result.search_time = time.time() - start_time
        return result
    
    def _search_hash(self, query: Union[str, Dict[str, Any]], search_type: str) -> IndexResult:
        """Search hash index."""
        if isinstance(query, dict):
            # Search by field-value pairs
            results = []
            for field, value in query.items():
                index_key = f"{field}:{value}"
                if index_key in self.indexes:
                    results.extend(self.indexes[index_key])
        else:
            # Search by value
            index_key = str(query)
            results = self.indexes.get(index_key, [])
        
        values = [entry.value for entry in results]
        return IndexResult(
            found=len(values) > 0,
            values=values,
            count=len(values),
            search_time=0
        )
    
    def _search_btree(self, query: Union[str, Dict[str, Any]], search_type: str) -> IndexResult:
        """Search B-tree index."""
        if isinstance(query, dict):
            results = []
            for field, value in query.items():
                if field in self.indexes and value in self.indexes[field]:
                    results.append(self.indexes[field][value])
        else:
            results = [self.indexes.get(query)]
        
        values = [entry.value for entry in results if entry is not None]
        return IndexResult(
            found=len(values) > 0,
            values=values,
            count=len(values),
            search_time=0
        )
    
    def _search_fulltext(self, query: str, search_type: str) -> IndexResult:
        """Search fulltext index."""
        if search_type == "exact":
            # Exact word match
            keys = self.indexes.get(query.lower(), set())
        elif search_type == "partial":
            # Partial word match
            keys = set()
            for word, key_set in self.indexes.items():
                if query.lower() in word:
                    keys.update(key_set)
        else:
            keys = set()
        
        values = [self.data[key] for key in keys if key in self.data]
        return IndexResult(
            found=len(values) > 0,
            values=values,
            count=len(values),
            search_time=0
        )
    
    def _search_composite(self, query: Dict[str, Any], search_type: str) -> IndexResult:
        """Search composite index."""
        results = []
        for composite_key, entries in self.indexes.items():
            match = True
            for field, value in query.items():
                if f"{field}:{value}" not in composite_key:
                    match = False
                    break
            if match:
                results.extend(entries.values())
        
        values = [entry.value for entry in results]
        return IndexResult(
            found=len(values) > 0,
            values=values,
            count=len(values),
            search_time=0
        )
    
    def remove_data(self, key: str):
        """Remove data from the index."""
        if key in self.data:
            value = self.data.pop(key)
            
            # Remove from indexes
            if self.index_type == IndexType.HASH:
                self._remove_hash_index(key, value)
            elif self.index_type == IndexType.BTREE:
                self._remove_btree_index(key, value)
            elif self.index_type == IndexType.FULLTEXT:
                self._remove_fulltext_index(key, value)
            elif self.index_type == IndexType.COMPOSITE:
                self._remove_composite_index(key, value)
            
            logger.debug(f"Removed data with key: {key}")
    
    def _remove_hash_index(self, key: str, value: Any):
        """Remove entry from hash index."""
        if isinstance(value, dict):
            for field, field_value in value.items():
                index_key = f"{field}:{field_value}"
                self.indexes[index_key] = [entry for entry in self.indexes[index_key] if entry.key != key]
        else:
            index_key = str(value)
            self.indexes[index_key] = [entry for entry in self.indexes[index_key] if entry.key != key]
    
    def _remove_btree_index(self, key: str, value: Any):
        """Remove entry from B-tree index."""
        if isinstance(value, dict):
            for field, field_value in value.items():
                if field in self.indexes and field_value in self.indexes[field]:
                    if self.indexes[field][field_value].key == key:
                        del self.indexes[field][field_value]
        else:
            if key in self.indexes:
                del self.indexes[key]
    
    def _remove_fulltext_index(self, key: str, value: Any):
        """Remove entry from fulltext index."""
        if isinstance(value, str):
            words = value.lower().split()
            for word in words:
                if key in self.indexes[word]:
                    self.indexes[word].remove(key)
        elif isinstance(value, dict):
            for field, field_value in value.items():
                if isinstance(field_value, str):
                    words = field_value.lower().split()
                    for word in words:
                        index_key = f"{field}:{word}"
                        if key in self.indexes[index_key]:
                            self.indexes[index_key].remove(key)
    
    def _remove_composite_index(self, key: str, value: Any):
        """Remove entry from composite index."""
        if isinstance(value, dict):
            fields = list(value.keys())
            for i in range(len(fields)):
                for j in range(i + 1, len(fields)):
                    field1, field2 = fields[i], fields[j]
                    composite_key = f"{field1}:{value[field1]}_{field2}:{value[field2]}"
                    if key in self.indexes[composite_key]:
                        del self.indexes[composite_key][key]
    
    def get_index_stats(self) -> Dict[str, Any]:
        """Get statistics about the index."""
        stats = {
            'index_type': self.index_type.value,
            'total_entries': len(self.data),
            'index_count': len(self.indexes),
            'index_sizes': {}
        }
        
        for index_name, index_data in self.indexes.items():
            if isinstance(index_data, list):
                stats['index_sizes'][index_name] = len(index_data)
            elif isinstance(index_data, dict):
                stats['index_sizes'][index_name] = len(index_data)
            elif isinstance(index_data, set):
                stats['index_sizes'][index_name] = len(index_data)
            else:
                stats['index_sizes'][index_name] = 1
        
        return stats
    
    def clear_index(self):
        """Clear all data and indexes."""
        self.data.clear()
        self.indexes.clear()
        self._setup_index()
        logger.info("Cleared all data and indexes")
    
    def rebuild_index(self):
        """Rebuild the index from existing data."""
        old_data = self.data.copy()
        self.clear_index()
        
        for key, value in old_data.items():
            self.add_data(key, value)
        
        logger.info("Rebuilt index from existing data")

def create_index(index_type: IndexType = IndexType.HASH) -> DataIndex:
    """Create a data index with specified type."""
    return DataIndex(index_type)

def search_data(index: DataIndex, query: Union[str, Dict[str, Any]], 
               search_type: str = "exact") -> IndexResult:
    """Convenience function to search data."""
    return index.search(query, search_type)

if __name__ == "__main__":
    # Example usage
    index = create_index(IndexType.HASH)
    
    # Sample data
    sample_data = [
        {"id": "1", "name": "John Doe", "age": 30, "city": "New York"},
        {"id": "2", "name": "Jane Smith", "age": 25, "city": "Los Angeles"},
        {"id": "3", "name": "Bob Johnson", "age": 35, "city": "Chicago"},
        {"id": "4", "name": "Alice Brown", "age": 28, "city": "New York"}
    ]
    
    # Add data to index
    for item in sample_data:
        index.add_data(item["id"], item)
    
    # Search examples
    print("=== Search Examples ===")
    
    # Search by city
    result = search_data(index, {"city": "New York"})
    print(f"People in New York: {len(result.values)}")
    for person in result.values:
        print(f"  - {person['name']}")
    
    # Search by age
    result = search_data(index, {"age": 30})
    print(f"People aged 30: {len(result.values)}")
    for person in result.values:
        print(f"  - {person['name']}")
    
    # Get index stats
    stats = index.get_index_stats()
    print(f"\nIndex stats: {stats}")
