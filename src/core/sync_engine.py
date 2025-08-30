#!/usr/bin/env python3
"""
Data Synchronization Engine

Provides a comprehensive data synchronization engine for Gitea-Kimai integration,
handling incremental syncs, conflict resolution, and data transformation.
"""

import os
import json
import logging
import threading
import time
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
import sqlite3
from pathlib import Path
import requests

logger = logging.getLogger(__name__)

class SyncStatus(Enum):
    """Sync operation status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class SyncType(Enum):
    """Sync operation type."""
    FULL = "full"
    INCREMENTAL = "incremental"
    SELECTIVE = "selective"

class ConflictResolution(Enum):
    """Conflict resolution strategy."""
    SOURCE_WINS = "source_wins"
    TARGET_WINS = "target_wins"
    MANUAL = "manual"
    MERGE = "merge"

@dataclass
class SyncItem:
    """Represents a syncable item."""
    id: str
    source_id: str
    target_id: Optional[str]
    source_data: Dict[str, Any]
    target_data: Optional[Dict[str, Any]]
    item_type: str
    last_modified: datetime
    sync_status: SyncStatus
    conflict_resolution: Optional[ConflictResolution] = None
    metadata: Dict[str, Any] = None

@dataclass
class SyncOperation:
    """Represents a sync operation."""
    id: str
    sync_type: SyncType
    source_system: str
    target_system: str
    status: SyncStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    items_processed: int = 0
    items_synced: int = 0
    items_failed: int = 0
    conflicts_resolved: int = 0
    errors: List[str] = None
    metadata: Dict[str, Any] = None

@dataclass
class SyncConfig:
    """Sync configuration."""
    sync_interval: int = 300  # seconds
    batch_size: int = 100
    max_retries: int = 3
    retry_delay: int = 60  # seconds
    conflict_resolution: ConflictResolution = ConflictResolution.SOURCE_WINS
    enable_incremental: bool = True
    enable_selective: bool = True
    data_validation: bool = True
    dry_run: bool = False

class DataTransformer:
    """Transforms data between different formats."""
    
    def __init__(self):
        self.transformers: Dict[str, Callable] = {}
        self._register_default_transformers()
    
    def _register_default_transformers(self):
        """Register default data transformers."""
        self.transformers['gitea_issue_to_kimai_timesheet'] = self._transform_gitea_issue_to_kimai_timesheet
        self.transformers['kimai_timesheet_to_gitea_issue'] = self._transform_kimai_timesheet_to_gitea_issue
        self.transformers['gitea_pr_to_kimai_project'] = self._transform_gitea_pr_to_kimai_project
        self.transformers['kimai_project_to_gitea_pr'] = self._transform_kimai_project_to_gitea_pr
    
    def _transform_gitea_issue_to_kimai_timesheet(self, issue_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Gitea issue to Kimai timesheet entry."""
        return {
            'description': f"Issue #{issue_data.get('number')}: {issue_data.get('title', '')}",
            'project': issue_data.get('repository', {}).get('name', ''),
            'activity': 'Issue Tracking',
            'tags': [label.get('name', '') for label in issue_data.get('labels', [])],
            'metaFields': {
                'issue_id': issue_data.get('id'),
                'issue_number': issue_data.get('number'),
                'repository': issue_data.get('repository', {}).get('full_name', ''),
                'assignee': issue_data.get('assignee', {}).get('login', ''),
                'state': issue_data.get('state', ''),
                'created_at': issue_data.get('created_at'),
                'updated_at': issue_data.get('updated_at')
            }
        }
    
    def _transform_kimai_timesheet_to_gitea_issue(self, timesheet_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Kimai timesheet entry to Gitea issue."""
        meta_fields = timesheet_data.get('metaFields', {})
        return {
            'title': timesheet_data.get('description', '').split(': ', 1)[-1] if ': ' in timesheet_data.get('description', '') else timesheet_data.get('description', ''),
            'body': f"Time tracking entry: {timesheet_data.get('description', '')}",
            'labels': timesheet_data.get('tags', []),
            'assignee': meta_fields.get('assignee'),
            'state': meta_fields.get('state', 'open')
        }
    
    def _transform_gitea_pr_to_kimai_project(self, pr_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Gitea pull request to Kimai project."""
        return {
            'name': f"PR #{pr_data.get('number')}: {pr_data.get('title', '')}",
            'comment': pr_data.get('body', ''),
            'orderNumber': pr_data.get('number'),
            'metaFields': {
                'pr_id': pr_data.get('id'),
                'pr_number': pr_data.get('number'),
                'repository': pr_data.get('head', {}).get('repo', {}).get('full_name', ''),
                'base_branch': pr_data.get('base', {}).get('ref', ''),
                'head_branch': pr_data.get('head', {}).get('ref', ''),
                'state': pr_data.get('state', ''),
                'created_at': pr_data.get('created_at'),
                'updated_at': pr_data.get('updated_at')
            }
        }
    
    def _transform_kimai_project_to_gitea_pr(self, project_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Kimai project to Gitea pull request."""
        meta_fields = project_data.get('metaFields', {})
        return {
            'title': project_data.get('name', '').split(': ', 1)[-1] if ': ' in project_data.get('name', '') else project_data.get('name', ''),
            'body': project_data.get('comment', ''),
            'state': meta_fields.get('state', 'open')
        }
    
    def transform(self, transformation_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data using the specified transformation."""
        if transformation_type not in self.transformers:
            raise ValueError(f"Unknown transformation type: {transformation_type}")
        
        return self.transformers[transformation_type](data)
    
    def register_transformer(self, name: str, transformer_func: Callable):
        """Register a custom transformer."""
        self.transformers[name] = transformer_func

class ConflictResolver:
    """Resolves conflicts between source and target data."""
    
    def __init__(self, default_strategy: ConflictResolution = ConflictResolution.SOURCE_WINS):
        self.default_strategy = default_strategy
        self.resolvers: Dict[str, Callable] = {}
        self._register_default_resolvers()
    
    def _register_default_resolvers(self):
        """Register default conflict resolvers."""
        self.resolvers['source_wins'] = self._resolve_source_wins
        self.resolvers['target_wins'] = self._resolve_target_wins
        self.resolvers['merge'] = self._resolve_merge
        self.resolvers['manual'] = self._resolve_manual
    
    def _resolve_source_wins(self, source_data: Dict[str, Any], target_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve conflict by using source data."""
        return source_data.copy()
    
    def _resolve_target_wins(self, source_data: Dict[str, Any], target_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve conflict by using target data."""
        return target_data.copy()
    
    def _resolve_merge(self, source_data: Dict[str, Any], target_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve conflict by merging data."""
        merged = target_data.copy()
        merged.update(source_data)
        merged['_merged_at'] = datetime.now().isoformat()
        return merged
    
    def _resolve_manual(self, source_data: Dict[str, Any], target_data: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve conflict manually (requires user intervention)."""
        # This would typically trigger a manual review process
        raise ValueError("Manual conflict resolution required")
    
    def resolve_conflict(self, source_data: Dict[str, Any], target_data: Dict[str, Any], 
                        strategy: ConflictResolution = None) -> Dict[str, Any]:
        """Resolve conflict using the specified strategy."""
        strategy = strategy or self.default_strategy
        
        if strategy.value not in self.resolvers:
            raise ValueError(f"Unknown conflict resolution strategy: {strategy}")
        
        return self.resolvers[strategy.value](source_data, target_data)
    
    def register_resolver(self, name: str, resolver_func: Callable):
        """Register a custom conflict resolver."""
        self.resolvers[name] = resolver_func

class SyncDatabase:
    """Manages sync state and metadata."""
    
    def __init__(self, db_path: str = "sync.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize the sync database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_operations (
                    id TEXT PRIMARY KEY,
                    sync_type TEXT NOT NULL,
                    source_system TEXT NOT NULL,
                    target_system TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    items_processed INTEGER DEFAULT 0,
                    items_synced INTEGER DEFAULT 0,
                    items_failed INTEGER DEFAULT 0,
                    conflicts_resolved INTEGER DEFAULT 0,
                    errors TEXT,
                    metadata TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_items (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    target_id TEXT,
                    source_data TEXT NOT NULL,
                    target_data TEXT,
                    item_type TEXT NOT NULL,
                    last_modified TEXT NOT NULL,
                    sync_status TEXT NOT NULL,
                    conflict_resolution TEXT,
                    metadata TEXT,
                    UNIQUE(source_id, item_type)
                )
            """)
            
            conn.commit()
    
    def save_sync_operation(self, operation: SyncOperation):
        """Save sync operation to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO sync_operations 
                (id, sync_type, source_system, target_system, status, started_at, completed_at,
                 items_processed, items_synced, items_failed, conflicts_resolved, errors, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                operation.id,
                operation.sync_type.value,
                operation.source_system,
                operation.target_system,
                operation.status.value,
                operation.started_at.isoformat(),
                operation.completed_at.isoformat() if operation.completed_at else None,
                operation.items_processed,
                operation.items_synced,
                operation.items_failed,
                operation.conflicts_resolved,
                json.dumps(operation.errors) if operation.errors else None,
                json.dumps(operation.metadata) if operation.metadata else None
            ))
            conn.commit()
    
    def get_sync_operation(self, operation_id: str) -> Optional[SyncOperation]:
        """Get sync operation by ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM sync_operations WHERE id = ?
            """, (operation_id,))
            row = cursor.fetchone()
            
            if row:
                return SyncOperation(
                    id=row[0],
                    sync_type=SyncType(row[1]),
                    source_system=row[2],
                    target_system=row[3],
                    status=SyncStatus(row[4]),
                    started_at=datetime.fromisoformat(row[5]),
                    completed_at=datetime.fromisoformat(row[6]) if row[6] else None,
                    items_processed=row[7],
                    items_synced=row[8],
                    items_failed=row[9],
                    conflicts_resolved=row[10],
                    errors=json.loads(row[11]) if row[11] else [],
                    metadata=json.loads(row[12]) if row[12] else {}
                )
            return None
    
    def save_sync_item(self, item: SyncItem):
        """Save sync item to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO sync_items 
                (id, source_id, target_id, source_data, target_data, item_type, last_modified,
                 sync_status, conflict_resolution, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item.id,
                item.source_id,
                item.target_id,
                json.dumps(item.source_data),
                json.dumps(item.target_data) if item.target_data else None,
                item.item_type,
                item.last_modified.isoformat(),
                item.sync_status.value,
                item.conflict_resolution.value if item.conflict_resolution else None,
                json.dumps(item.metadata) if item.metadata else None
            ))
            conn.commit()
    
    def get_sync_item(self, source_id: str, item_type: str) -> Optional[SyncItem]:
        """Get sync item by source ID and type."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM sync_items WHERE source_id = ? AND item_type = ?
            """, (source_id, item_type))
            row = cursor.fetchone()
            
            if row:
                return SyncItem(
                    id=row[0],
                    source_id=row[1],
                    target_id=row[2],
                    source_data=json.loads(row[3]),
                    target_data=json.loads(row[4]) if row[4] else None,
                    item_type=row[5],
                    last_modified=datetime.fromisoformat(row[6]),
                    sync_status=SyncStatus(row[7]),
                    conflict_resolution=ConflictResolution(row[8]) if row[8] else None,
                    metadata=json.loads(row[9]) if row[9] else {}
                )
            return None
    
    def get_last_sync_timestamp(self, source_system: str, target_system: str) -> Optional[datetime]:
        """Get last successful sync timestamp."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT completed_at FROM sync_operations 
                WHERE source_system = ? AND target_system = ? AND status = 'completed'
                ORDER BY completed_at DESC LIMIT 1
            """, (source_system, target_system))
            row = cursor.fetchone()
            
            if row and row[0]:
                return datetime.fromisoformat(row[0])
            return None

class SyncEngine:
    """Main synchronization engine."""
    
    def __init__(self, config: SyncConfig, source_client: Any, target_client: Any):
        self.config = config
        self.source_client = source_client
        self.target_client = target_client
        self.database = SyncDatabase()
        self.transformer = DataTransformer()
        self.conflict_resolver = ConflictResolver(config.conflict_resolution)
        self.running = False
        self.current_operation: Optional[SyncOperation] = None
        self.sync_thread = None
    
    def start(self):
        """Start the sync engine."""
        if not self.running:
            self.running = True
            self.sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
            self.sync_thread.start()
            logger.info("Sync engine started")
    
    def stop(self):
        """Stop the sync engine."""
        self.running = False
        if self.sync_thread:
            self.sync_thread.join()
        logger.info("Sync engine stopped")
    
    def _sync_loop(self):
        """Main sync loop."""
        while self.running:
            try:
                if self.current_operation and self.current_operation.status == SyncStatus.RUNNING:
                    # Continue current operation
                    self._process_sync_operation(self.current_operation)
                else:
                    # Start new sync operation
                    self._start_scheduled_sync()
                
                time.sleep(self.config.sync_interval)
                
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                time.sleep(60)  # Wait before retrying
    
    def _start_scheduled_sync(self):
        """Start a scheduled sync operation."""
        if self.config.enable_incremental:
            sync_type = SyncType.INCREMENTAL
        else:
            sync_type = SyncType.FULL
        
        operation = SyncOperation(
            id=f"sync_{int(time.time() * 1000)}",
            sync_type=sync_type,
            source_system="gitea",
            target_system="kimai",
            status=SyncStatus.PENDING,
            started_at=datetime.now()
        )
        
        self.current_operation = operation
        self.database.save_sync_operation(operation)
        
        logger.info(f"Started sync operation: {operation.id}")
    
    def _process_sync_operation(self, operation: SyncOperation):
        """Process a sync operation."""
        try:
            operation.status = SyncStatus.RUNNING
            self.database.save_sync_operation(operation)
            
            # Get items to sync
            items = self._get_items_to_sync(operation.sync_type)
            
            # Process items in batches
            for i in range(0, len(items), self.config.batch_size):
                batch = items[i:i + self.config.batch_size]
                self._process_batch(operation, batch)
                
                if not self.running:
                    break
            
            # Mark operation as completed
            operation.status = SyncStatus.COMPLETED
            operation.completed_at = datetime.now()
            self.database.save_sync_operation(operation)
            
            logger.info(f"Completed sync operation: {operation.id}")
            
        except Exception as e:
            logger.error(f"Failed sync operation {operation.id}: {e}")
            operation.status = SyncStatus.FAILED
            operation.completed_at = datetime.now()
            if not operation.errors:
                operation.errors = []
            operation.errors.append(str(e))
            self.database.save_sync_operation(operation)
    
    def _get_items_to_sync(self, sync_type: SyncType) -> List[SyncItem]:
        """Get items to sync based on sync type."""
        items = []
        
        if sync_type == SyncType.FULL:
            # Get all items from source
            items.extend(self._get_all_source_items())
        elif sync_type == SyncType.INCREMENTAL:
            # Get only modified items since last sync
            last_sync = self.database.get_last_sync_timestamp("gitea", "kimai")
            if last_sync:
                items.extend(self._get_modified_items_since(last_sync))
            else:
                # First sync - get all items
                items.extend(self._get_all_source_items())
        
        return items
    
    def _get_all_source_items(self) -> List[SyncItem]:
        """Get all items from source system."""
        items = []
        
        # Get issues
        try:
            issues = self.source_client.get_issues()
            for issue in issues:
                items.append(SyncItem(
                    id=f"issue_{issue['id']}",
                    source_id=str(issue['id']),
                    target_id=None,
                    source_data=issue,
                    target_data=None,
                    item_type="issue",
                    last_modified=datetime.fromisoformat(issue['updated_at']),
                    sync_status=SyncStatus.PENDING
                ))
        except Exception as e:
            logger.error(f"Failed to get issues: {e}")
        
        # Get pull requests
        try:
            pull_requests = self.source_client.get_pull_requests()
            for pr in pull_requests:
                items.append(SyncItem(
                    id=f"pr_{pr['id']}",
                    source_id=str(pr['id']),
                    target_id=None,
                    source_data=pr,
                    target_data=None,
                    item_type="pull_request",
                    last_modified=datetime.fromisoformat(pr['updated_at']),
                    sync_status=SyncStatus.PENDING
                ))
        except Exception as e:
            logger.error(f"Failed to get pull requests: {e}")
        
        return items
    
    def _get_modified_items_since(self, since: datetime) -> List[SyncItem]:
        """Get items modified since the specified time."""
        items = []
        
        # Get modified issues
        try:
            issues = self.source_client.get_issues_modified_since(since)
            for issue in issues:
                items.append(SyncItem(
                    id=f"issue_{issue['id']}",
                    source_id=str(issue['id']),
                    target_id=None,
                    source_data=issue,
                    target_data=None,
                    item_type="issue",
                    last_modified=datetime.fromisoformat(issue['updated_at']),
                    sync_status=SyncStatus.PENDING
                ))
        except Exception as e:
            logger.error(f"Failed to get modified issues: {e}")
        
        return items
    
    def _process_batch(self, operation: SyncOperation, items: List[SyncItem]):
        """Process a batch of sync items."""
        for item in items:
            try:
                operation.items_processed += 1
                
                # Check if item exists in target
                existing_item = self.database.get_sync_item(item.source_id, item.item_type)
                
                if existing_item and existing_item.target_id:
                    # Update existing item
                    success = self._update_item(item, existing_item)
                else:
                    # Create new item
                    success = self._create_item(item)
                
                if success:
                    operation.items_synced += 1
                    item.sync_status = SyncStatus.COMPLETED
                else:
                    operation.items_failed += 1
                    item.sync_status = SyncStatus.FAILED
                
                self.database.save_sync_item(item)
                
            except Exception as e:
                logger.error(f"Failed to process item {item.id}: {e}")
                operation.items_failed += 1
                item.sync_status = SyncStatus.FAILED
                self.database.save_sync_item(item)
        
        self.database.save_sync_operation(operation)
    
    def _create_item(self, item: SyncItem) -> bool:
        """Create a new item in the target system."""
        try:
            # Transform data
            if item.item_type == "issue":
                transformed_data = self.transformer.transform('gitea_issue_to_kimai_timesheet', item.source_data)
                target_id = self.target_client.create_timesheet_entry(transformed_data)
            elif item.item_type == "pull_request":
                transformed_data = self.transformer.transform('gitea_pr_to_kimai_project', item.source_data)
                target_id = self.target_client.create_project(transformed_data)
            else:
                raise ValueError(f"Unknown item type: {item.item_type}")
            
            item.target_id = str(target_id)
            return True
            
        except Exception as e:
            logger.error(f"Failed to create item {item.id}: {e}")
            return False
    
    def _update_item(self, item: SyncItem, existing_item: SyncItem) -> bool:
        """Update an existing item in the target system."""
        try:
            # Get current target data
            if item.item_type == "issue":
                current_target_data = self.target_client.get_timesheet_entry(existing_item.target_id)
            elif item.item_type == "pull_request":
                current_target_data = self.target_client.get_project(existing_item.target_id)
            else:
                raise ValueError(f"Unknown item type: {item.item_type}")
            
            # Check for conflicts
            if self._has_conflicts(item.source_data, current_target_data):
                # Resolve conflict
                resolved_data = self.conflict_resolver.resolve_conflict(
                    item.source_data, current_target_data, item.conflict_resolution
                )
                item.conflict_resolution = self.config.conflict_resolution
            else:
                # No conflict, use source data
                if item.item_type == "issue":
                    resolved_data = self.transformer.transform('gitea_issue_to_kimai_timesheet', item.source_data)
                elif item.item_type == "pull_request":
                    resolved_data = self.transformer.transform('gitea_pr_to_kimai_project', item.source_data)
            
            # Update target
            if item.item_type == "issue":
                self.target_client.update_timesheet_entry(existing_item.target_id, resolved_data)
            elif item.item_type == "pull_request":
                self.target_client.update_project(existing_item.target_id, resolved_data)
            
            item.target_data = current_target_data
            return True
            
        except Exception as e:
            logger.error(f"Failed to update item {item.id}: {e}")
            return False
    
    def _has_conflicts(self, source_data: Dict[str, Any], target_data: Dict[str, Any]) -> bool:
        """Check if there are conflicts between source and target data."""
        # Simple conflict detection - can be enhanced
        source_hash = hashlib.md5(json.dumps(source_data, sort_keys=True).encode()).hexdigest()
        target_hash = hashlib.md5(json.dumps(target_data, sort_keys=True).encode()).hexdigest()
        return source_hash != target_hash
    
    def start_manual_sync(self, sync_type: SyncType = SyncType.INCREMENTAL) -> str:
        """Start a manual sync operation."""
        operation = SyncOperation(
            id=f"manual_sync_{int(time.time() * 1000)}",
            sync_type=sync_type,
            source_system="gitea",
            target_system="kimai",
            status=SyncStatus.PENDING,
            started_at=datetime.now()
        )
        
        self.current_operation = operation
        self.database.save_sync_operation(operation)
        
        logger.info(f"Started manual sync operation: {operation.id}")
        return operation.id
    
    def get_sync_status(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get sync operation status."""
        operation = self.database.get_sync_operation(operation_id)
        if operation:
            return asdict(operation)
        return None
    
    def cancel_sync(self, operation_id: str) -> bool:
        """Cancel a sync operation."""
        if self.current_operation and self.current_operation.id == operation_id:
            self.current_operation.status = SyncStatus.CANCELLED
            self.current_operation.completed_at = datetime.now()
            self.database.save_sync_operation(self.current_operation)
            self.current_operation = None
            logger.info(f"Cancelled sync operation: {operation_id}")
            return True
        return False

def create_sync_engine(source_client: Any, target_client: Any, 
                      config_file: str = "sync_config.json") -> SyncEngine:
    """Create sync engine from configuration."""
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)
    else:
        config_data = {
            'sync_interval': 300,
            'batch_size': 100,
            'max_retries': 3,
            'retry_delay': 60,
            'conflict_resolution': 'source_wins',
            'enable_incremental': True,
            'enable_selective': True,
            'data_validation': True,
            'dry_run': False
        }
    
    config = SyncConfig(**config_data)
    return SyncEngine(config, source_client, target_client)

if __name__ == "__main__":
    # Example usage
    # This would require actual API clients
    # source_client = GiteaClient(...)
    # target_client = KimaiClient(...)
    # sync_engine = create_sync_engine(source_client, target_client)
    # sync_engine.start()
    pass
