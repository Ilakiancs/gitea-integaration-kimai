#!/usr/bin/env python3
"""
Data Migration Module

Handles database schema migrations, data transformations, and version
management for the sync system.
"""

import os
import sqlite3
import logging
import json
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from pathlib import Path
import hashlib

logger = logging.getLogger(__name__)

class Migration:
    """Represents a database migration."""
    
    def __init__(self, version: int, name: str, description: str = ""):
        self.version = version
        self.name = name
        self.description = description
        self.created_at = datetime.now()
    
    def up(self, connection: sqlite3.Connection) -> bool:
        """Apply the migration (upgrade)."""
        raise NotImplementedError
    
    def down(self, connection: sqlite3.Connection) -> bool:
        """Rollback the migration (downgrade)."""
        raise NotImplementedError

class SQLMigration(Migration):
    """Migration using SQL scripts."""
    
    def __init__(self, version: int, name: str, up_sql: str, down_sql: str = "", description: str = ""):
        super().__init__(version, name, description)
        self.up_sql = up_sql
        self.down_sql = down_sql
    
    def up(self, connection: sqlite3.Connection) -> bool:
        """Apply the migration using SQL."""
        try:
            connection.executescript(self.up_sql)
            connection.commit()
            logger.info(f"Applied migration {self.version}: {self.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to apply migration {self.version}: {e}")
            connection.rollback()
            return False
    
    def down(self, connection: sqlite3.Connection) -> bool:
        """Rollback the migration using SQL."""
        if not self.down_sql:
            logger.warning(f"No rollback SQL for migration {self.version}")
            return False
        
        try:
            connection.executescript(self.down_sql)
            connection.commit()
            logger.info(f"Rolled back migration {self.version}: {self.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to rollback migration {self.version}: {e}")
            connection.rollback()
            return False

class PythonMigration(Migration):
    """Migration using Python functions."""
    
    def __init__(self, version: int, name: str, up_func: Callable, down_func: Callable = None, description: str = ""):
        super().__init__(version, name, description)
        self.up_func = up_func
        self.down_func = down_func
    
    def up(self, connection: sqlite3.Connection) -> bool:
        """Apply the migration using Python function."""
        try:
            result = self.up_func(connection)
            if result:
                logger.info(f"Applied migration {self.version}: {self.name}")
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to apply migration {self.version}: {e}")
            return False
    
    def down(self, connection: sqlite3.Connection) -> bool:
        """Rollback the migration using Python function."""
        if not self.down_func:
            logger.warning(f"No rollback function for migration {self.version}")
            return False
        
        try:
            result = self.down_func(connection)
            if result:
                logger.info(f"Rolled back migration {self.version}: {self.name}")
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to rollback migration {self.version}: {e}")
            return False

class MigrationManager:
    """Manages database migrations."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.migrations: List[Migration] = []
        self._init_migration_table()
    
    def _init_migration_table(self):
        """Initialize the migrations table."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    checksum TEXT,
                    execution_time REAL
                )
            """)
            conn.commit()
    
    def add_migration(self, migration: Migration):
        """Add a migration to the manager."""
        # Check for version conflicts
        existing_versions = [m.version for m in self.migrations]
        if migration.version in existing_versions:
            raise ValueError(f"Migration version {migration.version} already exists")
        
        self.migrations.append(migration)
        # Sort by version
        self.migrations.sort(key=lambda m: m.version)
    
    def get_applied_migrations(self) -> List[Dict[str, Any]]:
        """Get list of applied migrations."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT version, name, description, applied_at, checksum, execution_time
                FROM migrations ORDER BY version
            """)
            
            return [
                {
                    'version': row[0],
                    'name': row[1],
                    'description': row[2],
                    'applied_at': row[3],
                    'checksum': row[4],
                    'execution_time': row[5]
                }
                for row in cursor.fetchall()
            ]
    
    def get_pending_migrations(self) -> List[Migration]:
        """Get list of pending migrations."""
        applied_versions = {m['version'] for m in self.get_applied_migrations()}
        return [m for m in self.migrations if m.version not in applied_versions]
    
    def get_current_version(self) -> int:
        """Get current database version."""
        applied_migrations = self.get_applied_migrations()
        if not applied_migrations:
            return 0
        return max(m['version'] for m in applied_migrations)
    
    def migrate(self, target_version: Optional[int] = None) -> bool:
        """Run migrations to reach target version."""
        current_version = self.get_current_version()
        
        if target_version is None:
            # Migrate to latest version
            target_version = max(m.version for m in self.migrations) if self.migrations else 0
        
        if target_version == current_version:
            logger.info(f"Database is already at version {current_version}")
            return True
        
        if target_version > current_version:
            # Upgrade
            return self._upgrade(target_version)
        else:
            # Downgrade
            return self._downgrade(target_version)
    
    def _upgrade(self, target_version: int) -> bool:
        """Upgrade database to target version."""
        current_version = self.get_current_version()
        pending_migrations = [m for m in self.migrations 
                            if current_version < m.version <= target_version]
        
        logger.info(f"Upgrading from version {current_version} to {target_version}")
        
        with sqlite3.connect(self.db_path) as conn:
            for migration in pending_migrations:
                start_time = datetime.now()
                
                try:
                    success = migration.up(conn)
                    if not success:
                        logger.error(f"Migration {migration.version} failed, stopping upgrade")
                        return False
                    
                    # Record migration
                    execution_time = (datetime.now() - start_time).total_seconds()
                    checksum = self._calculate_migration_checksum(migration)
                    
                    conn.execute("""
                        INSERT INTO migrations (version, name, description, checksum, execution_time)
                        VALUES (?, ?, ?, ?, ?)
                    """, (migration.version, migration.name, migration.description, checksum, execution_time))
                    conn.commit()
                    
                    logger.info(f"Successfully applied migration {migration.version}: {migration.name}")
                    
                except Exception as e:
                    logger.error(f"Failed to apply migration {migration.version}: {e}")
                    return False
        
        logger.info(f"Successfully upgraded to version {target_version}")
        return True
    
    def _downgrade(self, target_version: int) -> bool:
        """Downgrade database to target version."""
        current_version = self.get_current_version()
        applied_migrations = self.get_applied_migrations()
        
        # Get migrations to rollback (in reverse order)
        rollback_migrations = []
        for migration in reversed(self.migrations):
            if target_version < migration.version <= current_version:
                rollback_migrations.append(migration)
        
        logger.info(f"Downgrading from version {current_version} to {target_version}")
        
        with sqlite3.connect(self.db_path) as conn:
            for migration in rollback_migrations:
                try:
                    success = migration.down(conn)
                    if not success:
                        logger.error(f"Rollback of migration {migration.version} failed, stopping downgrade")
                        return False
                    
                    # Remove migration record
                    conn.execute("DELETE FROM migrations WHERE version = ?", (migration.version,))
                    conn.commit()
                    
                    logger.info(f"Successfully rolled back migration {migration.version}: {migration.name}")
                    
                except Exception as e:
                    logger.error(f"Failed to rollback migration {migration.version}: {e}")
                    return False
        
        logger.info(f"Successfully downgraded to version {target_version}")
        return True
    
    def _calculate_migration_checksum(self, migration: Migration) -> str:
        """Calculate checksum for migration."""
        if isinstance(migration, SQLMigration):
            content = f"{migration.up_sql}{migration.down_sql}"
        else:
            content = f"{migration.name}{migration.description}"
        
        return hashlib.md5(content.encode()).hexdigest()
    
    def create_migration(self, name: str, description: str = "") -> int:
        """Create a new migration file."""
        next_version = self.get_current_version() + 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{next_version:04d}_{name}.sql"
        
        migration_dir = Path("migrations")
        migration_dir.mkdir(exist_ok=True)
        
        file_path = migration_dir / filename
        
        template = f"""-- Migration: {name}
-- Version: {next_version}
-- Description: {description}
-- Created: {datetime.now().isoformat()}

-- UP Migration
-- Add your SQL statements here

-- DOWN Migration
-- Add rollback SQL statements here
"""
        
        with open(file_path, 'w') as f:
            f.write(template)
        
        logger.info(f"Created migration file: {file_path}")
        return next_version
    
    def load_migrations_from_directory(self, directory: str = "migrations"):
        """Load migrations from SQL files in directory."""
        migration_dir = Path(directory)
        if not migration_dir.exists():
            logger.warning(f"Migration directory {directory} does not exist")
            return
        
        for file_path in sorted(migration_dir.glob("*.sql")):
            try:
                migration = self._load_migration_from_file(file_path)
                if migration:
                    self.add_migration(migration)
            except Exception as e:
                logger.error(f"Failed to load migration from {file_path}: {e}")
    
    def _load_migration_from_file(self, file_path: Path) -> Optional[SQLMigration]:
        """Load migration from SQL file."""
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Parse migration metadata from comments
        lines = content.split('\n')
        name = file_path.stem
        description = ""
        version = None
        
        for line in lines:
            if line.startswith('-- Migration:'):
                name = line.split(':', 1)[1].strip()
            elif line.startswith('-- Version:'):
                version_str = line.split(':', 1)[1].strip()
                version = int(version_str)
            elif line.startswith('-- Description:'):
                description = line.split(':', 1)[1].strip()
        
        if version is None:
            # Try to extract version from filename
            parts = file_path.stem.split('_')
            if len(parts) >= 2:
                try:
                    version = int(parts[1])
                except ValueError:
                    logger.error(f"Could not extract version from filename: {file_path}")
                    return None
        
        if version is None:
            logger.error(f"Could not determine version for migration: {file_path}")
            return None
        
        # Split content into up and down migrations
        parts = content.split('-- DOWN Migration')
        up_sql = parts[0].replace('-- UP Migration', '').strip()
        down_sql = parts[1].strip() if len(parts) > 1 else ""
        
        return SQLMigration(version, name, up_sql, down_sql, description)
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get migration status information."""
        applied_migrations = self.get_applied_migrations()
        pending_migrations = self.get_pending_migrations()
        current_version = self.get_current_version()
        
        return {
            'current_version': current_version,
            'applied_count': len(applied_migrations),
            'pending_count': len(pending_migrations),
            'total_migrations': len(self.migrations),
            'applied_migrations': applied_migrations,
            'pending_migrations': [
                {
                    'version': m.version,
                    'name': m.name,
                    'description': m.description
                }
                for m in pending_migrations
            ]
        }

# Predefined migrations for the sync system
def create_initial_migrations() -> List[Migration]:
    """Create initial migrations for the sync system."""
    migrations = []
    
    # Migration 1: Create sync_metrics table
    migration_1 = SQLMigration(
        version=1,
        name="create_sync_metrics_table",
        description="Create sync_metrics table for tracking sync operations",
        up_sql="""
        CREATE TABLE IF NOT EXISTS sync_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            operation TEXT,
            repository TEXT,
            duration REAL,
            success BOOLEAN,
            items_processed INTEGER,
            items_synced INTEGER,
            errors_count INTEGER,
            details TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sync_metrics_timestamp ON sync_metrics(timestamp);
        CREATE INDEX IF NOT EXISTS idx_sync_metrics_operation ON sync_metrics(operation);
        CREATE INDEX IF NOT EXISTS idx_sync_metrics_repository ON sync_metrics(repository);
        """,
        down_sql="""
        DROP TABLE IF EXISTS sync_metrics;
        """
    )
    migrations.append(migration_1)
    
    # Migration 2: Create api_metrics table
    migration_2 = SQLMigration(
        version=2,
        name="create_api_metrics_table",
        description="Create api_metrics table for tracking API calls",
        up_sql="""
        CREATE TABLE IF NOT EXISTS api_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            endpoint TEXT,
            method TEXT,
            duration REAL,
            status_code INTEGER,
            success BOOLEAN,
            retry_count INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_api_metrics_timestamp ON api_metrics(timestamp);
        CREATE INDEX IF NOT EXISTS idx_api_metrics_endpoint ON api_metrics(endpoint);
        """,
        down_sql="""
        DROP TABLE IF EXISTS api_metrics;
        """
    )
    migrations.append(migration_2)
    
    # Migration 3: Create performance_metrics table
    migration_3 = SQLMigration(
        version=3,
        name="create_performance_metrics_table",
        description="Create performance_metrics table for tracking system performance",
        up_sql="""
        CREATE TABLE IF NOT EXISTS performance_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            metric_name TEXT,
            metric_value REAL,
            tags TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_performance_metrics_timestamp ON performance_metrics(timestamp);
        CREATE INDEX IF NOT EXISTS idx_performance_metrics_name ON performance_metrics(metric_name);
        """,
        down_sql="""
        DROP TABLE IF EXISTS performance_metrics;
        """
    )
    migrations.append(migration_3)
    
    return migrations
