#!/usr/bin/env python3
"""
Multi-Tenant Support for Gitea-Kimai Integration

Provides comprehensive multi-tenant architecture supporting multiple organizations,
teams, and isolated configurations for enterprise deployments.
"""

import os
import json
import logging
import sqlite3
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import uuid
import hashlib
from pathlib import Path

from ..config.config_manager import ConfigurationManager
from ..security.security import SecurityManager, Role, Permission
from ..storage.cache_manager import CacheManager
from ..utils.error_handler import ErrorHandler

logger = logging.getLogger(__name__)

class TenantType(Enum):
    """Types of tenants."""
    ORGANIZATION = "organization"
    TEAM = "team"
    DEPARTMENT = "department"
    PROJECT = "project"

class TenantStatus(Enum):
    """Tenant status."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    PENDING = "pending"
    ARCHIVED = "archived"

@dataclass
class Tenant:
    """Represents a tenant in the system."""
    id: str
    name: str
    tenant_type: TenantType
    status: TenantStatus
    parent_id: Optional[str] = None
    created_at: datetime = None
    updated_at: datetime = None
    settings: Dict[str, Any] = None
    limits: Dict[str, Any] = None
    metadata: Dict[str, Any] = None

@dataclass
class TenantConfiguration:
    """Tenant-specific configuration."""
    tenant_id: str
    gitea_config: Dict[str, Any]
    kimai_config: Dict[str, Any]
    sync_config: Dict[str, Any]
    security_config: Dict[str, Any]
    notification_config: Dict[str, Any]
    custom_config: Dict[str, Any] = None

@dataclass
class TenantUser:
    """User association with tenant."""
    user_id: str
    tenant_id: str
    role: Role
    permissions: List[Permission]
    active: bool = True
    created_at: datetime = None
    last_access: datetime = None

@dataclass
class TenantResource:
    """Resource usage tracking for tenant."""
    tenant_id: str
    resource_type: str  # 'api_calls', 'storage', 'sync_operations'
    current_usage: int
    limit: int
    period: str  # 'hourly', 'daily', 'monthly'
    reset_at: datetime

class TenantManager:
    """Manages tenant lifecycle and operations."""

    def __init__(self, db_path: str = "tenants.db"):
        self.db_path = db_path
        self.cache = CacheManager()
        self.security = SecurityManager()
        self.error_handler = ErrorHandler()
        self._setup_database()

    def _setup_database(self):
        """Setup tenant database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Tenants table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                tenant_type TEXT NOT NULL,
                status TEXT NOT NULL,
                parent_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settings TEXT,
                limits TEXT,
                metadata TEXT,
                FOREIGN KEY (parent_id) REFERENCES tenants(id)
            )
        ''')

        # Tenant configurations
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenant_configurations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                config_type TEXT NOT NULL,
                config_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id),
                UNIQUE(tenant_id, config_type)
            )
        ''')

        # Tenant users
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenant_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                role TEXT NOT NULL,
                permissions TEXT,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_access TIMESTAMP,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id),
                UNIQUE(user_id, tenant_id)
            )
        ''')

        # Resource usage tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenant_resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                current_usage INTEGER DEFAULT 0,
                limit_value INTEGER NOT NULL,
                period TEXT NOT NULL,
                reset_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id),
                UNIQUE(tenant_id, resource_type, period)
            )
        ''')

        # Tenant isolation tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tenant_data_isolation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                resource_id TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id),
                UNIQUE(tenant_id, resource_id, resource_type)
            )
        ''')

        conn.commit()
        conn.close()

    def create_tenant(self, name: str, tenant_type: TenantType, parent_id: Optional[str] = None,
                     settings: Dict[str, Any] = None, limits: Dict[str, Any] = None) -> str:
        """Create a new tenant."""
        tenant_id = str(uuid.uuid4())

        # Default settings
        default_settings = {
            'max_repositories': 100,
            'max_users': 50,
            'max_api_calls_per_hour': 1000,
            'storage_limit_gb': 10,
            'features': ['sync', 'api', 'webhooks', 'reports']
        }

        # Default limits
        default_limits = {
            'api_calls_per_hour': 1000,
            'api_calls_per_day': 10000,
            'storage_gb': 10,
            'sync_operations_per_hour': 100,
            'concurrent_sync_jobs': 5
        }

        tenant = Tenant(
            id=tenant_id,
            name=name,
            tenant_type=tenant_type,
            status=TenantStatus.ACTIVE,
            parent_id=parent_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            settings=settings or default_settings,
            limits=limits or default_limits,
            metadata={}
        )

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO tenants (id, name, tenant_type, status, parent_id, created_at, updated_at, settings, limits, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            tenant.id, tenant.name, tenant.tenant_type.value, tenant.status.value,
            tenant.parent_id, tenant.created_at, tenant.updated_at,
            json.dumps(tenant.settings), json.dumps(tenant.limits), json.dumps(tenant.metadata)
        ))

        conn.commit()
        conn.close()

        # Initialize default resource limits
        self._initialize_tenant_resources(tenant_id, tenant.limits)

        logger.info(f"Created tenant {name} (ID: {tenant_id})")
        return tenant_id

    def _initialize_tenant_resources(self, tenant_id: str, limits: Dict[str, Any]):
        """Initialize resource tracking for tenant."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Initialize resource limits
        resources = [
            ('api_calls', limits.get('api_calls_per_hour', 1000), 'hourly'),
            ('api_calls', limits.get('api_calls_per_day', 10000), 'daily'),
            ('storage', limits.get('storage_gb', 10) * 1024 * 1024 * 1024, 'unlimited'),
            ('sync_operations', limits.get('sync_operations_per_hour', 100), 'hourly')
        ]

        for resource_type, limit_value, period in resources:
            reset_at = self._calculate_reset_time(period)

            cursor.execute('''
                INSERT OR REPLACE INTO tenant_resources
                (tenant_id, resource_type, current_usage, limit_value, period, reset_at)
                VALUES (?, ?, 0, ?, ?, ?)
            ''', (tenant_id, resource_type, limit_value, period, reset_at))

        conn.commit()
        conn.close()

    def _calculate_reset_time(self, period: str) -> datetime:
        """Calculate when resource usage should reset."""
        now = datetime.now()

        if period == 'hourly':
            return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        elif period == 'daily':
            return now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        elif period == 'monthly':
            # Reset on first day of next month
            if now.month == 12:
                return datetime(now.year + 1, 1, 1)
            else:
                return datetime(now.year, now.month + 1, 1)
        else:
            return now + timedelta(days=365)  # Unlimited

    def get_tenant(self, tenant_id: str) -> Optional[Tenant]:
        """Get tenant by ID."""
        # Check cache first
        cached = self.cache.get(f"tenant:{tenant_id}")
        if cached:
            return Tenant(**cached)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT id, name, tenant_type, status, parent_id, created_at, updated_at, settings, limits, metadata
            FROM tenants WHERE id = ?
        ''', (tenant_id,))

        row = cursor.fetchone()
        conn.close()

        if row:
            tenant = Tenant(
                id=row[0],
                name=row[1],
                tenant_type=TenantType(row[2]),
                status=TenantStatus(row[3]),
                parent_id=row[4],
                created_at=datetime.fromisoformat(row[5]) if row[5] else None,
                updated_at=datetime.fromisoformat(row[6]) if row[6] else None,
                settings=json.loads(row[7]) if row[7] else {},
                limits=json.loads(row[8]) if row[8] else {},
                metadata=json.loads(row[9]) if row[9] else {}
            )

            # Cache for future use
            self.cache.set(f"tenant:{tenant_id}", asdict(tenant), ttl=300)
            return tenant

        return None

    def update_tenant(self, tenant_id: str, **kwargs) -> bool:
        """Update tenant information."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False

        # Update fields
        for key, value in kwargs.items():
            if hasattr(tenant, key):
                setattr(tenant, key, value)

        tenant.updated_at = datetime.now()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE tenants SET name = ?, tenant_type = ?, status = ?, parent_id = ?,
                             updated_at = ?, settings = ?, limits = ?, metadata = ?
            WHERE id = ?
        ''', (
            tenant.name, tenant.tenant_type.value, tenant.status.value,
            tenant.parent_id, tenant.updated_at,
            json.dumps(tenant.settings), json.dumps(tenant.limits), json.dumps(tenant.metadata),
            tenant_id
        ))

        conn.commit()
        conn.close()

        # Clear cache
        self.cache.delete(f"tenant:{tenant_id}")

        logger.info(f"Updated tenant {tenant_id}")
        return True

    def delete_tenant(self, tenant_id: str) -> bool:
        """Delete tenant and all associated data."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Delete tenant data isolation records
            cursor.execute('DELETE FROM tenant_data_isolation WHERE tenant_id = ?', (tenant_id,))

            # Delete tenant resources
            cursor.execute('DELETE FROM tenant_resources WHERE tenant_id = ?', (tenant_id,))

            # Delete tenant users
            cursor.execute('DELETE FROM tenant_users WHERE tenant_id = ?', (tenant_id,))

            # Delete tenant configurations
            cursor.execute('DELETE FROM tenant_configurations WHERE tenant_id = ?', (tenant_id,))

            # Delete the tenant
            cursor.execute('DELETE FROM tenants WHERE id = ?', (tenant_id,))

            conn.commit()

            # Clear cache
            self.cache.delete(f"tenant:{tenant_id}")

            logger.info(f"Deleted tenant {tenant_id}")
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Error deleting tenant {tenant_id}: {e}")
            return False
        finally:
            conn.close()

    def list_tenants(self, parent_id: Optional[str] = None, tenant_type: Optional[TenantType] = None,
                    status: Optional[TenantStatus] = None) -> List[Tenant]:
        """List tenants with optional filters."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        query = 'SELECT id, name, tenant_type, status, parent_id, created_at, updated_at, settings, limits, metadata FROM tenants WHERE 1=1'
        params = []

        if parent_id is not None:
            query += ' AND parent_id = ?'
            params.append(parent_id)

        if tenant_type:
            query += ' AND tenant_type = ?'
            params.append(tenant_type.value)

        if status:
            query += ' AND status = ?'
            params.append(status.value)

        query += ' ORDER BY created_at DESC'

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        tenants = []
        for row in rows:
            tenant = Tenant(
                id=row[0],
                name=row[1],
                tenant_type=TenantType(row[2]),
                status=TenantStatus(row[3]),
                parent_id=row[4],
                created_at=datetime.fromisoformat(row[5]) if row[5] else None,
                updated_at=datetime.fromisoformat(row[6]) if row[6] else None,
                settings=json.loads(row[7]) if row[7] else {},
                limits=json.loads(row[8]) if row[8] else {},
                metadata=json.loads(row[9]) if row[9] else {}
            )
            tenants.append(tenant)

        return tenants

    def add_user_to_tenant(self, user_id: str, tenant_id: str, role: Role,
                          permissions: List[Permission] = None) -> bool:
        """Add user to tenant with specific role and permissions."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO tenant_users (user_id, tenant_id, role, permissions, active, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            user_id, tenant_id, role.value,
            json.dumps([p.value for p in permissions]) if permissions else json.dumps([]),
            True, datetime.now()
        ))

        conn.commit()
        conn.close()

        logger.info(f"Added user {user_id} to tenant {tenant_id} with role {role.value}")
        return True

    def remove_user_from_tenant(self, user_id: str, tenant_id: str) -> bool:
        """Remove user from tenant."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('DELETE FROM tenant_users WHERE user_id = ? AND tenant_id = ?', (user_id, tenant_id))

        success = cursor.rowcount > 0
        conn.commit()
        conn.close()

        if success:
            logger.info(f"Removed user {user_id} from tenant {tenant_id}")

        return success

    def get_user_tenants(self, user_id: str) -> List[Tuple[Tenant, TenantUser]]:
        """Get all tenants for a user."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT t.id, t.name, t.tenant_type, t.status, t.parent_id, t.created_at, t.updated_at,
                   t.settings, t.limits, t.metadata,
                   tu.role, tu.permissions, tu.active, tu.created_at, tu.last_access
            FROM tenants t
            JOIN tenant_users tu ON t.id = tu.tenant_id
            WHERE tu.user_id = ? AND tu.active = TRUE
        ''', (user_id,))

        rows = cursor.fetchall()
        conn.close()

        results = []
        for row in rows:
            tenant = Tenant(
                id=row[0],
                name=row[1],
                tenant_type=TenantType(row[2]),
                status=TenantStatus(row[3]),
                parent_id=row[4],
                created_at=datetime.fromisoformat(row[5]) if row[5] else None,
                updated_at=datetime.fromisoformat(row[6]) if row[6] else None,
                settings=json.loads(row[7]) if row[7] else {},
                limits=json.loads(row[8]) if row[8] else {},
                metadata=json.loads(row[9]) if row[9] else {}
            )

            tenant_user = TenantUser(
                user_id=user_id,
                tenant_id=row[0],
                role=Role(row[10]),
                permissions=[Permission(p) for p in json.loads(row[11])],
                active=bool(row[12]),
                created_at=datetime.fromisoformat(row[13]) if row[13] else None,
                last_access=datetime.fromisoformat(row[14]) if row[14] else None
            )

            results.append((tenant, tenant_user))

        return results

    def check_resource_limit(self, tenant_id: str, resource_type: str, amount: int = 1) -> bool:
        """Check if tenant can use specified amount of resource."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get current resource usage for all periods
        cursor.execute('''
            SELECT current_usage, limit_value, period, reset_at
            FROM tenant_resources
            WHERE tenant_id = ? AND resource_type = ?
        ''', (tenant_id, resource_type))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return True  # No limits defined

        for current_usage, limit_value, period, reset_at_str in rows:
            reset_at = datetime.fromisoformat(reset_at_str)

            # Reset usage if period has expired
            if datetime.now() >= reset_at:
                self._reset_resource_usage(tenant_id, resource_type, period)
                current_usage = 0

            # Check if adding amount would exceed limit
            if current_usage + amount > limit_value:
                return False

        return True

    def consume_resource(self, tenant_id: str, resource_type: str, amount: int = 1) -> bool:
        """Consume specified amount of resource if within limits."""
        if not self.check_resource_limit(tenant_id, resource_type, amount):
            return False

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE tenant_resources
            SET current_usage = current_usage + ?, updated_at = ?
            WHERE tenant_id = ? AND resource_type = ?
        ''', (amount, datetime.now(), tenant_id, resource_type))

        conn.commit()
        conn.close()

        return True

    def _reset_resource_usage(self, tenant_id: str, resource_type: str, period: str):
        """Reset resource usage for a specific period."""
        reset_at = self._calculate_reset_time(period)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE tenant_resources
            SET current_usage = 0, reset_at = ?, updated_at = ?
            WHERE tenant_id = ? AND resource_type = ? AND period = ?
        ''', (reset_at, datetime.now(), tenant_id, resource_type, period))

        conn.commit()
        conn.close()

    def get_resource_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get current resource usage for tenant."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT resource_type, current_usage, limit_value, period, reset_at
            FROM tenant_resources
            WHERE tenant_id = ?
        ''', (tenant_id,))

        rows = cursor.fetchall()
        conn.close()

        usage = {}
        for resource_type, current_usage, limit_value, period, reset_at_str in rows:
            if resource_type not in usage:
                usage[resource_type] = {}

            usage[resource_type][period] = {
                'current': current_usage,
                'limit': limit_value,
                'percentage': (current_usage / limit_value * 100) if limit_value > 0 else 0,
                'reset_at': reset_at_str
            }

        return usage

class TenantIsolation:
    """Handles data isolation between tenants."""

    def __init__(self, tenant_manager: TenantManager):
        self.tenant_manager = tenant_manager

    def isolate_database_path(self, tenant_id: str, base_path: str) -> str:
        """Create tenant-specific database path."""
        tenant_dir = f"data/tenants/{tenant_id}"
        os.makedirs(tenant_dir, exist_ok=True)
        return os.path.join(tenant_dir, os.path.basename(base_path))

    def isolate_cache_key(self, tenant_id: str, key: str) -> str:
        """Create tenant-specific cache key."""
        return f"tenant:{tenant_id}:{key}"

    def isolate_config_path(self, tenant_id: str, config_name: str) -> str:
        """Create tenant-specific configuration path."""
        tenant_dir = f"config/tenants/{tenant_id}"
        os.makedirs(tenant_dir, exist_ok=True)
        return os.path.join(tenant_dir, config_name)

    def validate_tenant_access(self, user_id: str, tenant_id: str, permission: Permission) -> bool:
        """Validate if user has permission to access tenant resource."""
        user_tenants = self.tenant_manager.get_user_tenants(user_id)

        for tenant, tenant_user in user_tenants:
            if tenant.id == tenant_id and permission in tenant_user.permissions:
                return True

        return False

class MultiTenantConfigManager:
    """Manages tenant-specific configurations."""

    def __init__(self, tenant_manager: TenantManager):
        self.tenant_manager = tenant_manager

    def save_tenant_config(self, tenant_id: str, config_type: str, config_data: Dict[str, Any]) -> bool:
        """Save configuration for specific tenant."""
        conn = sqlite3.connect(self.tenant_manager.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO tenant_configurations
            (tenant_id, config_type, config_data, updated_at)
            VALUES (?, ?, ?, ?)
        ''', (tenant_id, config_type, json.dumps(config_data), datetime.now()))

        conn.commit()
        conn.close()

        logger.info(f"Saved {config_type} configuration for tenant {tenant_id}")
        return True

    def get_tenant_config(self, tenant_id: str, config_type: str) -> Optional[Dict[str, Any]]:
        """Get configuration for specific tenant."""
        conn = sqlite3.connect(self.tenant_manager.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT config_data FROM tenant_configurations
            WHERE tenant_id = ? AND config_type = ?
        ''', (tenant_id, config_type))

        row = cursor.fetchone()
        conn.close()

        if row:
            return json.loads(row[0])

        return None

    def create_tenant_configuration(self, tenant_id: str, gitea_config: Dict[str, Any],
                                  kimai_config: Dict[str, Any], sync_config: Dict[str, Any] = None) -> bool:
        """Create complete configuration for tenant."""
        configs = {
            'gitea': gitea_config,
            'kimai': kimai_config,
            'sync': sync_config or {},
            'security': {'encryption_enabled': True, 'audit_enabled': True},
            'notifications': {'enabled': False}
        }

        for config_type, config_data in configs.items():
            if not self.save_tenant_config(tenant_id, config_type, config_data):
                return False

        return True

# Global multi-tenant manager
tenant_manager = TenantManager()
tenant_isolation = TenantIsolation(tenant_manager)
multi_tenant_config = MultiTenantConfigManager(tenant_manager)
