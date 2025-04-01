#!/usr/bin/env python3
"""
Multi-Tenant Manager for Gitea-Kimai Integration

This module provides multi-tenancy support for enterprise users, allowing
multiple organizations to use the same instance with isolated data and configurations.
"""

import os
import json
import logging
import sqlite3
import threading
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum
import hashlib
import uuid

logger = logging.getLogger(__name__)

class TenantStatus(Enum):
    """Tenant status enumeration."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    INACTIVE = "inactive"
    PENDING = "pending"
    DELETED = "deleted"

class TenantPlan(Enum):
    """Tenant subscription plans."""
    FREE = "free"
    BASIC = "basic"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"

@dataclass
class TenantLimits:
    """Resource limits for a tenant."""
    max_repositories: int = 10
    max_users: int = 5
    max_sync_operations_per_hour: int = 100
    max_storage_mb: int = 1000
    max_api_calls_per_hour: int = 1000
    max_webhook_endpoints: int = 5
    backup_retention_days: int = 30
    concurrent_syncs: int = 2

@dataclass
class TenantConfig:
    """Tenant configuration."""
    tenant_id: str
    name: str
    display_name: str
    status: TenantStatus
    plan: TenantPlan
    created_at: datetime
    updated_at: datetime
    contact_email: str
    limits: TenantLimits = field(default_factory=TenantLimits)
    custom_settings: Dict[str, Any] = field(default_factory=dict)
    allowed_domains: List[str] = field(default_factory=list)
    features: Set[str] = field(default_factory=set)
    billing_info: Dict[str, Any] = field(default_factory=dict)

class TenantIsolation:
    """Handles data isolation between tenants."""

    def __init__(self, base_data_dir: str = "tenant_data"):
        self.base_data_dir = Path(base_data_dir)
        self.base_data_dir.mkdir(exist_ok=True)

    def get_tenant_data_dir(self, tenant_id: str) -> Path:
        """Get tenant-specific data directory."""
        tenant_dir = self.base_data_dir / tenant_id
        tenant_dir.mkdir(exist_ok=True)
        return tenant_dir

    def get_tenant_database_path(self, tenant_id: str) -> str:
        """Get tenant-specific database path."""
        return str(self.get_tenant_data_dir(tenant_id) / "tenant.db")

    def get_tenant_config_path(self, tenant_id: str) -> str:
        """Get tenant-specific config path."""
        return str(self.get_tenant_data_dir(tenant_id) / "config.json")

    def get_tenant_log_path(self, tenant_id: str) -> str:
        """Get tenant-specific log path."""
        return str(self.get_tenant_data_dir(tenant_id) / "tenant.log")

    def initialize_tenant_storage(self, tenant_id: str) -> bool:
        """Initialize storage structure for a new tenant."""
        try:
            tenant_dir = self.get_tenant_data_dir(tenant_id)

            # Create subdirectories
            (tenant_dir / "logs").mkdir(exist_ok=True)
            (tenant_dir / "backups").mkdir(exist_ok=True)
            (tenant_dir / "exports").mkdir(exist_ok=True)
            (tenant_dir / "cache").mkdir(exist_ok=True)
            (tenant_dir / "plugins").mkdir(exist_ok=True)

            # Initialize tenant database
            self._initialize_tenant_database(tenant_id)

            logger.info(f"Initialized storage for tenant: {tenant_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize storage for tenant {tenant_id}: {e}")
            return False

    def _initialize_tenant_database(self, tenant_id: str):
        """Initialize tenant-specific database."""
        db_path = self.get_tenant_database_path(tenant_id)

        with sqlite3.connect(db_path) as conn:
            # Create tenant-specific tables
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gitea_id TEXT NOT NULL,
                    kimai_id TEXT,
                    title TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    sync_status TEXT DEFAULT 'pending',
                    tenant_id TEXT NOT NULL,
                    UNIQUE(gitea_id, tenant_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS tenant_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS tenant_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT,
                    user_id TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_tenant ON sync_items(tenant_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_tenant ON tenant_metrics(tenant_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_tenant ON tenant_audit(tenant_id)")

            conn.commit()

    def cleanup_tenant_storage(self, tenant_id: str) -> bool:
        """Clean up storage for a deleted tenant."""
        try:
            import shutil
            tenant_dir = self.get_tenant_data_dir(tenant_id)

            if tenant_dir.exists():
                shutil.rmtree(tenant_dir)
                logger.info(f"Cleaned up storage for tenant: {tenant_id}")

            return True

        except Exception as e:
            logger.error(f"Failed to cleanup storage for tenant {tenant_id}: {e}")
            return False

class TenantResourceMonitor:
    """Monitors tenant resource usage and enforces limits."""

    def __init__(self, isolation: TenantIsolation):
        self.isolation = isolation
        self.usage_cache = {}
        self.cache_lock = threading.Lock()

    def check_resource_limits(self, tenant_id: str, tenant_config: TenantConfig) -> Dict[str, bool]:
        """Check if tenant is within resource limits."""
        checks = {
            'repositories': self._check_repository_limit(tenant_id, tenant_config.limits),
            'users': self._check_user_limit(tenant_id, tenant_config.limits),
            'sync_operations': self._check_sync_operations_limit(tenant_id, tenant_config.limits),
            'storage': self._check_storage_limit(tenant_id, tenant_config.limits),
            'api_calls': self._check_api_calls_limit(tenant_id, tenant_config.limits)
        }

        return checks

    def _check_repository_limit(self, tenant_id: str, limits: TenantLimits) -> bool:
        """Check repository count limit."""
        try:
            db_path = self.isolation.get_tenant_database_path(tenant_id)
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(DISTINCT repository) FROM sync_items WHERE tenant_id = ?
                """, (tenant_id,))
                repo_count = cursor.fetchone()[0]
                return repo_count <= limits.max_repositories
        except Exception:
            return True  # Allow on error

    def _check_user_limit(self, tenant_id: str, limits: TenantLimits) -> bool:
        """Check user count limit."""
        # This would check against user management system
        return True  # Placeholder

    def _check_sync_operations_limit(self, tenant_id: str, limits: TenantLimits) -> bool:
        """Check sync operations per hour limit."""
        try:
            db_path = self.isolation.get_tenant_database_path(tenant_id)
            with sqlite3.connect(db_path) as conn:
                one_hour_ago = datetime.now() - timedelta(hours=1)
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM tenant_metrics
                    WHERE tenant_id = ? AND metric_name = 'sync_operation'
                    AND timestamp >= ?
                """, (tenant_id, one_hour_ago.isoformat()))
                operation_count = cursor.fetchone()[0]
                return operation_count <= limits.max_sync_operations_per_hour
        except Exception:
            return True

    def _check_storage_limit(self, tenant_id: str, limits: TenantLimits) -> bool:
        """Check storage usage limit."""
        try:
            tenant_dir = self.isolation.get_tenant_data_dir(tenant_id)
            total_size = sum(f.stat().st_size for f in tenant_dir.rglob('*') if f.is_file())
            size_mb = total_size / (1024 * 1024)
            return size_mb <= limits.max_storage_mb
        except Exception:
            return True

    def _check_api_calls_limit(self, tenant_id: str, limits: TenantLimits) -> bool:
        """Check API calls per hour limit."""
        try:
            db_path = self.isolation.get_tenant_database_path(tenant_id)
            with sqlite3.connect(db_path) as conn:
                one_hour_ago = datetime.now() - timedelta(hours=1)
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM tenant_metrics
                    WHERE tenant_id = ? AND metric_name = 'api_call'
                    AND timestamp >= ?
                """, (tenant_id, one_hour_ago.isoformat()))
                api_count = cursor.fetchone()[0]
                return api_count <= limits.max_api_calls_per_hour
        except Exception:
            return True

    def record_resource_usage(self, tenant_id: str, resource_type: str, amount: float = 1.0):
        """Record resource usage for a tenant."""
        try:
            db_path = self.isolation.get_tenant_database_path(tenant_id)
            with sqlite3.connect(db_path) as conn:
                conn.execute("""
                    INSERT INTO tenant_metrics (tenant_id, metric_name, metric_value)
                    VALUES (?, ?, ?)
                """, (tenant_id, resource_type, amount))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to record resource usage for tenant {tenant_id}: {e}")

class TenantManager:
    """Main tenant management system."""

    def __init__(self, config_dir: str = "tenant_configs"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        self.tenants: Dict[str, TenantConfig] = {}
        self.isolation = TenantIsolation()
        self.resource_monitor = TenantResourceMonitor(self.isolation)
        self.lock = threading.RLock()
        self._load_tenants()

    def _load_tenants(self):
        """Load tenant configurations from storage."""
        try:
            for config_file in self.config_dir.glob("*.json"):
                tenant_id = config_file.stem
                with open(config_file, 'r') as f:
                    data = json.load(f)

                tenant_config = self._deserialize_tenant_config(data)
                self.tenants[tenant_id] = tenant_config

            logger.info(f"Loaded {len(self.tenants)} tenant configurations")

        except Exception as e:
            logger.error(f"Failed to load tenant configurations: {e}")

    def _save_tenant_config(self, tenant_config: TenantConfig):
        """Save tenant configuration to storage."""
        try:
            config_file = self.config_dir / f"{tenant_config.tenant_id}.json"
            data = self._serialize_tenant_config(tenant_config)

            with open(config_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)

        except Exception as e:
            logger.error(f"Failed to save tenant config {tenant_config.tenant_id}: {e}")

    def _serialize_tenant_config(self, config: TenantConfig) -> Dict[str, Any]:
        """Serialize tenant config to dictionary."""
        return {
            'tenant_id': config.tenant_id,
            'name': config.name,
            'display_name': config.display_name,
            'status': config.status.value,
            'plan': config.plan.value,
            'created_at': config.created_at.isoformat(),
            'updated_at': config.updated_at.isoformat(),
            'contact_email': config.contact_email,
            'limits': {
                'max_repositories': config.limits.max_repositories,
                'max_users': config.limits.max_users,
                'max_sync_operations_per_hour': config.limits.max_sync_operations_per_hour,
                'max_storage_mb': config.limits.max_storage_mb,
                'max_api_calls_per_hour': config.limits.max_api_calls_per_hour,
                'max_webhook_endpoints': config.limits.max_webhook_endpoints,
                'backup_retention_days': config.limits.backup_retention_days,
                'concurrent_syncs': config.limits.concurrent_syncs
            },
            'custom_settings': config.custom_settings,
            'allowed_domains': config.allowed_domains,
            'features': list(config.features),
            'billing_info': config.billing_info
        }

    def _deserialize_tenant_config(self, data: Dict[str, Any]) -> TenantConfig:
        """Deserialize tenant config from dictionary."""
        limits_data = data.get('limits', {})
        limits = TenantLimits(**limits_data)

        return TenantConfig(
            tenant_id=data['tenant_id'],
            name=data['name'],
            display_name=data['display_name'],
            status=TenantStatus(data['status']),
            plan=TenantPlan(data['plan']),
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at']),
            contact_email=data['contact_email'],
            limits=limits,
            custom_settings=data.get('custom_settings', {}),
            allowed_domains=data.get('allowed_domains', []),
            features=set(data.get('features', [])),
            billing_info=data.get('billing_info', {})
        )

    def create_tenant(self, name: str, display_name: str, contact_email: str,
                     plan: TenantPlan = TenantPlan.FREE) -> Optional[str]:
        """Create a new tenant."""
        with self.lock:
            # Generate tenant ID
            tenant_id = self._generate_tenant_id(name)

            if tenant_id in self.tenants:
                logger.error(f"Tenant {tenant_id} already exists")
                return None

            # Set limits based on plan
            limits = self._get_plan_limits(plan)

            # Create tenant configuration
            tenant_config = TenantConfig(
                tenant_id=tenant_id,
                name=name,
                display_name=display_name,
                status=TenantStatus.PENDING,
                plan=plan,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                contact_email=contact_email,
                limits=limits
            )

            # Initialize tenant storage
            if not self.isolation.initialize_tenant_storage(tenant_id):
                logger.error(f"Failed to initialize storage for tenant {tenant_id}")
                return None

            # Save configuration
            self.tenants[tenant_id] = tenant_config
            self._save_tenant_config(tenant_config)

            logger.info(f"Created tenant: {tenant_id}")
            return tenant_id

    def _generate_tenant_id(self, name: str) -> str:
        """Generate unique tenant ID from name."""
        # Normalize name
        normalized = re.sub(r'[^a-zA-Z0-9]', '_', name.lower())

        # Add hash for uniqueness
        hash_suffix = hashlib.md5(f"{name}{datetime.now()}".encode()).hexdigest()[:8]

        return f"{normalized}_{hash_suffix}"

    def _get_plan_limits(self, plan: TenantPlan) -> TenantLimits:
        """Get resource limits for a plan."""
        plan_limits = {
            TenantPlan.FREE: TenantLimits(
                max_repositories=5,
                max_users=2,
                max_sync_operations_per_hour=50,
                max_storage_mb=500,
                max_api_calls_per_hour=500,
                max_webhook_endpoints=2,
                backup_retention_days=7,
                concurrent_syncs=1
            ),
            TenantPlan.BASIC: TenantLimits(
                max_repositories=20,
                max_users=10,
                max_sync_operations_per_hour=200,
                max_storage_mb=2000,
                max_api_calls_per_hour=2000,
                max_webhook_endpoints=5,
                backup_retention_days=30,
                concurrent_syncs=2
            ),
            TenantPlan.PROFESSIONAL: TenantLimits(
                max_repositories=100,
                max_users=50,
                max_sync_operations_per_hour=1000,
                max_storage_mb=10000,
                max_api_calls_per_hour=10000,
                max_webhook_endpoints=20,
                backup_retention_days=90,
                concurrent_syncs=5
            ),
            TenantPlan.ENTERPRISE: TenantLimits(
                max_repositories=1000,
                max_users=500,
                max_sync_operations_per_hour=10000,
                max_storage_mb=100000,
                max_api_calls_per_hour=100000,
                max_webhook_endpoints=100,
                backup_retention_days=365,
                concurrent_syncs=20
            )
        }

        return plan_limits.get(plan, TenantLimits())

    def get_tenant(self, tenant_id: str) -> Optional[TenantConfig]:
        """Get tenant configuration."""
        return self.tenants.get(tenant_id)

    def activate_tenant(self, tenant_id: str) -> bool:
        """Activate a tenant."""
        with self.lock:
            tenant = self.tenants.get(tenant_id)
            if not tenant:
                return False

            tenant.status = TenantStatus.ACTIVE
            tenant.updated_at = datetime.now()
            self._save_tenant_config(tenant)

            logger.info(f"Activated tenant: {tenant_id}")
            return True

    def suspend_tenant(self, tenant_id: str, reason: str = "") -> bool:
        """Suspend a tenant."""
        with self.lock:
            tenant = self.tenants.get(tenant_id)
            if not tenant:
                return False

            tenant.status = TenantStatus.SUSPENDED
            tenant.updated_at = datetime.now()
            if reason:
                tenant.custom_settings['suspension_reason'] = reason
            self._save_tenant_config(tenant)

            logger.info(f"Suspended tenant: {tenant_id}")
            return True

    def delete_tenant(self, tenant_id: str) -> bool:
        """Delete a tenant and clean up resources."""
        with self.lock:
            tenant = self.tenants.get(tenant_id)
            if not tenant:
                return False

            # Mark as deleted
            tenant.status = TenantStatus.DELETED
            tenant.updated_at = datetime.now()
            self._save_tenant_config(tenant)

            # Schedule cleanup
            self.isolation.cleanup_tenant_storage(tenant_id)

            # Remove from active tenants
            del self.tenants[tenant_id]

            # Remove config file
            config_file = self.config_dir / f"{tenant_id}.json"
            if config_file.exists():
                config_file.unlink()

            logger.info(f"Deleted tenant: {tenant_id}")
            return True

    def update_tenant_plan(self, tenant_id: str, new_plan: TenantPlan) -> bool:
        """Update tenant subscription plan."""
        with self.lock:
            tenant = self.tenants.get(tenant_id)
            if not tenant:
                return False

            old_plan = tenant.plan
            tenant.plan = new_plan
            tenant.limits = self._get_plan_limits(new_plan)
            tenant.updated_at = datetime.now()
            self._save_tenant_config(tenant)

            logger.info(f"Updated tenant {tenant_id} plan from {old_plan.value} to {new_plan.value}")
            return True

    def check_tenant_access(self, tenant_id: str, user_email: str = None) -> bool:
        """Check if user has access to tenant."""
        tenant = self.tenants.get(tenant_id)
        if not tenant or tenant.status != TenantStatus.ACTIVE:
            return False

        # Check domain restrictions
        if tenant.allowed_domains and user_email:
            user_domain = user_email.split('@')[1] if '@' in user_email else ''
            if user_domain not in tenant.allowed_domains:
                return False

        return True

    def get_tenant_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get current resource usage for tenant."""
        tenant = self.tenants.get(tenant_id)
        if not tenant:
            return {}

        limit_checks = self.resource_monitor.check_resource_limits(tenant_id, tenant)

        return {
            'tenant_id': tenant_id,
            'plan': tenant.plan.value,
            'status': tenant.status.value,
            'limits': {
                'repositories': tenant.limits.max_repositories,
                'users': tenant.limits.max_users,
                'sync_operations_per_hour': tenant.limits.max_sync_operations_per_hour,
                'storage_mb': tenant.limits.max_storage_mb,
                'api_calls_per_hour': tenant.limits.max_api_calls_per_hour
            },
            'within_limits': limit_checks,
            'last_updated': tenant.updated_at.isoformat()
        }

    def list_tenants(self, status: TenantStatus = None) -> List[Dict[str, Any]]:
        """List all tenants with optional status filter."""
        result = []

        for tenant in self.tenants.values():
            if status and tenant.status != status:
                continue

            result.append({
                'tenant_id': tenant.tenant_id,
                'name': tenant.name,
                'display_name': tenant.display_name,
                'status': tenant.status.value,
                'plan': tenant.plan.value,
                'contact_email': tenant.contact_email,
                'created_at': tenant.created_at.isoformat(),
                'updated_at': tenant.updated_at.isoformat()
            })

        return sorted(result, key=lambda x: x['created_at'])

# Global tenant manager instance
_global_tenant_manager = None

def get_tenant_manager() -> TenantManager:
    """Get global tenant manager instance."""
    global _global_tenant_manager

    if _global_tenant_manager is None:
        _global_tenant_manager = TenantManager()

    return _global_tenant_manager

def require_tenant_access(tenant_id: str, user_email: str = None):
    """Decorator to require tenant access for functions."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            manager = get_tenant_manager()
            if not manager.check_tenant_access(tenant_id, user_email):
                raise PermissionError(f"Access denied to tenant {tenant_id}")
            return func(*args, **kwargs)
        return wrapper
    return decorator
