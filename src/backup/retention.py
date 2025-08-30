#!/usr/bin/env python3
"""
Backup Retention Module

Retention policy management for automated backup cleanup,
lifecycle management, and storage optimization.
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import threading
import json

logger = logging.getLogger(__name__)

@dataclass
class RetentionPolicy:
    """Retention policy configuration."""
    name: str
    enabled: bool = True
    max_backups: int = 30
    max_days: int = 90
    max_size_gb: int = 100
    keep_daily: int = 7
    keep_weekly: int = 4
    keep_monthly: int = 12
    keep_yearly: int = 5
    priority: int = 1

@dataclass
class RetentionResult:
    """Result of retention policy execution."""
    policy_name: str
    backups_checked: int
    backups_deleted: int
    space_freed: int
    errors: List[str]
    execution_time: float

class BackupRetention:
    """Retention policy manager for backup lifecycle management."""
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        self.policies: Dict[str, RetentionPolicy] = {}
        self.lock = threading.RLock()
        self._load_default_policies()
    
    def _load_default_policies(self):
        """Load default retention policies."""
        default_policies = [
            RetentionPolicy(
                name="daily",
                max_backups=7,
                max_days=7,
                max_size_gb=10,
                keep_daily=7,
                keep_weekly=0,
                keep_monthly=0,
                keep_yearly=0,
                priority=1
            ),
            RetentionPolicy(
                name="weekly",
                max_backups=4,
                max_days=30,
                max_size_gb=20,
                keep_daily=0,
                keep_weekly=4,
                keep_monthly=0,
                keep_yearly=0,
                priority=2
            ),
            RetentionPolicy(
                name="monthly",
                max_backups=12,
                max_days=365,
                max_size_gb=50,
                keep_daily=0,
                keep_weekly=0,
                keep_monthly=12,
                keep_yearly=0,
                priority=3
            ),
            RetentionPolicy(
                name="yearly",
                max_backups=5,
                max_days=1825,  # 5 years
                max_size_gb=100,
                keep_daily=0,
                keep_weekly=0,
                keep_monthly=0,
                keep_yearly=5,
                priority=4
            )
        ]
        
        for policy in default_policies:
            self.add_policy(policy)
    
    def add_policy(self, policy: RetentionPolicy):
        """Add a retention policy."""
        with self.lock:
            self.policies[policy.name] = policy
            logger.info(f"Added retention policy: {policy.name}")
    
    def remove_policy(self, policy_name: str) -> bool:
        """Remove a retention policy."""
        with self.lock:
            if policy_name in self.policies:
                del self.policies[policy_name]
                logger.info(f"Removed retention policy: {policy_name}")
                return True
            return False
    
    def get_policy(self, policy_name: str) -> Optional[RetentionPolicy]:
        """Get a retention policy by name."""
        return self.policies.get(policy_name)
    
    def list_policies(self) -> List[RetentionPolicy]:
        """List all retention policies."""
        return list(self.policies.values())
    
    def execute_retention_policies(self) -> List[RetentionResult]:
        """Execute all retention policies."""
        results = []
        
        # Sort policies by priority
        sorted_policies = sorted(self.policies.values(), key=lambda p: p.priority)
        
        for policy in sorted_policies:
            if policy.enabled:
                result = self._execute_policy(policy)
                results.append(result)
        
        return results
    
    def _execute_policy(self, policy: RetentionPolicy) -> RetentionResult:
        """Execute a single retention policy."""
        start_time = datetime.now()
        backups_checked = 0
        backups_deleted = 0
        space_freed = 0
        errors = []
        
        try:
            # Get all backup files
            backup_files = self._get_backup_files()
            backups_checked = len(backup_files)
            
            if not backup_files:
                return RetentionResult(
                    policy_name=policy.name,
                    backups_checked=0,
                    backups_deleted=0,
                    space_freed=0,
                    errors=[],
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
            
            # Apply retention rules
            files_to_delete = self._apply_retention_rules(backup_files, policy)
            
            # Delete files
            for file_path in files_to_delete:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    backups_deleted += 1
                    space_freed += file_size
                    logger.info(f"Deleted backup file: {file_path.name}")
                except Exception as e:
                    error_msg = f"Failed to delete {file_path.name}: {e}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            result = RetentionResult(
                policy_name=policy.name,
                backups_checked=backups_checked,
                backups_deleted=backups_deleted,
                space_freed=space_freed,
                errors=errors,
                execution_time=execution_time
            )
            
            logger.info(f"Retention policy '{policy.name}' executed: {backups_deleted} files deleted, {space_freed / (1024*1024):.1f} MB freed")
            return result
            
        except Exception as e:
            error_msg = f"Policy execution failed: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
            
            return RetentionResult(
                policy_name=policy.name,
                backups_checked=backups_checked,
                backups_deleted=backups_deleted,
                space_freed=space_freed,
                errors=errors,
                execution_time=(datetime.now() - start_time).total_seconds()
            )
    
    def _get_backup_files(self) -> List[Path]:
        """Get all backup files in the backup directory."""
        backup_files = []
        
        for file_path in self.backup_dir.glob("*.zip"):
            if file_path.is_file():
                backup_files.append(file_path)
        
        # Sort by modification time (newest first)
        backup_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        return backup_files
    
    def _apply_retention_rules(self, backup_files: List[Path], policy: RetentionPolicy) -> List[Path]:
        """Apply retention rules to determine which files to delete."""
        files_to_delete = []
        
        # Group files by time periods
        now = datetime.now()
        daily_files = []
        weekly_files = []
        monthly_files = []
        yearly_files = []
        other_files = []
        
        for file_path in backup_files:
            file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
            age_days = (now - file_time).days
            
            # Check max days rule
            if age_days > policy.max_days:
                files_to_delete.append(file_path)
                continue
            
            # Categorize files
            if age_days <= 7:
                daily_files.append((file_path, file_time))
            elif age_days <= 30:
                weekly_files.append((file_path, file_time))
            elif age_days <= 365:
                monthly_files.append((file_path, file_time))
            else:
                yearly_files.append((file_path, file_time))
        
        # Apply keep rules
        files_to_delete.extend(self._apply_keep_rule(daily_files, policy.keep_daily, "daily"))
        files_to_delete.extend(self._apply_keep_rule(weekly_files, policy.keep_weekly, "weekly"))
        files_to_delete.extend(self._apply_keep_rule(monthly_files, policy.keep_monthly, "monthly"))
        files_to_delete.extend(self._apply_keep_rule(yearly_files, policy.keep_yearly, "yearly"))
        
        # Apply max backups rule
        remaining_files = [f for f in backup_files if f not in files_to_delete]
        if len(remaining_files) > policy.max_backups:
            excess_files = remaining_files[policy.max_backups:]
            files_to_delete.extend(excess_files)
        
        # Apply max size rule
        remaining_files = [f for f in backup_files if f not in files_to_delete]
        total_size = sum(f.stat().st_size for f in remaining_files)
        max_size_bytes = policy.max_size_gb * 1024 * 1024 * 1024
        
        if total_size > max_size_bytes:
            # Remove oldest files until under size limit
            for file_path in reversed(remaining_files):
                if total_size <= max_size_bytes:
                    break
                file_size = file_path.stat().st_size
                files_to_delete.append(file_path)
                total_size -= file_size
        
        return list(set(files_to_delete))  # Remove duplicates
    
    def _apply_keep_rule(self, files: List[Tuple[Path, datetime]], keep_count: int, period: str) -> List[Path]:
        """Apply keep rule for a specific time period."""
        if keep_count <= 0 or not files:
            return [f[0] for f in files]  # Delete all files in this period
        
        # Sort by time (newest first)
        files.sort(key=lambda x: x[1], reverse=True)
        
        # Keep the specified number of files, delete the rest
        files_to_keep = files[:keep_count]
        files_to_delete = files[keep_count:]
        
        return [f[0] for f in files_to_delete]
    
    def get_retention_summary(self) -> Dict[str, Any]:
        """Get summary of retention policies and backup status."""
        backup_files = self._get_backup_files()
        total_size = sum(f.stat().st_size for f in backup_files)
        
        policy_summary = {}
        for policy_name, policy in self.policies.items():
            if policy.enabled:
                # Calculate how many files would be affected by this policy
                files_to_delete = self._apply_retention_rules(backup_files, policy)
                space_to_free = sum(f.stat().st_size for f in files_to_delete)
                
                policy_summary[policy_name] = {
                    'files_to_delete': len(files_to_delete),
                    'space_to_free': space_to_free,
                    'max_backups': policy.max_backups,
                    'max_days': policy.max_days,
                    'max_size_gb': policy.max_size_gb
                }
        
        return {
            'total_backups': len(backup_files),
            'total_size': total_size,
            'policies': policy_summary
        }
    
    def dry_run(self, policy_name: str = None) -> Dict[str, Any]:
        """Perform a dry run of retention policies without deleting files."""
        if policy_name:
            policy = self.get_policy(policy_name)
            if not policy:
                return {'error': f'Policy not found: {policy_name}'}
            policies = [policy]
        else:
            policies = [p for p in self.policies.values() if p.enabled]
        
        backup_files = self._get_backup_files()
        results = {}
        
        for policy in policies:
            files_to_delete = self._apply_retention_rules(backup_files, policy)
            space_to_free = sum(f.stat().st_size for f in files_to_delete)
            
            results[policy.name] = {
                'files_to_delete': [f.name for f in files_to_delete],
                'files_count': len(files_to_delete),
                'space_to_free': space_to_free,
                'space_to_free_mb': space_to_free / (1024 * 1024)
            }
        
        return results
    
    def save_policies(self, file_path: str = "retention_policies.json"):
        """Save retention policies to file."""
        try:
            policies_data = {}
            for name, policy in self.policies.items():
                policies_data[name] = {
                    'name': policy.name,
                    'enabled': policy.enabled,
                    'max_backups': policy.max_backups,
                    'max_days': policy.max_days,
                    'max_size_gb': policy.max_size_gb,
                    'keep_daily': policy.keep_daily,
                    'keep_weekly': policy.keep_weekly,
                    'keep_monthly': policy.keep_monthly,
                    'keep_yearly': policy.keep_yearly,
                    'priority': policy.priority
                }
            
            with open(file_path, 'w') as f:
                json.dump(policies_data, f, indent=2)
            
            logger.info(f"Retention policies saved to {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save retention policies: {e}")
    
    def load_policies(self, file_path: str = "retention_policies.json"):
        """Load retention policies from file."""
        try:
            if not Path(file_path).exists():
                logger.warning(f"Retention policies file not found: {file_path}")
                return
            
            with open(file_path, 'r') as f:
                policies_data = json.load(f)
            
            # Clear existing policies
            self.policies.clear()
            
            # Load policies from file
            for name, data in policies_data.items():
                policy = RetentionPolicy(
                    name=data['name'],
                    enabled=data['enabled'],
                    max_backups=data['max_backups'],
                    max_days=data['max_days'],
                    max_size_gb=data['max_size_gb'],
                    keep_daily=data['keep_daily'],
                    keep_weekly=data['keep_weekly'],
                    keep_monthly=data['keep_monthly'],
                    keep_yearly=data['keep_yearly'],
                    priority=data['priority']
                )
                self.policies[name] = policy
            
            logger.info(f"Retention policies loaded from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to load retention policies: {e}")

def create_retention(backup_dir: str = "backups") -> BackupRetention:
    """Create and return a backup retention instance."""
    return BackupRetention(backup_dir)
