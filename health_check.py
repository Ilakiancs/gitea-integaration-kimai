#!/usr/bin/env python3
"""
Health Check Module

Provides comprehensive health monitoring for the sync system,
including service availability, database status, and performance metrics.
"""

import time
import logging
import requests
import sqlite3
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import threading
import json

logger = logging.getLogger(__name__)

@dataclass
class HealthStatus:
    """Represents the health status of a component."""
    name: str
    status: str  # 'healthy', 'warning', 'critical', 'unknown'
    message: str
    timestamp: datetime
    response_time: Optional[float] = None
    details: Optional[Dict] = None

@dataclass
class SystemHealth:
    """Overall system health status."""
    overall_status: str
    components: List[HealthStatus]
    timestamp: datetime
    uptime: float
    version: str

class HealthChecker:
    """Base class for health checkers."""
    
    def __init__(self, name: str):
        self.name = name
        self.timeout = 30
    
    def check(self) -> HealthStatus:
        """Perform health check and return status."""
        start_time = time.time()
        
        try:
            result = self._perform_check()
            response_time = time.time() - start_time
            
            return HealthStatus(
                name=self.name,
                status=result['status'],
                message=result['message'],
                timestamp=datetime.now(),
                response_time=response_time,
                details=result.get('details')
            )
        except Exception as e:
            response_time = time.time() - start_time
            logger.error(f"Health check failed for {self.name}: {e}")
            
            return HealthStatus(
                name=self.name,
                status='critical',
                message=f"Check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time=response_time
            )
    
    def _perform_check(self) -> Dict[str, Any]:
        """Override this method to implement specific health checks."""
        raise NotImplementedError

class GiteaHealthChecker(HealthChecker):
    """Health checker for Gitea API."""
    
    def __init__(self, gitea_url: str, gitea_token: str):
        super().__init__("Gitea API")
        self.gitea_url = gitea_url.rstrip('/')
        self.gitea_token = gitea_token
        self.headers = {
            'Authorization': f'token {gitea_token}',
            'Content-Type': 'application/json'
        }
    
    def _perform_check(self) -> Dict[str, Any]:
        """Check Gitea API health."""
        # Check API version endpoint
        version_url = f"{self.gitea_url}/api/v1/version"
        response = requests.get(version_url, headers=self.headers, timeout=self.timeout)
        
        if response.status_code == 200:
            version_data = response.json()
            return {
                'status': 'healthy',
                'message': f"Gitea API is healthy (version: {version_data.get('version', 'unknown')})",
                'details': {
                    'version': version_data.get('version'),
                    'status_code': response.status_code
                }
            }
        elif response.status_code == 401:
            return {
                'status': 'critical',
                'message': 'Gitea API authentication failed',
                'details': {'status_code': response.status_code}
            }
        else:
            return {
                'status': 'warning',
                'message': f"Gitea API returned status {response.status_code}",
                'details': {'status_code': response.status_code}
            }

class KimaiHealthChecker(HealthChecker):
    """Health checker for Kimai API."""
    
    def __init__(self, kimai_url: str, kimai_username: str, kimai_password: str):
        super().__init__("Kimai API")
        self.kimai_url = kimai_url.rstrip('/')
        self.kimai_username = kimai_username
        self.kimai_password = kimai_password
    
    def _perform_check(self) -> Dict[str, Any]:
        """Check Kimai API health."""
        # Check API version endpoint
        version_url = f"{self.kimai_url}/api/version"
        
        try:
            response = requests.get(version_url, timeout=self.timeout)
            
            if response.status_code == 200:
                version_data = response.json()
                return {
                    'status': 'healthy',
                    'message': f"Kimai API is healthy (version: {version_data.get('version', 'unknown')})",
                    'details': {
                        'version': version_data.get('version'),
                        'status_code': response.status_code
                    }
                }
            else:
                return {
                    'status': 'warning',
                    'message': f"Kimai API returned status {response.status_code}",
                    'details': {'status_code': response.status_code}
                }
        except requests.exceptions.RequestException as e:
            return {
                'status': 'critical',
                'message': f"Kimai API connection failed: {str(e)}",
                'details': {'error': str(e)}
            }

class DatabaseHealthChecker(HealthChecker):
    """Health checker for SQLite database."""
    
    def __init__(self, db_path: str):
        super().__init__("Database")
        self.db_path = db_path
    
    def _perform_check(self) -> Dict[str, Any]:
        """Check database health."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if database is accessible
                cursor = conn.execute("SELECT sqlite_version()")
                version = cursor.fetchone()[0]
                
                # Check sync_metrics table
                cursor = conn.execute("SELECT COUNT(*) FROM sync_metrics")
                metrics_count = cursor.fetchone()[0]
                
                # Check database size
                cursor = conn.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor = conn.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                db_size_mb = (page_count * page_size) / (1024 * 1024)
                
                return {
                    'status': 'healthy',
                    'message': f"Database is healthy (SQLite {version})",
                    'details': {
                        'sqlite_version': version,
                        'metrics_count': metrics_count,
                        'database_size_mb': round(db_size_mb, 2)
                    }
                }
        except sqlite3.Error as e:
            return {
                'status': 'critical',
                'message': f"Database error: {str(e)}",
                'details': {'error': str(e)}
            }

class DiskSpaceHealthChecker(HealthChecker):
    """Health checker for disk space."""
    
    def __init__(self, path: str = ".", warning_threshold: float = 0.8, critical_threshold: float = 0.95):
        super().__init__("Disk Space")
        self.path = path
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
    
    def _perform_check(self) -> Dict[str, Any]:
        """Check disk space usage."""
        try:
            import shutil
            total, used, free = shutil.disk_usage(self.path)
            usage_percent = used / total
            
            if usage_percent >= self.critical_threshold:
                status = 'critical'
                message = f"Disk space critical: {usage_percent:.1%} used"
            elif usage_percent >= self.warning_threshold:
                status = 'warning'
                message = f"Disk space warning: {usage_percent:.1%} used"
            else:
                status = 'healthy'
                message = f"Disk space healthy: {usage_percent:.1%} used"
            
            return {
                'status': status,
                'message': message,
                'details': {
                    'total_gb': round(total / (1024**3), 2),
                    'used_gb': round(used / (1024**3), 2),
                    'free_gb': round(free / (1024**3), 2),
                    'usage_percent': round(usage_percent * 100, 1)
                }
            }
        except Exception as e:
            return {
                'status': 'unknown',
                'message': f"Could not check disk space: {str(e)}",
                'details': {'error': str(e)}
            }

class MemoryHealthChecker(HealthChecker):
    """Health checker for memory usage."""
    
    def __init__(self, warning_threshold: float = 0.8, critical_threshold: float = 0.95):
        super().__init__("Memory Usage")
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
    
    def _perform_check(self) -> Dict[str, Any]:
        """Check memory usage."""
        try:
            import psutil
            memory = psutil.virtual_memory()
            usage_percent = memory.percent / 100
            
            if usage_percent >= self.critical_threshold:
                status = 'critical'
                message = f"Memory usage critical: {memory.percent:.1f}%"
            elif usage_percent >= self.warning_threshold:
                status = 'warning'
                message = f"Memory usage warning: {memory.percent:.1f}%"
            else:
                status = 'healthy'
                message = f"Memory usage healthy: {memory.percent:.1f}%"
            
            return {
                'status': status,
                'message': message,
                'details': {
                    'total_gb': round(memory.total / (1024**3), 2),
                    'available_gb': round(memory.available / (1024**3), 2),
                    'used_gb': round(memory.used / (1024**3), 2),
                    'usage_percent': memory.percent
                }
            }
        except ImportError:
            return {
                'status': 'unknown',
                'message': 'psutil not available for memory monitoring',
                'details': {'error': 'psutil not installed'}
            }
        except Exception as e:
            return {
                'status': 'unknown',
                'message': f"Could not check memory usage: {str(e)}",
                'details': {'error': str(e)}
            }

class HealthMonitor:
    """Main health monitoring system."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.checkers: List[HealthChecker] = []
        self.health_history: List[SystemHealth] = []
        self.max_history_size = 100
        self.lock = threading.Lock()
        self._setup_checkers()
    
    def _setup_checkers(self):
        """Setup health checkers based on configuration."""
        # Gitea checker
        if 'gitea' in self.config:
            gitea_config = self.config['gitea']
            self.checkers.append(GiteaHealthChecker(
                gitea_config['url'],
                gitea_config['token']
            ))
        
        # Kimai checker
        if 'kimai' in self.config:
            kimai_config = self.config['kimai']
            self.checkers.append(KimaiHealthChecker(
                kimai_config['url'],
                kimai_config['username'],
                kimai_config['password']
            ))
        
        # Database checker
        if 'database' in self.config:
            db_config = self.config['database']
            self.checkers.append(DatabaseHealthChecker(db_config['path']))
        
        # System checkers
        self.checkers.append(DiskSpaceHealthChecker())
        self.checkers.append(MemoryHealthChecker())
    
    def run_health_check(self) -> SystemHealth:
        """Run all health checks and return overall status."""
        start_time = time.time()
        components = []
        
        # Run all health checks
        for checker in self.checkers:
            try:
                status = checker.check()
                components.append(status)
            except Exception as e:
                logger.error(f"Health check failed for {checker.name}: {e}")
                components.append(HealthStatus(
                    name=checker.name,
                    status='unknown',
                    message=f"Check failed: {str(e)}",
                    timestamp=datetime.now()
                ))
        
        # Determine overall status
        status_counts = {}
        for component in components:
            status_counts[component.status] = status_counts.get(component.status, 0) + 1
        
        if status_counts.get('critical', 0) > 0:
            overall_status = 'critical'
        elif status_counts.get('warning', 0) > 0:
            overall_status = 'warning'
        elif status_counts.get('healthy', 0) > 0:
            overall_status = 'healthy'
        else:
            overall_status = 'unknown'
        
        system_health = SystemHealth(
            overall_status=overall_status,
            components=components,
            timestamp=datetime.now(),
            uptime=time.time() - start_time,
            version="1.0.0"
        )
        
        # Store in history
        with self.lock:
            self.health_history.append(system_health)
            if len(self.health_history) > self.max_history_size:
                self.health_history.pop(0)
        
        return system_health
    
    def get_health_summary(self) -> Dict[str, Any]:
        """Get a summary of recent health checks."""
        with self.lock:
            if not self.health_history:
                return {'status': 'unknown', 'message': 'No health checks performed'}
            
            latest = self.health_history[-1]
            
            # Count statuses
            status_counts = {}
            for component in latest.components:
                status_counts[component.status] = status_counts.get(component.status, 0) + 1
            
            return {
                'overall_status': latest.overall_status,
                'timestamp': latest.timestamp.isoformat(),
                'component_count': len(latest.components),
                'status_breakdown': status_counts,
                'components': [
                    {
                        'name': c.name,
                        'status': c.status,
                        'message': c.message,
                        'response_time': c.response_time
                    }
                    for c in latest.components
                ]
            }
    
    def get_health_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get health check history for the specified time period."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        with self.lock:
            recent_checks = [
                check for check in self.health_history
                if check.timestamp >= cutoff_time
            ]
        
        return [
            {
                'timestamp': check.timestamp.isoformat(),
                'overall_status': check.overall_status,
                'component_count': len(check.components),
                'uptime': check.uptime
            }
            for check in recent_checks
        ]
    
    def export_health_report(self, file_path: str, hours: int = 24):
        """Export health check report to JSON file."""
        report = {
            'generated_at': datetime.now().isoformat(),
            'period_hours': hours,
            'summary': self.get_health_summary(),
            'history': self.get_health_history(hours)
        }
        
        with open(file_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Health report exported to {file_path}")
    
    def add_custom_checker(self, checker: HealthChecker):
        """Add a custom health checker."""
        self.checkers.append(checker)
    
    def remove_checker(self, name: str):
        """Remove a health checker by name."""
        self.checkers = [c for c in self.checkers if c.name != name]

def create_health_monitor_from_env() -> HealthMonitor:
    """Create health monitor from environment variables."""
    import os
    
    config = {}
    
    # Gitea config
    if os.getenv('GITEA_URL') and os.getenv('GITEA_TOKEN'):
        config['gitea'] = {
            'url': os.getenv('GITEA_URL'),
            'token': os.getenv('GITEA_TOKEN')
        }
    
    # Kimai config
    if os.getenv('KIMAI_URL') and os.getenv('KIMAI_USERNAME') and os.getenv('KIMAI_PASSWORD'):
        config['kimai'] = {
            'url': os.getenv('KIMAI_URL'),
            'username': os.getenv('KIMAI_USERNAME'),
            'password': os.getenv('KIMAI_PASSWORD')
        }
    
    # Database config
    config['database'] = {
        'path': os.getenv('DATABASE_PATH', 'sync.db')
    }
    
    return HealthMonitor(config)
