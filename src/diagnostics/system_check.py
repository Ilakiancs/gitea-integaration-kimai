#!/usr/bin/env python3
"""
System Diagnostic Checks

Comprehensive system diagnostic checks for the Gitea to Kimai integration.
"""

import os
import sys
import platform
import psutil
import sqlite3
import requests
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class SystemChecker:
    """System diagnostic checker."""
    
    def __init__(self):
        self.results = {}
        self.errors = []
        self.warnings = []
    
    def check_system_requirements(self) -> Dict[str, Any]:
        """Check system requirements."""
        results = {
            'python_version': self._check_python_version(),
            'platform': self._check_platform(),
            'memory': self._check_memory(),
            'disk_space': self._check_disk_space(),
            'network': self._check_network_connectivity(),
            'dependencies': self._check_dependencies()
        }
        
        self.results['system'] = results
        return results
    
    def check_database(self, db_path: str = "sync.db") -> Dict[str, Any]:
        """Check database status."""
        results = {
            'exists': False,
            'accessible': False,
            'tables': [],
            'size': 0,
            'integrity': False
        }
        
        try:
            db_file = Path(db_path)
            results['exists'] = db_file.exists()
            
            if results['exists']:
                results['size'] = db_file.stat().st_size
                
                # Test database connection
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Check tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                results['tables'] = [row[0] for row in cursor.fetchall()]
                
                # Check integrity
                cursor.execute("PRAGMA integrity_check;")
                integrity_result = cursor.fetchone()
                results['integrity'] = integrity_result[0] == 'ok'
                
                results['accessible'] = True
                conn.close()
                
        except Exception as e:
            self.errors.append(f"Database check failed: {e}")
        
        self.results['database'] = results
        return results
    
    def check_configuration(self, config_path: str = ".env") -> Dict[str, Any]:
        """Check configuration files."""
        results = {
            'env_exists': False,
            'env_valid': False,
            'required_vars': [],
            'missing_vars': [],
            'config_files': []
        }
        
        try:
            # Check .env file
            env_file = Path(config_path)
            results['env_exists'] = env_file.exists()
            
            if results['env_exists']:
                # Read and validate .env file
                with open(env_file, 'r') as f:
                    content = f.read()
                
                # Check for required variables
                required_vars = [
                    'GITEA_URL', 'GITEA_TOKEN', 'KIMAI_URL', 
                    'KIMAI_USERNAME', 'KIMAI_PASSWORD'
                ]
                
                for var in required_vars:
                    if var in content:
                        results['required_vars'].append(var)
                    else:
                        results['missing_vars'].append(var)
                
                results['env_valid'] = len(results['missing_vars']) == 0
            
            # Check other config files
            config_patterns = ['*.json', '*.yaml', '*.yml', 'config.*']
            for pattern in config_patterns:
                config_files = list(Path('.').glob(pattern))
                results['config_files'].extend([str(f) for f in config_files])
                
        except Exception as e:
            self.errors.append(f"Configuration check failed: {e}")
        
        self.results['configuration'] = results
        return results
    
    def check_api_connectivity(self, gitea_url: str = None, kimai_url: str = None) -> Dict[str, Any]:
        """Check API connectivity."""
        results = {
            'gitea': {'reachable': False, 'response_time': 0, 'status': 'unknown'},
            'kimai': {'reachable': False, 'response_time': 0, 'status': 'unknown'}
        }
        
        # Check Gitea
        if gitea_url:
            results['gitea'] = self._check_api_endpoint(gitea_url, 'Gitea')
        
        # Check Kimai
        if kimai_url:
            results['kimai'] = self._check_api_endpoint(kimai_url, 'Kimai')
        
        self.results['api_connectivity'] = results
        return results
    
    def check_file_permissions(self) -> Dict[str, Any]:
        """Check file permissions."""
        results = {
            'readable': [],
            'writable': [],
            'executable': [],
            'issues': []
        }
        
        try:
            # Check important files and directories
            paths_to_check = [
                '.', 'src/', 'logs/', 'backups/', 'exports/',
                'sync.db', '.env', 'requirements.txt'
            ]
            
            for path_str in paths_to_check:
                path = Path(path_str)
                if path.exists():
                    # Check read permission
                    if os.access(path, os.R_OK):
                        results['readable'].append(str(path))
                    else:
                        results['issues'].append(f"Cannot read: {path}")
                    
                    # Check write permission
                    if os.access(path, os.W_OK):
                        results['writable'].append(str(path))
                    else:
                        results['issues'].append(f"Cannot write: {path}")
                    
                    # Check execute permission (for directories)
                    if path.is_dir() and os.access(path, os.X_OK):
                        results['executable'].append(str(path))
                    elif path.is_dir():
                        results['issues'].append(f"Cannot access directory: {path}")
                        
        except Exception as e:
            self.errors.append(f"Permission check failed: {e}")
        
        self.results['permissions'] = results
        return results
    
    def check_logs(self, log_dir: str = "logs") -> Dict[str, Any]:
        """Check log files."""
        results = {
            'log_dir_exists': False,
            'log_files': [],
            'recent_errors': [],
            'log_size': 0
        }
        
        try:
            log_path = Path(log_dir)
            results['log_dir_exists'] = log_path.exists()
            
            if results['log_dir_exists']:
                log_files = list(log_path.glob("*.log"))
                results['log_files'] = [str(f) for f in log_files]
                
                # Check for recent errors
                for log_file in log_files:
                    try:
                        with open(log_file, 'r') as f:
                            content = f.read()
                            if 'ERROR' in content or 'CRITICAL' in content:
                                results['recent_errors'].append(str(log_file))
                        
                        results['log_size'] += log_file.stat().st_size
                    except Exception as e:
                        self.warnings.append(f"Cannot read log file {log_file}: {e}")
                        
        except Exception as e:
            self.errors.append(f"Log check failed: {e}")
        
        self.results['logs'] = results
        return results
    
    def run_full_diagnostic(self) -> Dict[str, Any]:
        """Run full system diagnostic."""
        logger.info("Starting full system diagnostic...")
        
        # Run all checks
        self.check_system_requirements()
        self.check_database()
        self.check_configuration()
        self.check_file_permissions()
        self.check_logs()
        
        # Add summary
        self.results['summary'] = {
            'timestamp': datetime.now().isoformat(),
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'status': 'healthy' if len(self.errors) == 0 else 'issues_found'
        }
        
        return self.results
    
    def _check_python_version(self) -> Dict[str, Any]:
        """Check Python version."""
        version = sys.version_info
        return {
            'version': f"{version.major}.{version.minor}.{version.micro}",
            'compatible': version >= (3, 7),
            'recommended': '3.8+'
        }
    
    def _check_platform(self) -> Dict[str, Any]:
        """Check platform information."""
        return {
            'system': platform.system(),
            'release': platform.release(),
            'architecture': platform.architecture()[0],
            'processor': platform.processor()
        }
    
    def _check_memory(self) -> Dict[str, Any]:
        """Check memory usage."""
        try:
            memory = psutil.virtual_memory()
            return {
                'total': memory.total,
                'available': memory.available,
                'used': memory.used,
                'percent': memory.percent,
                'sufficient': memory.available > 100 * 1024 * 1024  # 100MB
            }
        except Exception:
            return {'error': 'Cannot access memory information'}
    
    def _check_disk_space(self) -> Dict[str, Any]:
        """Check disk space."""
        try:
            disk = psutil.disk_usage('.')
            return {
                'total': disk.total,
                'free': disk.free,
                'used': disk.used,
                'percent': disk.percent,
                'sufficient': disk.free > 500 * 1024 * 1024  # 500MB
            }
        except Exception:
            return {'error': 'Cannot access disk information'}
    
    def _check_network_connectivity(self) -> Dict[str, Any]:
        """Check network connectivity."""
        try:
            # Test basic internet connectivity
            response = requests.get('https://httpbin.org/get', timeout=5)
            return {
                'internet': response.status_code == 200,
                'response_time': response.elapsed.total_seconds()
            }
        except Exception:
            return {'internet': False, 'error': 'Network connectivity test failed'}
    
    def _check_dependencies(self) -> Dict[str, Any]:
        """Check required dependencies."""
        required_packages = [
            'requests', 'python-dotenv', 'pandas', 'openpyxl', 
            'PyYAML', 'beautifulsoup4', 'argparse', 'python-dateutil'
        ]
        
        results = {}
        for package in required_packages:
            try:
                __import__(package)
                results[package] = True
            except ImportError:
                results[package] = False
                self.warnings.append(f"Missing dependency: {package}")
        
        return results
    
    def _check_api_endpoint(self, url: str, service_name: str) -> Dict[str, Any]:
        """Check API endpoint connectivity."""
        try:
            start_time = datetime.now()
            response = requests.get(url, timeout=10)
            response_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'reachable': True,
                'response_time': response_time,
                'status': response.status_code,
                'headers': dict(response.headers)
            }
        except Exception as e:
            return {
                'reachable': False,
                'response_time': 0,
                'status': 'error',
                'error': str(e)
            }

def create_system_checker() -> SystemChecker:
    """Create and return a system checker instance."""
    return SystemChecker()
