#!/usr/bin/env python3
"""
Metrics Collection Module

Tracks performance metrics, sync statistics, and operational data
for monitoring and optimization purposes.
"""

import time
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import sqlite3
import threading

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collects and stores various metrics for sync operations."""
    
    def __init__(self, db_path: str = "metrics.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()
        
    def init_database(self):
        """Initialize metrics database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
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
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    endpoint TEXT,
                    method TEXT,
                    duration REAL,
                    status_code INTEGER,
                    success BOOLEAN,
                    retry_count INTEGER
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    metric_name TEXT,
                    metric_value REAL,
                    tags TEXT
                )
            """)
            
            conn.commit()
    
    def record_sync_operation(self, operation: str, repository: str, duration: float, 
                            success: bool, items_processed: int = 0, items_synced: int = 0, 
                            errors_count: int = 0, details: Optional[Dict] = None):
        """Record a sync operation metric."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO sync_metrics 
                    (operation, repository, duration, success, items_processed, 
                     items_synced, errors_count, details)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    operation, repository, duration, success, items_processed,
                    items_synced, errors_count, json.dumps(details) if details else None
                ))
                conn.commit()
    
    def record_api_call(self, endpoint: str, method: str, duration: float, 
                       status_code: int, success: bool, retry_count: int = 0):
        """Record an API call metric."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO api_metrics 
                    (endpoint, method, duration, status_code, success, retry_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (endpoint, method, duration, status_code, success, retry_count))
                conn.commit()
    
    def record_performance_metric(self, metric_name: str, metric_value: float, 
                                tags: Optional[Dict] = None):
        """Record a performance metric."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO performance_metrics (metric_name, metric_value, tags)
                    VALUES (?, ?, ?)
                """, (metric_name, metric_value, json.dumps(tags) if tags else None))
                conn.commit()
    
    def get_sync_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get sync statistics for the last N days."""
        with sqlite3.connect(self.db_path) as conn:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Overall statistics
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                    AVG(duration) as avg_duration,
                    SUM(items_processed) as total_items_processed,
                    SUM(items_synced) as total_items_synced,
                    SUM(errors_count) as total_errors
                FROM sync_metrics 
                WHERE timestamp >= ?
            """, (cutoff_date,))
            
            overall_stats = cursor.fetchone()
            
            # Repository breakdown
            cursor = conn.execute("""
                SELECT 
                    repository,
                    COUNT(*) as operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(duration) as avg_duration,
                    SUM(items_synced) as items_synced
                FROM sync_metrics 
                WHERE timestamp >= ?
                GROUP BY repository
                ORDER BY items_synced DESC
            """, (cutoff_date,))
            
            repo_stats = cursor.fetchall()
            
            # Operation type breakdown
            cursor = conn.execute("""
                SELECT 
                    operation,
                    COUNT(*) as count,
                    AVG(duration) as avg_duration,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful
                FROM sync_metrics 
                WHERE timestamp >= ?
                GROUP BY operation
            """, (cutoff_date,))
            
            operation_stats = cursor.fetchall()
            
        return {
            'overall': {
                'total_operations': overall_stats[0] or 0,
                'successful_operations': overall_stats[1] or 0,
                'success_rate': (overall_stats[1] or 0) / max(overall_stats[0] or 1, 1) * 100,
                'avg_duration': overall_stats[2] or 0,
                'total_items_processed': overall_stats[3] or 0,
                'total_items_synced': overall_stats[4] or 0,
                'total_errors': overall_stats[5] or 0
            },
            'repositories': [
                {
                    'repository': row[0],
                    'operations': row[1],
                    'successful': row[2],
                    'success_rate': row[2] / max(row[1], 1) * 100,
                    'avg_duration': row[3] or 0,
                    'items_synced': row[4] or 0
                }
                for row in repo_stats
            ],
            'operations': [
                {
                    'operation': row[0],
                    'count': row[1],
                    'avg_duration': row[2] or 0,
                    'successful': row[3],
                    'success_rate': row[3] / max(row[1], 1) * 100
                }
                for row in operation_stats
            ]
        }
    
    def get_api_statistics(self, days: int = 7) -> Dict[str, Any]:
        """Get API call statistics for the last N days."""
        with sqlite3.connect(self.db_path) as conn:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Overall API stats
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_calls,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_calls,
                    AVG(duration) as avg_duration,
                    AVG(retry_count) as avg_retries,
                    COUNT(DISTINCT endpoint) as unique_endpoints
                FROM api_metrics 
                WHERE timestamp >= ?
            """, (cutoff_date,))
            
            overall_stats = cursor.fetchone()
            
            # Endpoint breakdown
            cursor = conn.execute("""
                SELECT 
                    endpoint,
                    method,
                    COUNT(*) as calls,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(duration) as avg_duration,
                    AVG(retry_count) as avg_retries
                FROM api_metrics 
                WHERE timestamp >= ?
                GROUP BY endpoint, method
                ORDER BY calls DESC
            """, (cutoff_date,))
            
            endpoint_stats = cursor.fetchall()
            
            # Status code distribution
            cursor = conn.execute("""
                SELECT 
                    status_code,
                    COUNT(*) as count
                FROM api_metrics 
                WHERE timestamp >= ?
                GROUP BY status_code
                ORDER BY count DESC
            """, (cutoff_date,))
            
            status_stats = cursor.fetchall()
            
        return {
            'overall': {
                'total_calls': overall_stats[0] or 0,
                'successful_calls': overall_stats[1] or 0,
                'success_rate': (overall_stats[1] or 0) / max(overall_stats[0] or 1, 1) * 100,
                'avg_duration': overall_stats[2] or 0,
                'avg_retries': overall_stats[3] or 0,
                'unique_endpoints': overall_stats[4] or 0
            },
            'endpoints': [
                {
                    'endpoint': row[0],
                    'method': row[1],
                    'calls': row[2],
                    'successful': row[3],
                    'success_rate': row[3] / max(row[2], 1) * 100,
                    'avg_duration': row[4] or 0,
                    'avg_retries': row[5] or 0
                }
                for row in endpoint_stats
            ],
            'status_codes': [
                {
                    'status_code': row[0],
                    'count': row[1]
                }
                for row in status_stats
            ]
        }
    
    def export_metrics(self, output_file: str, days: int = 30):
        """Export metrics to JSON file."""
        sync_stats = self.get_sync_statistics(days)
        api_stats = self.get_api_statistics(days)
        
        export_data = {
            'export_date': datetime.now().isoformat(),
            'period_days': days,
            'sync_statistics': sync_stats,
            'api_statistics': api_stats
        }
        
        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"Metrics exported to {output_file}")
    
    def cleanup_old_metrics(self, days_to_keep: int = 90):
        """Clean up old metrics data."""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM sync_metrics WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM api_metrics WHERE timestamp < ?", (cutoff_date,))
            conn.execute("DELETE FROM performance_metrics WHERE timestamp < ?", (cutoff_date,))
            conn.commit()
        
        logger.info(f"Cleaned up metrics older than {days_to_keep} days")

class MetricsDecorator:
    """Decorator for automatically collecting metrics."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
    
    def track_sync_operation(self, operation_name: str):
        """Decorator to track sync operations."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = time.time()
                success = False
                items_processed = 0
                items_synced = 0
                errors_count = 0
                details = {}
                
                try:
                    result = func(*args, **kwargs)
                    success = True
                    
                    # Try to extract metrics from result
                    if isinstance(result, dict):
                        items_processed = result.get('processed', 0)
                        items_synced = result.get('synced', 0)
                        errors_count = result.get('errors', 0)
                        details = result.get('details', {})
                    
                    return result
                except Exception as e:
                    errors_count = 1
                    details['error'] = str(e)
                    raise
                finally:
                    duration = time.time() - start_time
                    repository = kwargs.get('repository', 'unknown')
                    
                    self.metrics.record_sync_operation(
                        operation_name, repository, duration, success,
                        items_processed, items_synced, errors_count, details
                    )
            
            return wrapper
        return decorator
    
    def track_api_call(self, endpoint: str, method: str = "GET"):
        """Decorator to track API calls."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = time.time()
                retry_count = 0
                
                try:
                    result = func(*args, **kwargs)
                    
                    # Handle requests.Response objects
                    if hasattr(result, 'status_code'):
                        status_code = result.status_code
                        success = 200 <= status_code < 400
                    else:
                        status_code = 200
                        success = True
                    
                    return result
                except Exception as e:
                    status_code = 500
                    success = False
                    raise
                finally:
                    duration = time.time() - start_time
                    
                    self.metrics.record_api_call(
                        endpoint, method, duration, status_code, success, retry_count
                    )
            
            return wrapper
        return decorator
