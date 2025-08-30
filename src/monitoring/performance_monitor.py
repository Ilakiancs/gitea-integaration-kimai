#!/usr/bin/env python3
"""
Performance Monitoring System

Provides comprehensive performance monitoring for the sync system,
tracking system metrics, API performance, and providing real-time monitoring.
"""

import os
import json
import logging
import threading
import time
import psutil
import sqlite3
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
import statistics
from collections import deque, defaultdict

logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"

class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class Metric:
    """Represents a performance metric."""
    name: str
    value: Union[int, float]
    metric_type: MetricType
    timestamp: datetime
    labels: Dict[str, str] = None
    description: str = ""

@dataclass
class Alert:
    """Represents a performance alert."""
    id: str
    name: str
    message: str
    severity: AlertSeverity
    timestamp: datetime
    metric_name: str
    threshold: Union[int, float]
    current_value: Union[int, float]
    resolved: bool = False
    resolved_at: Optional[datetime] = None

@dataclass
class PerformanceConfig:
    """Performance monitoring configuration."""
    enabled: bool = True
    collection_interval: int = 60  # seconds
    retention_days: int = 30
    alerting_enabled: bool = True
    metrics_enabled: bool = True
    system_metrics: bool = True
    api_metrics: bool = True
    custom_metrics: bool = True

class SystemMetricsCollector:
    """Collects system-level metrics."""
    
    def __init__(self):
        self.last_cpu_times = psutil.cpu_times()
        self.last_cpu_time = time.time()
    
    def collect_cpu_metrics(self) -> Dict[str, Metric]:
        """Collect CPU-related metrics."""
        metrics = {}
        
        # CPU usage percentage
        cpu_percent = psutil.cpu_percent(interval=1)
        metrics['cpu_usage_percent'] = Metric(
            name='cpu_usage_percent',
            value=cpu_percent,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='CPU usage percentage'
        )
        
        # CPU times
        cpu_times = psutil.cpu_times()
        current_time = time.time()
        time_delta = current_time - self.last_cpu_time
        
        if time_delta > 0:
            cpu_percent_by_type = {}
            for key in cpu_times._fields:
                current = getattr(cpu_times, key)
                last = getattr(self.last_cpu_times, key)
                percent = ((current - last) / time_delta) * 100
                cpu_percent_by_type[key] = percent
                
                metrics[f'cpu_{key}_percent'] = Metric(
                    name=f'cpu_{key}_percent',
                    value=percent,
                    metric_type=MetricType.GAUGE,
                    timestamp=datetime.now(),
                    description=f'CPU {key} usage percentage'
                )
        
        self.last_cpu_times = cpu_times
        self.last_cpu_time = current_time
        
        return metrics
    
    def collect_memory_metrics(self) -> Dict[str, Metric]:
        """Collect memory-related metrics."""
        metrics = {}
        
        memory = psutil.virtual_memory()
        
        metrics['memory_total_bytes'] = Metric(
            name='memory_total_bytes',
            value=memory.total,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Total memory in bytes'
        )
        
        metrics['memory_available_bytes'] = Metric(
            name='memory_available_bytes',
            value=memory.available,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Available memory in bytes'
        )
        
        metrics['memory_used_bytes'] = Metric(
            name='memory_used_bytes',
            value=memory.used,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Used memory in bytes'
        )
        
        metrics['memory_usage_percent'] = Metric(
            name='memory_usage_percent',
            value=memory.percent,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Memory usage percentage'
        )
        
        return metrics
    
    def collect_disk_metrics(self) -> Dict[str, Metric]:
        """Collect disk-related metrics."""
        metrics = {}
        
        disk_usage = psutil.disk_usage('/')
        
        metrics['disk_total_bytes'] = Metric(
            name='disk_total_bytes',
            value=disk_usage.total,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Total disk space in bytes'
        )
        
        metrics['disk_used_bytes'] = Metric(
            name='disk_used_bytes',
            value=disk_usage.used,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Used disk space in bytes'
        )
        
        metrics['disk_free_bytes'] = Metric(
            name='disk_free_bytes',
            value=disk_usage.free,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Free disk space in bytes'
        )
        
        metrics['disk_usage_percent'] = Metric(
            name='disk_usage_percent',
            value=(disk_usage.used / disk_usage.total) * 100,
            metric_type=MetricType.GAUGE,
            timestamp=datetime.now(),
            description='Disk usage percentage'
        )
        
        return metrics
    
    def collect_network_metrics(self) -> Dict[str, Metric]:
        """Collect network-related metrics."""
        metrics = {}
        
        network_io = psutil.net_io_counters()
        
        metrics['network_bytes_sent'] = Metric(
            name='network_bytes_sent',
            value=network_io.bytes_sent,
            metric_type=MetricType.COUNTER,
            timestamp=datetime.now(),
            description='Total bytes sent'
        )
        
        metrics['network_bytes_recv'] = Metric(
            name='network_bytes_recv',
            value=network_io.bytes_recv,
            metric_type=MetricType.COUNTER,
            timestamp=datetime.now(),
            description='Total bytes received'
        )
        
        metrics['network_packets_sent'] = Metric(
            name='network_packets_sent',
            value=network_io.packets_sent,
            metric_type=MetricType.COUNTER,
            timestamp=datetime.now(),
            description='Total packets sent'
        )
        
        metrics['network_packets_recv'] = Metric(
            name='network_packets_recv',
            value=network_io.packets_recv,
            metric_type=MetricType.COUNTER,
            timestamp=datetime.now(),
            description='Total packets received'
        )
        
        return metrics
    
    def collect_all_metrics(self) -> Dict[str, Metric]:
        """Collect all system metrics."""
        metrics = {}
        
        try:
            metrics.update(self.collect_cpu_metrics())
        except Exception as e:
            logger.error(f"Failed to collect CPU metrics: {e}")
        
        try:
            metrics.update(self.collect_memory_metrics())
        except Exception as e:
            logger.error(f"Failed to collect memory metrics: {e}")
        
        try:
            metrics.update(self.collect_disk_metrics())
        except Exception as e:
            logger.error(f"Failed to collect disk metrics: {e}")
        
        try:
            metrics.update(self.collect_network_metrics())
        except Exception as e:
            logger.error(f"Failed to collect network metrics: {e}")
        
        return metrics

class APIMetricsCollector:
    """Collects API performance metrics."""
    
    def __init__(self):
        self.request_times = defaultdict(list)
        self.request_counts = defaultdict(int)
        self.error_counts = defaultdict(int)
        self.response_sizes = defaultdict(list)
    
    def record_request(self, endpoint: str, method: str, duration: float, 
                      status_code: int, response_size: int = 0):
        """Record an API request."""
        key = f"{method}_{endpoint}"
        
        # Record request time
        self.request_times[key].append(duration)
        if len(self.request_times[key]) > 1000:  # Keep last 1000 requests
            self.request_times[key] = self.request_times[key][-1000:]
        
        # Record request count
        self.request_counts[key] += 1
        
        # Record error count
        if status_code >= 400:
            self.error_counts[key] += 1
        
        # Record response size
        if response_size > 0:
            self.response_sizes[key].append(response_size)
            if len(self.response_sizes[key]) > 1000:
                self.response_sizes[key] = self.response_sizes[key][-1000:]
    
    def collect_metrics(self) -> Dict[str, Metric]:
        """Collect API metrics."""
        metrics = {}
        timestamp = datetime.now()
        
        for key in self.request_counts:
            # Request count
            metrics[f'api_requests_total_{key}'] = Metric(
                name=f'api_requests_total_{key}',
                value=self.request_counts[key],
                metric_type=MetricType.COUNTER,
                timestamp=timestamp,
                labels={'endpoint': key},
                description=f'Total requests for {key}'
            )
            
            # Error count
            error_count = self.error_counts.get(key, 0)
            metrics[f'api_errors_total_{key}'] = Metric(
                name=f'api_errors_total_{key}',
                value=error_count,
                metric_type=MetricType.COUNTER,
                timestamp=timestamp,
                labels={'endpoint': key},
                description=f'Total errors for {key}'
            )
            
            # Response time statistics
            if self.request_times[key]:
                times = self.request_times[key]
                metrics[f'api_response_time_avg_{key}'] = Metric(
                    name=f'api_response_time_avg_{key}',
                    value=statistics.mean(times),
                    metric_type=MetricType.GAUGE,
                    timestamp=timestamp,
                    labels={'endpoint': key},
                    description=f'Average response time for {key}'
                )
                
                metrics[f'api_response_time_p95_{key}'] = Metric(
                    name=f'api_response_time_p95_{key}',
                    value=statistics.quantiles(times, n=20)[18] if len(times) >= 20 else max(times),
                    metric_type=MetricType.GAUGE,
                    timestamp=timestamp,
                    labels={'endpoint': key},
                    description=f'95th percentile response time for {key}'
                )
                
                metrics[f'api_response_time_max_{key}'] = Metric(
                    name=f'api_response_time_max_{key}',
                    value=max(times),
                    metric_type=MetricType.GAUGE,
                    timestamp=timestamp,
                    labels={'endpoint': key},
                    description=f'Maximum response time for {key}'
                )
            
            # Response size statistics
            if self.response_sizes[key]:
                sizes = self.response_sizes[key]
                metrics[f'api_response_size_avg_{key}'] = Metric(
                    name=f'api_response_size_avg_{key}',
                    value=statistics.mean(sizes),
                    metric_type=MetricType.GAUGE,
                    timestamp=timestamp,
                    labels={'endpoint': key},
                    description=f'Average response size for {key}'
                )
        
        return metrics

class PerformanceDatabase:
    """Manages performance metrics storage."""
    
    def __init__(self, db_path: str = "performance.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize the performance database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value REAL NOT NULL,
                    metric_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    labels TEXT,
                    description TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    message TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    threshold REAL NOT NULL,
                    current_value REAL NOT NULL,
                    resolved INTEGER DEFAULT 0,
                    resolved_at TEXT
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_name_timestamp 
                ON metrics(name, timestamp)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_alerts_timestamp 
                ON alerts(timestamp)
            """)
            
            conn.commit()
    
    def save_metric(self, metric: Metric):
        """Save a metric to the database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO metrics (name, value, metric_type, timestamp, labels, description)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                metric.name,
                metric.value,
                metric.metric_type.value,
                metric.timestamp.isoformat(),
                json.dumps(metric.labels) if metric.labels else None,
                metric.description
            ))
            conn.commit()
    
    def save_metrics(self, metrics: List[Metric]):
        """Save multiple metrics to the database."""
        with sqlite3.connect(self.db_path) as conn:
            for metric in metrics:
                conn.execute("""
                    INSERT INTO metrics (name, value, metric_type, timestamp, labels, description)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    metric.name,
                    metric.value,
                    metric.metric_type.value,
                    metric.timestamp.isoformat(),
                    json.dumps(metric.labels) if metric.labels else None,
                    metric.description
                ))
            conn.commit()
    
    def get_metrics(self, name: str, start_time: datetime, end_time: datetime) -> List[Metric]:
        """Get metrics by name and time range."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT name, value, metric_type, timestamp, labels, description
                FROM metrics 
                WHERE name = ? AND timestamp BETWEEN ? AND ?
                ORDER BY timestamp
            """, (name, start_time.isoformat(), end_time.isoformat()))
            
            metrics = []
            for row in cursor.fetchall():
                metrics.append(Metric(
                    name=row[0],
                    value=row[1],
                    metric_type=MetricType(row[2]),
                    timestamp=datetime.fromisoformat(row[3]),
                    labels=json.loads(row[4]) if row[4] else None,
                    description=row[5]
                ))
            
            return metrics
    
    def save_alert(self, alert: Alert):
        """Save an alert to the database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO alerts 
                (id, name, message, severity, timestamp, metric_name, threshold, current_value, resolved, resolved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert.id,
                alert.name,
                alert.message,
                alert.severity.value,
                alert.timestamp.isoformat(),
                alert.metric_name,
                alert.threshold,
                alert.current_value,
                1 if alert.resolved else 0,
                alert.resolved_at.isoformat() if alert.resolved_at else None
            ))
            conn.commit()
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active (unresolved) alerts."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT id, name, message, severity, timestamp, metric_name, threshold, current_value, resolved, resolved_at
                FROM alerts 
                WHERE resolved = 0
                ORDER BY timestamp DESC
            """)
            
            alerts = []
            for row in cursor.fetchall():
                alerts.append(Alert(
                    id=row[0],
                    name=row[1],
                    message=row[2],
                    severity=AlertSeverity(row[3]),
                    timestamp=datetime.fromisoformat(row[4]),
                    metric_name=row[5],
                    threshold=row[6],
                    current_value=row[7],
                    resolved=bool(row[8]),
                    resolved_at=datetime.fromisoformat(row[9]) if row[9] else None
                ))
            
            return alerts
    
    def cleanup_old_metrics(self, days: int):
        """Clean up metrics older than specified days."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                DELETE FROM metrics WHERE timestamp < ?
            """, (cutoff_date.isoformat(),))
            conn.commit()

class AlertManager:
    """Manages performance alerts."""
    
    def __init__(self, database: PerformanceDatabase):
        self.database = database
        self.alert_rules: Dict[str, Dict[str, Any]] = {}
        self._setup_default_alerts()
    
    def _setup_default_alerts(self):
        """Setup default alert rules."""
        self.add_alert_rule(
            name="high_cpu_usage",
            metric_name="cpu_usage_percent",
            threshold=80.0,
            severity=AlertSeverity.WARNING,
            message="CPU usage is high"
        )
        
        self.add_alert_rule(
            name="critical_cpu_usage",
            metric_name="cpu_usage_percent",
            threshold=95.0,
            severity=AlertSeverity.CRITICAL,
            message="CPU usage is critical"
        )
        
        self.add_alert_rule(
            name="high_memory_usage",
            metric_name="memory_usage_percent",
            threshold=85.0,
            severity=AlertSeverity.WARNING,
            message="Memory usage is high"
        )
        
        self.add_alert_rule(
            name="critical_memory_usage",
            metric_name="memory_usage_percent",
            threshold=95.0,
            severity=AlertSeverity.CRITICAL,
            message="Memory usage is critical"
        )
        
        self.add_alert_rule(
            name="high_disk_usage",
            metric_name="disk_usage_percent",
            threshold=90.0,
            severity=AlertSeverity.WARNING,
            message="Disk usage is high"
        )
        
        self.add_alert_rule(
            name="critical_disk_usage",
            metric_name="disk_usage_percent",
            threshold=95.0,
            severity=AlertSeverity.CRITICAL,
            message="Disk usage is critical"
        )
    
    def add_alert_rule(self, name: str, metric_name: str, threshold: Union[int, float],
                      severity: AlertSeverity, message: str, operator: str = ">"):
        """Add an alert rule."""
        self.alert_rules[name] = {
            'metric_name': metric_name,
            'threshold': threshold,
            'severity': severity,
            'message': message,
            'operator': operator
        }
    
    def check_alerts(self, metrics: Dict[str, Metric]):
        """Check metrics against alert rules."""
        for rule_name, rule in self.alert_rules.items():
            metric_name = rule['metric_name']
            
            if metric_name in metrics:
                metric = metrics[metric_name]
                threshold = rule['threshold']
                operator = rule['operator']
                
                should_alert = False
                if operator == ">":
                    should_alert = metric.value > threshold
                elif operator == ">=":
                    should_alert = metric.value >= threshold
                elif operator == "<":
                    should_alert = metric.value < threshold
                elif operator == "<=":
                    should_alert = metric.value <= threshold
                elif operator == "==":
                    should_alert = metric.value == threshold
                
                if should_alert:
                    self._create_alert(rule_name, rule, metric)
    
    def _create_alert(self, rule_name: str, rule: Dict[str, Any], metric: Metric):
        """Create an alert."""
        alert = Alert(
            id=f"alert_{int(time.time() * 1000)}",
            name=rule_name,
            message=rule['message'],
            severity=rule['severity'],
            timestamp=datetime.now(),
            metric_name=metric.name,
            threshold=rule['threshold'],
            current_value=metric.value
        )
        
        self.database.save_alert(alert)
        logger.warning(f"Alert triggered: {alert.message} (Value: {alert.current_value}, Threshold: {alert.threshold})")

class PerformanceMonitor:
    """Main performance monitoring system."""
    
    def __init__(self, config: PerformanceConfig):
        self.config = config
        self.database = PerformanceDatabase()
        self.system_collector = SystemMetricsCollector()
        self.api_collector = APIMetricsCollector()
        self.alert_manager = AlertManager(self.database)
        
        self.running = False
        self.monitor_thread = None
        self.custom_metrics: Dict[str, Callable] = {}
    
    def start(self):
        """Start the performance monitor."""
        if not self.running:
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            logger.info("Performance monitor started")
    
    def stop(self):
        """Stop the performance monitor."""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("Performance monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                self._collect_and_store_metrics()
                time.sleep(self.config.collection_interval)
                
            except Exception as e:
                logger.error(f"Error in performance monitor loop: {e}")
                time.sleep(60)  # Wait before retrying
    
    def _collect_and_store_metrics(self):
        """Collect and store all metrics."""
        all_metrics = {}
        
        # Collect system metrics
        if self.config.system_metrics:
            try:
                system_metrics = self.system_collector.collect_all_metrics()
                all_metrics.update(system_metrics)
            except Exception as e:
                logger.error(f"Failed to collect system metrics: {e}")
        
        # Collect API metrics
        if self.config.api_metrics:
            try:
                api_metrics = self.api_collector.collect_metrics()
                all_metrics.update(api_metrics)
            except Exception as e:
                logger.error(f"Failed to collect API metrics: {e}")
        
        # Collect custom metrics
        if self.config.custom_metrics:
            try:
                custom_metrics = self._collect_custom_metrics()
                all_metrics.update(custom_metrics)
            except Exception as e:
                logger.error(f"Failed to collect custom metrics: {e}")
        
        # Store metrics
        if all_metrics:
            self.database.save_metrics(list(all_metrics.values()))
            
            # Check for alerts
            if self.config.alerting_enabled:
                self.alert_manager.check_alerts(all_metrics)
    
    def _collect_custom_metrics(self) -> Dict[str, Metric]:
        """Collect custom metrics."""
        metrics = {}
        timestamp = datetime.now()
        
        for name, collector_func in self.custom_metrics.items():
            try:
                value = collector_func()
                metrics[name] = Metric(
                    name=name,
                    value=value,
                    metric_type=MetricType.GAUGE,
                    timestamp=timestamp,
                    description=f'Custom metric: {name}'
                )
            except Exception as e:
                logger.error(f"Failed to collect custom metric {name}: {e}")
        
        return metrics
    
    def record_api_request(self, endpoint: str, method: str, duration: float, 
                          status_code: int, response_size: int = 0):
        """Record an API request for metrics."""
        self.api_collector.record_request(endpoint, method, duration, status_code, response_size)
    
    def add_custom_metric(self, name: str, collector_func: Callable[[], Union[int, float]]):
        """Add a custom metric collector."""
        self.custom_metrics[name] = collector_func
    
    def get_metrics(self, name: str, hours: int = 24) -> List[Metric]:
        """Get metrics for the last N hours."""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        return self.database.get_metrics(name, start_time, end_time)
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        return self.database.get_active_alerts()
    
    def get_system_summary(self) -> Dict[str, Any]:
        """Get a summary of current system metrics."""
        summary = {}
        
        # Get latest metrics for key system indicators
        key_metrics = [
            'cpu_usage_percent',
            'memory_usage_percent',
            'disk_usage_percent',
            'memory_available_bytes'
        ]
        
        for metric_name in key_metrics:
            metrics = self.get_metrics(metric_name, hours=1)
            if metrics:
                summary[metric_name] = {
                    'current': metrics[-1].value,
                    'average': statistics.mean([m.value for m in metrics]),
                    'max': max([m.value for m in metrics]),
                    'min': min([m.value for m in metrics])
                }
        
        # Get active alerts
        summary['active_alerts'] = len(self.get_active_alerts())
        
        return summary
    
    def cleanup_old_data(self):
        """Clean up old metrics data."""
        self.database.cleanup_old_metrics(self.config.retention_days)

def create_performance_monitor(config_file: str = "performance_config.json") -> PerformanceMonitor:
    """Create performance monitor from configuration."""
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)
    else:
        config_data = {
            'enabled': True,
            'collection_interval': 60,
            'retention_days': 30,
            'alerting_enabled': True,
            'metrics_enabled': True,
            'system_metrics': True,
            'api_metrics': True,
            'custom_metrics': True
        }
    
    config = PerformanceConfig(**config_data)
    return PerformanceMonitor(config)

if __name__ == "__main__":
    # Example usage
    monitor = create_performance_monitor()
    monitor.start()
    
    try:
        # Add a custom metric
        monitor.add_custom_metric('custom_counter', lambda: time.time() % 100)
        
        # Simulate some API requests
        for i in range(10):
            monitor.record_api_request('/api/test', 'GET', 0.1 + (i * 0.01), 200, 1024)
            time.sleep(1)
        
        # Get system summary
        summary = monitor.get_system_summary()
        print("System Summary:", json.dumps(summary, indent=2, default=str))
        
        time.sleep(60)  # Let it run for a minute
        
    finally:
        monitor.stop()
