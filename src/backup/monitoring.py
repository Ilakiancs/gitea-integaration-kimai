#!/usr/bin/env python3
"""
Advanced Backup Monitoring Module

Enhanced monitoring and alerting system for backup operations with advanced metrics
collection, predictive health checks, anomaly detection, and intelligent notification capabilities.
"""

import os
import time
import logging
import threading
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from prometheus_client import Counter, Gauge, Histogram, start_http_server, Summary
import psutil
import numpy as np
from collections import deque
import requests
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class BackupMetrics:
    """Enhanced backup operation metrics."""
    total_backups: int = 0
    successful_backups: int = 0
    failed_backups: int = 0
    total_backup_size: int = 0
    total_backup_time: float = 0.0
    average_backup_time: float = 0.0
    last_backup_time: Optional[datetime] = None
    last_backup_size: int = 0
    backup_success_rate: float = 0.0
    compression_ratio: float = 0.0
    deduplication_savings: float = 0.0
    cloud_sync_status: str = "unknown"
    encryption_overhead: float = 0.0

@dataclass
class SystemMetrics:
    """Enhanced system resource metrics."""
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    disk_usage: float = 0.0
    network_io: Dict[str, float] = field(default_factory=dict)
    disk_io: Dict[str, float] = field(default_factory=dict)
    temperature: Optional[float] = None
    power_consumption: Optional[float] = None
    process_count: int = 0
    load_average: Tuple[float, float, float] = (0.0, 0.0, 0.0)

@dataclass
class HealthCheck:
    """Health check configuration and results."""
    name: str
    check_type: str  # 'system', 'backup', 'storage', 'network', 'security'
    status: str = "unknown"  # 'healthy', 'warning', 'critical', 'unknown'
    message: str = ""
    timestamp: datetime = field(default_factory=datetime.now)
    duration: float = 0.0
    threshold: float = 0.0
    current_value: float = 0.0
    trend: str = "stable"  # 'improving', 'stable', 'degrading'

@dataclass
class AlertRule:
    """Enhanced alert rule configuration."""
    name: str
    condition: str  # 'backup_failure', 'disk_space', 'cpu_high', 'memory_high', 'anomaly'
    threshold: float
    duration: int = 300  # seconds
    enabled: bool = True
    severity: str = "warning"  # 'info', 'warning', 'critical'
    notification_channels: List[str] = field(default_factory=list)
    cooldown_period: int = 3600  # seconds
    last_triggered: Optional[datetime] = None

@dataclass
class AnomalyDetection:
    """Anomaly detection configuration."""
    enabled: bool = True
    window_size: int = 100  # Number of data points for baseline
    sensitivity: float = 2.0  # Standard deviations for anomaly detection
    metrics: List[str] = field(default_factory=lambda: ['cpu_usage', 'memory_usage', 'backup_duration'])

class AdvancedBackupMonitor:
    """Advanced monitor for backup operations and system health."""
    
    def __init__(self, metrics_port: int = 8000, enable_prometheus: bool = True, 
                 db_path: str = "monitoring.db"):
        self.metrics_port = metrics_port
        self.enable_prometheus = enable_prometheus
        self.db_path = Path(db_path)
        self.backup_metrics = BackupMetrics()
        self.system_metrics = SystemMetrics()
        self.alert_rules: List[AlertRule] = []
        self.health_checks: List[HealthCheck] = []
        self.notification_handlers: Dict[str, Callable] = {}
        self.anomaly_detection = AnomalyDetection()
        self.monitoring_thread = None
        self.running = False
        self.lock = threading.RLock()
        
        # Historical data for anomaly detection
        self.historical_data: Dict[str, deque] = {
            'cpu_usage': deque(maxlen=self.anomaly_detection.window_size),
            'memory_usage': deque(maxlen=self.anomaly_detection.window_size),
            'backup_duration': deque(maxlen=self.anomaly_detection.window_size),
            'backup_size': deque(maxlen=self.anomaly_detection.window_size)
        }
        
        # Initialize database
        self._init_database()
        
        # Initialize Prometheus metrics
        if self.enable_prometheus:
            self._init_prometheus_metrics()
            start_http_server(self.metrics_port)
            logger.info(f"Prometheus metrics server started on port {self.metrics_port}")
        
        # Setup default health checks
        self._setup_default_health_checks()
    
    def _init_database(self):
        """Initialize monitoring database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Metrics history table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS metrics_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp REAL,
                        metric_name TEXT,
                        metric_value REAL,
                        metric_type TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Health checks history
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS health_checks_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        check_name TEXT,
                        status TEXT,
                        message TEXT,
                        duration REAL,
                        timestamp REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Alerts history
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS alerts_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        rule_name TEXT,
                        severity TEXT,
                        message TEXT,
                        triggered_at REAL,
                        resolved_at REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Anomalies history
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS anomalies_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        metric_name TEXT,
                        anomaly_value REAL,
                        baseline_mean REAL,
                        baseline_std REAL,
                        severity TEXT,
                        timestamp REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize monitoring database: {e}")
    
    def _init_prometheus_metrics(self):
        """Initialize enhanced Prometheus metrics."""
        self.backup_counter = Counter('backup_total', 'Total number of backups', ['status', 'type'])
        self.backup_duration = Histogram('backup_duration_seconds', 'Backup duration in seconds', ['type'])
        self.backup_size = Histogram('backup_size_bytes', 'Backup size in bytes', ['type'])
        self.backup_compression_ratio = Gauge('backup_compression_ratio', 'Backup compression ratio')
        self.backup_deduplication_savings = Gauge('backup_deduplication_savings', 'Deduplication savings percentage')
        
        self.system_cpu = Gauge('system_cpu_usage_percent', 'CPU usage percentage')
        self.system_memory = Gauge('system_memory_usage_percent', 'Memory usage percentage')
        self.system_disk = Gauge('system_disk_usage_percent', 'Disk usage percentage')
        self.system_temperature = Gauge('system_temperature_celsius', 'System temperature')
        self.system_load = Gauge('system_load_average', 'System load average', ['period'])
        
        self.backup_success_rate = Gauge('backup_success_rate', 'Backup success rate')
        self.health_check_status = Gauge('health_check_status', 'Health check status', ['check_name'])
        self.anomaly_detected = Counter('anomaly_detected_total', 'Number of anomalies detected', ['metric_name'])
        
        # New metrics
        self.backup_encryption_overhead = Gauge('backup_encryption_overhead', 'Encryption overhead percentage')
        self.cloud_sync_status = Gauge('cloud_sync_status', 'Cloud sync status', ['status'])
        self.storage_utilization = Gauge('storage_utilization_bytes', 'Storage utilization', ['type'])
    
    def _setup_default_health_checks(self):
        """Setup default health checks."""
        default_checks = [
            HealthCheck("CPU Usage", "system", threshold=80.0),
            HealthCheck("Memory Usage", "system", threshold=85.0),
            HealthCheck("Disk Usage", "system", threshold=90.0),
            HealthCheck("Backup Success Rate", "backup", threshold=95.0),
            HealthCheck("Backup Duration", "backup", threshold=3600.0),  # 1 hour
            HealthCheck("Network Connectivity", "network", threshold=0.0),
            HealthCheck("Storage Integrity", "storage", threshold=0.0),
            HealthCheck("Security Status", "security", threshold=0.0)
        ]
        
        for check in default_checks:
            self.add_health_check(check)
    
    def add_health_check(self, health_check: HealthCheck):
        """Add a health check."""
        with self.lock:
            self.health_checks.append(health_check)
            logger.info(f"Added health check: {health_check.name}")
    
    def remove_health_check(self, check_name: str):
        """Remove a health check."""
        with self.lock:
            self.health_checks = [check for check in self.health_checks if check.name != check_name]
            logger.info(f"Removed health check: {check_name}")
    
    def run_health_checks(self) -> List[HealthCheck]:
        """Run all health checks and return results."""
        results = []
        
        for check in self.health_checks:
            start_time = time.time()
            
            try:
                if check.check_type == "system":
                    result = self._run_system_health_check(check)
                elif check.check_type == "backup":
                    result = self._run_backup_health_check(check)
                elif check.check_type == "network":
                    result = self._run_network_health_check(check)
                elif check.check_type == "storage":
                    result = self._run_storage_health_check(check)
                elif check.check_type == "security":
                    result = self._run_security_health_check(check)
                else:
                    result = check
                    result.status = "unknown"
                    result.message = f"Unknown check type: {check.check_type}"
                
                result.duration = time.time() - start_time
                result.timestamp = datetime.now()
                
                # Update Prometheus metric
                if self.enable_prometheus:
                    status_value = 1 if result.status == "healthy" else 0
                    self.health_check_status.labels(check_name=result.name).set(status_value)
                
                # Store in database
                self._store_health_check_result(result)
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Health check {check.name} failed: {e}")
                check.status = "critical"
                check.message = f"Health check failed: {e}"
                check.duration = time.time() - start_time
                check.timestamp = datetime.now()
                results.append(check)
        
        return results
    
    def _run_system_health_check(self, check: HealthCheck) -> HealthCheck:
        """Run system health check."""
        if check.name == "CPU Usage":
            cpu_percent = psutil.cpu_percent(interval=1)
            check.current_value = cpu_percent
            check.status = "healthy" if cpu_percent < check.threshold else "warning"
            check.message = f"CPU usage: {cpu_percent:.1f}%"
            
        elif check.name == "Memory Usage":
            memory = psutil.virtual_memory()
            check.current_value = memory.percent
            check.status = "healthy" if memory.percent < check.threshold else "warning"
            check.message = f"Memory usage: {memory.percent:.1f}%"
            
        elif check.name == "Disk Usage":
            disk = psutil.disk_usage('/')
            check.current_value = disk.percent
            check.status = "healthy" if disk.percent < check.threshold else "warning"
            check.message = f"Disk usage: {disk.percent:.1f}%"
        
        return check
    
    def _run_backup_health_check(self, check: HealthCheck) -> HealthCheck:
        """Run backup health check."""
        if check.name == "Backup Success Rate":
            success_rate = self.backup_metrics.backup_success_rate * 100
            check.current_value = success_rate
            check.status = "healthy" if success_rate >= check.threshold else "warning"
            check.message = f"Backup success rate: {success_rate:.1f}%"
            
        elif check.name == "Backup Duration":
            avg_duration = self.backup_metrics.average_backup_time
            check.current_value = avg_duration
            check.status = "healthy" if avg_duration <= check.threshold else "warning"
            check.message = f"Average backup duration: {avg_duration:.1f}s"
        
        return check
    
    def _run_network_health_check(self, check: HealthCheck) -> HealthCheck:
        """Run network health check."""
        try:
            # Test connectivity to common services
            test_urls = [
                "https://www.google.com",
                "https://www.cloudflare.com",
                "https://httpbin.org/status/200"
            ]
            
            successful_tests = 0
            for url in test_urls:
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code == 200:
                        successful_tests += 1
                except:
                    pass
            
            connectivity_rate = (successful_tests / len(test_urls)) * 100
            check.current_value = connectivity_rate
            check.status = "healthy" if connectivity_rate > 50 else "critical"
            check.message = f"Network connectivity: {connectivity_rate:.1f}%"
            
        except Exception as e:
            check.status = "critical"
            check.message = f"Network check failed: {e}"
        
        return check
    
    def _run_storage_health_check(self, check: HealthCheck) -> HealthCheck:
        """Run storage health check."""
        try:
            # Check disk health using smartctl if available
            import subprocess
            
            try:
                result = subprocess.run(['smartctl', '-H', '/dev/sda'], 
                                      capture_output=True, text=True, timeout=10)
                if 'PASSED' in result.stdout:
                    check.status = "healthy"
                    check.message = "Storage health: PASSED"
                else:
                    check.status = "warning"
                    check.message = "Storage health: Check recommended"
            except (subprocess.TimeoutExpired, FileNotFoundError):
                # Fallback to basic disk check
                disk = psutil.disk_usage('/')
                if disk.percent < 90:
                    check.status = "healthy"
                    check.message = f"Storage available: {100 - disk.percent:.1f}%"
                else:
                    check.status = "critical"
                    check.message = f"Storage critical: {disk.percent:.1f}% used"
                    
        except Exception as e:
            check.status = "critical"
            check.message = f"Storage check failed: {e}"
        
        return check
    
    def _run_security_health_check(self, check: HealthCheck) -> HealthCheck:
        """Run security health check."""
        try:
            # Check for common security issues
            security_score = 100
            
            # Check file permissions
            backup_dir = Path("backups")
            if backup_dir.exists():
                stat = backup_dir.stat()
                if stat.st_mode & 0o777 != 0o700:  # Should be 700
                    security_score -= 20
                    check.message = "Backup directory permissions too open"
            
            # Check for encryption
            if self.backup_metrics.encryption_overhead > 0:
                security_score += 10
            else:
                security_score -= 30
                check.message = "No encryption detected"
            
            check.current_value = security_score
            if security_score >= 80:
                check.status = "healthy"
            elif security_score >= 60:
                check.status = "warning"
            else:
                check.status = "critical"
            
            if not check.message:
                check.message = f"Security score: {security_score}/100"
                
        except Exception as e:
            check.status = "critical"
            check.message = f"Security check failed: {e}"
        
        return check
    
    def detect_anomalies(self) -> List[Dict[str, Any]]:
        """Detect anomalies in monitored metrics."""
        anomalies = []
        
        if not self.anomaly_detection.enabled:
            return anomalies
        
        for metric_name in self.anomaly_detection.metrics:
            if metric_name in self.historical_data and len(self.historical_data[metric_name]) >= 10:
                data = list(self.historical_data[metric_name])
                mean = np.mean(data)
                std = np.std(data)
                
                if std > 0:
                    # Get current value
                    current_value = self._get_current_metric_value(metric_name)
                    if current_value is not None:
                        z_score = abs(current_value - mean) / std
                        
                        if z_score > self.anomaly_detection.sensitivity:
                            anomaly = {
                                'metric_name': metric_name,
                                'current_value': current_value,
                                'baseline_mean': mean,
                                'baseline_std': std,
                                'z_score': z_score,
                                'severity': 'high' if z_score > 3 else 'medium',
                                'timestamp': datetime.now()
                            }
                            
                            anomalies.append(anomaly)
                            
                            # Update Prometheus metric
                            if self.enable_prometheus:
                                self.anomaly_detected.labels(metric_name=metric_name).inc()
                            
                            # Store in database
                            self._store_anomaly(anomaly)
        
        return anomalies
    
    def _get_current_metric_value(self, metric_name: str) -> Optional[float]:
        """Get current value for a metric."""
        if metric_name == 'cpu_usage':
            return psutil.cpu_percent()
        elif metric_name == 'memory_usage':
            return psutil.virtual_memory().percent
        elif metric_name == 'backup_duration':
            return self.backup_metrics.average_backup_time
        elif metric_name == 'backup_size':
            return self.backup_metrics.last_backup_size
        return None
    
    def update_metrics(self, backup_metrics: Optional[BackupMetrics] = None, 
                      system_metrics: Optional[SystemMetrics] = None):
        """Update metrics with new data."""
        with self.lock:
            if backup_metrics:
                self.backup_metrics = backup_metrics
            
            if system_metrics:
                self.system_metrics = system_metrics
            else:
                # Collect current system metrics
                self.system_metrics = self._collect_system_metrics()
            
            # Update historical data
            self._update_historical_data()
            
            # Update Prometheus metrics
            if self.enable_prometheus:
                self._update_prometheus_metrics()
    
    def _collect_system_metrics(self) -> SystemMetrics:
        """Collect current system metrics."""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # Network I/O
            net_io = psutil.net_io_counters()
            network_io = {
                'bytes_sent': net_io.bytes_sent,
                'bytes_recv': net_io.bytes_recv,
                'packets_sent': net_io.packets_sent,
                'packets_recv': net_io.packets_recv
            }
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            disk_io_dict = {
                'read_bytes': disk_io.read_bytes if disk_io else 0,
                'write_bytes': disk_io.write_bytes if disk_io else 0,
                'read_count': disk_io.read_count if disk_io else 0,
                'write_count': disk_io.write_count if disk_io else 0
            }
            
            # Load average
            load_avg = psutil.getloadavg()
            
            # Process count
            process_count = len(psutil.pids())
            
            return SystemMetrics(
                cpu_usage=cpu_percent,
                memory_usage=memory.percent,
                disk_usage=disk.percent,
                network_io=network_io,
                disk_io=disk_io_dict,
                load_average=load_avg,
                process_count=process_count
            )
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
            return SystemMetrics()
    
    def _update_historical_data(self):
        """Update historical data for anomaly detection."""
        timestamp = time.time()
        
        # Add current metrics to historical data
        self.historical_data['cpu_usage'].append(self.system_metrics.cpu_usage)
        self.historical_data['memory_usage'].append(self.system_metrics.memory_usage)
        self.historical_data['backup_duration'].append(self.backup_metrics.average_backup_time)
        self.historical_data['backup_size'].append(self.backup_metrics.last_backup_size)
        
        # Store in database
        self._store_metrics_history(timestamp)
    
    def _update_prometheus_metrics(self):
        """Update Prometheus metrics."""
        try:
            # System metrics
            self.system_cpu.set(self.system_metrics.cpu_usage)
            self.system_memory.set(self.system_metrics.memory_usage)
            self.system_disk.set(self.system_metrics.disk_usage)
            
            # Load average
            for i, load in enumerate(self.system_metrics.load_average):
                self.system_load.labels(period=f"{i+1}m").set(load)
            
            # Backup metrics
            self.backup_success_rate.set(self.backup_metrics.backup_success_rate)
            self.backup_compression_ratio.set(self.backup_metrics.compression_ratio)
            self.backup_deduplication_savings.set(self.backup_metrics.deduplication_savings)
            self.backup_encryption_overhead.set(self.backup_metrics.encryption_overhead)
            
            # Cloud sync status
            status_value = 1 if self.backup_metrics.cloud_sync_status == "synced" else 0
            self.cloud_sync_status.labels(status=self.backup_metrics.cloud_sync_status).set(status_value)
            
        except Exception as e:
            logger.error(f"Failed to update Prometheus metrics: {e}")
    
    def _store_metrics_history(self, timestamp: float):
        """Store metrics history in database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                metrics_to_store = [
                    ('cpu_usage', self.system_metrics.cpu_usage, 'system'),
                    ('memory_usage', self.system_metrics.memory_usage, 'system'),
                    ('disk_usage', self.system_metrics.disk_usage, 'system'),
                    ('backup_success_rate', self.backup_metrics.backup_success_rate, 'backup'),
                    ('backup_duration', self.backup_metrics.average_backup_time, 'backup'),
                    ('backup_size', self.backup_metrics.last_backup_size, 'backup')
                ]
                
                for metric_name, metric_value, metric_type in metrics_to_store:
                    conn.execute("""
                        INSERT INTO metrics_history (timestamp, metric_name, metric_value, metric_type)
                        VALUES (?, ?, ?, ?)
                    """, (timestamp, metric_name, metric_value, metric_type))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to store metrics history: {e}")
    
    def _store_health_check_result(self, result: HealthCheck):
        """Store health check result in database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO health_checks_history 
                    (check_name, status, message, duration, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                """, (result.name, result.status, result.message, result.duration, result.timestamp.timestamp()))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to store health check result: {e}")
    
    def _store_anomaly(self, anomaly: Dict[str, Any]):
        """Store anomaly in database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO anomalies_history 
                    (metric_name, anomaly_value, baseline_mean, baseline_std, severity, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    anomaly['metric_name'],
                    anomaly['current_value'],
                    anomaly['baseline_mean'],
                    anomaly['baseline_std'],
                    anomaly['severity'],
                    anomaly['timestamp'].timestamp()
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to store anomaly: {e}")
