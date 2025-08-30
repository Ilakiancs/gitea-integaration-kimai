#!/usr/bin/env python3
"""
Backup Monitoring Module

Monitoring and alerting system for backup operations with metrics
collection, health checks, and notification capabilities.
"""

import os
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import psutil

logger = logging.getLogger(__name__)

@dataclass
class BackupMetrics:
    """Backup operation metrics."""
    total_backups: int = 0
    successful_backups: int = 0
    failed_backups: int = 0
    total_backup_size: int = 0
    total_backup_time: float = 0.0
    average_backup_time: float = 0.0
    last_backup_time: Optional[datetime] = None
    last_backup_size: int = 0
    backup_success_rate: float = 0.0

@dataclass
class SystemMetrics:
    """System resource metrics."""
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    disk_usage: float = 0.0
    network_io: Dict[str, float] = field(default_factory=dict)
    disk_io: Dict[str, float] = field(default_factory=dict)

@dataclass
class AlertRule:
    """Alert rule configuration."""
    name: str
    condition: str  # 'backup_failure', 'disk_space', 'cpu_high', 'memory_high'
    threshold: float
    duration: int = 300  # seconds
    enabled: bool = True
    notification_channels: List[str] = field(default_factory=list)

class BackupMonitor:
    """Monitor for backup operations and system health."""
    
    def __init__(self, metrics_port: int = 8000, enable_prometheus: bool = True):
        self.metrics_port = metrics_port
        self.enable_prometheus = enable_prometheus
        self.backup_metrics = BackupMetrics()
        self.system_metrics = SystemMetrics()
        self.alert_rules: List[AlertRule] = []
        self.notification_handlers: Dict[str, Callable] = {}
        self.monitoring_thread = None
        self.running = False
        self.lock = threading.RLock()
        
        # Initialize Prometheus metrics
        if self.enable_prometheus:
            self._init_prometheus_metrics()
            start_http_server(self.metrics_port)
            logger.info(f"Prometheus metrics server started on port {self.metrics_port}")
    
    def _init_prometheus_metrics(self):
        """Initialize Prometheus metrics."""
        self.backup_counter = Counter('backup_total', 'Total number of backups', ['status'])
        self.backup_duration = Histogram('backup_duration_seconds', 'Backup duration in seconds')
        self.backup_size = Histogram('backup_size_bytes', 'Backup size in bytes')
        self.system_cpu = Gauge('system_cpu_usage_percent', 'CPU usage percentage')
        self.system_memory = Gauge('system_memory_usage_percent', 'Memory usage percentage')
        self.system_disk = Gauge('system_disk_usage_percent', 'Disk usage percentage')
        self.backup_success_rate = Gauge('backup_success_rate', 'Backup success rate')
    
    def start_monitoring(self):
        """Start the monitoring system."""
        if self.running:
            logger.warning("Monitoring is already running")
            return
        
        self.running = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        logger.info("Backup monitoring started")
    
    def stop_monitoring(self):
        """Stop the monitoring system."""
        self.running = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        logger.info("Backup monitoring stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                # Collect system metrics
                self._collect_system_metrics()
                
                # Check alert rules
                self._check_alerts()
                
                # Update Prometheus metrics
                if self.enable_prometheus:
                    self._update_prometheus_metrics()
                
                time.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                time.sleep(60)
    
    def _collect_system_metrics(self):
        """Collect system resource metrics."""
        try:
            # CPU usage
            self.system_metrics.cpu_usage = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            self.system_metrics.memory_usage = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            self.system_metrics.disk_usage = (disk.used / disk.total) * 100
            
            # Network I/O
            net_io = psutil.net_io_counters()
            self.system_metrics.network_io = {
                'bytes_sent': net_io.bytes_sent,
                'bytes_recv': net_io.bytes_recv,
                'packets_sent': net_io.packets_sent,
                'packets_recv': net_io.packets_recv
            }
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            if disk_io:
                self.system_metrics.disk_io = {
                    'read_bytes': disk_io.read_bytes,
                    'write_bytes': disk_io.write_bytes,
                    'read_count': disk_io.read_count,
                    'write_count': disk_io.write_count
                }
                
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
    
    def _check_alerts(self):
        """Check alert rules and trigger notifications."""
        for rule in self.alert_rules:
            if not rule.enabled:
                continue
            
            try:
                if rule.condition == 'backup_failure':
                    if self.backup_metrics.failed_backups > rule.threshold:
                        self._trigger_alert(rule, f"Backup failures exceeded threshold: {self.backup_metrics.failed_backups}")
                
                elif rule.condition == 'disk_space':
                    if self.system_metrics.disk_usage > rule.threshold:
                        self._trigger_alert(rule, f"Disk usage exceeded threshold: {self.system_metrics.disk_usage:.1f}%")
                
                elif rule.condition == 'cpu_high':
                    if self.system_metrics.cpu_usage > rule.threshold:
                        self._trigger_alert(rule, f"CPU usage exceeded threshold: {self.system_metrics.cpu_usage:.1f}%")
                
                elif rule.condition == 'memory_high':
                    if self.system_metrics.memory_usage > rule.threshold:
                        self._trigger_alert(rule, f"Memory usage exceeded threshold: {self.system_metrics.memory_usage:.1f}%")
                
            except Exception as e:
                logger.error(f"Error checking alert rule {rule.name}: {e}")
    
    def _trigger_alert(self, rule: AlertRule, message: str):
        """Trigger an alert and send notifications."""
        logger.warning(f"Alert triggered: {rule.name} - {message}")
        
        for channel in rule.notification_channels:
            if channel in self.notification_handlers:
                try:
                    self.notification_handlers[channel](rule.name, message)
                except Exception as e:
                    logger.error(f"Failed to send notification via {channel}: {e}")
    
    def _update_prometheus_metrics(self):
        """Update Prometheus metrics."""
        try:
            self.system_cpu.set(self.system_metrics.cpu_usage)
            self.system_memory.set(self.system_metrics.memory_usage)
            self.system_disk.set(self.system_metrics.disk_usage)
            self.backup_success_rate.set(self.backup_metrics.backup_success_rate)
        except Exception as e:
            logger.error(f"Failed to update Prometheus metrics: {e}")
    
    def record_backup_operation(self, success: bool, size: int, duration: float):
        """Record a backup operation."""
        with self.lock:
            self.backup_metrics.total_backups += 1
            
            if success:
                self.backup_metrics.successful_backups += 1
                if self.enable_prometheus:
                    self.backup_counter.labels(status='success').inc()
            else:
                self.backup_metrics.failed_backups += 1
                if self.enable_prometheus:
                    self.backup_counter.labels(status='failure').inc()
            
            self.backup_metrics.total_backup_size += size
            self.backup_metrics.total_backup_time += duration
            self.backup_metrics.last_backup_time = datetime.now()
            self.backup_metrics.last_backup_size = size
            
            # Calculate averages
            if self.backup_metrics.total_backups > 0:
                self.backup_metrics.average_backup_time = (
                    self.backup_metrics.total_backup_time / self.backup_metrics.total_backups
                )
                self.backup_metrics.backup_success_rate = (
                    self.backup_metrics.successful_backups / self.backup_metrics.total_backups
                )
            
            # Update Prometheus metrics
            if self.enable_prometheus:
                self.backup_duration.observe(duration)
                self.backup_size.observe(size)
    
    def add_alert_rule(self, rule: AlertRule):
        """Add an alert rule."""
        with self.lock:
            self.alert_rules.append(rule)
            logger.info(f"Added alert rule: {rule.name}")
    
    def remove_alert_rule(self, rule_name: str) -> bool:
        """Remove an alert rule."""
        with self.lock:
            for i, rule in enumerate(self.alert_rules):
                if rule.name == rule_name:
                    del self.alert_rules[i]
                    logger.info(f"Removed alert rule: {rule_name}")
                    return True
            return False
    
    def register_notification_handler(self, channel: str, handler: Callable):
        """Register a notification handler."""
        self.notification_handlers[channel] = handler
        logger.info(f"Registered notification handler for channel: {channel}")
    
    def get_backup_metrics(self) -> Dict[str, Any]:
        """Get backup metrics."""
        with self.lock:
            return {
                'total_backups': self.backup_metrics.total_backups,
                'successful_backups': self.backup_metrics.successful_backups,
                'failed_backups': self.backup_metrics.failed_backups,
                'total_backup_size': self.backup_metrics.total_backup_size,
                'average_backup_time': self.backup_metrics.average_backup_time,
                'last_backup_time': self.backup_metrics.last_backup_time.isoformat() if self.backup_metrics.last_backup_time else None,
                'last_backup_size': self.backup_metrics.last_backup_size,
                'backup_success_rate': self.backup_metrics.backup_success_rate
            }
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get system metrics."""
        return {
            'cpu_usage': self.system_metrics.cpu_usage,
            'memory_usage': self.system_metrics.memory_usage,
            'disk_usage': self.system_metrics.disk_usage,
            'network_io': self.system_metrics.network_io,
            'disk_io': self.system_metrics.disk_io
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        health_status = {
            'status': 'healthy',
            'backup_health': 'good',
            'system_health': 'good',
            'issues': []
        }
        
        # Check backup health
        if self.backup_metrics.backup_success_rate < 0.9:
            health_status['backup_health'] = 'warning'
            health_status['issues'].append('Low backup success rate')
        
        if self.backup_metrics.failed_backups > 5:
            health_status['backup_health'] = 'critical'
            health_status['issues'].append('Multiple backup failures')
        
        # Check system health
        if self.system_metrics.disk_usage > 90:
            health_status['system_health'] = 'critical'
            health_status['issues'].append('High disk usage')
        elif self.system_metrics.disk_usage > 80:
            health_status['system_health'] = 'warning'
            health_status['issues'].append('Elevated disk usage')
        
        if self.system_metrics.cpu_usage > 90:
            health_status['system_health'] = 'critical'
            health_status['issues'].append('High CPU usage')
        elif self.system_metrics.cpu_usage > 80:
            health_status['system_health'] = 'warning'
            health_status['issues'].append('Elevated CPU usage')
        
        if self.system_metrics.memory_usage > 90:
            health_status['system_health'] = 'critical'
            health_status['issues'].append('High memory usage')
        elif self.system_metrics.memory_usage > 80:
            health_status['system_health'] = 'warning'
            health_status['issues'].append('Elevated memory usage')
        
        # Overall status
        if health_status['backup_health'] == 'critical' or health_status['system_health'] == 'critical':
            health_status['status'] = 'critical'
        elif health_status['backup_health'] == 'warning' or health_status['system_health'] == 'warning':
            health_status['status'] = 'warning'
        
        return health_status

def create_monitor(metrics_port: int = 8000, enable_prometheus: bool = True) -> BackupMonitor:
    """Create and return a backup monitor instance."""
    return BackupMonitor(metrics_port, enable_prometheus)
