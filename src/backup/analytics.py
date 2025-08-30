#!/usr/bin/env python3
"""
Backup Analytics Module

Analytics and reporting system for backup performance analysis,
trends, and insights.
"""

import os
import logging
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import threading
from collections import defaultdict
import statistics

logger = logging.getLogger(__name__)

@dataclass
class BackupMetrics:
    """Backup performance metrics."""
    backup_id: str
    timestamp: datetime
    duration: float
    size: int
    compression_ratio: float
    success: bool
    error_message: Optional[str] = None

@dataclass
class AnalyticsReport:
    """Analytics report data."""
    period_start: datetime
    period_end: datetime
    total_backups: int
    successful_backups: int
    failed_backups: int
    success_rate: float
    average_duration: float
    average_size: float
    total_size: int
    average_compression_ratio: float
    trends: Dict[str, Any]
    recommendations: List[str]

class BackupAnalytics:
    """Analytics system for backup performance analysis."""
    
    def __init__(self, db_path: str = "backup_analytics.db"):
        self.db_path = Path(db_path)
        self.lock = threading.RLock()
        self._init_database()
    
    def _init_database(self):
        """Initialize analytics database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS backup_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backup_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        duration REAL NOT NULL,
                        size INTEGER NOT NULL,
                        compression_ratio REAL NOT NULL,
                        success BOOLEAN NOT NULL,
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        cpu_usage REAL NOT NULL,
                        memory_usage REAL NOT NULL,
                        disk_usage REAL NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize analytics database: {e}")
    
    def record_backup_metrics(self, metrics: BackupMetrics):
        """Record backup metrics in the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO backup_metrics 
                    (backup_id, timestamp, duration, size, compression_ratio, success, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.backup_id,
                    metrics.timestamp.isoformat(),
                    metrics.duration,
                    metrics.size,
                    metrics.compression_ratio,
                    metrics.success,
                    metrics.error_message
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to record backup metrics: {e}")
    
    def record_system_metrics(self, cpu_usage: float, memory_usage: float, disk_usage: float):
        """Record system metrics in the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO system_metrics (timestamp, cpu_usage, memory_usage, disk_usage)
                    VALUES (?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(),
                    cpu_usage,
                    memory_usage,
                    disk_usage
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to record system metrics: {e}")
    
    def get_backup_metrics(self, start_date: datetime = None, end_date: datetime = None) -> List[BackupMetrics]:
        """Get backup metrics for a date range."""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
        if end_date is None:
            end_date = datetime.now()
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT backup_id, timestamp, duration, size, compression_ratio, success, error_message
                    FROM backup_metrics
                    WHERE timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                """, (start_date.isoformat(), end_date.isoformat()))
                
                metrics = []
                for row in cursor.fetchall():
                    metrics.append(BackupMetrics(
                        backup_id=row[0],
                        timestamp=datetime.fromisoformat(row[1]),
                        duration=row[2],
                        size=row[3],
                        compression_ratio=row[4],
                        success=bool(row[5]),
                        error_message=row[6]
                    ))
                
                return metrics
                
        except Exception as e:
            logger.error(f"Failed to get backup metrics: {e}")
            return []
    
    def generate_report(self, period_days: int = 30) -> AnalyticsReport:
        """Generate analytics report for the specified period."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        metrics = self.get_backup_metrics(start_date, end_date)
        
        if not metrics:
            return AnalyticsReport(
                period_start=start_date,
                period_end=end_date,
                total_backups=0,
                successful_backups=0,
                failed_backups=0,
                success_rate=0.0,
                average_duration=0.0,
                average_size=0.0,
                total_size=0,
                average_compression_ratio=0.0,
                trends={},
                recommendations=[]
            )
        
        # Calculate basic statistics
        total_backups = len(metrics)
        successful_backups = sum(1 for m in metrics if m.success)
        failed_backups = total_backups - successful_backups
        success_rate = successful_backups / total_backups if total_backups > 0 else 0.0
        
        successful_metrics = [m for m in metrics if m.success]
        
        if successful_metrics:
            average_duration = statistics.mean(m.duration for m in successful_metrics)
            average_size = statistics.mean(m.size for m in successful_metrics)
            total_size = sum(m.size for m in successful_metrics)
            average_compression_ratio = statistics.mean(m.compression_ratio for m in successful_metrics)
        else:
            average_duration = 0.0
            average_size = 0.0
            total_size = 0
            average_compression_ratio = 0.0
        
        # Analyze trends
        trends = self._analyze_trends(metrics)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(metrics, trends)
        
        return AnalyticsReport(
            period_start=start_date,
            period_end=end_date,
            total_backups=total_backups,
            successful_backups=successful_backups,
            failed_backups=failed_backups,
            success_rate=success_rate,
            average_duration=average_duration,
            average_size=average_size,
            total_size=total_size,
            average_compression_ratio=average_compression_ratio,
            trends=trends,
            recommendations=recommendations
        )
    
    def _analyze_trends(self, metrics: List[BackupMetrics]) -> Dict[str, Any]:
        """Analyze trends in backup metrics."""
        if not metrics:
            return {}
        
        # Group by day
        daily_stats = defaultdict(list)
        for metric in metrics:
            day = metric.timestamp.date()
            daily_stats[day].append(metric)
        
        # Calculate daily averages
        daily_averages = {}
        for day, day_metrics in daily_stats.items():
            successful_metrics = [m for m in day_metrics if m.success]
            if successful_metrics:
                daily_averages[day] = {
                    'count': len(day_metrics),
                    'success_count': len(successful_metrics),
                    'avg_duration': statistics.mean(m.duration for m in successful_metrics),
                    'avg_size': statistics.mean(m.size for m in successful_metrics),
                    'avg_compression': statistics.mean(m.compression_ratio for m in successful_metrics)
                }
        
        # Calculate trends
        if len(daily_averages) >= 2:
            days = sorted(daily_averages.keys())
            
            # Duration trend
            durations = [daily_averages[day]['avg_duration'] for day in days]
            duration_trend = self._calculate_trend(durations)
            
            # Size trend
            sizes = [daily_averages[day]['avg_size'] for day in days]
            size_trend = self._calculate_trend(sizes)
            
            # Compression trend
            compressions = [daily_averages[day]['avg_compression'] for day in days]
            compression_trend = self._calculate_trend(compressions)
            
            return {
                'duration_trend': duration_trend,
                'size_trend': size_trend,
                'compression_trend': compression_trend,
                'daily_averages': daily_averages
            }
        
        return {}
    
    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction from a list of values."""
        if len(values) < 2:
            return "stable"
        
        # Simple linear trend calculation
        first_half = values[:len(values)//2]
        second_half = values[len(values)//2:]
        
        if not first_half or not second_half:
            return "stable"
        
        first_avg = statistics.mean(first_half)
        second_avg = statistics.mean(second_half)
        
        change_percent = ((second_avg - first_avg) / first_avg) * 100 if first_avg > 0 else 0
        
        if change_percent > 10:
            return "increasing"
        elif change_percent < -10:
            return "decreasing"
        else:
            return "stable"
    
    def _generate_recommendations(self, metrics: List[BackupMetrics], trends: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on metrics and trends."""
        recommendations = []
        
        if not metrics:
            return ["No backup data available for analysis"]
        
        # Success rate recommendations
        success_rate = sum(1 for m in metrics if m.success) / len(metrics)
        if success_rate < 0.9:
            recommendations.append("Backup success rate is below 90%. Review backup configuration and error logs.")
        
        # Duration recommendations
        successful_metrics = [m for m in metrics if m.success]
        if successful_metrics:
            avg_duration = statistics.mean(m.duration for m in successful_metrics)
            if avg_duration > 3600:  # More than 1 hour
                recommendations.append("Average backup duration is over 1 hour. Consider optimizing backup strategy.")
            
            # Check for duration outliers
            durations = [m.duration for m in successful_metrics]
            if len(durations) > 10:
                q75, q25 = statistics.quantiles(durations, n=4)[2], statistics.quantiles(durations, n=4)[0]
                iqr = q75 - q25
                outlier_threshold = q75 + 1.5 * iqr
                outliers = [d for d in durations if d > outlier_threshold]
                if outliers:
                    recommendations.append(f"Found {len(outliers)} backup duration outliers. Investigate performance issues.")
        
        # Compression recommendations
        if successful_metrics:
            avg_compression = statistics.mean(m.compression_ratio for m in successful_metrics)
            if avg_compression < 0.1:  # Less than 10% compression
                recommendations.append("Low compression ratio detected. Consider using different compression settings.")
        
        # Trend-based recommendations
        if trends:
            if trends.get('duration_trend') == 'increasing':
                recommendations.append("Backup duration is trending upward. Monitor system performance and backup size.")
            
            if trends.get('size_trend') == 'increasing':
                recommendations.append("Backup size is increasing. Review retention policies and data growth.")
        
        return recommendations
    
    def get_performance_summary(self, days: int = 7) -> Dict[str, Any]:
        """Get performance summary for the last N days."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        metrics = self.get_backup_metrics(start_date, end_date)
        
        if not metrics:
            return {
                'total_backups': 0,
                'success_rate': 0.0,
                'avg_duration': 0.0,
                'total_size': 0,
                'avg_compression': 0.0
            }
        
        successful_metrics = [m for m in metrics if m.success]
        
        return {
            'total_backups': len(metrics),
            'success_rate': len(successful_metrics) / len(metrics),
            'avg_duration': statistics.mean(m.duration for m in successful_metrics) if successful_metrics else 0.0,
            'total_size': sum(m.size for m in successful_metrics),
            'avg_compression': statistics.mean(m.compression_ratio for m in successful_metrics) if successful_metrics else 0.0
        }
    
    def export_report(self, report: AnalyticsReport, format: str = "json") -> str:
        """Export analytics report in specified format."""
        if format == "json":
            return json.dumps({
                'period_start': report.period_start.isoformat(),
                'period_end': report.period_end.isoformat(),
                'total_backups': report.total_backups,
                'successful_backups': report.successful_backups,
                'failed_backups': report.failed_backups,
                'success_rate': report.success_rate,
                'average_duration': report.average_duration,
                'average_size': report.average_size,
                'total_size': report.total_size,
                'average_compression_ratio': report.average_compression_ratio,
                'trends': report.trends,
                'recommendations': report.recommendations
            }, indent=2)
        
        elif format == "csv":
            # Create CSV format
            csv_lines = [
                "Metric,Value",
                f"Period Start,{report.period_start.isoformat()}",
                f"Period End,{report.period_end.isoformat()}",
                f"Total Backups,{report.total_backups}",
                f"Successful Backups,{report.successful_backups}",
                f"Failed Backups,{report.failed_backups}",
                f"Success Rate,{report.success_rate:.2%}",
                f"Average Duration,{report.average_duration:.2f}s",
                f"Average Size,{report.average_size / (1024*1024):.2f}MB",
                f"Total Size,{report.total_size / (1024*1024):.2f}MB",
                f"Average Compression,{report.average_compression_ratio:.2%}"
            ]
            
            # Add recommendations
            for i, rec in enumerate(report.recommendations, 1):
                csv_lines.append(f"Recommendation {i},{rec}")
            
            return "\n".join(csv_lines)
        
        else:
            raise ValueError(f"Unsupported export format: {format}")

def create_analytics(db_path: str = "backup_analytics.db") -> BackupAnalytics:
    """Create and return a backup analytics instance."""
    return BackupAnalytics(db_path)
