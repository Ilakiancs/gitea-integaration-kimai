#!/usr/bin/env python3
"""
Advanced Backup Analytics Module

Enhanced analytics and reporting system for backup performance analysis,
trends, insights, and predictive analytics with machine learning capabilities.
"""

import os
import logging
import sqlite3
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
import threading
from collections import defaultdict
import statistics
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

@dataclass
class BackupMetrics:
    """Enhanced backup performance metrics."""
    backup_id: str
    timestamp: datetime
    duration: float
    size: int
    compressed_size: int
    compression_ratio: float
    success: bool
    error_message: Optional[str] = None
    file_count: int = 0
    deduplication_savings: float = 0.0
    encryption_overhead: float = 0.0
    network_transfer_time: float = 0.0
    cloud_sync_status: str = "not_synced"
    tags: List[str] = field(default_factory=list)

@dataclass
class SystemMetrics:
    """System performance metrics."""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_io: float
    disk_io: float
    temperature: Optional[float] = None
    power_consumption: Optional[float] = None

@dataclass
class PredictiveInsights:
    """Predictive analytics insights."""
    next_backup_time: datetime
    estimated_duration: float
    estimated_size: int
    confidence_level: float
    risk_factors: List[str]
    optimization_suggestions: List[str]

@dataclass
class AnalyticsReport:
    """Enhanced analytics report data."""
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
    predictive_insights: Optional[PredictiveInsights] = None
    performance_anomalies: List[Dict[str, Any]] = field(default_factory=list)
    cost_analysis: Dict[str, float] = field(default_factory=dict)
    security_metrics: Dict[str, Any] = field(default_factory=dict)

class AdvancedBackupAnalytics:
    """Advanced analytics system with machine learning capabilities."""
    
    def __init__(self, db_path: str = "backup_analytics.db"):
        self.db_path = Path(db_path)
        self.lock = threading.RLock()
        self.ml_models: Dict[str, Any] = {}
        self.scaler = StandardScaler()
        self._init_database()
        self._initialize_ml_models()
    
    def _init_database(self):
        """Initialize enhanced analytics database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Enhanced backup metrics table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS backup_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backup_id TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        duration REAL NOT NULL,
                        size INTEGER NOT NULL,
                        compressed_size INTEGER NOT NULL,
                        compression_ratio REAL NOT NULL,
                        success BOOLEAN NOT NULL,
                        error_message TEXT,
                        file_count INTEGER DEFAULT 0,
                        deduplication_savings REAL DEFAULT 0.0,
                        encryption_overhead REAL DEFAULT 0.0,
                        network_transfer_time REAL DEFAULT 0.0,
                        cloud_sync_status TEXT DEFAULT 'not_synced',
                        tags TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Enhanced system metrics table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        cpu_usage REAL NOT NULL,
                        memory_usage REAL NOT NULL,
                        disk_usage REAL NOT NULL,
                        network_io REAL DEFAULT 0.0,
                        disk_io REAL DEFAULT 0.0,
                        temperature REAL,
                        power_consumption REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # New table for predictive analytics
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS predictions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        prediction_type TEXT NOT NULL,
                        predicted_value REAL NOT NULL,
                        confidence_level REAL NOT NULL,
                        timestamp TEXT NOT NULL,
                        features TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # New table for cost analysis
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cost_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backup_id TEXT NOT NULL,
                        storage_cost REAL DEFAULT 0.0,
                        network_cost REAL DEFAULT 0.0,
                        compute_cost REAL DEFAULT 0.0,
                        total_cost REAL DEFAULT 0.0,
                        timestamp TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize analytics database: {e}")
    
    def _initialize_ml_models(self):
        """Initialize machine learning models."""
        try:
            # Duration prediction model
            self.ml_models['duration'] = RandomForestRegressor(
                n_estimators=100,
                random_state=42,
                max_depth=10
            )
            
            # Size prediction model
            self.ml_models['size'] = RandomForestRegressor(
                n_estimators=100,
                random_state=42,
                max_depth=10
            )
            
            # Success prediction model
            self.ml_models['success'] = RandomForestRegressor(
                n_estimators=100,
                random_state=42,
                max_depth=10
            )
            
            logger.info("Machine learning models initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ML models: {e}")

    def record_backup_metrics(self, metrics: BackupMetrics):
        """Record enhanced backup metrics in the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO backup_metrics 
                    (backup_id, timestamp, duration, size, compressed_size, compression_ratio, 
                     success, error_message, file_count, deduplication_savings, encryption_overhead,
                     network_transfer_time, cloud_sync_status, tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.backup_id,
                    metrics.timestamp.isoformat(),
                    metrics.duration,
                    metrics.size,
                    metrics.compressed_size,
                    metrics.compression_ratio,
                    metrics.success,
                    metrics.error_message,
                    metrics.file_count,
                    metrics.deduplication_savings,
                    metrics.encryption_overhead,
                    metrics.network_transfer_time,
                    metrics.cloud_sync_status,
                    json.dumps(metrics.tags)
                ))
                conn.commit()
                
                # Update ML models with new data
                self._update_ml_models()
                
        except Exception as e:
            logger.error(f"Failed to record backup metrics: {e}")
    
    def record_system_metrics(self, metrics: SystemMetrics):
        """Record system performance metrics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO system_metrics 
                    (timestamp, cpu_usage, memory_usage, disk_usage, network_io, 
                     disk_io, temperature, power_consumption)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.timestamp.isoformat(),
                    metrics.cpu_usage,
                    metrics.memory_usage,
                    metrics.disk_usage,
                    metrics.network_io,
                    metrics.disk_io,
                    metrics.temperature,
                    metrics.power_consumption
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to record system metrics: {e}")
    
    def _update_ml_models(self):
        """Update machine learning models with new data."""
        try:
            # Get recent data for training
            df = self._get_backup_data_for_ml()
            
            if len(df) < 10:  # Need minimum data for training
                return
            
            # Prepare features
            features = ['size', 'file_count', 'compression_ratio', 'hour_of_day', 'day_of_week']
            X = df[features].fillna(0)
            
            # Train duration model
            if 'duration' in df.columns:
                y_duration = df['duration']
                self.ml_models['duration'].fit(X, y_duration)
            
            # Train size model
            if 'size' in df.columns:
                y_size = df['size']
                self.ml_models['size'].fit(X, y_size)
            
            # Train success model
            if 'success' in df.columns:
                y_success = df['success'].astype(int)
                self.ml_models['success'].fit(X, y_success)
            
            logger.info("ML models updated successfully")
            
        except Exception as e:
            logger.error(f"Failed to update ML models: {e}")
    
    def _get_backup_data_for_ml(self) -> pd.DataFrame:
        """Get backup data for machine learning training."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT * FROM backup_metrics 
                    WHERE timestamp >= datetime('now', '-30 days')
                    ORDER BY timestamp DESC
                """
                df = pd.read_sql_query(query, conn)
                
                if not df.empty:
                    # Add time-based features
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df['hour_of_day'] = df['timestamp'].dt.hour
                    df['day_of_week'] = df['timestamp'].dt.dayofweek
                    df['month'] = df['timestamp'].dt.month
                
                return df
        except Exception as e:
            logger.error(f"Failed to get backup data for ML: {e}")
            return pd.DataFrame()

    def generate_predictive_insights(self) -> PredictiveInsights:
        """Generate predictive insights for future backups."""
        try:
            df = self._get_backup_data_for_ml()
            
            if df.empty or len(df) < 5:
                return PredictiveInsights(
                    next_backup_time=datetime.now() + timedelta(hours=1),
                    estimated_duration=300.0,
                    estimated_size=1024*1024*100,  # 100MB default
                    confidence_level=0.5,
                    risk_factors=["Insufficient historical data"],
                    optimization_suggestions=["Collect more backup data for better predictions"]
                )
            
            # Predict next backup time based on patterns
            time_intervals = df['timestamp'].diff().dropna()
            avg_interval = time_intervals.mean()
            next_backup_time = df['timestamp'].iloc[-1] + avg_interval
            
            # Prepare features for prediction
            latest_features = df[['size', 'file_count', 'compression_ratio', 'hour_of_day', 'day_of_week']].iloc[-1:].fillna(0)
            
            # Make predictions
            estimated_duration = self.ml_models['duration'].predict(latest_features)[0]
            estimated_size = self.ml_models['size'].predict(latest_features)[0]
            success_probability = self.ml_models['success'].predict_proba(latest_features)[0][1]
            
            # Calculate confidence level
            confidence_level = min(success_probability * 0.8 + 0.2, 0.95)
            
            # Identify risk factors
            risk_factors = []
            if df['success'].mean() < 0.9:
                risk_factors.append("Low success rate in recent backups")
            if df['duration'].std() > df['duration'].mean() * 0.5:
                risk_factors.append("High variability in backup duration")
            if df['size'].std() > df['size'].mean() * 0.8:
                risk_factors.append("High variability in backup size")
            
            # Generate optimization suggestions
            suggestions = []
            if df['compression_ratio'].mean() < 0.3:
                suggestions.append("Consider enabling better compression algorithms")
            if df['duration'].mean() > 600:  # 10 minutes
                suggestions.append("Consider incremental backups to reduce duration")
            if df['success'].mean() < 0.95:
                suggestions.append("Review backup configuration and error logs")
            
            return PredictiveInsights(
                next_backup_time=next_backup_time,
                estimated_duration=estimated_duration,
                estimated_size=int(estimated_size),
                confidence_level=confidence_level,
                risk_factors=risk_factors,
                optimization_suggestions=suggestions
            )
            
        except Exception as e:
            logger.error(f"Failed to generate predictive insights: {e}")
            return PredictiveInsights(
                next_backup_time=datetime.now() + timedelta(hours=1),
                estimated_duration=300.0,
                estimated_size=1024*1024*100,
                confidence_level=0.5,
                risk_factors=["Error in prediction model"],
                optimization_suggestions=["Check analytics system configuration"]
            )

    def detect_anomalies(self, days: int = 7) -> List[Dict[str, Any]]:
        """Detect performance anomalies in recent backups."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = f"""
                    SELECT * FROM backup_metrics 
                    WHERE timestamp >= datetime('now', '-{days} days')
                    ORDER BY timestamp DESC
                """
                df = pd.read_sql_query(query, conn)
            
            if df.empty:
                return []
            
            anomalies = []
            
            # Duration anomalies
            duration_mean = df['duration'].mean()
            duration_std = df['duration'].std()
            duration_threshold = duration_mean + 2 * duration_std
            
            for _, row in df.iterrows():
                if row['duration'] > duration_threshold:
                    anomalies.append({
                        'type': 'duration_anomaly',
                        'backup_id': row['backup_id'],
                        'timestamp': row['timestamp'],
                        'value': row['duration'],
                        'threshold': duration_threshold,
                        'severity': 'high' if row['duration'] > duration_mean + 3 * duration_std else 'medium'
                    })
            
            # Size anomalies
            size_mean = df['size'].mean()
            size_std = df['size'].std()
            size_threshold = size_mean + 2 * size_std
            
            for _, row in df.iterrows():
                if row['size'] > size_threshold:
                    anomalies.append({
                        'type': 'size_anomaly',
                        'backup_id': row['backup_id'],
                        'timestamp': row['timestamp'],
                        'value': row['size'],
                        'threshold': size_threshold,
                        'severity': 'high' if row['size'] > size_mean + 3 * size_std else 'medium'
                    })
            
            # Success rate anomalies
            recent_success_rate = df['success'].mean()
            if recent_success_rate < 0.8:
                anomalies.append({
                    'type': 'success_rate_anomaly',
                    'timestamp': df['timestamp'].iloc[-1],
                    'value': recent_success_rate,
                    'threshold': 0.8,
                    'severity': 'high' if recent_success_rate < 0.6 else 'medium'
                })
            
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect anomalies: {e}")
            return []

    def generate_cost_analysis(self, period_days: int = 30) -> Dict[str, float]:
        """Generate cost analysis for backup operations."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = f"""
                    SELECT * FROM backup_metrics 
                    WHERE timestamp >= datetime('now', '-{period_days} days')
                """
                df = pd.read_sql_query(query, conn)
            
            if df.empty:
                return {
                    'storage_cost': 0.0,
                    'network_cost': 0.0,
                    'compute_cost': 0.0,
                    'total_cost': 0.0
                }
            
            # Calculate costs (example rates - should be configurable)
            storage_rate = 0.023  # $0.023 per GB per month
            network_rate = 0.09   # $0.09 per GB
            compute_rate = 0.10   # $0.10 per hour
            
            total_storage_gb = df['compressed_size'].sum() / (1024**3)
            total_network_gb = df['compressed_size'].sum() / (1024**3)
            total_compute_hours = df['duration'].sum() / 3600
            
            storage_cost = total_storage_gb * storage_rate * (period_days / 30)
            network_cost = total_network_gb * network_rate
            compute_cost = total_compute_hours * compute_rate
            total_cost = storage_cost + network_cost + compute_cost
            
            return {
                'storage_cost': round(storage_cost, 2),
                'network_cost': round(network_cost, 2),
                'compute_cost': round(compute_cost, 2),
                'total_cost': round(total_cost, 2),
                'total_storage_gb': round(total_storage_gb, 2),
                'total_network_gb': round(total_network_gb, 2),
                'total_compute_hours': round(total_compute_hours, 2)
            }
            
        except Exception as e:
            logger.error(f"Failed to generate cost analysis: {e}")
            return {
                'storage_cost': 0.0,
                'network_cost': 0.0,
                'compute_cost': 0.0,
                'total_cost': 0.0
            }

    def generate_comprehensive_report(self, period_days: int = 30) -> AnalyticsReport:
        """Generate comprehensive analytics report."""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=period_days)
            
            with sqlite3.connect(self.db_path) as conn:
                query = f"""
                    SELECT * FROM backup_metrics 
                    WHERE timestamp >= datetime('now', '-{period_days} days')
                    ORDER BY timestamp DESC
                """
                df = pd.read_sql_query(query, conn)
            
            if df.empty:
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
                    recommendations=["No backup data available for analysis"]
                )
            
            # Calculate basic metrics
            total_backups = len(df)
            successful_backups = df['success'].sum()
            failed_backups = total_backups - successful_backups
            success_rate = successful_backups / total_backups if total_backups > 0 else 0.0
            
            average_duration = df['duration'].mean()
            average_size = df['size'].mean()
            total_size = df['size'].sum()
            average_compression_ratio = df['compression_ratio'].mean()
            
            # Analyze trends
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date
            
            daily_stats = df.groupby('date').agg({
                'duration': 'mean',
                'size': 'sum',
                'success': 'mean'
            }).reset_index()
            
            trends = {
                'duration_trend': daily_stats['duration'].tolist(),
                'size_trend': daily_stats['size'].tolist(),
                'success_trend': daily_stats['success'].tolist(),
                'dates': [str(date) for date in daily_stats['date']]
            }
            
            # Generate recommendations
            recommendations = []
            if success_rate < 0.95:
                recommendations.append("Backup success rate is below 95%. Review error logs and configuration.")
            if average_duration > 600:
                recommendations.append("Average backup duration is high. Consider incremental backups.")
            if average_compression_ratio < 0.3:
                recommendations.append("Compression ratio is low. Consider better compression algorithms.")
            
            # Get predictive insights
            predictive_insights = self.generate_predictive_insights()
            
            # Detect anomalies
            performance_anomalies = self.detect_anomalies(period_days)
            
            # Generate cost analysis
            cost_analysis = self.generate_cost_analysis(period_days)
            
            # Security metrics
            security_metrics = {
                'encrypted_backups': len(df[df['encryption_overhead'] > 0]),
                'cloud_synced_backups': len(df[df['cloud_sync_status'] == 'synced']),
                'average_encryption_overhead': df['encryption_overhead'].mean()
            }
            
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
                recommendations=recommendations,
                predictive_insights=predictive_insights,
                performance_anomalies=performance_anomalies,
                cost_analysis=cost_analysis,
                security_metrics=security_metrics
            )
            
        except Exception as e:
            logger.error(f"Failed to generate comprehensive report: {e}")
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
                recommendations=[f"Error generating report: {e}"]
            )

    def export_report_to_json(self, report: AnalyticsReport, output_path: str) -> bool:
        """Export analytics report to JSON format."""
        try:
            report_data = {
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
                'recommendations': report.recommendations,
                'cost_analysis': report.cost_analysis,
                'security_metrics': report.security_metrics,
                'performance_anomalies': report.performance_anomalies
            }
            
            if report.predictive_insights:
                report_data['predictive_insights'] = {
                    'next_backup_time': report.predictive_insights.next_backup_time.isoformat(),
                    'estimated_duration': report.predictive_insights.estimated_duration,
                    'estimated_size': report.predictive_insights.estimated_size,
                    'confidence_level': report.predictive_insights.confidence_level,
                    'risk_factors': report.predictive_insights.risk_factors,
                    'optimization_suggestions': report.predictive_insights.optimization_suggestions
                }
            
            with open(output_path, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            logger.info(f"Analytics report exported to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export report: {e}")
            return False
