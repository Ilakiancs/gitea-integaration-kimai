#!/usr/bin/env python3
"""
Data Export Module

Provides comprehensive data export capabilities for sync data,
metrics, and reports in various formats including CSV, JSON, Excel, and PDF.
"""

import os
import csv
import json
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import pandas as pd
from io import StringIO, BytesIO

logger = logging.getLogger(__name__)

class DataExporter:
    """Base class for data exporters."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.export_dir = Path("exports")
        self.export_dir.mkdir(exist_ok=True)
    
    def export_sync_data(self, start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None,
                        repositories: Optional[List[str]] = None) -> str:
        """Export sync data with filters."""
        raise NotImplementedError
    
    def export_metrics(self, days: int = 30) -> str:
        """Export metrics data."""
        raise NotImplementedError
    
    def export_reports(self, report_type: str, **kwargs) -> str:
        """Export reports."""
        raise NotImplementedError

class CSVExporter(DataExporter):
    """Exports data to CSV format."""
    
    def export_sync_data(self, start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None,
                        repositories: Optional[List[str]] = None) -> str:
        """Export sync data to CSV."""
        query = """
            SELECT 
                timestamp,
                operation,
                repository,
                duration,
                success,
                items_processed,
                items_synced,
                errors_count,
                details
            FROM sync_metrics
            WHERE 1=1
        """
        params = []
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())
        
        if repositories:
            placeholders = ','.join(['?' for _ in repositories])
            query += f" AND repository IN ({placeholders})"
            params.extend(repositories)
        
        query += " ORDER BY timestamp DESC"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sync_data_{timestamp}.csv"
        file_path = self.export_dir / filename
        
        # Write CSV
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Timestamp', 'Operation', 'Repository', 'Duration', 
                'Success', 'Items Processed', 'Items Synced', 'Errors', 'Details'
            ])
            writer.writerows(rows)
        
        logger.info(f"Exported {len(rows)} sync records to {file_path}")
        return str(file_path)
    
    def export_metrics(self, days: int = 30) -> str:
        """Export metrics data to CSV."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Export sync metrics
        sync_query = """
            SELECT 
                DATE(timestamp) as date,
                operation,
                repository,
                COUNT(*) as operations,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                AVG(duration) as avg_duration,
                SUM(items_processed) as total_processed,
                SUM(items_synced) as total_synced,
                SUM(errors_count) as total_errors
            FROM sync_metrics 
            WHERE timestamp >= ?
            GROUP BY DATE(timestamp), operation, repository
            ORDER BY date DESC, operation, repository
        """
        
        # Export API metrics
        api_query = """
            SELECT 
                DATE(timestamp) as date,
                endpoint,
                method,
                COUNT(*) as calls,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                AVG(duration) as avg_duration,
                AVG(retry_count) as avg_retries
            FROM api_metrics 
            WHERE timestamp >= ?
            GROUP BY DATE(timestamp), endpoint, method
            ORDER BY date DESC, endpoint, method
        """
        
        with sqlite3.connect(self.db_path) as conn:
            sync_data = conn.execute(sync_query, (cutoff_date.isoformat(),)).fetchall()
            api_data = conn.execute(api_query, (cutoff_date.isoformat(),)).fetchall()
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"metrics_{timestamp}.csv"
        file_path = self.export_dir / filename
        
        # Write CSV with multiple sheets (using pandas)
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write sync metrics
            writer.writerow(['=== SYNC METRICS ==='])
            writer.writerow([
                'Date', 'Operation', 'Repository', 'Operations', 'Successful',
                'Avg Duration', 'Total Processed', 'Total Synced', 'Total Errors'
            ])
            writer.writerows(sync_data)
            
            writer.writerow([])  # Empty row
            
            # Write API metrics
            writer.writerow(['=== API METRICS ==='])
            writer.writerow([
                'Date', 'Endpoint', 'Method', 'Calls', 'Successful',
                'Avg Duration', 'Avg Retries'
            ])
            writer.writerows(api_data)
        
        logger.info(f"Exported metrics data to {file_path}")
        return str(file_path)
    
    def export_reports(self, report_type: str, **kwargs) -> str:
        """Export reports to CSV."""
        if report_type == "daily_summary":
            return self._export_daily_summary(**kwargs)
        elif report_type == "repository_analysis":
            return self._export_repository_analysis(**kwargs)
        elif report_type == "error_analysis":
            return self._export_error_analysis(**kwargs)
        else:
            raise ValueError(f"Unknown report type: {report_type}")
    
    def _export_daily_summary(self, days: int = 7) -> str:
        """Export daily summary report."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = """
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as total_operations,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                AVG(duration) as avg_duration,
                SUM(items_processed) as total_items_processed,
                SUM(items_synced) as total_items_synced,
                SUM(errors_count) as total_errors
            FROM sync_metrics 
            WHERE timestamp >= ?
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        """
        
        with sqlite3.connect(self.db_path) as conn:
            data = conn.execute(query, (cutoff_date.isoformat(),)).fetchall()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"daily_summary_{timestamp}.csv"
        file_path = self.export_dir / filename
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Date', 'Total Operations', 'Successful Operations', 'Success Rate',
                'Avg Duration', 'Total Processed', 'Total Synced', 'Total Errors'
            ])
            
            for row in data:
                date, total_ops, successful_ops, avg_duration, processed, synced, errors = row
                success_rate = (successful_ops / total_ops * 100) if total_ops > 0 else 0
                writer.writerow([date, total_ops, successful_ops, f"{success_rate:.1f}%", 
                               f"{avg_duration:.2f}s", processed, synced, errors])
        
        return str(file_path)
    
    def _export_repository_analysis(self) -> str:
        """Export repository analysis report."""
        query = """
            SELECT 
                repository,
                COUNT(*) as total_operations,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                AVG(duration) as avg_duration,
                SUM(items_processed) as total_items_processed,
                SUM(items_synced) as total_items_synced,
                SUM(errors_count) as total_errors,
                MAX(timestamp) as last_sync
            FROM sync_metrics 
            GROUP BY repository
            ORDER BY total_operations DESC
        """
        
        with sqlite3.connect(self.db_path) as conn:
            data = conn.execute(query).fetchall()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"repository_analysis_{timestamp}.csv"
        file_path = self.export_dir / filename
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Repository', 'Total Operations', 'Successful Operations', 'Success Rate',
                'Avg Duration', 'Total Processed', 'Total Synced', 'Total Errors', 'Last Sync'
            ])
            
            for row in data:
                repo, total_ops, successful_ops, avg_duration, processed, synced, errors, last_sync = row
                success_rate = (successful_ops / total_ops * 100) if total_ops > 0 else 0
                writer.writerow([repo, total_ops, successful_ops, f"{success_rate:.1f}%", 
                               f"{avg_duration:.2f}s", processed, synced, errors, last_sync])
        
        return str(file_path)
    
    def _export_error_analysis(self, days: int = 30) -> str:
        """Export error analysis report."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = """
            SELECT 
                repository,
                operation,
                COUNT(*) as error_count,
                AVG(duration) as avg_duration,
                MAX(timestamp) as last_error,
                details
            FROM sync_metrics 
            WHERE errors_count > 0 AND timestamp >= ?
            GROUP BY repository, operation
            ORDER BY error_count DESC
        """
        
        with sqlite3.connect(self.db_path) as conn:
            data = conn.execute(query, (cutoff_date.isoformat(),)).fetchall()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"error_analysis_{timestamp}.csv"
        file_path = self.export_dir / filename
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Repository', 'Operation', 'Error Count', 'Avg Duration', 'Last Error', 'Details'
            ])
            writer.writerows(data)
        
        return str(file_path)

class JSONExporter(DataExporter):
    """Exports data to JSON format."""
    
    def export_sync_data(self, start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None,
                        repositories: Optional[List[str]] = None) -> str:
        """Export sync data to JSON."""
        query = """
            SELECT 
                timestamp,
                operation,
                repository,
                duration,
                success,
                items_processed,
                items_synced,
                errors_count,
                details
            FROM sync_metrics
            WHERE 1=1
        """
        params = []
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())
        
        if repositories:
            placeholders = ','.join(['?' for _ in repositories])
            query += f" AND repository IN ({placeholders})"
            params.extend(repositories)
        
        query += " ORDER BY timestamp DESC"
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        data = []
        for row in rows:
            record = dict(zip(columns, row))
            # Parse JSON details if present
            if record['details']:
                try:
                    record['details'] = json.loads(record['details'])
                except json.JSONDecodeError:
                    pass  # Keep as string if not valid JSON
            data.append(record)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sync_data_{timestamp}.json"
        file_path = self.export_dir / filename
        
        # Write JSON
        export_data = {
            'export_info': {
                'generated_at': datetime.now().isoformat(),
                'record_count': len(data),
                'filters': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None,
                    'repositories': repositories
                }
            },
            'data': data
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        logger.info(f"Exported {len(data)} sync records to {file_path}")
        return str(file_path)
    
    def export_metrics(self, days: int = 30) -> str:
        """Export metrics data to JSON."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            # Get sync metrics summary
            sync_query = """
                SELECT 
                    DATE(timestamp) as date,
                    operation,
                    repository,
                    COUNT(*) as operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(duration) as avg_duration,
                    SUM(items_processed) as total_processed,
                    SUM(items_synced) as total_synced,
                    SUM(errors_count) as total_errors
                FROM sync_metrics 
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp), operation, repository
                ORDER BY date DESC, operation, repository
            """
            
            sync_data = conn.execute(sync_query, (cutoff_date.isoformat(),)).fetchall()
            
            # Get API metrics summary
            api_query = """
                SELECT 
                    DATE(timestamp) as date,
                    endpoint,
                    method,
                    COUNT(*) as calls,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(duration) as avg_duration,
                    AVG(retry_count) as avg_retries
                FROM api_metrics 
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp), endpoint, method
                ORDER BY date DESC, endpoint, method
            """
            
            api_data = conn.execute(api_query, (cutoff_date.isoformat(),)).fetchall()
        
        # Convert to structured data
        sync_metrics = []
        for row in sync_data:
            sync_metrics.append({
                'date': row[0],
                'operation': row[1],
                'repository': row[2],
                'operations': row[3],
                'successful': row[4],
                'avg_duration': row[5],
                'total_processed': row[6],
                'total_synced': row[7],
                'total_errors': row[8]
            })
        
        api_metrics = []
        for row in api_data:
            api_metrics.append({
                'date': row[0],
                'endpoint': row[1],
                'method': row[2],
                'calls': row[3],
                'successful': row[4],
                'avg_duration': row[5],
                'avg_retries': row[6]
            })
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"metrics_{timestamp}.json"
        file_path = self.export_dir / filename
        
        # Write JSON
        export_data = {
            'export_info': {
                'generated_at': datetime.now().isoformat(),
                'period_days': days,
                'sync_metrics_count': len(sync_metrics),
                'api_metrics_count': len(api_metrics)
            },
            'sync_metrics': sync_metrics,
            'api_metrics': api_metrics
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        logger.info(f"Exported metrics data to {file_path}")
        return str(file_path)
    
    def export_reports(self, report_type: str, **kwargs) -> str:
        """Export reports to JSON."""
        if report_type == "system_summary":
            return self._export_system_summary(**kwargs)
        else:
            raise ValueError(f"Unknown report type: {report_type}")
    
    def _export_system_summary(self) -> str:
        """Export system summary report."""
        with sqlite3.connect(self.db_path) as conn:
            # Get overall statistics
            overall_query = """
                SELECT 
                    COUNT(*) as total_operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                    AVG(duration) as avg_duration,
                    SUM(items_processed) as total_items_processed,
                    SUM(items_synced) as total_items_synced,
                    SUM(errors_count) as total_errors
                FROM sync_metrics
            """
            overall_stats = conn.execute(overall_query).fetchone()
            
            # Get repository breakdown
            repo_query = """
                SELECT 
                    repository,
                    COUNT(*) as operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(duration) as avg_duration,
                    SUM(items_synced) as items_synced
                FROM sync_metrics 
                GROUP BY repository
                ORDER BY items_synced DESC
            """
            repo_stats = conn.execute(repo_query).fetchall()
            
            # Get recent activity
            recent_query = """
                SELECT 
                    timestamp,
                    operation,
                    repository,
                    success,
                    items_synced
                FROM sync_metrics 
                ORDER BY timestamp DESC
                LIMIT 50
            """
            recent_activity = conn.execute(recent_query).fetchall()
        
        # Structure the data
        summary = {
            'overall': {
                'total_operations': overall_stats[0],
                'successful_operations': overall_stats[1],
                'success_rate': (overall_stats[1] / overall_stats[0] * 100) if overall_stats[0] > 0 else 0,
                'avg_duration': overall_stats[2],
                'total_items_processed': overall_stats[3],
                'total_items_synced': overall_stats[4],
                'total_errors': overall_stats[5]
            },
            'repositories': [
                {
                    'repository': row[0],
                    'operations': row[1],
                    'successful': row[2],
                    'success_rate': (row[2] / row[1] * 100) if row[1] > 0 else 0,
                    'avg_duration': row[3],
                    'items_synced': row[4]
                }
                for row in repo_stats
            ],
            'recent_activity': [
                {
                    'timestamp': row[0],
                    'operation': row[1],
                    'repository': row[2],
                    'success': bool(row[3]),
                    'items_synced': row[4]
                }
                for row in recent_activity
            ]
        }
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"system_summary_{timestamp}.json"
        file_path = self.export_dir / filename
        
        # Write JSON
        export_data = {
            'export_info': {
                'generated_at': datetime.now().isoformat(),
                'report_type': 'system_summary'
            },
            'summary': summary
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        return str(file_path)

class ExcelExporter(DataExporter):
    """Exports data to Excel format."""
    
    def export_sync_data(self, start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None,
                        repositories: Optional[List[str]] = None) -> str:
        """Export sync data to Excel."""
        # Use pandas to read from database and export to Excel
        query = """
            SELECT 
                timestamp,
                operation,
                repository,
                duration,
                success,
                items_processed,
                items_synced,
                errors_count,
                details
            FROM sync_metrics
            WHERE 1=1
        """
        params = []
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
        
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())
        
        if repositories:
            placeholders = ','.join(['?' for _ in repositories])
            query += f" AND repository IN ({placeholders})"
            params.extend(repositories)
        
        query += " ORDER BY timestamp DESC"
        
        # Read data into pandas DataFrame
        df = pd.read_sql_query(query, sqlite3.connect(self.db_path), params=params)
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sync_data_{timestamp}.xlsx"
        file_path = self.export_dir / filename
        
        # Export to Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Sync Data', index=False)
            
            # Add summary sheet
            summary_data = {
                'Metric': ['Total Records', 'Successful Operations', 'Failed Operations', 
                          'Success Rate', 'Total Items Processed', 'Total Items Synced', 'Total Errors'],
                'Value': [
                    len(df),
                    df['success'].sum(),
                    (~df['success']).sum(),
                    f"{(df['success'].sum() / len(df) * 100):.1f}%" if len(df) > 0 else "0%",
                    df['items_processed'].sum(),
                    df['items_synced'].sum(),
                    df['errors_count'].sum()
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Exported {len(df)} sync records to {file_path}")
        return str(file_path)
    
    def export_metrics(self, days: int = 30) -> str:
        """Export metrics data to Excel."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Read sync metrics
        sync_query = """
            SELECT 
                DATE(timestamp) as date,
                operation,
                repository,
                COUNT(*) as operations,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                AVG(duration) as avg_duration,
                SUM(items_processed) as total_processed,
                SUM(items_synced) as total_synced,
                SUM(errors_count) as total_errors
            FROM sync_metrics 
            WHERE timestamp >= ?
            GROUP BY DATE(timestamp), operation, repository
            ORDER BY date DESC, operation, repository
        """
        
        # Read API metrics
        api_query = """
            SELECT 
                DATE(timestamp) as date,
                endpoint,
                method,
                COUNT(*) as calls,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                AVG(duration) as avg_duration,
                AVG(retry_count) as avg_retries
            FROM api_metrics 
            WHERE timestamp >= ?
            GROUP BY DATE(timestamp), endpoint, method
            ORDER BY date DESC, endpoint, method
        """
        
        sync_df = pd.read_sql_query(sync_query, sqlite3.connect(self.db_path), params=[cutoff_date.isoformat()])
        api_df = pd.read_sql_query(api_query, sqlite3.connect(self.db_path), params=[cutoff_date.isoformat()])
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"metrics_{timestamp}.xlsx"
        file_path = self.export_dir / filename
        
        # Export to Excel with multiple sheets
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            sync_df.to_excel(writer, sheet_name='Sync Metrics', index=False)
            api_df.to_excel(writer, sheet_name='API Metrics', index=False)
            
            # Add summary sheet
            summary_data = {
                'Metric': ['Period (days)', 'Sync Records', 'API Records', 'Total Operations', 'Total API Calls'],
                'Value': [days, len(sync_df), len(api_df), sync_df['operations'].sum(), api_df['calls'].sum()]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Exported metrics data to {file_path}")
        return str(file_path)
    
    def export_reports(self, report_type: str, **kwargs) -> str:
        """Export reports to Excel."""
        if report_type == "comprehensive_report":
            return self._export_comprehensive_report(**kwargs)
        else:
            raise ValueError(f"Unknown report type: {report_type}")
    
    def _export_comprehensive_report(self, days: int = 30) -> str:
        """Export comprehensive report to Excel."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Get various data for the report
        with sqlite3.connect(self.db_path) as conn:
            # Daily summary
            daily_query = """
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as total_operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                    AVG(duration) as avg_duration,
                    SUM(items_processed) as total_items_processed,
                    SUM(items_synced) as total_items_synced,
                    SUM(errors_count) as total_errors
                FROM sync_metrics 
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp)
                ORDER BY date DESC
            """
            daily_df = pd.read_sql_query(daily_query, conn, params=[cutoff_date.isoformat()])
            
            # Repository analysis
            repo_query = """
                SELECT 
                    repository,
                    COUNT(*) as total_operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                    AVG(duration) as avg_duration,
                    SUM(items_processed) as total_items_processed,
                    SUM(items_synced) as total_items_synced,
                    SUM(errors_count) as total_errors
                FROM sync_metrics 
                WHERE timestamp >= ?
                GROUP BY repository
                ORDER BY total_operations DESC
            """
            repo_df = pd.read_sql_query(repo_query, conn, params=[cutoff_date.isoformat()])
            
            # Error analysis
            error_query = """
                SELECT 
                    repository,
                    operation,
                    COUNT(*) as error_count,
                    AVG(duration) as avg_duration,
                    MAX(timestamp) as last_error
                FROM sync_metrics 
                WHERE errors_count > 0 AND timestamp >= ?
                GROUP BY repository, operation
                ORDER BY error_count DESC
            """
            error_df = pd.read_sql_query(error_query, conn, params=[cutoff_date.isoformat()])
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"comprehensive_report_{timestamp}.xlsx"
        file_path = self.export_dir / filename
        
        # Export to Excel with multiple sheets
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            daily_df.to_excel(writer, sheet_name='Daily Summary', index=False)
            repo_df.to_excel(writer, sheet_name='Repository Analysis', index=False)
            error_df.to_excel(writer, sheet_name='Error Analysis', index=False)
            
            # Add overview sheet
            overview_data = {
                'Metric': ['Report Period (days)', 'Total Days', 'Total Operations', 'Successful Operations', 
                          'Success Rate', 'Total Items Processed', 'Total Items Synced', 'Total Errors'],
                'Value': [
                    days,
                    len(daily_df),
                    daily_df['total_operations'].sum(),
                    daily_df['successful_operations'].sum(),
                    f"{(daily_df['successful_operations'].sum() / daily_df['total_operations'].sum() * 100):.1f}%" if daily_df['total_operations'].sum() > 0 else "0%",
                    daily_df['total_items_processed'].sum(),
                    daily_df['total_items_synced'].sum(),
                    daily_df['total_errors'].sum()
                ]
            }
            overview_df = pd.DataFrame(overview_data)
            overview_df.to_excel(writer, sheet_name='Overview', index=False)
        
        return str(file_path)
