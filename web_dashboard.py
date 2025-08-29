#!/usr/bin/env python3
"""
Web Dashboard Module

Provides a web-based dashboard for monitoring and managing the sync system,
including real-time status, metrics visualization, and configuration management.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash
from flask_socketio import SocketIO, emit
import threading
import time

logger = logging.getLogger(__name__)

class WebDashboard:
    """Web dashboard for the sync system."""
    
    def __init__(self, db_path: str = "sync.db", port: int = 5000, debug: bool = False):
        self.db_path = db_path
        self.port = port
        self.debug = debug
        self.app = Flask(__name__)
        self.app.secret_key = os.urandom(24)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        self.setup_routes()
        self.setup_socketio()
    
    def setup_routes(self):
        """Setup Flask routes."""
        
        @self.app.route('/')
        def index():
            """Main dashboard page."""
            return render_template('dashboard.html')
        
        @self.app.route('/api/status')
        def api_status():
            """Get system status."""
            try:
                status = self.get_system_status()
                return jsonify(status)
            except Exception as e:
                logger.error(f"Error getting status: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/metrics')
        def api_metrics():
            """Get system metrics."""
            try:
                days = request.args.get('days', 7, type=int)
                metrics = self.get_system_metrics(days)
                return jsonify(metrics)
            except Exception as e:
                logger.error(f"Error getting metrics: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/repositories')
        def api_repositories():
            """Get repository information."""
            try:
                repos = self.get_repository_info()
                return jsonify(repos)
            except Exception as e:
                logger.error(f"Error getting repositories: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/recent-activity')
        def api_recent_activity():
            """Get recent sync activity."""
            try:
                limit = request.args.get('limit', 50, type=int)
                activity = self.get_recent_activity(limit)
                return jsonify(activity)
            except Exception as e:
                logger.error(f"Error getting recent activity: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/errors')
        def api_errors():
            """Get recent errors."""
            try:
                days = request.args.get('days', 7, type=int)
                errors = self.get_recent_errors(days)
                return jsonify(errors)
            except Exception as e:
                logger.error(f"Error getting errors: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/health')
        def api_health():
            """Get system health information."""
            try:
                health = self.get_health_status()
                return jsonify(health)
            except Exception as e:
                logger.error(f"Error getting health: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/config')
        def api_config():
            """Get configuration information."""
            try:
                config = self.get_config_info()
                return jsonify(config)
            except Exception as e:
                logger.error(f"Error getting config: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/sync/start', methods=['POST'])
        def api_sync_start():
            """Start a manual sync."""
            try:
                data = request.get_json()
                repository = data.get('repository')
                force = data.get('force', False)
                
                # This would integrate with the actual sync system
                result = self.start_manual_sync(repository, force)
                return jsonify(result)
            except Exception as e:
                logger.error(f"Error starting sync: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/export', methods=['POST'])
        def api_export():
            """Export data."""
            try:
                data = request.get_json()
                export_type = data.get('type', 'csv')
                days = data.get('days', 30)
                
                result = self.export_data(export_type, days)
                return jsonify(result)
            except Exception as e:
                logger.error(f"Error exporting data: {e}")
                return jsonify({'error': str(e)}), 500
    
    def setup_socketio(self):
        """Setup Socket.IO events."""
        
        @self.socketio.on('connect')
        def handle_connect():
            logger.info("Client connected to dashboard")
            emit('status', {'message': 'Connected to sync dashboard'})
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            logger.info("Client disconnected from dashboard")
        
        @self.socketio.on('request_update')
        def handle_update_request():
            """Handle real-time update requests."""
            try:
                status = self.get_system_status()
                emit('status_update', status)
            except Exception as e:
                logger.error(f"Error sending status update: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status."""
        with sqlite3.connect(self.db_path) as conn:
            # Get overall statistics
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                    AVG(duration) as avg_duration,
                    SUM(items_processed) as total_items_processed,
                    SUM(items_synced) as total_items_synced,
                    SUM(errors_count) as total_errors
                FROM sync_metrics
            """)
            overall_stats = cursor.fetchone()
            
            # Get recent activity count
            cursor = conn.execute("""
                SELECT COUNT(*) FROM sync_metrics 
                WHERE timestamp >= datetime('now', '-1 hour')
            """)
            recent_activity = cursor.fetchone()[0]
            
            # Get running operations
            cursor = conn.execute("""
                SELECT COUNT(*) FROM sync_metrics 
                WHERE timestamp >= datetime('now', '-5 minutes')
            """)
            running_ops = cursor.fetchone()[0]
        
        if overall_stats[0] is None:
            return {
                'status': 'no_data',
                'message': 'No sync data available',
                'overall': {
                    'total_operations': 0,
                    'successful_operations': 0,
                    'success_rate': 0,
                    'avg_duration': 0,
                    'total_items_processed': 0,
                    'total_items_synced': 0,
                    'total_errors': 0
                },
                'recent': {
                    'activity_last_hour': 0,
                    'running_operations': 0
                }
            }
        
        total_ops, successful_ops, avg_duration, processed, synced, errors = overall_stats
        
        return {
            'status': 'operational',
            'timestamp': datetime.now().isoformat(),
            'overall': {
                'total_operations': total_ops,
                'successful_operations': successful_ops,
                'success_rate': (successful_ops / total_ops * 100) if total_ops > 0 else 0,
                'avg_duration': avg_duration or 0,
                'total_items_processed': processed or 0,
                'total_items_synced': synced or 0,
                'total_errors': errors or 0
            },
            'recent': {
                'activity_last_hour': recent_activity,
                'running_operations': running_ops
            }
        }
    
    def get_system_metrics(self, days: int = 7) -> Dict[str, Any]:
        """Get system metrics for the specified period."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            # Daily metrics
            cursor = conn.execute("""
                SELECT 
                    DATE(timestamp) as date,
                    COUNT(*) as operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(duration) as avg_duration,
                    SUM(items_synced) as items_synced
                FROM sync_metrics 
                WHERE timestamp >= ?
                GROUP BY DATE(timestamp)
                ORDER BY date
            """, (cutoff_date.isoformat(),))
            
            daily_metrics = []
            for row in cursor.fetchall():
                date, ops, successful, avg_duration, synced = row
                daily_metrics.append({
                    'date': date,
                    'operations': ops,
                    'successful': successful,
                    'success_rate': (successful / ops * 100) if ops > 0 else 0,
                    'avg_duration': avg_duration or 0,
                    'items_synced': synced or 0
                })
            
            # Repository metrics
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
            """, (cutoff_date.isoformat(),))
            
            repo_metrics = []
            for row in cursor.fetchall():
                repo, ops, successful, avg_duration, synced = row
                repo_metrics.append({
                    'repository': repo,
                    'operations': ops,
                    'successful': successful,
                    'success_rate': (successful / ops * 100) if ops > 0 else 0,
                    'avg_duration': avg_duration or 0,
                    'items_synced': synced or 0
                })
        
        return {
            'period_days': days,
            'daily_metrics': daily_metrics,
            'repository_metrics': repo_metrics
        }
    
    def get_repository_info(self) -> List[Dict[str, Any]]:
        """Get repository information."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT 
                    repository,
                    COUNT(*) as total_operations,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_operations,
                    MAX(timestamp) as last_sync,
                    AVG(duration) as avg_duration,
                    SUM(items_synced) as total_items_synced
                FROM sync_metrics 
                GROUP BY repository
                ORDER BY last_sync DESC
            """)
            
            repositories = []
            for row in cursor.fetchall():
                repo, total_ops, successful_ops, last_sync, avg_duration, total_synced = row
                repositories.append({
                    'repository': repo,
                    'total_operations': total_ops,
                    'successful_operations': successful_ops,
                    'success_rate': (successful_ops / total_ops * 100) if total_ops > 0 else 0,
                    'last_sync': last_sync,
                    'avg_duration': avg_duration or 0,
                    'total_items_synced': total_synced or 0,
                    'status': 'active' if last_sync and (datetime.now() - datetime.fromisoformat(last_sync)).days < 1 else 'inactive'
                })
        
        return repositories
    
    def get_recent_activity(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent sync activity."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT 
                    timestamp,
                    operation,
                    repository,
                    duration,
                    success,
                    items_processed,
                    items_synced,
                    errors_count
                FROM sync_metrics 
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            activity = []
            for row in cursor.fetchall():
                timestamp, operation, repo, duration, success, processed, synced, errors = row
                activity.append({
                    'timestamp': timestamp,
                    'operation': operation,
                    'repository': repo,
                    'duration': duration,
                    'success': bool(success),
                    'items_processed': processed or 0,
                    'items_synced': synced or 0,
                    'errors_count': errors or 0
                })
        
        return activity
    
    def get_recent_errors(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get recent errors."""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT 
                    timestamp,
                    operation,
                    repository,
                    duration,
                    errors_count,
                    details
                FROM sync_metrics 
                WHERE errors_count > 0 AND timestamp >= ?
                ORDER BY timestamp DESC
                LIMIT 100
            """, (cutoff_date.isoformat(),))
            
            errors = []
            for row in cursor.fetchall():
                timestamp, operation, repo, duration, errors_count, details = row
                errors.append({
                    'timestamp': timestamp,
                    'operation': operation,
                    'repository': repo,
                    'duration': duration,
                    'errors_count': errors_count,
                    'details': details
                })
        
        return errors
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get system health status."""
        # This would integrate with the health check module
        return {
            'overall_status': 'healthy',
            'components': [
                {
                    'name': 'Database',
                    'status': 'healthy',
                    'message': 'Database connection is working'
                },
                {
                    'name': 'Gitea API',
                    'status': 'healthy',
                    'message': 'Gitea API is accessible'
                },
                {
                    'name': 'Kimai API',
                    'status': 'healthy',
                    'message': 'Kimai API is accessible'
                }
            ],
            'timestamp': datetime.now().isoformat()
        }
    
    def get_config_info(self) -> Dict[str, Any]:
        """Get configuration information."""
        # This would integrate with the config manager
        return {
            'gitea_url': os.getenv('GITEA_URL', 'Not configured'),
            'kimai_url': os.getenv('KIMAI_URL', 'Not configured'),
            'repositories': os.getenv('REPOS_TO_SYNC', '').split(','),
            'sync_interval': os.getenv('SYNC_INTERVAL', '60'),
            'read_only_mode': os.getenv('READ_ONLY_MODE', 'false').lower() == 'true'
        }
    
    def start_manual_sync(self, repository: str, force: bool = False) -> Dict[str, Any]:
        """Start a manual sync operation."""
        # This would integrate with the actual sync system
        logger.info(f"Manual sync requested for repository: {repository}, force: {force}")
        
        return {
            'status': 'started',
            'repository': repository,
            'force': force,
            'message': f'Sync started for {repository}',
            'timestamp': datetime.now().isoformat()
        }
    
    def export_data(self, export_type: str, days: int) -> Dict[str, Any]:
        """Export data in specified format."""
        # This would integrate with the data export module
        logger.info(f"Data export requested: {export_type}, {days} days")
        
        return {
            'status': 'exporting',
            'type': export_type,
            'days': days,
            'message': f'Exporting {days} days of data in {export_type} format',
            'timestamp': datetime.now().isoformat()
        }
    
    def start_background_updates(self):
        """Start background thread for real-time updates."""
        def update_loop():
            while True:
                try:
                    status = self.get_system_status()
                    self.socketio.emit('status_update', status)
                    time.sleep(30)  # Update every 30 seconds
                except Exception as e:
                    logger.error(f"Error in background update: {e}")
                    time.sleep(60)  # Wait longer on error
        
        update_thread = threading.Thread(target=update_loop, daemon=True)
        update_thread.start()
    
    def run(self, host: str = '0.0.0.0', port: int = None):
        """Run the web dashboard."""
        if port is None:
            port = self.port
        
        # Start background updates
        self.start_background_updates()
        
        logger.info(f"Starting web dashboard on {host}:{port}")
        self.socketio.run(self.app, host=host, port=port, debug=self.debug)

def create_dashboard_templates():
    """Create HTML templates for the dashboard."""
    templates_dir = Path("templates")
    templates_dir.mkdir(exist_ok=True)
    
    # Main dashboard template
    dashboard_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gitea-Kimai Sync Dashboard</title>
    <script src="https://cdn.socket.io/4.0.1/socket.io.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .status-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .status-card h3 {
            margin: 0 0 10px 0;
            color: #333;
        }
        .status-value {
            font-size: 2em;
            font-weight: bold;
            color: #007bff;
        }
        .chart-container {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .activity-list {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .activity-item {
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }
        .activity-item:last-child {
            border-bottom: none;
        }
        .success { color: #28a745; }
        .error { color: #dc3545; }
        .warning { color: #ffc107; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Gitea-Kimai Sync Dashboard</h1>
            <p>Real-time monitoring and management of sync operations</p>
        </div>
        
        <div class="status-grid">
            <div class="status-card">
                <h3>Total Operations</h3>
                <div class="status-value" id="total-operations">-</div>
            </div>
            <div class="status-card">
                <h3>Success Rate</h3>
                <div class="status-value" id="success-rate">-</div>
            </div>
            <div class="status-card">
                <h3>Items Synced</h3>
                <div class="status-value" id="items-synced">-</div>
            </div>
            <div class="status-card">
                <h3>Recent Activity</h3>
                <div class="status-value" id="recent-activity">-</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h3>Daily Sync Activity</h3>
            <canvas id="activity-chart" width="400" height="200"></canvas>
        </div>
        
        <div class="activity-list">
            <h3>Recent Activity</h3>
            <div id="activity-list"></div>
        </div>
    </div>
    
    <script>
        const socket = io();
        
        socket.on('connect', function() {
            console.log('Connected to dashboard');
        });
        
        socket.on('status_update', function(data) {
            updateDashboard(data);
        });
        
        function updateDashboard(data) {
            document.getElementById('total-operations').textContent = data.overall.total_operations;
            document.getElementById('success-rate').textContent = data.overall.success_rate.toFixed(1) + '%';
            document.getElementById('items-synced').textContent = data.overall.total_items_synced;
            document.getElementById('recent-activity').textContent = data.recent.activity_last_hour;
        }
        
        function loadInitialData() {
            fetch('/api/status')
                .then(response => response.json())
                .then(data => updateDashboard(data));
            
            fetch('/api/recent-activity?limit=10')
                .then(response => response.json())
                .then(data => updateActivityList(data));
        }
        
        function updateActivityList(activities) {
            const container = document.getElementById('activity-list');
            container.innerHTML = '';
            
            activities.forEach(activity => {
                const item = document.createElement('div');
                item.className = 'activity-item';
                item.innerHTML = `
                    <strong>${activity.repository}</strong> - ${activity.operation}
                    <span class="${activity.success ? 'success' : 'error'}">
                        ${activity.success ? '✓' : '✗'}
                    </span>
                    <small>${new Date(activity.timestamp).toLocaleString()}</small>
                `;
                container.appendChild(item);
            });
        }
        
        // Load initial data
        loadInitialData();
        
        // Refresh data every 30 seconds
        setInterval(loadInitialData, 30000);
    </script>
</body>
</html>"""
    
    with open(templates_dir / "dashboard.html", "w") as f:
        f.write(dashboard_html)
    
    logger.info("Created dashboard templates")

if __name__ == "__main__":
    # Create templates
    create_dashboard_templates()
    
    # Start dashboard
    dashboard = WebDashboard(port=5000, debug=True)
    dashboard.run()
