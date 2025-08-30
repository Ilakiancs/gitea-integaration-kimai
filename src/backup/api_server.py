#!/usr/bin/env python3
"""
Backup API Server Module

RESTful API server for backup management, monitoring, and integration
with external systems.
"""

import os
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import threading
import time

logger = logging.getLogger(__name__)

@dataclass
class APIConfig:
    """API server configuration."""
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False
    enable_cors: bool = True
    api_key: Optional[str] = None
    rate_limit: int = 100  # requests per minute

class BackupAPIServer:
    """RESTful API server for backup management."""
    
    def __init__(self, config: APIConfig, backup_manager=None, restore_manager=None):
        self.config = config
        self.backup_manager = backup_manager
        self.restore_manager = restore_manager
        self.app = Flask(__name__)
        self.server_thread = None
        self.running = False
        
        # Enable CORS if configured
        if config.enable_cors:
            CORS(self.app)
        
        # Setup routes
        self._setup_routes()
        
        # Setup middleware
        self._setup_middleware()
    
    def _setup_routes(self):
        """Setup API routes."""
        
        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """Health check endpoint."""
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'version': '2.0.0'
            })
        
        @self.app.route('/api/backups', methods=['GET'])
        def list_backups():
            """List all available backups."""
            try:
                if self.restore_manager:
                    backups = self.restore_manager.list_available_backups()
                    return jsonify({
                        'backups': backups,
                        'count': len(backups)
                    })
                else:
                    return jsonify({'error': 'Restore manager not available'}), 500
            except Exception as e:
                logger.error(f"Failed to list backups: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/backups/<backup_name>', methods=['GET'])
        def get_backup_info(backup_name):
            """Get information about a specific backup."""
            try:
                if self.restore_manager:
                    backup_info = self.restore_manager.get_backup_info(backup_name)
                    if backup_info:
                        return jsonify(backup_info)
                    else:
                        return jsonify({'error': 'Backup not found'}), 404
                else:
                    return jsonify({'error': 'Restore manager not available'}), 500
            except Exception as e:
                logger.error(f"Failed to get backup info: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/backups/<backup_name>/download', methods=['GET'])
        def download_backup(backup_name):
            """Download a backup file."""
            try:
                if self.restore_manager:
                    backup_path = Path(self.restore_manager.backup_dir) / backup_name
                    if backup_path.exists():
                        return send_file(
                            backup_path,
                            as_attachment=True,
                            download_name=backup_name
                        )
                    else:
                        return jsonify({'error': 'Backup file not found'}), 404
                else:
                    return jsonify({'error': 'Restore manager not available'}), 500
            except Exception as e:
                logger.error(f"Failed to download backup: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/backups/<backup_name>/validate', methods=['POST'])
        def validate_backup(backup_name):
            """Validate a backup file."""
            try:
                if self.restore_manager:
                    is_valid = self.restore_manager.validate_backup(backup_name)
                    return jsonify({
                        'backup_name': backup_name,
                        'valid': is_valid
                    })
                else:
                    return jsonify({'error': 'Restore manager not available'}), 500
            except Exception as e:
                logger.error(f"Failed to validate backup: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/restore', methods=['POST'])
        def restore_backup():
            """Restore a backup."""
            try:
                data = request.get_json()
                backup_name = data.get('backup_name')
                target_dir = data.get('target_dir', '.')
                overwrite = data.get('overwrite', False)
                
                if not backup_name:
                    return jsonify({'error': 'backup_name is required'}), 400
                
                if self.restore_manager:
                    success = self.restore_manager.restore_backup(backup_name, target_dir, overwrite)
                    return jsonify({
                        'backup_name': backup_name,
                        'target_dir': target_dir,
                        'success': success
                    })
                else:
                    return jsonify({'error': 'Restore manager not available'}), 500
            except Exception as e:
                logger.error(f"Failed to restore backup: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/backup', methods=['POST'])
        def create_backup():
            """Create a new backup."""
            try:
                data = request.get_json() or {}
                source_paths = data.get('source_paths', ['.'])
                backup_name = data.get('backup_name')
                description = data.get('description', '')
                
                if self.backup_manager:
                    # Create backup using the manager
                    backup_result = self.backup_manager.create_backup(
                        source_paths=source_paths,
                        backup_name=backup_name,
                        description=description
                    )
                    
                    return jsonify({
                        'success': True,
                        'backup_result': backup_result
                    })
                else:
                    return jsonify({'error': 'Backup manager not available'}), 500
            except Exception as e:
                logger.error(f"Failed to create backup: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/status', methods=['GET'])
        def get_status():
            """Get system status."""
            try:
                status = {
                    'timestamp': datetime.now().isoformat(),
                    'backup_manager_available': self.backup_manager is not None,
                    'restore_manager_available': self.restore_manager is not None,
                    'server_running': self.running
                }
                
                # Add backup manager status if available
                if self.backup_manager:
                    status['backup_manager'] = {
                        'backup_dir': str(self.backup_manager.backup_dir),
                        'total_backups': len(list(self.backup_manager.backup_dir.glob("*.zip")))
                    }
                
                # Add restore manager status if available
                if self.restore_manager:
                    status['restore_manager'] = {
                        'backup_dir': str(self.restore_manager.backup_dir),
                        'available_backups': len(self.restore_manager.list_available_backups())
                    }
                
                return jsonify(status)
            except Exception as e:
                logger.error(f"Failed to get status: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/metrics', methods=['GET'])
        def get_metrics():
            """Get system metrics."""
            try:
                metrics = {
                    'timestamp': datetime.now().isoformat(),
                    'system': self._get_system_metrics()
                }
                
                # Add backup metrics if available
                if self.backup_manager:
                    metrics['backup'] = self._get_backup_metrics()
                
                return jsonify(metrics)
            except Exception as e:
                logger.error(f"Failed to get metrics: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/config', methods=['GET'])
        def get_config():
            """Get current configuration."""
            try:
                config = {
                    'api': {
                        'host': self.config.host,
                        'port': self.config.port,
                        'debug': self.config.debug,
                        'enable_cors': self.config.enable_cors
                    }
                }
                
                # Add backup manager config if available
                if self.backup_manager:
                    config['backup_manager'] = {
                        'backup_dir': str(self.backup_manager.backup_dir)
                    }
                
                # Add restore manager config if available
                if self.restore_manager:
                    config['restore_manager'] = {
                        'backup_dir': str(self.restore_manager.backup_dir)
                    }
                
                return jsonify(config)
            except Exception as e:
                logger.error(f"Failed to get config: {e}")
                return jsonify({'error': str(e)}), 500
    
    def _setup_middleware(self):
        """Setup API middleware."""
        
        @self.app.before_request
        def before_request():
            """Middleware to run before each request."""
            # API key authentication
            if self.config.api_key:
                api_key = request.headers.get('X-API-Key')
                if not api_key or api_key != self.config.api_key:
                    return jsonify({'error': 'Invalid API key'}), 401
            
            # Rate limiting (simple implementation)
            # In production, use a proper rate limiting library
            pass
        
        @self.app.after_request
        def after_request(response):
            """Middleware to run after each request."""
            # Add CORS headers
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-API-Key'
            
            # Add request logging
            logger.info(f"{request.method} {request.path} - {response.status_code}")
            
            return response
    
    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system metrics."""
        try:
            import psutil
            
            return {
                'cpu_usage': psutil.cpu_percent(),
                'memory_usage': psutil.virtual_memory().percent,
                'disk_usage': psutil.disk_usage('/').percent
            }
        except ImportError:
            return {'error': 'psutil not available'}
    
    def _get_backup_metrics(self) -> Dict[str, Any]:
        """Get backup metrics."""
        try:
            if not self.backup_manager:
                return {'error': 'Backup manager not available'}
            
            backup_files = list(self.backup_manager.backup_dir.glob("*.zip"))
            total_size = sum(f.stat().st_size for f in backup_files)
            
            return {
                'total_backups': len(backup_files),
                'total_size': total_size,
                'total_size_mb': total_size / (1024 * 1024),
                'backup_dir': str(self.backup_manager.backup_dir)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def start(self):
        """Start the API server."""
        if self.running:
            logger.warning("API server is already running")
            return
        
        self.running = True
        self.server_thread = threading.Thread(
            target=self._run_server,
            daemon=True
        )
        self.server_thread.start()
        logger.info(f"API server started on {self.config.host}:{self.config.port}")
    
    def stop(self):
        """Stop the API server."""
        self.running = False
        if self.server_thread:
            self.server_thread.join(timeout=5)
        logger.info("API server stopped")
    
    def _run_server(self):
        """Run the Flask server."""
        try:
            self.app.run(
                host=self.config.host,
                port=self.config.port,
                debug=self.config.debug,
                use_reloader=False
            )
        except Exception as e:
            logger.error(f"API server error: {e}")
            self.running = False

def create_api_server(config: APIConfig, backup_manager=None, restore_manager=None) -> BackupAPIServer:
    """Create and return an API server instance."""
    return BackupAPIServer(config, backup_manager, restore_manager)
