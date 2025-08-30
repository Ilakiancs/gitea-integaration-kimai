#!/usr/bin/env python3
"""
CLI Commands

Implementation of command-line interface commands for the Gitea to Kimai integration.
"""

import argparse
import sys
import logging
from typing import Dict, Any, Optional
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.sync import EnhancedGiteeKimaiSync
from src.utils.diagnose import run_diagnostics
from src.utils.report import ResultsViewer
from src.utils.test_connection import test_connections
from src.storage.backup_manager import BackupManager
from src.web.web_dashboard import start_dashboard
from src.api.api import start_api_server
from src.monitoring.health_check import HealthMonitor

logger = logging.getLogger(__name__)

class CLICommands:
    """Command-line interface commands implementation."""
    
    def __init__(self):
        self.sync = None
        self.backup_mgr = None
        self.monitor = None
    
    def run_sync(self, args: argparse.Namespace) -> int:
        """Run synchronization command."""
        try:
            print("Starting Gitea to Kimai synchronization...")
            
            self.sync = EnhancedGiteeKimaiSync()
            
            if args.dry_run:
                print("Running in dry-run mode (no changes will be made)")
                self.sync.read_only = True
            
            if args.repos:
                repos = [repo.strip() for repo in args.repos.split(',')]
                self.sync.repos_to_sync = repos
            
            if args.include_prs:
                self.sync.sync_pull_requests = True
            
            self.sync.run()
            return 0
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            return 1
    
    def run_diagnose(self, args: argparse.Namespace) -> int:
        """Run diagnostics command."""
        try:
            print("Running diagnostics...")
            
            if args.all:
                run_diagnostics(all_checks=True)
            else:
                checks = []
                if args.network:
                    checks.append('network')
                if args.api:
                    checks.append('api')
                if args.database:
                    checks.append('database')
                
                if not checks:
                    checks = ['network', 'api', 'database']  # Default checks
                
                run_diagnostics(checks=checks)
            
            return 0
            
        except Exception as e:
            logger.error(f"Diagnostics failed: {e}")
            return 1
    
    def run_report(self, args: argparse.Namespace) -> int:
        """Show sync report command."""
        try:
            print("Generating sync report...")
            
            viewer = ResultsViewer()
            
            if args.detailed:
                viewer.show_detailed_report()
            else:
                viewer.show_summary()
            
            if args.export:
                viewer.export_report(args.export)
            
            return 0
            
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            return 1
    
    def run_test(self, args: argparse.Namespace) -> int:
        """Test connections command."""
        try:
            print("Testing connections...")
            
            if args.gitea:
                test_connections(gitea_only=True)
            elif args.kimai:
                test_connections(kimai_only=True)
            else:
                test_connections()
            
            return 0
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return 1
    
    def run_backup(self, args: argparse.Namespace) -> int:
        """Backup operations command."""
        try:
            self.backup_mgr = BackupManager()
            
            if args.action == 'create':
                print("Creating backup...")
                self.backup_mgr.create_backup()
            elif args.action == 'list':
                print("Listing backups...")
                self.backup_mgr.list_backups()
            elif args.action == 'restore':
                if not args.file:
                    print("Error: --file is required for restore operation")
                    return 1
                print(f"Restoring from {args.file}...")
                self.backup_mgr.restore_backup(args.file)
            
            return 0
            
        except Exception as e:
            logger.error(f"Backup operation failed: {e}")
            return 1
    
    def run_dashboard(self, args: argparse.Namespace) -> int:
        """Start web dashboard command."""
        try:
            print(f"Starting web dashboard on {args.host}:{args.port}...")
            start_dashboard(host=args.host, port=args.port)
            return 0
            
        except Exception as e:
            logger.error(f"Dashboard failed: {e}")
            return 1
    
    def run_api(self, args: argparse.Namespace) -> int:
        """Start API server command."""
        try:
            print(f"Starting API server on {args.host}:{args.port}...")
            start_api_server(host=args.host, port=args.port)
            return 0
            
        except Exception as e:
            logger.error(f"API server failed: {e}")
            return 1
    
    def run_health(self, args: argparse.Namespace) -> int:
        """Check system health command."""
        try:
            self.monitor = HealthMonitor()
            
            if args.monitor:
                print("Starting health monitoring...")
                self.monitor.start_monitoring()
            else:
                print("Checking system health...")
                status = self.monitor.check_all()
                self.monitor.print_status(status)
            
            return 0
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return 1

def create_commands() -> CLICommands:
    """Create and return CLI commands instance."""
    return CLICommands()
