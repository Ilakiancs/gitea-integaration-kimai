#!/usr/bin/env python3
"""
Gitea to Kimai Integration - Main Entry Point

This is the main entry point for the Gitea to Kimai integration application.
It provides a command-line interface for all the available operations.
"""

import sys
import os
import argparse
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.sync import EnhancedGiteeKimaiSync
from utils.diagnose import run_diagnostics
from utils.report import ResultsViewer
from utils.test_connection import test_connections
from storage.backup_manager import BackupManager
from web.web_dashboard import start_dashboard
from api.api import start_api_server
from monitoring.health_check import HealthMonitor
from utils.notification_system import NotificationManager

def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description="Gitea to Kimai Integration Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py sync                    # Run synchronization
  python main.py sync --dry-run          # Test run without making changes
  python main.py diagnose                # Run diagnostics
  python main.py report                  # Show sync report
  python main.py test                    # Test connections
  python main.py backup create           # Create backup
  python main.py dashboard               # Start web dashboard
  python main.py api                     # Start API server
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Run synchronization')
    sync_parser.add_argument('--dry-run', action='store_true', help='Test run without making changes')
    sync_parser.add_argument('--repos', help='Comma-separated list of repositories to sync')
    sync_parser.add_argument('--include-prs', action='store_true', help='Include pull requests in sync')
    sync_parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    # Diagnose command
    diagnose_parser = subparsers.add_parser('diagnose', help='Run diagnostics')
    diagnose_parser.add_argument('--all', action='store_true', help='Run all diagnostics')
    diagnose_parser.add_argument('--network', action='store_true', help='Check network connectivity')
    diagnose_parser.add_argument('--api', action='store_true', help='Check API connectivity')
    diagnose_parser.add_argument('--database', action='store_true', help='Check database')
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Show sync report')
    report_parser.add_argument('--detailed', action='store_true', help='Show detailed report')
    report_parser.add_argument('--export', help='Export report to file')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test connections')
    test_parser.add_argument('--gitea', action='store_true', help='Test Gitea connection only')
    test_parser.add_argument('--kimai', action='store_true', help='Test Kimai connection only')
    
    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Backup operations')
    backup_parser.add_argument('action', choices=['create', 'list', 'restore'], help='Backup action')
    backup_parser.add_argument('--file', help='Backup file for restore')
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser('dashboard', help='Start web dashboard')
    dashboard_parser.add_argument('--host', default='localhost', help='Host to bind to')
    dashboard_parser.add_argument('--port', type=int, default=8080, help='Port to bind to')
    
    # API command
    api_parser = subparsers.add_parser('api', help='Start API server')
    api_parser.add_argument('--host', default='localhost', help='Host to bind to')
    api_parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    
    # Health command
    health_parser = subparsers.add_parser('health', help='Check system health')
    health_parser.add_argument('--monitor', action='store_true', help='Start health monitoring')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'sync':
            run_sync(args)
        elif args.command == 'diagnose':
            run_diagnose(args)
        elif args.command == 'report':
            run_report(args)
        elif args.command == 'test':
            run_test(args)
        elif args.command == 'backup':
            run_backup(args)
        elif args.command == 'dashboard':
            run_dashboard(args)
        elif args.command == 'api':
            run_api(args)
        elif args.command == 'health':
            run_health(args)
        else:
            print(f"Unknown command: {args.command}")
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def run_sync(args):
    """Run synchronization."""
    print("Starting Gitea to Kimai synchronization...")
    
    sync = EnhancedGiteeKimaiSync()
    
    if args.dry_run:
        print("Running in dry-run mode (no changes will be made)")
        sync.read_only = True
    
    if args.repos:
        repos = [repo.strip() for repo in args.repos.split(',')]
        sync.repos_to_sync = repos
    
    if args.include_prs:
        sync.sync_pull_requests = True
    
    sync.run()

def run_diagnose(args):
    """Run diagnostics."""
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

def run_report(args):
    """Show sync report."""
    print("Generating sync report...")
    
    viewer = ResultsViewer()
    
    if args.detailed:
        viewer.show_detailed_report()
    else:
        viewer.show_summary()
    
    if args.export:
        viewer.export_report(args.export)

def run_test(args):
    """Test connections."""
    print("Testing connections...")
    
    if args.gitea:
        test_connections(gitea_only=True)
    elif args.kimai:
        test_connections(kimai_only=True)
    else:
        test_connections()

def run_backup(args):
    """Run backup operations."""
    backup_mgr = BackupManager()
    
    if args.action == 'create':
        print("Creating backup...")
        backup_mgr.create_backup()
    elif args.action == 'list':
        print("Listing backups...")
        backup_mgr.list_backups()
    elif args.action == 'restore':
        if not args.file:
            print("Error: --file is required for restore operation")
            return
        print(f"Restoring from {args.file}...")
        backup_mgr.restore_backup(args.file)

def run_dashboard(args):
    """Start web dashboard."""
    print(f"Starting web dashboard on {args.host}:{args.port}...")
    start_dashboard(host=args.host, port=args.port)

def run_api(args):
    """Start API server."""
    print(f"Starting API server on {args.host}:{args.port}...")
    start_api_server(host=args.host, port=args.port)

def run_health(args):
    """Check system health."""
    monitor = HealthMonitor()
    
    if args.monitor:
        print("Starting health monitoring...")
        monitor.start_monitoring()
    else:
        print("Checking system health...")
        status = monitor.check_all()
        monitor.print_status(status)

if __name__ == "__main__":
    main()
