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
from cli.security_commands import SecurityCLI

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
    
    # Security command
    security_parser = subparsers.add_parser('security', help='Security operations')
    security_parser.add_argument('action', choices=[
        'create-user', 'authenticate', 'validate-token', 'list-users',
        'change-password', 'update-role', 'generate-password', 'validate-password',
        'generate-token', 'validate-config', 'generate-config', 'health-check'
    ], help='Security action')
    security_parser.add_argument('--username', help='Username')
    security_parser.add_argument('--email', help='Email address')
    security_parser.add_argument('--password', help='Password')
    security_parser.add_argument('--role', choices=['viewer', 'operator', 'admin', 'super_admin'], help='User role')
    security_parser.add_argument('--user-id', help='User ID')
    security_parser.add_argument('--old-password', help='Old password')
    security_parser.add_argument('--new-password', help='New password')
    security_parser.add_argument('--token', help='JWT token')
    security_parser.add_argument('--save-token', help='Save token to file')
    security_parser.add_argument('--length', type=int, help='Length for generated items')
    security_parser.add_argument('--no-symbols', action='store_true', help='Exclude symbols from password')
    security_parser.add_argument('--type', choices=['secure', 'api', 'session', 'verification', 'recovery'], help='Token type')
    security_parser.add_argument('--prefix', help='API key prefix')
    security_parser.add_argument('--config-file', help='Config file path')
    security_parser.add_argument('--output-file', help='Output file path')
    
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
        elif args.command == 'security':
            run_security(args)
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


def run_security(args):
    """Run security operations."""
    cli = SecurityCLI()
    
    # Map arguments to the expected format
    class Args:
        pass
    
    cli_args = Args()
    
    # Set attributes based on the action
    if args.action == 'create-user':
        cli_args.username = args.username
        cli_args.email = args.email
        cli_args.password = args.password
        cli_args.role = args.role or 'viewer'
    elif args.action == 'authenticate':
        cli_args.username = args.username
        cli_args.password = args.password
        cli_args.save_token = args.save_token
    elif args.action == 'validate-token':
        cli_args.token = args.token
    elif args.action == 'change-password':
        cli_args.user_id = args.user_id
        cli_args.old_password = args.old_password
        cli_args.new_password = args.new_password
    elif args.action == 'update-role':
        cli_args.user_id = args.user_id
        cli_args.role = args.role
    elif args.action == 'generate-password':
        cli_args.length = args.length or 16
        cli_args.no_symbols = args.no_symbols
    elif args.action == 'validate-password':
        cli_args.password = args.password
    elif args.action == 'generate-token':
        cli_args.type = args.type or 'secure'
        cli_args.length = args.length or 32
        cli_args.prefix = args.prefix or 'api'
    elif args.action == 'validate-config':
        cli_args.config_file = args.config_file or 'security_config.json'
    elif args.action == 'generate-config':
        cli_args.output_file = args.output_file or 'security_config.json'
    
    # Execute the command
    command_map = {
        'create-user': cli.create_user,
        'authenticate': cli.authenticate_user,
        'validate-token': cli.validate_token,
        'list-users': cli.list_users,
        'change-password': cli.change_password,
        'update-role': cli.update_user_role,
        'generate-password': cli.generate_password,
        'validate-password': cli.validate_password,
        'generate-token': cli.generate_token,
        'validate-config': cli.validate_config,
        'generate-config': cli.generate_config,
        'health-check': cli.health_check
    }
    
    if args.action in command_map:
        return command_map[args.action](cli_args)
    else:
        print(f"Unknown security action: {args.action}")
        return 1

if __name__ == "__main__":
    main()
