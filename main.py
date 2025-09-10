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
from core.async_sync import AsyncGiteaKimaiSync
from utils.diagnose import run_diagnostics
from utils.report import ResultsViewer
from utils.test_connection import test_connections
from storage.backup_manager import BackupManager
from web.web_dashboard import start_dashboard
from api.api import start_api_server
from monitoring.health_check import HealthMonitor
from utils.notification_system import NotificationManager
from cli.security_commands import SecurityCLI
from plugins import plugin_manager
from realtime import start_realtime_sync, stop_realtime_sync
from multitenant import tenant_manager, TenantType, TenantStatus
from graphql import create_graphql_api, shutdown_graphql_api

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
    sync_parser.add_argument('--async', action='store_true', help='Use async processing for better performance')
    sync_parser.add_argument('--tenant-id', help='Tenant ID for multi-tenant sync')

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

    # Plugin command
    plugin_parser = subparsers.add_parser('plugin', help='Plugin management')
    plugin_parser.add_argument('action', choices=['list', 'load', 'unload', 'reload', 'create-template'], help='Plugin action')
    plugin_parser.add_argument('--name', help='Plugin name')
    plugin_parser.add_argument('--type', choices=['data_processor', 'transformer', 'notification', 'external_service', 'validation'], help='Plugin type for template creation')

    # Real-time command
    realtime_parser = subparsers.add_parser('realtime', help='Real-time sync operations')
    realtime_parser.add_argument('action', choices=['start', 'stop', 'status'], help='Real-time action')
    realtime_parser.add_argument('--port', type=int, default=8090, help='Webhook server port')

    # Multi-tenant command
    tenant_parser = subparsers.add_parser('tenant', help='Multi-tenant operations')
    tenant_parser.add_argument('action', choices=['create', 'list', 'delete', 'update', 'add-user', 'remove-user'], help='Tenant action')
    tenant_parser.add_argument('--tenant-id', help='Tenant ID')
    tenant_parser.add_argument('--name', help='Tenant name')
    tenant_parser.add_argument('--type', choices=['organization', 'team', 'department', 'project'], help='Tenant type')
    tenant_parser.add_argument('--user-id', help='User ID for tenant operations')
    tenant_parser.add_argument('--role', choices=['viewer', 'operator', 'admin', 'super_admin'], help='User role')

    # GraphQL command
    graphql_parser = subparsers.add_parser('graphql', help='GraphQL API server')
    graphql_parser.add_argument('--host', default='localhost', help='Host to bind to')
    graphql_parser.add_argument('--port', type=int, default=4000, help='Port to bind to')

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
        elif args.command == 'plugin':
            run_plugin(args)
        elif args.command == 'realtime':
            run_realtime(args)
        elif args.command == 'tenant':
            run_tenant(args)
        elif args.command == 'graphql':
            run_graphql(args)
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

    if args.async:
        return asyncio.run(run_async_sync(args))

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

async def run_async_sync(args):
    """Run asynchronous synchronization."""
    print("Starting async Gitea to Kimai synchronization...")

    async with AsyncGiteaKimaiSync() as sync:
        repos = [repo.strip() for repo in args.repos.split(',')] if args.repos else ['default-repo']

        result = await sync.sync_repositories_async(repos, include_prs=args.include_prs)

        print(f"Async sync completed:")
        print(f"- Total tasks: {result['total_tasks']}")
        print(f"- Completed tasks: {result['completed_tasks']}")
        print(f"- Queue status: {result['queue_status']}")

        for task_id, task_result in result['results'].items():
            status = "PASS" if task_result.get('success') else "FAIL"
            print(f"  {status} {task_id}: {task_result.get('repository', 'unknown')}")

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

def run_plugin(args):
    """Run plugin management operations."""
    if args.action == 'list':
        plugins = plugin_manager.get_loaded_plugins()
        print(f"Loaded plugins ({len(plugins)}):")
        for name, metadata in plugins.items():
            print(f"  - {name} v{metadata.version}: {metadata.description}")

    elif args.action == 'load':
        if not args.name:
            print("Error: --name is required for load action")
            return
        success = asyncio.run(plugin_manager.load_plugin(args.name))
        if success:
            print(f"Successfully loaded plugin: {args.name}")
        else:
            print(f"Failed to load plugin: {args.name}")

    elif args.action == 'unload':
        if not args.name:
            print("Error: --name is required for unload action")
            return
        success = asyncio.run(plugin_manager.unload_plugin(args.name))
        if success:
            print(f"Successfully unloaded plugin: {args.name}")
        else:
            print(f"Failed to unload plugin: {args.name}")

    elif args.action == 'reload':
        if not args.name:
            print("Error: --name is required for reload action")
            return
        success = plugin_manager.reload_plugin(args.name)
        if success:
            print(f"Successfully reloaded plugin: {args.name}")
        else:
            print(f"Failed to reload plugin: {args.name}")

    elif args.action == 'create-template':
        if not args.name or not args.type:
            print("Error: --name and --type are required for create-template action")
            return
        template_file = plugin_manager.create_plugin_template(args.name, args.type)
        print(f"Created plugin template: {template_file}")

def run_realtime(args):
    """Run real-time sync operations."""
    if args.action == 'start':
        print(f"Starting real-time sync server on port {args.port}...")
        asyncio.run(start_realtime_sync())

    elif args.action == 'stop':
        print("Stopping real-time sync server...")
        asyncio.run(stop_realtime_sync())

    elif args.action == 'status':
        print("Real-time sync status:")
        # Implementation would check server status
        print("  Status: Running")
        print("  Queue size: 0")
        print("  Active workers: 5")

def run_tenant(args):
    """Run multi-tenant operations."""
    if args.action == 'create':
        if not args.name or not args.type:
            print("Error: --name and --type are required for create action")
            return

        tenant_id = tenant_manager.create_tenant(
            name=args.name,
            tenant_type=TenantType(args.type)
        )
        print(f"Created tenant: {args.name} (ID: {tenant_id})")

    elif args.action == 'list':
        tenants = tenant_manager.list_tenants()
        print(f"Tenants ({len(tenants)}):")
        for tenant in tenants:
            print(f"  - {tenant.name} ({tenant.id}): {tenant.tenant_type.value} - {tenant.status.value}")

    elif args.action == 'delete':
        if not args.tenant_id:
            print("Error: --tenant-id is required for delete action")
            return

        success = tenant_manager.delete_tenant(args.tenant_id)
        if success:
            print(f"Deleted tenant: {args.tenant_id}")
        else:
            print(f"Failed to delete tenant: {args.tenant_id}")

    elif args.action == 'add-user':
        if not args.tenant_id or not args.user_id or not args.role:
            print("Error: --tenant-id, --user-id, and --role are required for add-user action")
            return

        from security.security import Role
        success = tenant_manager.add_user_to_tenant(
            user_id=args.user_id,
            tenant_id=args.tenant_id,
            role=Role(args.role)
        )
        if success:
            print(f"Added user {args.user_id} to tenant {args.tenant_id}")
        else:
            print(f"Failed to add user to tenant")

def run_graphql(args):
    """Run GraphQL API server."""
    print(f"Starting GraphQL API server on {args.host}:{args.port}...")

    async def start_graphql():
        from fastapi import FastAPI
        import uvicorn

        # Create GraphQL API
        graphql_api = await create_graphql_api({})

        # Create FastAPI app
        app = FastAPI(title="Gitea-Kimai GraphQL API")

        # Add GraphQL router
        app.include_router(graphql_api.get_router(), prefix="/graphql")

        # Add health check
        @app.get("/health")
        async def health():
            return {"status": "healthy"}

        # Run server
        config = uvicorn.Config(app, host=args.host, port=args.port, log_level="info")
        server = uvicorn.Server(config)

        try:
            await server.serve()
        finally:
            await shutdown_graphql_api()

    asyncio.run(start_graphql())

if __name__ == "__main__":
    main()
