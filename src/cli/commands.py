#!/usr/bin/env python3
"""
CLI Commands

Implementation of command-line interface commands for the Gitea to Kimai integration.
"""

import argparse
import sys
import logging
import re
import os
from typing import Dict, Any, Optional, List
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

    def validate_common_args(self, args: argparse.Namespace) -> List[str]:
        """Validate common arguments across commands."""
        errors = []

        # Validate configuration file if specified
        if hasattr(args, 'config') and args.config:
            if not os.path.exists(args.config):
                errors.append(f"Configuration file does not exist: {args.config}")
            elif not args.config.endswith(('.json', '.yml', '.yaml')):
                errors.append(f"Configuration file must be JSON or YAML: {args.config}")

        # Validate log level if specified
        if hasattr(args, 'log_level') and args.log_level:
            valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if args.log_level.upper() not in valid_levels:
                errors.append(f"Invalid log level: {args.log_level}. Must be one of: {', '.join(valid_levels)}")

        # Validate output directory permissions if specified
        if hasattr(args, 'output_dir') and args.output_dir:
            output_path = Path(args.output_dir)
            if output_path.exists() and not os.access(output_path, os.W_OK):
                errors.append(f"Output directory is not writable: {args.output_dir}")
            elif not output_path.exists():
                try:
                    output_path.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    errors.append(f"Cannot create output directory: {args.output_dir} - {e}")

        return errors

    def validate_args(self, args: argparse.Namespace, command: str) -> List[str]:
        """Validate command arguments and return list of errors."""
        errors = []

        if command == 'sync':
            # Validate repository names
            if hasattr(args, 'repos') and args.repos:
                repos = [repo.strip() for repo in args.repos.split(',')]
                for repo in repos:
                    if not re.match(r'^[a-zA-Z0-9_.-]+(/[a-zA-Z0-9_.-]+)?$', repo):
                        errors.append(f"Invalid repository name format: {repo}")

            # Validate sync interval if specified
            if hasattr(args, 'interval') and args.interval:
                if args.interval < 60:
                    errors.append("Sync interval must be at least 60 seconds")
                elif args.interval > 86400:
                    errors.append("Sync interval cannot exceed 24 hours (86400 seconds)")

        elif command == 'backup':
            # Validate backup file path
            if hasattr(args, 'file') and args.file:
                if args.action == 'restore':
                    if not os.path.exists(args.file):
                        errors.append(f"Backup file does not exist: {args.file}")
                    elif not args.file.endswith('.bak'):
                        errors.append("Backup file must have .bak extension")

        elif command in ['dashboard', 'api']:
            # Validate host and port
            if hasattr(args, 'host') and args.host:
                if not re.match(r'^[\w.-]+$', args.host) and args.host != 'localhost':
                    errors.append(f"Invalid host format: {args.host}")

            if hasattr(args, 'port') and args.port:
                if not (1024 <= args.port <= 65535):
                    errors.append(f"Port must be between 1024 and 65535, got: {args.port}")

        elif command == 'report':
            # Validate export format
            if hasattr(args, 'export') and args.export:
                valid_formats = ['.json', '.csv', '.html']
                if not any(args.export.endswith(fmt) for fmt in valid_formats):
                    errors.append(f"Export file must end with one of: {', '.join(valid_formats)}")

        return errors

    def check_prerequisites(self, command: str) -> List[str]:
        """Check command prerequisites and return list of warnings/errors."""
        issues = []

        # Check for required environment variables
        required_env_vars = ['GITEA_TOKEN', 'KIMAI_TOKEN']
        for var in required_env_vars:
            if not os.getenv(var):
                issues.append(f"Warning: Environment variable {var} not set")

        # Check for config file
        config_files = ['config.json', 'config.yml', 'config.yaml']
        if not any(os.path.exists(f) for f in config_files):
            issues.append("Warning: No configuration file found (config.json/yml)")

        # Command-specific checks
        if command == 'sync':
            # Check database accessibility
            db_path = os.getenv('DATABASE_PATH', 'sync.db')
            if os.path.exists(db_path):
                try:
                    # Try to access the database
                    import sqlite3
                    with sqlite3.connect(db_path) as conn:
                        conn.execute("SELECT 1").fetchone()
                except Exception as e:
                    issues.append(f"Warning: Database access issue: {e}")

        return issues

    def run_sync(self, args: argparse.Namespace) -> int:
        """Run synchronization command."""
        try:
            # Validate arguments
            errors = self.validate_args(args, 'sync')
            if errors:
                for error in errors:
                    print(f"Error: {error}")
                return 1

            # Check prerequisites
            issues = self.check_prerequisites('sync')
            for issue in issues:
                print(issue)

            print("Starting Gitea to Kimai synchronization...")

            self.sync = EnhancedGiteeKimaiSync()

            if hasattr(args, 'dry_run') and args.dry_run:
                print("Running in dry-run mode (no changes will be made)")
                self.sync.read_only = True

            if hasattr(args, 'repos') and args.repos:
                repos = [repo.strip() for repo in args.repos.split(',')]
                self.sync.repos_to_sync = repos
                print(f"Syncing repositories: {', '.join(repos)}")

            if hasattr(args, 'include_prs') and args.include_prs:
                self.sync.sync_pull_requests = True
                print("Including pull requests in sync")

            if hasattr(args, 'verbose') and args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)
                print("Verbose logging enabled")

            self.sync.run()
            return 0

        except KeyboardInterrupt:
            print("\nSync interrupted by user")
            return 130
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            return 1

    def run_diagnose(self, args: argparse.Namespace) -> int:
        """Run diagnostics command."""
        try:
            # Validate arguments
            errors = self.validate_common_args(args)

            if errors:
                for error in errors:
                    print(f"Error: {error}")
                return 1

            print("Running diagnostics...")

            if hasattr(args, 'all') and args.all:
                print("Running all diagnostic checks...")
                run_diagnostics(all_checks=True)
            else:
                checks = []
                if hasattr(args, 'network') and args.network:
                    checks.append('network')
                if hasattr(args, 'api') and args.api:
                    checks.append('api')
                if hasattr(args, 'database') and args.database:
                    checks.append('database')
                if hasattr(args, 'config') and args.config:
                    checks.append('config')
                if hasattr(args, 'permissions') and args.permissions:
                    checks.append('permissions')

                if not checks:
                    checks = ['network', 'api', 'database']  # Default checks

                print(f"Running checks: {', '.join(checks)}")
                run_diagnostics(checks=checks)

            return 0

        except Exception as e:
            logger.error(f"Diagnostics failed: {e}")
            return 1

    def run_report(self, args: argparse.Namespace) -> int:
        """Show sync report command."""
        try:
            # Validate arguments
            errors = self.validate_common_args(args)

            # Validate report-specific arguments
            if hasattr(args, 'export') and args.export:
                export_path = Path(args.export)
                if not export_path.suffix in ['.json', '.csv', '.html']:
                    errors.append(f"Export file must have .json, .csv, or .html extension: {args.export}")

                parent_dir = export_path.parent
                if not parent_dir.exists():
                    try:
                        parent_dir.mkdir(parents=True, exist_ok=True)
                    except OSError as e:
                        errors.append(f"Cannot create export directory: {parent_dir} - {e}")

            if hasattr(args, 'days') and args.days:
                if args.days < 1 or args.days > 365:
                    errors.append(f"Days must be between 1 and 365, got: {args.days}")

            if errors:
                for error in errors:
                    print(f"Error: {error}")
                return 1

            print("Generating sync report...")

            viewer = ResultsViewer()

            # Set report parameters
            if hasattr(args, 'days') and args.days:
                viewer.set_date_range(days=args.days)
                print(f"Report covering last {args.days} days")

            if hasattr(args, 'detailed') and args.detailed:
                print("Generating detailed report...")
                viewer.show_detailed_report()
            else:
                print("Generating summary report...")
                viewer.show_summary()

            if hasattr(args, 'export') and args.export:
                print(f"Exporting report to: {args.export}")
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
            # Validate arguments
            errors = self.validate_args(args, 'backup')
            if errors:
                for error in errors:
                    print(f"Error: {error}")
                return 1

            self.backup_mgr = BackupManager()

            if args.action == 'create':
                print("Creating backup...")
                if hasattr(args, 'compress') and args.compress:
                    print("Using compression...")
                self.backup_mgr.create_backup()
            elif args.action == 'list':
                print("Listing backups...")
                self.backup_mgr.list_backups()
            elif args.action == 'restore':
                if not hasattr(args, 'file') or not args.file:
                    print("Error: --file is required for restore operation")
                    return 1

                # Confirm restore operation
                if not hasattr(args, 'force') or not args.force:
                    response = input(f"Are you sure you want to restore from {args.file}? This will overwrite current data. [y/N]: ")
                    if response.lower() != 'y':
                        print("Restore cancelled")
                        return 0

                print(f"Restoring from {args.file}...")
                self.backup_mgr.restore_backup(args.file)

            return 0

        except Exception as e:
            logger.error(f"Backup operation failed: {e}")
            return 1

    def run_dashboard(self, args: argparse.Namespace) -> int:
        """Start web dashboard command."""
        try:
            # Validate arguments
            errors = self.validate_args(args, 'dashboard')
            if errors:
                for error in errors:
                    print(f"Error: {error}")
                return 1

            host = getattr(args, 'host', '127.0.0.1')
            port = getattr(args, 'port', 8080)

            print(f"Starting web dashboard on {host}:{port}...")
            if hasattr(args, 'debug') and args.debug:
                print("Debug mode enabled")

            start_dashboard(host=host, port=port, debug=getattr(args, 'debug', False))
            return 0

        except KeyboardInterrupt:
            print("\nDashboard stopped by user")
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
