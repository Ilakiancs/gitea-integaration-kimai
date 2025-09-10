#!/usr/bin/env python3
"""
Next-Generation Backup CLI

Advanced command-line interface for the backup and restore system with
interactive commands, progress bars, and comprehensive options.
"""

import os
import sys
import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.syntax import Syntax

from .restore import AdvancedBackupRestore, create_restore
from .backup_manager import IntelligentBackupManager, BackupConfig, create_backup_manager

console = Console()
logger = logging.getLogger(__name__)

class BackupCLI:
    """Next-generation backup CLI with rich interface."""

    def __init__(self):
        self.restore = create_restore()
        self.backup_manager = None
        self.config = None

    def setup_backup_manager(self, config_path: str = None):
        """Setup backup manager with configuration."""
        if config_path and Path(config_path).exists():
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                self.config = BackupConfig(**config_data)
        else:
            # Default configuration
            self.config = BackupConfig(
                source_paths=['.', 'data', 'config'],
                backup_dir='backups',
                retention_days=30,
                compression_level=6,
                encryption_enabled=False,
                deduplication_enabled=True,
                cloud_sync_enabled=False,
                schedule_enabled=False,
                schedule_interval='daily',
                cloud_provider='local',
                cloud_config={}
            )

        self.backup_manager = create_backup_manager(self.config)

    def display_header(self):
        """Display CLI header."""
        header = """
╔══════════════════════════════════════════════════════════════╗
║                Next-Generation Backup System                 ║
║                    Gitea-Kimai Integration                   ║
╚══════════════════════════════════════════════════════════════╝
        """
        console.print(Panel(header, style="bold blue"))

    def list_backups(self, detailed: bool = False):
        """List available backups with rich formatting."""
        backups = self.restore.list_available_backups(include_integrity=True)

        if not backups:
            console.print("[yellow]No backups found[/yellow]")
            return

        table = Table(title="Available Backups")
        table.add_column("File", style="cyan", no_wrap=True)
        table.add_column("Size", style="magenta")
        table.add_column("Created", style="green")
        table.add_column("Type", style="blue")
        table.add_column("Status", style="red")

        if detailed:
            table.add_column("Files", style="yellow")
            table.add_column("Compression", style="cyan")

        for backup in backups:
            status_style = "green" if backup.get('integrity_status') == 'valid' else "red"
            status_text = backup.get('integrity_status', 'unknown')

            row = [
                backup['file'],
                f"{backup['size'] / (1024*1024):.1f} MB",
                backup.get('created', 'Unknown'),
                backup.get('backup_type', 'full'),
                f"[{status_style}]{status_text}[/{status_style}]"
            ]

            if detailed:
                row.extend([
                    str(backup.get('file_count', 0)),
                    f"{backup.get('compression_ratio', 0)*100:.1f}%"
                ])

            table.add_row(*row)

        console.print(table)

    def create_backup(self, interactive: bool = True):
        """Create a new backup with interactive options."""
        if not self.backup_manager:
            self.setup_backup_manager()

        if interactive:
            console.print("\n[bold]Backup Configuration[/bold]")

            # Source paths
            source_paths = Prompt.ask(
                "Source paths (comma-separated)",
                default=",".join(self.config.source_paths)
            ).split(",")

            # Backup options
            compression_level = int(Prompt.ask(
                "Compression level (1-9)",
                default=str(self.config.compression_level)
            ))

            encryption = Confirm.ask("Enable encryption?", default=self.config.encryption_enabled)
            deduplication = Confirm.ask("Enable deduplication?", default=self.config.deduplication_enabled)

            # Update config
            self.config.source_paths = source_paths
            self.config.compression_level = compression_level
            self.config.encryption_enabled = encryption
            self.config.deduplication_enabled = deduplication

            self.backup_manager = create_backup_manager(self.config)

        # Create backup with progress
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:

            task = progress.add_task("Creating backup...", total=None)

            try:
                success, metadata = self.backup_manager.create_intelligent_backup()

                if success:
                    progress.update(task, description="Backup completed successfully!")
                    console.print(f"\n[green]Backup created: {metadata.backup_id}[/green]")
                    console.print(f"Files: {metadata.file_count}")
                    console.print(f"Size: {metadata.total_size / (1024*1024):.1f} MB")
                    console.print(f"Compression: {metadata.compression_ratio*100:.1f}%")
                else:
                    progress.update(task, description="Backup failed!")
                    console.print("\n[red]Backup creation failed[/red]")

            except Exception as e:
                progress.update(task, description="Backup failed!")
                console.print(f"\n[red]Error: {e}[/red]")

    def restore_backup(self, backup_file: str = None, interactive: bool = True):
        """Restore a backup with interactive options."""
        if not backup_file:
            backups = self.restore.list_available_backups()
            if not backups:
                console.print("[yellow]No backups available for restore[/yellow]")
                return

            # Show backup selection
            console.print("\n[bold]Select backup to restore:[/bold]")
            for i, backup in enumerate(backups[:10]):  # Show first 10
                console.print(f"{i+1}. {backup['file']} ({backup.get('created', 'Unknown')})")

            choice = int(Prompt.ask("Enter backup number", default="1")) - 1
            if 0 <= choice < len(backups):
                backup_file = backups[choice]['file']
            else:
                console.print("[red]Invalid selection[/red]")
                return

        if interactive:
            console.print(f"\n[bold]Restore Configuration[/bold]")
            console.print(f"Backup: {backup_file}")

            # Restore type
            restore_type = Prompt.ask(
                "Restore type",
                choices=["full", "incremental", "selective"],
                default="full"
            )

            # Target directory
            target_dir = Prompt.ask("Target directory", default=".")

            # Options
            overwrite = Confirm.ask("Overwrite existing files?", default=False)
            verify_integrity = Confirm.ask("Verify integrity?", default=True)
            create_rollback = Confirm.ask("Create rollback?", default=True)

            file_patterns = None
            if restore_type == "selective":
                patterns = Prompt.ask("File patterns (comma-separated)", default="*.db,*.json")
                file_patterns = patterns.split(",")
        else:
            restore_type = "full"
            target_dir = "."
            overwrite = False
            verify_integrity = True
            create_rollback = True
            file_patterns = None

        # Perform restore with progress
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:

            task = progress.add_task("Restoring backup...", total=None)

            try:
                success, metadata = self.restore.restore_backup(
                    backup_file=backup_file,
                    target_dir=target_dir,
                    restore_type=restore_type,
                    file_patterns=file_patterns,
                    overwrite=overwrite,
                    verify_integrity=verify_integrity,
                    create_rollback=create_rollback
                )

                if success:
                    progress.update(task, description="Restore completed successfully!")
                    console.print(f"\n[green]Backup restored successfully[/green]")
                    console.print(f"Files restored: {metadata.file_count}")
                    console.print(f"Total size: {metadata.total_size / (1024*1024):.1f} MB")
                    console.print(f"Restore time: {metadata.performance_metrics['restore_time']:.2f}s")
                else:
                    progress.update(task, description="Restore failed!")
                    console.print("\n[red]Restore failed[/red]")

            except Exception as e:
                progress.update(task, description="Restore failed!")
                console.print(f"\n[red]Error: {e}[/red]")

    def validate_backup(self, backup_file: str):
        """Validate a backup file."""
        console.print(f"\n[bold]Validating backup: {backup_file}[/bold]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:

            task = progress.add_task("Validating...", total=None)

            try:
                result = self.restore.validate_backup(backup_file)

                if result['valid']:
                    progress.update(task, description="Validation completed!")
                    console.print(f"\n[green]Backup is valid[/green]")
                    console.print(f"File size: {result['file_size'] / (1024*1024):.1f} MB")
                    console.print(f"File count: {result['file_count']}")

                    if result['warnings']:
                        console.print("\n[yellow]Warnings:[/yellow]")
                        for warning in result['warnings']:
                            console.print(f"  - {warning}")
                else:
                    progress.update(task, description="Validation failed!")
                    console.print(f"\n[red]Backup is invalid: {result.get('error', 'Unknown error')}[/red]")

            except Exception as e:
                progress.update(task, description="Validation failed!")
                console.print(f"\n[red]Error: {e}[/red]")

    def show_statistics(self):
        """Show backup statistics."""
        if not self.backup_manager:
            self.setup_backup_manager()

        stats = self.backup_manager.get_backup_statistics()

        if not stats:
            console.print("[yellow]No backup statistics available[/yellow]")
            return

        console.print("\n[bold]Backup Statistics[/bold]")

        table = Table()
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total backups", str(stats['total_backups']))
        table.add_row("Total size", f"{stats['total_size'] / (1024*1024*1024):.2f} GB")
        table.add_row("Compressed size", f"{stats['total_compressed_size'] / (1024*1024*1024):.2f} GB")
        table.add_row("Space saved", f"{stats['space_saved'] / (1024*1024*1024):.2f} GB")
        table.add_row("Avg compression", f"{stats['average_compression_ratio']*100:.1f}%")
        table.add_row("Cloud sync rate", f"{stats['cloud_sync_success_rate']*100:.1f}%")
        table.add_row("Encryption", "Enabled" if stats['encryption_enabled'] else "Disabled")
        table.add_row("Deduplication", "Enabled" if stats['deduplication_enabled'] else "Disabled")

        console.print(table)

    def show_restore_history(self):
        """Show restore history."""
        history = self.restore.get_restore_history()

        if not history:
            console.print("[yellow]No restore history available[/yellow]")
            return

        console.print("\n[bold]Restore History[/bold]")

        table = Table()
        table.add_column("Backup ID", style="cyan")
        table.add_column("Timestamp", style="green")
        table.add_column("Type", style="blue")
        table.add_column("Files", style="yellow")
        table.add_column("Size", style="magenta")
        table.add_column("Time", style="red")

        for restore in history[-10:]:  # Show last 10
            table.add_row(
                restore.backup_id,
                restore.restore_timestamp,
                restore.restore_type,
                str(restore.file_count),
                f"{restore.total_size / (1024*1024):.1f} MB",
                f"{restore.performance_metrics['restore_time']:.2f}s"
            )

        console.print(table)

    def rollback_last_restore(self):
        """Rollback the last restore operation."""
        if Confirm.ask("Rollback last restore operation?"):
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:

                task = progress.add_task("Rolling back...", total=None)

                try:
                    success = self.restore.rollback_last_restore()

                    if success:
                        progress.update(task, description="Rollback completed!")
                        console.print("\n[green]Rollback completed successfully[/green]")
                    else:
                        progress.update(task, description="Rollback failed!")
                        console.print("\n[red]Rollback failed[/red]")

                except Exception as e:
                    progress.update(task, description="Rollback failed!")
                    console.print(f"\n[red]Error: {e}[/red]")

@click.group()
@click.option('--config', '-c', help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def cli(config, verbose):
    """Next-Generation Backup System CLI."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    cli_instance = BackupCLI()
    if config:
        cli_instance.setup_backup_manager(config)
    else:
        cli_instance.setup_backup_manager()

    # Store instance in context
    click.get_current_context().obj = cli_instance

@cli.command()
@click.option('--detailed', '-d', is_flag=True, help='Show detailed information')
@click.pass_context
def list(ctx, detailed):
    """List available backups."""
    cli_instance = ctx.obj
    cli_instance.display_header()
    cli_instance.list_backups(detailed=detailed)

@cli.command()
@click.option('--interactive', '-i', is_flag=True, help='Interactive mode')
@click.pass_context
def backup(ctx, interactive):
    """Create a new backup."""
    cli_instance = ctx.obj
    cli_instance.display_header()
    cli_instance.create_backup(interactive=interactive)

@cli.command()
@click.argument('backup_file', required=False)
@click.option('--interactive', '-i', is_flag=True, help='Interactive mode')
@click.pass_context
def restore(ctx, backup_file, interactive):
    """Restore a backup."""
    cli_instance = ctx.obj
    cli_instance.display_header()
    cli_instance.restore_backup(backup_file, interactive=interactive)

@cli.command()
@click.argument('backup_file')
@click.pass_context
def validate(ctx, backup_file):
    """Validate a backup file."""
    cli_instance = ctx.obj
    cli_instance.display_header()
    cli_instance.validate_backup(backup_file)

@cli.command()
@click.pass_context
def stats(ctx):
    """Show backup statistics."""
    cli_instance = ctx.obj
    cli_instance.display_header()
    cli_instance.show_statistics()

@cli.command()
@click.pass_context
def history(ctx):
    """Show restore history."""
    cli_instance = ctx.obj
    cli_instance.display_header()
    cli_instance.show_restore_history()

@cli.command()
@click.pass_context
def rollback(ctx):
    """Rollback last restore operation."""
    cli_instance = ctx.obj
    cli_instance.display_header()
    cli_instance.rollback_last_restore()

if __name__ == '__main__':
    cli()
