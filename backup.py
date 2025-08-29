#!/usr/bin/env python3
"""
Backup and Restore Utility for Gitea-Kimai Integration

This script provides functionality to backup and restore the database, cache,
and configuration files for the Gitea-Kimai Integration.

Usage:
  python backup.py backup [--output DIRECTORY]
  python backup.py restore BACKUP_FILE
  python backup.py list [--detail]
"""

import os
import sys
import argparse
import sqlite3
import json
import shutil
import time
import zipfile
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')
CACHE_DIR = os.getenv('CACHE_DIR', '.cache')
EXPORT_DIR = os.getenv('EXPORT_DIR', 'exports')
BACKUP_DIR = os.getenv('BACKUP_DIR', 'backups')

# Ensure backup directory exists
if not os.path.exists(BACKUP_DIR):
    os.makedirs(BACKUP_DIR)

def get_timestamp():
    """Get a formatted timestamp for backup naming."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def create_backup(output_dir=None):
    """Create a backup of the database, cache, and configuration files."""
    timestamp = get_timestamp()
    if output_dir is None:
        output_dir = BACKUP_DIR

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    backup_file = os.path.join(output_dir, f"gitea_kimai_backup_{timestamp}.zip")

    print(f"Creating backup: {backup_file}")
    backup_info = {
        "timestamp": timestamp,
        "created_at": datetime.now().isoformat(),
        "contents": [],
        "database_stats": {},
        "cache_stats": {},
        "env_vars": []
    }

    try:
        with zipfile.ZipFile(backup_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Backup database if it exists
            if os.path.exists(DATABASE_PATH):
                zipf.write(DATABASE_PATH, os.path.basename(DATABASE_PATH))
                backup_info["contents"].append(os.path.basename(DATABASE_PATH))

                # Get database stats
                try:
                    conn = sqlite3.connect(DATABASE_PATH)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM activity_sync")
                    record_count = cursor.fetchone()[0]
                    backup_info["database_stats"] = {
                        "file_size": os.path.getsize(DATABASE_PATH),
                        "record_count": record_count
                    }
                    conn.close()
                except sqlite3.Error as e:
                    print(f"Warning: Could not get database stats: {e}")
            else:
                print(f"Warning: Database file {DATABASE_PATH} not found, skipping")

            # Backup .env file if it exists
            if os.path.exists('.env'):
                zipf.write('.env', '.env')
                backup_info["contents"].append('.env')

                # List environment variables (without values for security)
                with open('.env', 'r') as env_file:
                    env_vars = []
                    for line in env_file:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            if '=' in line:
                                env_vars.append(line.split('=')[0])
                    backup_info["env_vars"] = env_vars

            # Backup cache directory if it exists
            if os.path.exists(CACHE_DIR):
                cache_files = [f for f in os.listdir(CACHE_DIR) if os.path.isfile(os.path.join(CACHE_DIR, f))]
                backup_info["cache_stats"] = {
                    "file_count": len(cache_files),
                    "total_size": sum(os.path.getsize(os.path.join(CACHE_DIR, f)) for f in cache_files)
                }

                for file in cache_files:
                    file_path = os.path.join(CACHE_DIR, file)
                    zipf.write(file_path, os.path.join('cache', file))

                backup_info["contents"].append(CACHE_DIR)

            # Backup export directory if it exists
            if os.path.exists(EXPORT_DIR):
                export_files = [f for f in os.listdir(EXPORT_DIR) if os.path.isfile(os.path.join(EXPORT_DIR, f))]

                for file in export_files:
                    file_path = os.path.join(EXPORT_DIR, file)
                    zipf.write(file_path, os.path.join('exports', file))

                backup_info["contents"].append(EXPORT_DIR)

            # Add backup info JSON
            zipf.writestr('backup_info.json', json.dumps(backup_info, indent=2))

        print(f"Backup completed successfully: {backup_file}")
        print(f"Database records: {backup_info.get('database_stats', {}).get('record_count', 'N/A')}")
        print(f"Cache files: {backup_info.get('cache_stats', {}).get('file_count', 'N/A')}")
        return backup_file

    except Exception as e:
        print(f"Error creating backup: {e}")
        if os.path.exists(backup_file):
            os.remove(backup_file)
        return None

def restore_backup(backup_file):
    """Restore from a backup file."""
    if not os.path.exists(backup_file):
        print(f"Error: Backup file {backup_file} not found")
        return False

    try:
        print(f"Restoring from backup: {backup_file}")

        # Create a temporary directory for extraction
        temp_dir = os.path.join(BACKUP_DIR, f"temp_restore_{get_timestamp()}")
        os.makedirs(temp_dir, exist_ok=True)

        # Extract backup
        with zipfile.ZipFile(backup_file, 'r') as zipf:
            zipf.extractall(temp_dir)

            # Check for backup info
            backup_info_path = os.path.join(temp_dir, 'backup_info.json')
            if os.path.exists(backup_info_path):
                with open(backup_info_path, 'r') as f:
                    backup_info = json.load(f)
                    print(f"Backup created on: {backup_info.get('created_at', 'Unknown')}")
                    print(f"Contents: {', '.join(backup_info.get('contents', []))}")

            # Restore database
            db_file = os.path.join(temp_dir, os.path.basename(DATABASE_PATH))
            if os.path.exists(db_file):
                # Backup existing database if it exists
                if os.path.exists(DATABASE_PATH):
                    backup_db = f"{DATABASE_PATH}.bak_{get_timestamp()}"
                    shutil.copy2(DATABASE_PATH, backup_db)
                    print(f"Existing database backed up to: {backup_db}")

                # Copy the restored database
                shutil.copy2(db_file, DATABASE_PATH)
                print(f"Database restored to: {DATABASE_PATH}")

            # Restore .env file if it exists in backup
            env_file = os.path.join(temp_dir, '.env')
            if os.path.exists(env_file):
                if os.path.exists('.env'):
                    backup_env = f".env.bak_{get_timestamp()}"
                    shutil.copy2('.env', backup_env)
                    print(f"Existing .env file backed up to: {backup_env}")

                # Ask for confirmation before overwriting .env
                response = input("Do you want to restore the .env file? (y/n): ")
                if response.lower() == 'y':
                    shutil.copy2(env_file, '.env')
                    print(".env file restored")
                else:
                    print("Skipping .env file restoration")

            # Restore cache directory
            cache_dir_in_backup = os.path.join(temp_dir, 'cache')
            if os.path.exists(cache_dir_in_backup):
                if os.path.exists(CACHE_DIR):
                    # Clear existing cache
                    response = input(f"Clear existing cache in {CACHE_DIR}? (y/n): ")
                    if response.lower() == 'y':
                        shutil.rmtree(CACHE_DIR)
                        os.makedirs(CACHE_DIR)
                else:
                    os.makedirs(CACHE_DIR, exist_ok=True)

                # Copy cache files
                for item in os.listdir(cache_dir_in_backup):
                    src = os.path.join(cache_dir_in_backup, item)
                    dst = os.path.join(CACHE_DIR, item)
                    if os.path.isfile(src):
                        shutil.copy2(src, dst)

                print(f"Cache restored to: {CACHE_DIR}")

            # Restore export directory
            export_dir_in_backup = os.path.join(temp_dir, 'exports')
            if os.path.exists(export_dir_in_backup):
                if not os.path.exists(EXPORT_DIR):
                    os.makedirs(EXPORT_DIR, exist_ok=True)

                # Copy export files
                for item in os.listdir(export_dir_in_backup):
                    src = os.path.join(export_dir_in_backup, item)
                    dst = os.path.join(EXPORT_DIR, item)
                    if os.path.isfile(src):
                        shutil.copy2(src, dst)

                print(f"Exports restored to: {EXPORT_DIR}")

        print("Restoration completed successfully")

        # Clean up
        shutil.rmtree(temp_dir)
        return True

    except Exception as e:
        print(f"Error restoring backup: {e}")
        return False

def list_backups(detail=False):
    """List available backups."""
    if not os.path.exists(BACKUP_DIR):
        print(f"Backup directory {BACKUP_DIR} does not exist")
        return

    backups = [f for f in os.listdir(BACKUP_DIR) if f.startswith('gitea_kimai_backup_') and f.endswith('.zip')]

    if not backups:
        print("No backups found")
        return

    print(f"Found {len(backups)} backups:")

    for i, backup in enumerate(sorted(backups, reverse=True)):
        backup_path = os.path.join(BACKUP_DIR, backup)
        backup_size = os.path.getsize(backup_path) / (1024 * 1024)  # Size in MB
        backup_time = os.path.getmtime(backup_path)
        backup_date = datetime.fromtimestamp(backup_time).strftime("%Y-%m-%d %H:%M:%S")

        print(f"{i+1}. {backup} ({backup_size:.2f} MB) - {backup_date}")

        if detail:
            try:
                with zipfile.ZipFile(backup_path, 'r') as zipf:
                    if 'backup_info.json' in zipf.namelist():
                        with zipf.open('backup_info.json') as f:
                            backup_info = json.load(f)
                            print(f"   Created: {backup_info.get('created_at', 'Unknown')}")
                            print(f"   Contents: {', '.join(backup_info.get('contents', []))}")
                            db_stats = backup_info.get('database_stats', {})
                            if db_stats:
                                print(f"   Database: {db_stats.get('record_count', 'N/A')} records, {db_stats.get('file_size', 0) / 1024:.2f} KB")
                            cache_stats = backup_info.get('cache_stats', {})
                            if cache_stats:
                                print(f"   Cache: {cache_stats.get('file_count', 'N/A')} files, {cache_stats.get('total_size', 0) / 1024:.2f} KB")
                    else:
                        file_count = len(zipf.namelist())
                        print(f"   {file_count} files (no detailed info available)")
            except Exception as e:
                print(f"   Error reading backup info: {e}")

        if i < len(backups) - 1:
            print()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Backup and restore utility for Gitea-Kimai integration")
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Create a backup')
    backup_parser.add_argument('--output', help='Output directory for backup')

    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore from a backup')
    restore_parser.add_argument('backup_file', help='Backup file to restore from')

    # List command
    list_parser = subparsers.add_parser('list', help='List available backups')
    list_parser.add_argument('--detail', action='store_true', help='Show detailed information about backups')

    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()

    if args.command == 'backup':
        create_backup(args.output)
    elif args.command == 'restore':
        restore_backup(args.backup_file)
    elif args.command == 'list':
        list_backups(args.detail)
    else:
        print("No command specified. Use one of: backup, restore, list")
        print("Run with -h for help")

if __name__ == "__main__":
    main()
