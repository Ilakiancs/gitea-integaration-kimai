#!/usr/bin/env python3
"""
Diagnostic tool for Gitea-Kimai integration

This script performs various diagnostic tests on the environment, configuration,
and connectivity to help troubleshoot issues with the Gitea-Kimai integration.

Usage:
  python diagnose.py
  python diagnose.py --database
  python diagnose.py --network
  python diagnose.py --all
"""

import os
import sys
import json
import time
import sqlite3
import argparse
import platform
import requests
import socket
import logging
import urllib.parse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration variables
GITEA_URL = os.getenv('GITEA_URL')
GITEA_TOKEN = os.getenv('GITEA_TOKEN')
GITEA_ORGANIZATION = os.getenv('GITEA_ORGANIZATION')
KIMAI_URL = os.getenv('KIMAI_URL')
KIMAI_USERNAME = os.getenv('KIMAI_USERNAME')
KIMAI_PASSWORD = os.getenv('KIMAI_PASSWORD')
KIMAI_TOKEN = os.getenv('KIMAI_TOKEN')
REPOS_TO_SYNC = os.getenv('REPOS_TO_SYNC', '').split(',')
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')
READ_ONLY_MODE = os.getenv('READ_ONLY_MODE', 'false').lower() == 'true'
CACHE_ENABLED = os.getenv('CACHE_ENABLED', 'true').lower() == 'true'
CACHE_DIR = os.getenv('CACHE_DIR', '.cache')
EXPORT_ENABLED = os.getenv('EXPORT_ENABLED', 'false').lower() == 'true'
EXPORT_DIR = os.getenv('EXPORT_DIR', 'exports')

def print_header(title):
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)

def print_section(title):
    """Print a formatted section title."""
    print(f"\n--- {title} ---")

def print_result(message, success=True):
    """Print a formatted result message."""
    if success:
        print(f"[OK] {message}")
    else:
        print(f"[ERROR] {message}")

def print_warning(message):
    """Print a formatted warning message."""
    print(f"[WARNING] {message}")

def print_info(message):
    """Print a formatted info message."""
    print(f"[INFO] {message}")

def check_system_info():
    """Check system information."""
    print_header("System Information")

    print(f"Date and Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Operating System: {platform.system()} {platform.release()}")
    print(f"Python Version: {platform.python_version()}")
    print(f"Script Directory: {os.path.dirname(os.path.abspath(__file__))}")

    # Check available disk space
    try:
        if platform.system() == 'Windows':
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(os.getcwd()), None, None, ctypes.pointer(free_bytes))
            free_space = free_bytes.value / (1024 * 1024 * 1024)  # Convert to GB
        else:
            stats = os.statvfs(os.getcwd())
            free_space = stats.f_frsize * stats.f_bavail / (1024 * 1024 * 1024)  # Convert to GB

        print(f"Free Disk Space: {free_space:.2f} GB")
    except Exception as e:
        print(f"Error checking disk space: {e}")

    # Check memory
    try:
        import psutil
        vm = psutil.virtual_memory()
        print(f"Total Memory: {vm.total / (1024 * 1024 * 1024):.2f} GB")
        print(f"Available Memory: {vm.available / (1024 * 1024 * 1024):.2f} GB")
    except ImportError:
        print("Memory info not available (psutil not installed)")
    except Exception as e:
        print(f"Error checking memory: {e}")

def check_dependencies():
    """Check Python dependencies."""
    print_header("Dependencies Check")

    required_packages = [
        'requests', 'python-dotenv', 'sqlite3', 'pandas', 'openpyxl', 'PyYAML',
        'beautifulsoup4', 'argparse', 'python-dateutil'
    ]

    try:
        import pkg_resources
        for package in required_packages:
            try:
                version = pkg_resources.get_distribution(package).version
                print_result(f"{package} {version} installed")
            except pkg_resources.DistributionNotFound:
                print_result(f"{package} not installed", False)
    except ImportError:
        print_warning("pkg_resources not available, can't check package versions")
        import importlib
        for package in required_packages:
            try:
                importlib.import_module(package)
                print_result(f"{package} installed")
            except ImportError:
                print_result(f"{package} not installed", False)

def check_environment():
    """Check environment variables and configuration."""
    print_header("Environment Configuration")

    required_vars = [
        ('GITEA_URL', GITEA_URL),
        ('GITEA_TOKEN', GITEA_TOKEN, True),
        ('GITEA_ORGANIZATION', GITEA_ORGANIZATION),
        ('KIMAI_URL', KIMAI_URL)
    ]

    # Check required variables
    for var_info in required_vars:
        if len(var_info) == 3:
            name, value, is_secret = var_info
        else:
            name, value = var_info
            is_secret = False

        if value:
            if is_secret:
                masked_value = value[:4] + '*' * (len(value) - 4) if len(value) > 4 else '****'
                print_result(f"{name}: {masked_value}")
            else:
                print_result(f"{name}: {value}")
        else:
            print_result(f"{name} not set", False)

    # Check Kimai authentication
    if KIMAI_TOKEN:
        print_result("Kimai authentication: API token")
    elif KIMAI_USERNAME and KIMAI_PASSWORD:
        print_result("Kimai authentication: Username/Password")
    else:
        print_result("No Kimai authentication method configured", False)

    # Check repositories
    valid_repos = [repo.strip() for repo in REPOS_TO_SYNC if repo.strip()]
    if valid_repos:
        print_result(f"Repositories to sync: {', '.join(valid_repos)}")
    else:
        print_result("No repositories configured for synchronization", False)

    # Check optional settings
    print_section("Optional Settings")
    print_info(f"Database Path: {DATABASE_PATH}")
    print_info(f"Read-only Mode: {READ_ONLY_MODE}")
    print_info(f"Cache Enabled: {CACHE_ENABLED} (Directory: {CACHE_DIR})")
    print_info(f"Export Enabled: {EXPORT_ENABLED} (Directory: {EXPORT_DIR})")

def check_directories():
    """Check required directories."""
    print_header("Directory Check")

    directories = [
        (CACHE_DIR, CACHE_ENABLED, "Cache"),
        (EXPORT_DIR, EXPORT_ENABLED, "Export")
    ]

    for directory, is_enabled, name in directories:
        if is_enabled:
            if os.path.exists(directory):
                if os.path.isdir(directory):
                    print_result(f"{name} directory exists: {directory}")
                    # Check permissions
                    try:
                        test_file = os.path.join(directory, 'test_write_permission')
                        with open(test_file, 'w') as f:
                            f.write('test')
                        os.remove(test_file)
                        print_result(f"{name} directory is writable")
                    except Exception as e:
                        print_result(f"{name} directory is not writable: {e}", False)
                else:
                    print_result(f"{name} path exists but is not a directory: {directory}", False)
            else:
                print_warning(f"{name} directory does not exist: {directory} (will be created when needed)")
        else:
            print_info(f"{name} is disabled, skipping directory check")

    # Check for the database file
    db_path = Path(DATABASE_PATH)
    if db_path.exists():
        if db_path.is_file():
            print_result(f"Database file exists: {DATABASE_PATH}")
            # Check if file is readable
            try:
                conn = sqlite3.connect(DATABASE_PATH)
                conn.close()
                print_result("Database file is readable")
            except sqlite3.Error as e:
                print_result(f"Database file exists but cannot be read: {e}", False)
        else:
            print_result(f"Database path exists but is not a file: {DATABASE_PATH}", False)
    else:
        print_warning(f"Database file does not exist: {DATABASE_PATH} (will be created when needed)")

def check_database():
    """Check database schema and contents."""
    print_header("Database Analysis")

    if not os.path.exists(DATABASE_PATH):
        print_warning(f"Database file {DATABASE_PATH} does not exist")
        return

    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check if activity_sync table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activity_sync'")
        if not cursor.fetchone():
            print_result("activity_sync table doesn't exist", False)
            return

        # Get table schema
        cursor.execute("PRAGMA table_info(activity_sync)")
        columns = cursor.fetchall()

        print_section("Database Schema")
        for col in columns:
            print_info(f"Column: {col['name']} ({col['type']})")

        # Get record count
        cursor.execute("SELECT COUNT(*) as count FROM activity_sync")
        count = cursor.fetchone()['count']
        print_section("Database Statistics")
        print_info(f"Total records: {count}")

        if count > 0:
            # Get repository distribution
            cursor.execute("""
                SELECT repository_name, COUNT(*) as count
                FROM activity_sync
                GROUP BY repository_name
                ORDER BY count DESC
            """)
            repos = cursor.fetchall()
            print_info("Records by repository:")
            for repo in repos:
                print(f"  - {repo['repository_name']}: {repo['count']} records")

            # Get state distribution
            cursor.execute("""
                SELECT issue_state, COUNT(*) as count
                FROM activity_sync
                GROUP BY issue_state
            """)
            states = cursor.fetchall()
            print_info("Records by state:")
            for state in states:
                print(f"  - {state['issue_state']}: {state['count']} records")

            # Get latest sync time
            cursor.execute("SELECT MAX(updated_at) as last_sync FROM activity_sync")
            last_sync = cursor.fetchone()['last_sync']
            print_info(f"Last sync time: {last_sync}")

            # Sample records
            cursor.execute("""
                SELECT * FROM activity_sync
                ORDER BY updated_at DESC
                LIMIT 3
            """)
            print_section("Recent Sync Records (sample)")
            for i, row in enumerate(cursor.fetchall()):
                print(f"Record {i+1}:")
                print(f"  Repository: {row['repository_name']}")
                print(f"  Issue: #{row['issue_number']} - {row['issue_title'][:50]}...")
                print(f"  Gitea ID: {row['gitea_issue_id']}, Kimai Activity ID: {row['kimai_activity_id']}")
                print(f"  Updated: {row['updated_at']}")
                print()

        conn.close()

    except sqlite3.Error as e:
        print_result(f"Database error: {e}", False)

def check_network_connectivity():
    """Check network connectivity to Gitea and Kimai instances."""
    print_header("Network Connectivity")

    if not GITEA_URL or not KIMAI_URL:
        print_warning("Missing URL configuration, skipping network check")
        return

    endpoints = [
        (GITEA_URL, "Gitea"),
        (KIMAI_URL, "Kimai")
    ]

    for url, name in endpoints:
        print_section(f"{name} Connectivity")

        # Parse URL
        try:
            parsed_url = urllib.parse.urlparse(url)
            hostname = parsed_url.netloc
            port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)

            # DNS resolution test
            try:
                start_time = time.time()
                ip_address = socket.gethostbyname(hostname)
                dns_time = time.time() - start_time
                print_result(f"DNS resolution: {hostname} -> {ip_address} ({dns_time:.3f}s)")
            except socket.gaierror as e:
                print_result(f"DNS resolution failed: {e}", False)
                continue

            # TCP connection test
            try:
                start_time = time.time()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((hostname, port))
                s.close()
                tcp_time = time.time() - start_time
                print_result(f"TCP connection to {hostname}:{port} successful ({tcp_time:.3f}s)")
            except socket.error as e:
                print_result(f"TCP connection failed: {e}", False)
                continue

            # HTTP(S) request test
            try:
                start_time = time.time()
                response = requests.get(url, timeout=10)
                http_time = time.time() - start_time
                print_result(f"HTTP request successful: {response.status_code} ({http_time:.3f}s)")
            except requests.RequestException as e:
                print_result(f"HTTP request failed: {e}", False)

        except Exception as e:
            print_result(f"Error checking {name} connectivity: {e}", False)

def check_api_access():
    """Check API access to Gitea and Kimai."""
    print_header("API Access Check")

    # Check Gitea API
    print_section("Gitea API")
    if not GITEA_URL or not GITEA_TOKEN:
        print_warning("Missing Gitea configuration, skipping API check")
    else:
        headers = {
            'Authorization': f'token {GITEA_TOKEN}',
            'Accept': 'application/json'
        }

        # Test endpoints
        endpoints = [
            ("/api/v1/version", "Version info"),
            ("/api/v1/user", "User info"),
            (f"/api/v1/orgs/{GITEA_ORGANIZATION}", "Organization info"),
            (f"/api/v1/orgs/{GITEA_ORGANIZATION}/repos", "Repository list")
        ]

        for endpoint, description in endpoints:
            try:
                response = requests.get(f"{GITEA_URL}{endpoint}", headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list):
                        print_result(f"{description}: Success ({len(data)} items)")
                    else:
                        print_result(f"{description}: Success")
                else:
                    print_result(f"{description}: Failed with status {response.status_code}", False)
            except Exception as e:
                print_result(f"{description}: Error - {e}", False)

    # Check Kimai API
    print_section("Kimai API")
    if not KIMAI_URL:
        print_warning("Missing Kimai URL, skipping API check")
        return

    session = requests.Session()

    # Set up authentication
    if KIMAI_TOKEN:
        session.headers.update({
            'Authorization': f'Bearer {KIMAI_TOKEN}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    elif KIMAI_USERNAME and KIMAI_PASSWORD:
        session.auth = (KIMAI_USERNAME, KIMAI_PASSWORD)
        session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    else:
        print_warning("Missing Kimai authentication, skipping API check")
        return

    # Test endpoints
    endpoints = [
        ("/api/version", "Version info"),
        ("/api/users/me", "User info"),
        ("/api/projects", "Projects list"),
        ("/api/activities", "Activities list")
    ]

    for endpoint, description in endpoints:
        try:
            response = session.get(f"{KIMAI_URL}{endpoint}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    print_result(f"{description}: Success ({len(data)} items)")
                else:
                    print_result(f"{description}: Success")
            else:
                print_result(f"{description}: Failed with status {response.status_code}", False)
        except Exception as e:
            print_result(f"{description}: Error - {e}", False)

def check_cache_status():
    """Check cache status."""
    print_header("Cache Status")

    if not CACHE_ENABLED:
        print_info("Caching is disabled")
        return

    if not os.path.exists(CACHE_DIR):
        print_warning(f"Cache directory {CACHE_DIR} does not exist")
        return

    # Check cache files
    cache_files = [f for f in os.listdir(CACHE_DIR) if f.endswith('.cache')]

    print_info(f"Total cache files: {len(cache_files)}")

    if not cache_files:
        print_info("No cache files found")
        return

    # Get total size
    total_size = sum(os.path.getsize(os.path.join(CACHE_DIR, f)) for f in cache_files)
    print_info(f"Total cache size: {total_size / 1024:.2f} KB")

    # Get oldest and newest cache files
    file_times = [(f, os.path.getmtime(os.path.join(CACHE_DIR, f))) for f in cache_files]
    oldest_file = min(file_times, key=lambda x: x[1])
    newest_file = max(file_times, key=lambda x: x[1])

    print_info(f"Oldest cache file: {oldest_file[0]} ({datetime.fromtimestamp(oldest_file[1]).strftime('%Y-%m-%d %H:%M:%S')})")
    print_info(f"Newest cache file: {newest_file[0]} ({datetime.fromtimestamp(newest_file[1]).strftime('%Y-%m-%d %H:%M:%S')})")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Diagnostic tool for Gitea-Kimai integration")
    parser.add_argument("--all", action="store_true", help="Run all diagnostic tests")
    parser.add_argument("--system", action="store_true", help="Check system information")
    parser.add_argument("--dependencies", action="store_true", help="Check dependencies")
    parser.add_argument("--environment", action="store_true", help="Check environment configuration")
    parser.add_argument("--directories", action="store_true", help="Check directories")
    parser.add_argument("--database", action="store_true", help="Check database")
    parser.add_argument("--network", action="store_true", help="Check network connectivity")
    parser.add_argument("--api", action="store_true", help="Check API access")
    parser.add_argument("--cache", action="store_true", help="Check cache status")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()

    # If no specific checks are requested, run the basic ones
    if not any([
        args.all, args.system, args.dependencies, args.environment,
        args.directories, args.database, args.network, args.api, args.cache
    ]):
        args.environment = True
        args.directories = True
        args.dependencies = True

    # If --all is specified, run all checks
    if args.all:
        args.system = True
        args.dependencies = True
        args.environment = True
        args.directories = True
        args.database = True
        args.network = True
        args.api = True
        args.cache = True

    # Run the requested checks
    if args.system:
        check_system_info()

    if args.dependencies:
        check_dependencies()

    if args.environment:
        check_environment()

    if args.directories:
        check_directories()

    if args.database:
        check_database()

    if args.network:
        check_network_connectivity()

    if args.api:
        check_api_access()

    if args.cache:
        check_cache_status()

    print("\nDiagnostic check completed")

if __name__ == "__main__":
    main()
