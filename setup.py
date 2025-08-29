#!/usr/bin/env python3
"""
Interactive Setup Wizard for Gitea-Kimai Integration

This script provides an interactive wizard to set up the Gitea-Kimai
integration by guiding the user through configuration options and
creating the necessary files and directories.

Usage:
  python setup.py
"""

import os
import sys
import re
import getpass
import shutil
import argparse
import sqlite3
import requests
from pathlib import Path
from dotenv import load_dotenv

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text):
    """Print a formatted header."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{text}{Colors.ENDC}")
    print("-" * len(text))

def print_success(text):
    """Print a success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")

def print_error(text):
    """Print an error message."""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}")

def print_warning(text):
    """Print a warning message."""
    print(f"{Colors.YELLOW}! {text}{Colors.ENDC}")

def print_info(text):
    """Print an info message."""
    print(f"{Colors.CYAN}ℹ {text}{Colors.ENDC}")

def get_input(prompt, default=None, password=False, validate=None, error_msg=None):
    """Get input from the user with validation."""
    while True:
        if default:
            display_prompt = f"{prompt} [{default}]: "
        else:
            display_prompt = f"{prompt}: "

        if password:
            value = getpass.getpass(display_prompt)
        else:
            value = input(display_prompt)

        # Use default if input is empty
        if not value and default:
            value = default

        # Validate input if validator function is provided
        if validate and not validate(value):
            if error_msg:
                print_error(error_msg)
            continue

        return value

def validate_url(url):
    """Validate URL format."""
    pattern = re.compile(
        r'^https?://'
        r'([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?'
        r'(/[a-zA-Z0-9_.-~%&:=+,$#!*?()]*)*/?$'
    )
    return bool(pattern.match(url))

def validate_not_empty(value):
    """Validate that value is not empty."""
    return bool(value and value.strip())

def test_gitea_connection(url, token, organization):
    """Test connection to Gitea API."""
    print_info("Testing Gitea connection...")

    try:
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/json'
        }

        # Test basic API access
        response = requests.get(f"{url}/api/v1/version", headers=headers, timeout=10)
        if response.status_code != 200:
            print_error(f"Could not connect to Gitea API. Status code: {response.status_code}")
            return False

        version_info = response.json()
        print_success(f"Gitea API accessible - Version: {version_info.get('version', 'unknown')}")

        # Test organization access
        response = requests.get(f"{url}/api/v1/orgs/{organization}", headers=headers, timeout=10)
        if response.status_code != 200:
            print_error(f"Organization '{organization}' not accessible. Status code: {response.status_code}")
            return False

        print_success(f"Organization '{organization}' accessible")

        # Test repository access
        response = requests.get(f"{url}/api/v1/orgs/{organization}/repos", headers=headers, timeout=10)
        if response.status_code != 200:
            print_error(f"Could not list repositories. Status code: {response.status_code}")
            return False

        repos = response.json()
        print_success(f"Repository access confirmed - Found {len(repos)} repositories")

        return True

    except requests.RequestException as e:
        print_error(f"Connection failed: {e}")
        return False

def test_kimai_connection(url, username=None, password=None, token=None):
    """Test connection to Kimai API."""
    print_info("Testing Kimai connection...")

    try:
        session = requests.Session()

        # Set up authentication
        if token:
            session.headers.update({
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            auth_method = "API token"
        elif username and password:
            session.auth = (username, password)
            session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            auth_method = "username/password"
        else:
            print_error("No authentication method provided")
            return False

        # Test API access
        response = session.get(f"{url}/api/version", timeout=10)
        if response.status_code != 200:
            print_error(f"Could not connect to Kimai API using {auth_method}. Status code: {response.status_code}")
            return False

        version_info = response.json()
        print_success(f"Kimai API accessible - Version: {version_info.get('version', 'unknown')}")

        # Test user permissions
        response = session.get(f"{url}/api/users/me", timeout=10)
        if response.status_code != 200:
            print_error(f"Could not access user information. Status code: {response.status_code}")
            return False

        user_info = response.json()
        print_success(f"Authenticated as: {user_info.get('username', user_info.get('alias', 'unknown'))}")

        # Test projects access
        response = session.get(f"{url}/api/projects", timeout=10)
        if response.status_code != 200:
            print_error(f"Could not access projects. Status code: {response.status_code}")
            print_warning("Make sure your user has the 'view_project' permission")
            return False

        projects = response.json()
        print_success(f"Projects accessible - Found {len(projects)} projects")

        return True

    except requests.RequestException as e:
        print_error(f"Connection failed: {e}")
        return False

def get_gitea_repositories(url, token, organization):
    """Get available repositories from Gitea."""
    try:
        headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/json'
        }

        response = requests.get(f"{url}/api/v1/orgs/{organization}/repos", headers=headers, timeout=10)
        response.raise_for_status()

        repos = response.json()
        return [repo['name'] for repo in repos]

    except requests.RequestException as e:
        print_error(f"Failed to fetch repositories: {e}")
        return []

def setup_environment():
    """Set up the environment configuration."""
    print_header("Gitea-Kimai Integration Setup Wizard")
    print("This wizard will help you set up the Gitea-Kimai integration.\n")

    # Check if .env already exists
    if os.path.exists('.env'):
        overwrite = input("An existing configuration file (.env) was found. Overwrite? (y/n): ")
        if overwrite.lower() != 'y':
            print_info("Setup cancelled. Using existing configuration.")
            return False

    config = {}

    # Gitea configuration
    print_header("Gitea Configuration")

    config['GITEA_URL'] = get_input(
        "Gitea URL (e.g., https://gitea.yourdomain.com)",
        validate=validate_url,
        error_msg="Please enter a valid URL (starting with http:// or https://)"
    )

    config['GITEA_TOKEN'] = get_input(
        "Gitea API token",
        password=True,
        validate=validate_not_empty,
        error_msg="API token cannot be empty"
    )

    config['GITEA_ORGANIZATION'] = get_input(
        "Gitea organization or username",
        validate=validate_not_empty,
        error_msg="Organization name cannot be empty"
    )

    # Test Gitea connection
    connection_ok = test_gitea_connection(
        config['GITEA_URL'],
        config['GITEA_TOKEN'],
        config['GITEA_ORGANIZATION']
    )

    if not connection_ok:
        retry = input("Gitea connection test failed. Continue anyway? (y/n): ")
        if retry.lower() != 'y':
            print_info("Setup cancelled.")
            return False

    # Get repositories
    if connection_ok:
        available_repos = get_gitea_repositories(
            config['GITEA_URL'],
            config['GITEA_TOKEN'],
            config['GITEA_ORGANIZATION']
        )

        if available_repos:
            print_info(f"Available repositories ({len(available_repos)}):")
            for i, repo in enumerate(available_repos):
                print(f"  {i+1}. {repo}")

            selected_repos = input("Enter repository numbers to sync (comma-separated, or 'all'): ")

            if selected_repos.lower() == 'all':
                config['REPOS_TO_SYNC'] = ','.join(available_repos)
            else:
                try:
                    indices = [int(idx.strip()) - 1 for idx in selected_repos.split(',')]
                    selected = [available_repos[idx] for idx in indices if 0 <= idx < len(available_repos)]
                    if selected:
                        config['REPOS_TO_SYNC'] = ','.join(selected)
                    else:
                        config['REPOS_TO_SYNC'] = input("Enter repository names to sync (comma-separated): ")
                except (ValueError, IndexError):
                    config['REPOS_TO_SYNC'] = input("Enter repository names to sync (comma-separated): ")
        else:
            config['REPOS_TO_SYNC'] = input("Enter repository names to sync (comma-separated): ")
    else:
        config['REPOS_TO_SYNC'] = input("Enter repository names to sync (comma-separated): ")

    # Kimai configuration
    print_header("Kimai Configuration")

    config['KIMAI_URL'] = get_input(
        "Kimai URL (e.g., https://kimai.yourdomain.com)",
        validate=validate_url,
        error_msg="Please enter a valid URL (starting with http:// or https://)"
    )

    auth_method = get_input(
        "Authentication method (token or password)",
        default="token"
    )

    if auth_method.lower() == 'token':
        config['KIMAI_TOKEN'] = get_input(
            "Kimai API token",
            password=True,
            validate=validate_not_empty,
            error_msg="API token cannot be empty"
        )

        # Test Kimai connection
        connection_ok = test_kimai_connection(
            config['KIMAI_URL'],
            token=config['KIMAI_TOKEN']
        )
    else:
        config['KIMAI_USERNAME'] = get_input(
            "Kimai username",
            validate=validate_not_empty,
            error_msg="Username cannot be empty"
        )

        config['KIMAI_PASSWORD'] = get_input(
            "Kimai password",
            password=True,
            validate=validate_not_empty,
            error_msg="Password cannot be empty"
        )

        # Test Kimai connection
        connection_ok = test_kimai_connection(
            config['KIMAI_URL'],
            username=config['KIMAI_USERNAME'],
            password=config['KIMAI_PASSWORD']
        )

    if not connection_ok:
        retry = input("Kimai connection test failed. Continue anyway? (y/n): ")
        if retry.lower() != 'y':
            print_info("Setup cancelled.")
            return False

    # Advanced configuration
    print_header("Advanced Configuration")

    advanced = input("Configure advanced settings? (y/n): ")

    if advanced.lower() == 'y':
        config['DATABASE_PATH'] = get_input("Database path", default="sync.db")
        config['LOG_LEVEL'] = get_input("Log level (DEBUG, INFO, WARNING, ERROR)", default="INFO")
        config['READ_ONLY_MODE'] = get_input("Read-only mode (true/false)", default="false")
        config['SYNC_PULL_REQUESTS'] = get_input("Sync pull requests (true/false)", default="false")
        config['RATE_LIMIT_ENABLED'] = get_input("Enable API rate limiting (true/false)", default="true")
        config['CACHE_ENABLED'] = get_input("Enable data caching (true/false)", default="true")
        config['EXPORT_ENABLED'] = get_input("Enable data export (true/false)", default="false")
    else:
        # Set defaults for advanced settings
        config['DATABASE_PATH'] = "sync.db"
        config['LOG_LEVEL'] = "INFO"
        config['READ_ONLY_MODE'] = "false"
        config['SYNC_PULL_REQUESTS'] = "false"
        config['RATE_LIMIT_ENABLED'] = "true"
        config['CACHE_ENABLED'] = "true"
        config['EXPORT_ENABLED'] = "false"

    # Write configuration to .env file
    with open('.env', 'w') as f:
        f.write("# Gitea-Kimai Integration Configuration\n")
        f.write("# Generated by setup wizard\n\n")

        f.write("# Gitea Configuration\n")
        f.write(f"GITEA_URL={config['GITEA_URL']}\n")
        f.write(f"GITEA_TOKEN={config['GITEA_TOKEN']}\n")
        f.write(f"GITEA_ORGANIZATION={config['GITEA_ORGANIZATION']}\n\n")

        f.write("# Kimai Configuration\n")
        f.write(f"KIMAI_URL={config['KIMAI_URL']}\n")
        if 'KIMAI_TOKEN' in config:
            f.write(f"KIMAI_TOKEN={config['KIMAI_TOKEN']}\n")
        if 'KIMAI_USERNAME' in config:
            f.write(f"KIMAI_USERNAME={config['KIMAI_USERNAME']}\n")
        if 'KIMAI_PASSWORD' in config:
            f.write(f"KIMAI_PASSWORD={config['KIMAI_PASSWORD']}\n")
        f.write("\n")

        f.write("# Repositories to Sync\n")
        f.write(f"REPOS_TO_SYNC={config['REPOS_TO_SYNC']}\n\n")

        f.write("# Advanced Configuration\n")
        f.write(f"DATABASE_PATH={config['DATABASE_PATH']}\n")
        f.write(f"LOG_LEVEL={config['LOG_LEVEL']}\n")
        f.write(f"READ_ONLY_MODE={config['READ_ONLY_MODE']}\n")
        f.write(f"SYNC_PULL_REQUESTS={config['SYNC_PULL_REQUESTS']}\n")
        f.write(f"RATE_LIMIT_ENABLED={config['RATE_LIMIT_ENABLED']}\n")
        f.write(f"CACHE_ENABLED={config['CACHE_ENABLED']}\n")
        f.write(f"EXPORT_ENABLED={config['EXPORT_ENABLED']}\n")

    print_success("Configuration saved to .env file")
    return True

def create_directories():
    """Create necessary directories."""
    print_header("Creating Directories")

    directories = [
        '.cache',
        'exports',
        'backups'
    ]

    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print_success(f"Created directory: {directory}")
        else:
            print_info(f"Directory already exists: {directory}")

    return True

def initialize_database():
    """Initialize the database."""
    print_header("Initializing Database")

    # Load environment variables
    load_dotenv()

    database_path = os.getenv('DATABASE_PATH', 'sync.db')

    try:
        conn = sqlite3.connect(database_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS activity_sync (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gitea_issue_id INTEGER NOT NULL,
                kimai_activity_id INTEGER NOT NULL,
                kimai_project_id INTEGER NOT NULL,
                project_name TEXT NOT NULL,
                repository_name TEXT NOT NULL,
                issue_number INTEGER NOT NULL,
                issue_title TEXT NOT NULL,
                issue_state TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(gitea_issue_id, repository_name)
            )
        ''')
        conn.commit()
        conn.close()

        print_success(f"Database initialized: {database_path}")
        return True

    except sqlite3.Error as e:
        print_error(f"Database initialization failed: {e}")
        return False

def check_permissions():
    """Check file and directory permissions."""
    print_header("Checking Permissions")

    # Check current directory
    try:
        test_file = "permission_test.tmp"
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print_success("Current directory is writable")
    except Exception as e:
        print_error(f"Current directory is not writable: {e}")
        return False

    # Check Python executable permissions
    try:
        python_executable = sys.executable
        if os.access(python_executable, os.X_OK):
            print_success(f"Python executable is executable: {python_executable}")
        else:
            print_warning(f"Python executable may not be executable: {python_executable}")
    except Exception as e:
        print_warning(f"Could not check Python executable permissions: {e}")

    return True

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Setup wizard for Gitea-Kimai integration")
    parser.add_argument('--non-interactive', action='store_true', help='Run in non-interactive mode using .env.template')
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()

    if args.non_interactive:
        print_info("Running in non-interactive mode")

        if not os.path.exists('.env.template'):
            print_error(".env.template not found. Cannot continue in non-interactive mode.")
            return 1

        shutil.copy('.env.template', '.env')
        print_success("Created .env from template")

        create_directories()
        initialize_database()
        check_permissions()

        print_warning("Setup completed in non-interactive mode.")
        print_warning("You must edit .env file manually to configure the integration.")

        return 0

    # Interactive setup
    if not setup_environment():
        return 1

    create_directories()
    initialize_database()
    check_permissions()

    print_header("Setup Complete")
    print_success("The Gitea-Kimai integration has been successfully set up!")
    print_info("You can now run the integration with:")
    print("  python sync.py")
    print("  or")
    print("  ./run_sync.sh")

    print_info("To test the connection:")
    print("  python test_connection.py")

    print_info("To run in read-only mode (no changes to Kimai):")
    print("  python sync.py --dry-run")

    return 0

if __name__ == "__main__":
    sys.exit(main())
