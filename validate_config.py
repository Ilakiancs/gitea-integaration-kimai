#!/usr/bin/env python3
"""
Configuration Validation Tool for Gitea-Kimai Integration

This script validates the configuration in .env file and verifies that
all required settings are present and properly formatted.

Usage:
  python validate_config.py
  python validate_config.py --fix
  python validate_config.py --template
"""

import os
import re
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Define required and optional configuration items
REQUIRED_VARS = [
    'GITEA_URL',
    'GITEA_TOKEN',
    'GITEA_ORGANIZATION',
    'KIMAI_URL',
]

# Either KIMAI_TOKEN or both KIMAI_USERNAME and KIMAI_PASSWORD are required
AUTH_VARS = [
    ['KIMAI_TOKEN'],
    ['KIMAI_USERNAME', 'KIMAI_PASSWORD']
]

OPTIONAL_VARS = [
    'REPOS_TO_SYNC',
    'DATABASE_PATH',
    'LOG_LEVEL',
    'READ_ONLY_MODE',
    'SYNC_PULL_REQUESTS',
    'RATE_LIMIT_ENABLED',
    'RATE_LIMIT_REQUESTS',
    'RATE_LIMIT_PERIOD',
    'PAGE_SIZE',
    'MAX_PAGES',
    'CACHE_ENABLED',
    'CACHE_TTL',
    'CACHE_DIR',
    'EXPORT_ENABLED',
    'EXPORT_DIR',
    'BACKUP_DIR',
]

# Default values for optional variables
DEFAULT_VALUES = {
    'DATABASE_PATH': 'sync.db',
    'LOG_LEVEL': 'INFO',
    'READ_ONLY_MODE': 'false',
    'SYNC_PULL_REQUESTS': 'false',
    'RATE_LIMIT_ENABLED': 'true',
    'RATE_LIMIT_REQUESTS': '10',
    'RATE_LIMIT_PERIOD': '60',
    'PAGE_SIZE': '100',
    'MAX_PAGES': '10',
    'CACHE_ENABLED': 'true',
    'CACHE_TTL': '3600',
    'CACHE_DIR': '.cache',
    'EXPORT_ENABLED': 'false',
    'EXPORT_DIR': 'exports',
    'BACKUP_DIR': 'backups',
}

# Variable validation rules (regex patterns)
VALIDATION_RULES = {
    'GITEA_URL': r'^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(:[0-9]+)?(/.*)?$',
    'KIMAI_URL': r'^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(:[0-9]+)?(/.*)?$',
    'LOG_LEVEL': r'^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$',
    'READ_ONLY_MODE': r'^(true|false)$',
    'SYNC_PULL_REQUESTS': r'^(true|false)$',
    'RATE_LIMIT_ENABLED': r'^(true|false)$',
    'RATE_LIMIT_REQUESTS': r'^\d+$',
    'RATE_LIMIT_PERIOD': r'^\d+$',
    'PAGE_SIZE': r'^\d+$',
    'MAX_PAGES': r'^\d+$',
    'CACHE_ENABLED': r'^(true|false)$',
    'CACHE_TTL': r'^\d+$',
    'EXPORT_ENABLED': r'^(true|false)$',
}

def print_status(message, status):
    """Print a status message with color."""
    if status == "OK":
        print(f"✓ {message}")
    elif status == "WARNING":
        print(f"⚠ {message}")
    elif status == "ERROR":
        print(f"✗ {message}")
    else:
        print(f"  {message}")

def validate_env_file(env_path='.env', fix=False):
    """Validate the .env configuration file."""
    if not os.path.exists(env_path):
        print_status(f"Environment file '{env_path}' not found.", "ERROR")
        if fix:
            create_from_template(env_path)
        return False

    # Load environment variables
    load_dotenv(env_path)

    print(f"Validating configuration file: {env_path}\n")

    all_valid = True
    fixed_items = []

    # Check required variables
    print("Required variables:")
    for var in REQUIRED_VARS:
        value = os.getenv(var)
        if not value:
            print_status(f"{var} is not set", "ERROR")
            all_valid = False
        elif var in VALIDATION_RULES and not re.match(VALIDATION_RULES[var], value):
            print_status(f"{var} has invalid format: {value}", "ERROR")
            all_valid = False
        else:
            # Mask sensitive values
            if var.endswith('_TOKEN') or var.endswith('_PASSWORD'):
                masked_value = value[:4] + '*' * (len(value) - 4) if len(value) > 4 else '****'
                print_status(f"{var} = {masked_value}", "OK")
            else:
                print_status(f"{var} = {value}", "OK")

    # Check authentication variables
    print("\nAuthentication variables:")
    auth_valid = False
    for auth_group in AUTH_VARS:
        group_valid = all(os.getenv(var) for var in auth_group)
        if group_valid:
            auth_valid = True
            print_status(f"Using authentication method: {' and '.join(auth_group)}", "OK")

    if not auth_valid:
        print_status("No valid authentication method configured", "ERROR")
        print("  Either KIMAI_TOKEN or both KIMAI_USERNAME and KIMAI_PASSWORD must be set")
        all_valid = False

    # Check optional variables
    print("\nOptional variables:")
    for var in OPTIONAL_VARS:
        value = os.getenv(var)
        if not value:
            if var in DEFAULT_VALUES:
                if fix:
                    # Add default value to env file
                    with open(env_path, 'a') as f:
                        f.write(f"\n{var}={DEFAULT_VALUES[var]}")
                    fixed_items.append(var)
                    print_status(f"{var} was missing, added default: {DEFAULT_VALUES[var]}", "WARNING")
                else:
                    print_status(f"{var} is not set (default: {DEFAULT_VALUES[var]})", "WARNING")
        elif var in VALIDATION_RULES and not re.match(VALIDATION_RULES[var], value):
            print_status(f"{var} has invalid format: {value}", "ERROR")
            all_valid = False
        else:
            print_status(f"{var} = {value}", "OK")

    # Check for repositories to sync
    repos = os.getenv('REPOS_TO_SYNC', '')
    if repos:
        repos_list = [r.strip() for r in repos.split(',') if r.strip()]
        if repos_list:
            print(f"\nRepositories to sync ({len(repos_list)}):")
            for repo in repos_list:
                print_status(repo, "INFO")
        else:
            print("\nNo repositories specified for syncing.")
            all_valid = False

    if fixed_items and fix:
        print(f"\nFixed {len(fixed_items)} configuration items: {', '.join(fixed_items)}")

    print("\nValidation result:", "OK" if all_valid else "ERROR")
    return all_valid

def create_from_template(env_path='.env'):
    """Create a new .env file from the template."""
    template_path = '.env.template'

    if not os.path.exists(template_path):
        print_status(f"Template file '{template_path}' not found.", "ERROR")
        return False

    try:
        # Copy template to .env
        with open(template_path, 'r') as template_file:
            template_content = template_file.read()

        with open(env_path, 'w') as env_file:
            env_file.write(template_content)

        print_status(f"Created new configuration file '{env_path}' from template", "OK")
        print_status("Please edit the file and fill in your actual configuration values", "INFO")
        return True

    except Exception as e:
        print_status(f"Error creating configuration file: {e}", "ERROR")
        return False

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Validate Gitea-Kimai integration configuration")
    parser.add_argument('--fix', action='store_true', help='Fix configuration issues by adding missing variables')
    parser.add_argument('--template', action='store_true', help='Create a new .env file from template')
    args = parser.parse_args()

    if args.template:
        create_from_template()
    else:
        valid = validate_env_file(fix=args.fix)
        if not valid and not args.fix:
            print("\nRun with --fix to automatically add missing optional variables")
            print("Run with --template to create a new .env file from template")
            return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
