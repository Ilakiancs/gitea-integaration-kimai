#!/usr/bin/env python3
"""
Test script to validate Gitea and Kimai connectivity and configuration.
Run this before using the main sync script to ensure everything is set up correctly.
"""

import os
import sys
import requests
import argparse
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
GITEA_URL = os.getenv('GITEA_URL')
GITEA_TOKEN = os.getenv('GITEA_TOKEN')
GITEA_ORGANIZATION = os.getenv('GITEA_ORGANIZATION')
KIMAI_URL = os.getenv('KIMAI_URL')
KIMAI_USERNAME = os.getenv('KIMAI_USERNAME')
KIMAI_PASSWORD = os.getenv('KIMAI_PASSWORD')
KIMAI_TOKEN = os.getenv('KIMAI_TOKEN')
REPOS_TO_SYNC = os.getenv('REPOS_TO_SYNC', '').split(',')
READ_ONLY_MODE = os.getenv('READ_ONLY_MODE', 'false').lower() == 'true'
CACHE_ENABLED = os.getenv('CACHE_ENABLED', 'true').lower() == 'true'
RATE_LIMIT_ENABLED = os.getenv('RATE_LIMIT_ENABLED', 'true').lower() == 'true'
EXPORT_ENABLED = os.getenv('EXPORT_ENABLED', 'false').lower() == 'true'

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Test Gitea and Kimai connectivity")
    parser.add_argument('--json', action='store_true', help="Output results in JSON format")
    parser.add_argument('--verbose', action='store_true', help="Show detailed information")
    parser.add_argument('--check-cache', action='store_true', help="Check cache directory and files")
    parser.add_argument('--check-export', action='store_true', help="Check export directory")
    return parser.parse_args()

def test_env_vars():
    """Test that all required environment variables are set."""
    print("Testing environment variables...")

    required_vars = [
        'GITEA_URL', 'GITEA_TOKEN', 'GITEA_ORGANIZATION',
        'KIMAI_URL'
    ]

    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            print(f"  [OK] {var}: {'*' * min(len(value), 10)}...")

    if missing_vars:
        print(f"  [ERROR] Missing variables: {', '.join(missing_vars)}")
        return False

    if not REPOS_TO_SYNC or REPOS_TO_SYNC == ['']:
        print("  [ERROR] REPOS_TO_SYNC is not configured")
        return False
    else:
        print(f"  [OK] REPOS_TO_SYNC: {', '.join(REPOS_TO_SYNC)}")

    # Check that at least one Kimai authentication method is configured
    if not (KIMAI_TOKEN or (KIMAI_USERNAME and KIMAI_PASSWORD)):
        print("  [ERROR] Missing Kimai authentication: need either KIMAI_TOKEN or both KIMAI_USERNAME and KIMAI_PASSWORD")
        return False

    print("  [OK] All environment variables are set")

    # Check optional configurations
    print("\nOptional configurations:")
    print(f"  - READ_ONLY_MODE: {READ_ONLY_MODE}")
    print(f"  - CACHE_ENABLED: {CACHE_ENABLED}")
    print(f"  - RATE_LIMIT_ENABLED: {RATE_LIMIT_ENABLED}")
    print(f"  - EXPORT_ENABLED: {EXPORT_ENABLED}")

    return True


def test_gitea_connection():
    """Test Gitea API connection and authentication."""
    print("\nTesting Gitea connection...")

    try:
        headers = {
            'Authorization': f'token {GITEA_TOKEN}',
            'Accept': 'application/json'
        }

        # Test basic API access
        response = requests.get(f"{GITEA_URL}/api/v1/version", headers=headers, timeout=10)
        response.raise_for_status()

        version_info = response.json()
        print(f"  [OK] Gitea API accessible - Version: {version_info.get('version', 'unknown')}")

        # Test organization access
        response = requests.get(f"{GITEA_URL}/api/v1/orgs/{GITEA_ORGANIZATION}", headers=headers, timeout=10)
        response.raise_for_status()

        org_info = response.json()
        print(f"  [OK] Organization '{GITEA_ORGANIZATION}' accessible")

        # Test repository access
        for repo in REPOS_TO_SYNC:
            repo = repo.strip()
            if repo:
                response = requests.get(f"{GITEA_URL}/api/v1/repos/{GITEA_ORGANIZATION}/{repo}", headers=headers, timeout=10)
                if response.status_code == 200:
                    print(f"  [OK] Repository '{repo}' accessible")
                else:
                    print(f"  [WARNING] Repository '{repo}' not accessible (status: {response.status_code})")

        return True

    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Gitea connection failed: {e}")
        return False

def test_kimai_connection():
    """Test Kimai API connection and authentication."""
    print("\nTesting Kimai connection...")

    try:
        session = requests.Session()

        # Try API token authentication first (preferred)
        if KIMAI_TOKEN:
            session.headers.update({
                'Authorization': f'Bearer {KIMAI_TOKEN}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            print("  [OK] Using Kimai API token authentication")
        # Fall back to HTTP Basic Auth
        elif KIMAI_USERNAME and KIMAI_PASSWORD:
            session.auth = (KIMAI_USERNAME, KIMAI_PASSWORD)
            session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            print("  [OK] Using Kimai HTTP Basic authentication")
        else:
            print("  [ERROR] No Kimai authentication method configured")
            return False

        # Test basic API access
        response = session.get(f"{KIMAI_URL}/api/version", timeout=10)
        response.raise_for_status()

        version_info = response.json()
        print(f"  [OK] Kimai API accessible - Version: {version_info.get('version', 'unknown')}")

        # Test user permissions
        response = session.get(f"{KIMAI_URL}/api/users/me", timeout=10)
        response.raise_for_status()

        user_info = response.json()
        print(f"  [OK] User '{user_info.get('username', user_info.get('alias', 'unknown'))}' authenticated")

        # Test projects access
        response = session.get(f"{KIMAI_URL}/api/projects", timeout=10)
        response.raise_for_status()

        projects = response.json()
        print(f"  [OK] Projects accessible - Found {len(projects)} existing projects")

        # Test activities access
        response = session.get(f"{KIMAI_URL}/api/activities", timeout=10)
        response.raise_for_status()

        activities = response.json()
        print(f"  [OK] Activities accessible - Found {len(activities)} existing activities")

        return True

    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Kimai connection failed: {e}")
        return False

def test_cache_directory():
    """Test cache directory configuration."""
    print("\nTesting cache directory...")

    cache_dir = os.getenv('CACHE_DIR', '.cache')

    if not CACHE_ENABLED:
        print("  [INFO] Caching is disabled")
        return True

    if not os.path.exists(cache_dir):
        print(f"  [WARNING] Cache directory '{cache_dir}' does not exist (will be created during sync)")
    else:
        print(f"  [OK] Cache directory exists: {cache_dir}")
        # Check cache files
        cache_files = [f for f in os.listdir(cache_dir) if f.endswith('.cache')]
        print(f"  [INFO] Found {len(cache_files)} cache files")

    return True

def test_export_directory():
    """Test export directory configuration."""
    print("\nTesting export directory...")

    export_dir = os.getenv('EXPORT_DIR', 'exports')

    if not EXPORT_ENABLED:
        print("  [INFO] Exporting is disabled")
        return True

    if not os.path.exists(export_dir):
        print(f"  [WARNING] Export directory '{export_dir}' does not exist (will be created during sync)")
    else:
        print(f"  [OK] Export directory exists: {export_dir}")
        # Check export files
        export_files = [f for f in os.listdir(export_dir) if f.endswith('.csv')]
        print(f"  [INFO] Found {len(export_files)} export files")

    return True

def test_sample_issue_fetch():
    """Test fetching a sample issue from the first repository."""
    print("\nTesting sample issue fetch...")

    try:
        headers = {
            'Authorization': f'token {GITEA_TOKEN}',
            'Accept': 'application/json'
        }

        first_repo = REPOS_TO_SYNC[0].strip()
        if not first_repo:
            print("  [WARNING] No repositories configured for testing")
            return True

        url = f"{GITEA_URL}/api/v1/repos/{GITEA_ORGANIZATION}/{first_repo}/issues"
        params = {'state': 'all', 'limit': 1}

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()

        issues = response.json()
        if issues:
            issue = issues[0]
            print(f"  [OK] Sample issue fetched: #{issue['number']} - {issue['title'][:50]}...")
            print(f"     Repository: {first_repo}")
            print(f"     State: {issue['state']}")
            print(f"     Created: {issue['created_at']}")
        else:
            print(f"  [INFO] No issues found in repository '{first_repo}'")

        return True

    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Sample issue fetch failed: {e}")
        return False

def main():
    """Run all tests."""
    args = parse_arguments()
    print("Starting connectivity and configuration tests...\n")

    tests = [
        ("Environment Variables", test_env_vars),
        ("Gitea Connection", test_gitea_connection),
        ("Kimai Connection", test_kimai_connection),
        ("Sample Issue Fetch", test_sample_issue_fetch)
    ]

    # Add optional tests based on args
    if args.check_cache:
        tests.append(("Cache Directory", test_cache_directory))

    if args.check_export:
        tests.append(("Export Directory", test_export_directory))

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"\n[ERROR] {test_name} failed")
        except Exception as e:
            print(f"\n[ERROR] {test_name} failed with exception: {e}")

    print(f"\n{'='*50}")
    print(f"Test Results: {passed}/{total} tests passed")

    results = {
        "timestamp": datetime.now().isoformat(),
        "total_tests": total,
        "passed_tests": passed,
        "success": passed == total,
        "tests": {
            test_name: {"passed": test_name not in failures}
            for test_name, _ in tests
        },
        "env_vars": {
            "gitea_url": GITEA_URL,
            "gitea_org": GITEA_ORGANIZATION,
            "kimai_url": KIMAI_URL,
            "read_only": READ_ONLY_MODE,
            "cache_enabled": CACHE_ENABLED,
            "rate_limit_enabled": RATE_LIMIT_ENABLED,
            "export_enabled": EXPORT_ENABLED,
            "repos_count": len([r for r in REPOS_TO_SYNC if r.strip()])
        }
    }

    if args.json:
        print("\nJSON Results:")
        print(json.dumps(results, indent=2))
    else:
        if passed == total:
            print("All tests passed! You're ready to run the sync script.")
        else:
            print("[WARNING] Some tests failed. Please check your configuration.")

    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())
