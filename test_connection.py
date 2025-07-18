#!/usr/bin/env python3
"""
Test script to validate Gitea and Kimai connectivity and configuration.
Run this before using the main sync script to ensure everything is set up correctly.
"""

import os
import sys
import requests
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

def test_env_vars():
    """Test that all required environment variables are set."""
    print("🔍 Testing environment variables...")

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
            print(f"  ✅ {var}: {'*' * min(len(value), 10)}...")

    if missing_vars:
        print(f"  ❌ Missing variables: {', '.join(missing_vars)}")
        return False

    if not REPOS_TO_SYNC or REPOS_TO_SYNC == ['']:
        print("  ❌ REPOS_TO_SYNC is not configured")
        return False
    else:
        print(f"  ✅ REPOS_TO_SYNC: {', '.join(REPOS_TO_SYNC)}")

    # Check that at least one Kimai authentication method is configured
    if not (KIMAI_TOKEN or (KIMAI_USERNAME and KIMAI_PASSWORD)):
        print("  ❌ Missing Kimai authentication: need either KIMAI_TOKEN or both KIMAI_USERNAME and KIMAI_PASSWORD")
        return False

    print("  ✅ All environment variables are set")
    return True

def test_gitea_connection():
    """Test Gitea API connection and authentication."""
    print("\n🔍 Testing Gitea connection...")

    try:
        headers = {
            'Authorization': f'token {GITEA_TOKEN}',
            'Accept': 'application/json'
        }

        # Test basic API access
        response = requests.get(f"{GITEA_URL}/api/v1/version", headers=headers, timeout=10)
        response.raise_for_status()

        version_info = response.json()
        print(f"  ✅ Gitea API accessible - Version: {version_info.get('version', 'unknown')}")

        # Test organization access
        response = requests.get(f"{GITEA_URL}/api/v1/orgs/{GITEA_ORGANIZATION}", headers=headers, timeout=10)
        response.raise_for_status()

        org_info = response.json()
        print(f"  ✅ Organization '{GITEA_ORGANIZATION}' accessible")

        # Test repository access
        for repo in REPOS_TO_SYNC:
            repo = repo.strip()
            if repo:
                response = requests.get(f"{GITEA_URL}/api/v1/repos/{GITEA_ORGANIZATION}/{repo}", headers=headers, timeout=10)
                if response.status_code == 200:
                    print(f"  ✅ Repository '{repo}' accessible")
                else:
                    print(f"  ⚠️  Repository '{repo}' not accessible (status: {response.status_code})")

        return True

    except requests.exceptions.RequestException as e:
        print(f"  ❌ Gitea connection failed: {e}")
        return False

def test_kimai_connection():
    """Test Kimai API connection and authentication."""
    print("\n🔍 Testing Kimai connection...")

    try:
        session = requests.Session()

        # Try API token authentication first (preferred)
        if KIMAI_TOKEN:
            session.headers.update({
                'Authorization': f'Bearer {KIMAI_TOKEN}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            print("  ✅ Using Kimai API token authentication")
        # Fall back to HTTP Basic Auth
        elif KIMAI_USERNAME and KIMAI_PASSWORD:
            session.auth = (KIMAI_USERNAME, KIMAI_PASSWORD)
            session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            print("  ✅ Using Kimai HTTP Basic authentication")
        else:
            print("  ❌ No Kimai authentication method configured")
            return False

        # Test basic API access
        response = session.get(f"{KIMAI_URL}/api/version", timeout=10)
        response.raise_for_status()

        version_info = response.json()
        print(f"  ✅ Kimai API accessible - Version: {version_info.get('version', 'unknown')}")

        # Test user permissions
        response = session.get(f"{KIMAI_URL}/api/users/me", timeout=10)
        response.raise_for_status()

        user_info = response.json()
        print(f"  ✅ User '{user_info.get('username', user_info.get('alias', 'unknown'))}' authenticated")

        # Test projects access
        response = session.get(f"{KIMAI_URL}/api/projects", timeout=10)
        response.raise_for_status()

        projects = response.json()
        print(f"  ✅ Projects accessible - Found {len(projects)} existing projects")

        # Test activities access
        response = session.get(f"{KIMAI_URL}/api/activities", timeout=10)
        response.raise_for_status()

        activities = response.json()
        print(f"  ✅ Activities accessible - Found {len(activities)} existing activities")

        return True

    except requests.exceptions.RequestException as e:
        print(f"  ❌ Kimai connection failed: {e}")
        return False

def test_sample_issue_fetch():
    """Test fetching a sample issue from the first repository."""
    print("\n🔍 Testing sample issue fetch...")

    try:
        headers = {
            'Authorization': f'token {GITEA_TOKEN}',
            'Accept': 'application/json'
        }

        first_repo = REPOS_TO_SYNC[0].strip()
        if not first_repo:
            print("  ⚠️  No repositories configured for testing")
            return True

        url = f"{GITEA_URL}/api/v1/repos/{GITEA_ORGANIZATION}/{first_repo}/issues"
        params = {'state': 'all', 'limit': 1}

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()

        issues = response.json()
        if issues:
            issue = issues[0]
            print(f"  ✅ Sample issue fetched: #{issue['number']} - {issue['title'][:50]}...")
            print(f"     Repository: {first_repo}")
            print(f"     State: {issue['state']}")
            print(f"     Created: {issue['created_at']}")
        else:
            print(f"  ℹ️  No issues found in repository '{first_repo}'")

        return True

    except requests.exceptions.RequestException as e:
        print(f"  ❌ Sample issue fetch failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting connectivity and configuration tests...\n")

    tests = [
        ("Environment Variables", test_env_vars),
        ("Gitea Connection", test_gitea_connection),
        ("Kimai Connection", test_kimai_connection),
        ("Sample Issue Fetch", test_sample_issue_fetch)
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"\n❌ {test_name} failed")
        except Exception as e:
            print(f"\n❌ {test_name} failed with exception: {e}")

    print(f"\n{'='*50}")
    print(f"Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! You're ready to run the sync script.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check your configuration.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
