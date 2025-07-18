#!/usr/bin/env python3
"""
Non-interactive Results Viewer for Gitea-Kimai Sync

This script provides a comprehensive view of the sync status without requiring interaction.
It shows information about:
- Kimai projects and activities
- Gitea repositories and issues/PRs
- Sync history from the database
- API permissions
- What would be synced in the next run

Usage:
  python show_results.py
"""

import os
import sys
import sqlite3
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
GITEA_URL = os.getenv('GITEA_URL')
GITEA_TOKEN = os.getenv('GITEA_TOKEN')
GITEA_ORGANIZATION = os.getenv('GITEA_ORGANIZATION')
KIMAI_URL = os.getenv('KIMAI_URL')
KIMAI_TOKEN = os.getenv('KIMAI_TOKEN')
KIMAI_USERNAME = os.getenv('KIMAI_USERNAME')
KIMAI_PASSWORD = os.getenv('KIMAI_PASSWORD')
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')

# ANSI colors for terminal output
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

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 80}")
    print(f"{text}")
    print(f"{'=' * 80}{Colors.ENDC}")

def print_section(text: str):
    print(f"\n{Colors.BLUE}{Colors.BOLD}{text}")
    print(f"{'-' * 60}{Colors.ENDC}")

def print_success(text: str):
    print(f"{Colors.GREEN}{text}{Colors.ENDC}")

def print_warning(text: str):
    print(f"{Colors.YELLOW}{text}{Colors.ENDC}")

def print_error(text: str):
    print(f"{Colors.RED}{text}{Colors.ENDC}")

def print_info(text: str):
    print(f"{Colors.CYAN}{text}{Colors.ENDC}")

class ResultsViewer:
    def __init__(self):
        self.gitea_session = self._init_gitea_session()
        self.kimai_session = self._init_kimai_session()
        self.db_conn = self._init_database()

    def _init_gitea_session(self):
        """Initialize Gitea API session."""
        session = requests.Session()
        if GITEA_TOKEN:
            session.headers.update({
                'Authorization': f'token {GITEA_TOKEN}',
                'Accept': 'application/json'
            })
        return session

    def _init_kimai_session(self):
        """Initialize Kimai API session."""
        session = requests.Session()

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

        return session

    def _init_database(self):
        """Initialize database connection."""
        if not Path(DATABASE_PATH).exists():
            print_warning(f"Database file not found: {DATABASE_PATH}")
            print_info("No sync history is available.")
            return None

        try:
            conn = sqlite3.connect(DATABASE_PATH)
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            return conn
        except sqlite3.Error as e:
            print_error(f"Database error: {e}")
            return None

    def view_sync_history(self):
        """View sync history from database."""
        print_header("SYNC HISTORY")

        if not self.db_conn:
            print_warning("Database not available. No sync history to display.")
            return

        try:
            cursor = self.db_conn.cursor()

            # Check if the table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='activity_sync'")
            if not cursor.fetchone():
                print_warning("No sync history found. The activity_sync table doesn't exist.")
                return

            # Get record count
            cursor.execute("SELECT COUNT(*) as count FROM activity_sync")
            count = cursor.fetchone()['count']

            if count == 0:
                print_warning("No sync records found in database.")
                return

            print_info(f"Found {count} sync records in database")

            # Get summary by repository
            cursor.execute("""
                SELECT repository_name, COUNT(*) as count, MAX(updated_at) as last_sync
                FROM activity_sync
                GROUP BY repository_name
                ORDER BY count DESC
            """)

            repo_stats = cursor.fetchall()
            if repo_stats:
                print_section("Sync Summary by Repository")
                for stat in repo_stats:
                    print(f"  • {stat['repository_name']}: {stat['count']} items (Last sync: {stat['last_sync']})")

                # Get latest syncs
                cursor.execute("""
                    SELECT * FROM activity_sync
                    ORDER BY updated_at DESC
                    LIMIT 10
                """)

                latest_syncs = cursor.fetchall()
                if latest_syncs:
                    print_section("10 Most Recent Sync Records")
                    for i, sync in enumerate(latest_syncs):
                        print(f"  {i+1}. {sync['repository_name']} - {sync['issue_title']} (Issue #{sync['issue_number']})")
                        print(f"     Gitea ID: {sync['gitea_issue_id']}, Kimai Activity ID: {sync['kimai_activity_id']}")
                        print(f"     Last Updated: {sync['updated_at']}")
                        print()

                # Display by state
                cursor.execute("""
                    SELECT issue_state, COUNT(*) as count
                    FROM activity_sync
                    GROUP BY issue_state
                """)

                state_stats = cursor.fetchall()
                if state_stats:
                    print_section("Sync Records by State")
                    for stat in state_stats:
                        print(f"  • {stat['issue_state']}: {stat['count']} items")

        except sqlite3.Error as e:
            print_error(f"Database error while retrieving sync history: {e}")

    def view_kimai_data(self):
        """View current Kimai projects and activities."""
        print_header("KIMAI CURRENT DATA")

        # Check projects
        try:
            response = self.kimai_session.get(f"{KIMAI_URL}/api/projects", timeout=10)
            if response.status_code != 200:
                print_error(f"Failed to fetch Kimai projects: Status {response.status_code}")
                return

            projects = response.json()

            print_section(f"Kimai Projects ({len(projects)} total)")
            for project in projects:
                print(f"  • ID: {project['id']} - {project['name']}")
                if project.get('comment'):
                    print(f"    Description: {project['comment']}")
                print(f"    Visible: {project['visible']}")
                print()

            # Check activities
            response = self.kimai_session.get(f"{KIMAI_URL}/api/activities", timeout=10)
            if response.status_code != 200:
                print_error(f"Failed to fetch Kimai activities: Status {response.status_code}")
                return

            activities = response.json()

            print_section(f"Kimai Activities ({len(activities)} total)")
            for activity in activities:
                project_id = activity.get('project')
                project_name = "None"
                for p in projects:
                    if p['id'] == project_id:
                        project_name = p['name']
                        break

                print(f"  • ID: {activity['id']} - {activity['name']}")
                print(f"    Project: {project_name} (ID: {project_id})")
                if activity.get('comment'):
                    print(f"    Description: {activity['comment']}")
                print()

        except requests.exceptions.RequestException as e:
            print_error(f"Error fetching Kimai data: {e}")

    def view_gitea_data(self):
        """View Gitea repositories and issues/PRs."""
        print_header("GITEA REPOSITORIES AND ISSUES")

        try:
            # Get repositories
            response = self.gitea_session.get(f"{GITEA_URL}/api/v1/orgs/{GITEA_ORGANIZATION}/repos", timeout=10)

            if response.status_code != 200:
                print_error(f"Failed to fetch repositories: Status {response.status_code}")
                return

            repositories = response.json()

            print_section(f"Repositories in {GITEA_ORGANIZATION} ({len(repositories)} total)")
            for repo in repositories:
                print(f"  • {repo['name']} ({repo['full_name']})")
                print(f"    Created: {repo['created_at']}, Stars: {repo['stars_count']}")
                print()

            # Get issues/PRs for the first 3 repos (to avoid too much output)
            for repo in repositories[:3]:
                repo_name = repo['name']
                print_section(f"Issues/PRs for {repo_name}")

                response = self.gitea_session.get(
                    f"{GITEA_URL}/api/v1/repos/{GITEA_ORGANIZATION}/{repo_name}/issues",
                    params={'state': 'all', 'limit': 5},
                    timeout=10
                )

                if response.status_code != 200:
                    print_warning(f"  Could not fetch issues for {repo_name}: {response.status_code}")
                    continue

                issues = response.json()

                if not issues:
                    print_info(f"  No issues/PRs found in {repo_name}")
                    continue

                for issue in issues:
                    is_pr = 'pull_request' in issue
                    item_type = "PR" if is_pr else "Issue"
                    state_color = Colors.GREEN if issue['state'] == 'open' else Colors.YELLOW

                    print(f"  • [{item_type}] #{issue['number']}: {issue['title']}")
                    print(f"    State: {state_color}{issue['state']}{Colors.ENDC}, Created: {issue['created_at']}")
                    print(f"    URL: {issue['html_url']}")
                    print()

        except requests.exceptions.RequestException as e:
            print_error(f"Error fetching Gitea data: {e}")

    def run_dry_sync_simulation(self):
        """Run a dry sync simulation to show what would be synced."""
        print_header("DRY SYNC SIMULATION")

        print_info("Showing what would be synced in the next run:")
        print()

        try:
            # Create a simple simulation of what would be synced
            all_repos = []
            total_items = 0
            would_create = 0
            would_update = 0

            # Get all repositories in the organization
            response = self.gitea_session.get(f"{GITEA_URL}/api/v1/orgs/{GITEA_ORGANIZATION}/repos", timeout=10)
            if response.status_code != 200:
                print_error(f"Failed to fetch repositories: Status {response.status_code}")
                return

            repositories = response.json()

            # Use just the first 3 repositories for simulation to avoid too much output
            simulation_repos = repositories[:3]
            print_section(f"Simulating sync for {len(simulation_repos)} repositories")

            for repo in simulation_repos:
                repo_name = repo['name']
                print_info(f"Repository: {repo_name}")

                # Get issues/PRs
                response = self.gitea_session.get(
                    f"{GITEA_URL}/api/v1/repos/{GITEA_ORGANIZATION}/{repo_name}/issues",
                    params={'state': 'all', 'limit': 10},
                    timeout=10
                )

                if response.status_code != 200:
                    print_warning(f"  Could not fetch issues for {repo_name}: {response.status_code}")
                    continue

                issues = response.json()
                issues_count = len([i for i in issues if 'pull_request' not in i])
                prs_count = len([i for i in issues if 'pull_request' in i])

                print(f"  Found {len(issues)} items ({issues_count} issues, {prs_count} PRs)")

                if issues:
                    # Get existing records from database
                    existing_ids = []
                    if self.db_conn:
                        cursor = self.db_conn.cursor()
                        placeholders = ','.join(['?' for _ in issues])
                        issue_ids = [issue['id'] for issue in issues]
                        cursor.execute(
                            f"SELECT gitea_issue_id FROM activity_sync WHERE gitea_issue_id IN ({placeholders})",
                            issue_ids
                        )
                        existing_ids = [row['gitea_issue_id'] for row in cursor.fetchall()]

                    # Show what would be created vs updated
                    new_items = [i for i in issues if i['id'] not in existing_ids]
                    updated_items = [i for i in issues if i['id'] in existing_ids]

                    would_create += len(new_items)
                    would_update += len(updated_items)

                    if new_items:
                        print(f"  {Colors.GREEN}Would create {len(new_items)} new activities:{Colors.ENDC}")
                        for i, issue in enumerate(new_items[:5]):  # Show max 5 examples
                            item_type = "PR" if 'pull_request' in issue else "Issue"
                            print(f"    • [{item_type}] #{issue['number']}: {issue['title']}")

                        if len(new_items) > 5:
                            print(f"      ...and {len(new_items) - 5} more")

                    if updated_items:
                        print(f"  {Colors.YELLOW}Would update {len(updated_items)} existing activities:{Colors.ENDC}")
                        for i, issue in enumerate(updated_items[:5]):  # Show max 5 examples
                            item_type = "PR" if 'pull_request' in issue else "Issue"
                            print(f"    • [{item_type}] #{issue['number']}: {issue['title']}")

                        if len(updated_items) > 5:
                            print(f"      ...and {len(updated_items) - 5} more")

                print()
                total_items += len(issues)
                all_repos.append(repo_name)

            print_section("Simulation Summary")
            print_success(f"Total repositories that would be synced: {len(all_repos)}")
            print_success(f"Total items that would be processed: {total_items}")
            print_success(f"Would create {would_create} new activities")
            print_success(f"Would update {would_update} existing activities")
            print_info("To perform the actual sync, set READ_ONLY_MODE=false in .env")
            print_info("and run: python sync_enhanced.py")

        except requests.exceptions.RequestException as e:
            print_error(f"Error during simulation: {e}")

    def check_api_permissions(self):
        """Check API permissions for both Gitea and Kimai."""
        print_header("API PERMISSIONS CHECK")

        # Check Gitea permissions
        print_section("Gitea API Permissions")
        try:
            # Check user
            response = self.gitea_session.get(f"{GITEA_URL}/api/v1/user", timeout=5)
            if response.status_code == 200:
                user = response.json()
                print_success(f"✅ Authenticated as: {user.get('login')} ({user.get('full_name', 'N/A')})")
            else:
                print_warning(f"⚠️ User authentication issue: {response.status_code}")

            # Check organization access
            response = self.gitea_session.get(f"{GITEA_URL}/api/v1/orgs/{GITEA_ORGANIZATION}", timeout=5)
            if response.status_code == 200:
                print_success(f"✅ Organization '{GITEA_ORGANIZATION}' access: OK")
            else:
                print_warning(f"⚠️ Organization access issue: {response.status_code}")

            # Check repository access
            response = self.gitea_session.get(f"{GITEA_URL}/api/v1/orgs/{GITEA_ORGANIZATION}/repos", timeout=5)
            if response.status_code == 200:
                repos = response.json()
                print_success(f"✅ Repository access: OK ({len(repos)} repositories found)")
            else:
                print_warning(f"⚠️ Repository access issue: {response.status_code}")

            # Check issue access with a test repository
            if response.status_code == 200 and repos:
                test_repo = repos[0]['name']
                response = self.gitea_session.get(
                    f"{GITEA_URL}/api/v1/repos/{GITEA_ORGANIZATION}/{test_repo}/issues",
                    timeout=5
                )
                if response.status_code == 200:
                    print_success(f"✅ Issues access: OK (tested with '{test_repo}')")
                else:
                    print_warning(f"⚠️ Issues access issue: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print_error(f"Error checking Gitea permissions: {e}")

        # Check Kimai permissions
        print_section("Kimai API Permissions")
        endpoints = [
            ('GET', 'version', None, "Version info"),
            ('GET', 'users/me', None, "User account"),
            ('GET', 'projects', None, "Projects list"),
            ('GET', 'activities', None, "Activities list"),
            ('POST', 'activities', {'name': 'Test', 'project': 1}, "Create activity"),
            ('POST', 'projects', {'name': 'Test'}, "Create project"),
            ('GET', 'timesheets', None, "Timesheets"),
        ]

        for method, endpoint, data, description in endpoints:
            try:
                if method == 'GET':
                    response = self.kimai_session.get(f"{KIMAI_URL}/api/{endpoint}", timeout=5)
                else:
                    response = self.kimai_session.post(f"{KIMAI_URL}/api/{endpoint}", json=data, timeout=5)

                if response.status_code == 200:
                    print_success(f"✅ {description}: Permission granted")
                elif response.status_code == 403:
                    print_warning(f"⚠️ {description}: Permission denied (403 Forbidden)")
                else:
                    print_warning(f"⚠️ {description}: Status {response.status_code}")

            except Exception as e:
                print_error(f"Error testing {description}: {e}")

    def display_info(self):
        """Show all result information."""
        print_header("GITEA TO KIMAI SYNC - RESULTS VIEWER")
        print(f"Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Gitea URL: {GITEA_URL}")
        print(f"Kimai URL: {KIMAI_URL}")

        self.check_api_permissions()
        self.view_kimai_data()
        self.view_gitea_data()
        self.view_sync_history()
        self.run_dry_sync_simulation()

        print_header("CONCLUSION")
        print_info("To view the Kimai activities in the web interface:")
        print(f"1. Go to {KIMAI_URL}")
        print("2. Login with your credentials")
        print("3. Navigate to Activities section")
        print("4. Filter by project name to see all synced activities")
        print("\nTo run the actual sync (after getting write API permissions):")
        print("1. Edit .env file and set READ_ONLY_MODE=false")
        print("2. Run: python sync_enhanced.py")
        print("\nTo see detailed sync logs:")
        print("Check the sync_enhanced.log file")

    def close(self):
        """Clean up resources."""
        if self.db_conn:
            self.db_conn.close()

def main():
    try:
        viewer = ResultsViewer()
        viewer.display_info()
    except KeyboardInterrupt:
        print("\n\nViewer interrupted by user")
    except Exception as e:
        print_error(f"\nUnexpected error: {e}")
    finally:
        if 'viewer' in locals():
            viewer.close()

if __name__ == "__main__":
    main()
