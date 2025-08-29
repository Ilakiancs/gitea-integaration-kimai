#!/usr/bin/env python3
"""
Enhanced Gitea to Kimai Issue Sync Script with Project Mapping

This script synchronizes issues from Gitea repositories to activities in Kimai
with support for manual project mapping and read-only mode for testing.

Features:
- Manual project mapping configuration
- Read-only mode for testing without write permissions
- Enhanced error handling and logging
- Support for both API token and basic auth
- Flexible repository to project mapping
"""

import os
import sys
import sqlite3
import logging
import requests
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
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
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
READ_ONLY_MODE = os.getenv('READ_ONLY_MODE', 'false').lower() == 'true'
SYNC_PULL_REQUESTS = os.getenv('SYNC_PULL_REQUESTS', 'false').lower() == 'true'

# Project mapping configuration
PROJECT_MAPPING = {
    'tourtree-app': 1,
    'tourtree-backend': 1,
    'tourtree-website': 1,
    'tourtree-svelte': 1,
    'tourtree-images': 1,
    # Add more mappings as needed
    # 'repo-name': project_id
}

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_enhanced.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class EnhancedGiteeKimaiSync:
    def __init__(self):
        self.read_only = READ_ONLY_MODE
        self.project_mapping = PROJECT_MAPPING
        self.validate_config()
        self.setup_database()
        self.session = requests.Session()
        self.kimai_session = requests.Session()
        self.authenticate_kimai()
        self.existing_projects = {}
        self.existing_activities = {}
        self.load_existing_kimai_data()

    def validate_config(self):
        """Validate that all required configuration is present."""
        required_vars = [
            'GITEA_URL', 'GITEA_TOKEN', 'GITEA_ORGANIZATION',
            'KIMAI_URL'
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            sys.exit(1)

        # Validate URL formats
        if not self._is_valid_url(GITEA_URL):
            logger.error(f"Invalid GITEA_URL format: {GITEA_URL}")
            sys.exit(1)

        if not self._is_valid_url(KIMAI_URL):
            logger.error(f"Invalid KIMAI_URL format: {KIMAI_URL}")
            sys.exit(1)

        # Check that at least one Kimai authentication method is configured
        if not (KIMAI_TOKEN or (KIMAI_USERNAME and KIMAI_PASSWORD)):
            logger.error("Missing Kimai authentication: need either KIMAI_TOKEN or both KIMAI_USERNAME and KIMAI_PASSWORD")
            sys.exit(1)

        if not REPOS_TO_SYNC or REPOS_TO_SYNC == ['']:
            logger.error("REPOS_TO_SYNC is not configured")
            sys.exit(1)

        # Validate repository list
        if any(not repo.strip() for repo in REPOS_TO_SYNC):
            logger.warning("Empty repository names found in REPOS_TO_SYNC - these will be skipped")

        # Validate repository names
        for repo in REPOS_TO_SYNC:
            if repo.strip() and not self._is_valid_repo_name(repo.strip()):
                logger.warning(f"Repository name '{repo}' contains potentially unsafe characters")

        if self.read_only:
            logger.info("Running in READ-ONLY mode - no changes will be made to Kimai")

        if SYNC_PULL_REQUESTS:
            logger.info("Pull request syncing is ENABLED")
        else:
            logger.info("Pull request syncing is DISABLED (issues only)")

        # Validate database path
        if not self._is_valid_file_path(DATABASE_PATH):
            logger.warning(f"Database path contains potentially unsafe characters: {DATABASE_PATH}")

        logger.info("Configuration validated successfully")

    def setup_database(self):
        """Initialize SQLite database and create tables if they don't exist."""
        try:
            self.conn = sqlite3.connect(DATABASE_PATH)
            self.conn.execute('''
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
            self.conn.commit()
            logger.info("Database initialized successfully")
        except sqlite3.Error as e:
            logger.error(f"Database setup failed: {e}")
            sys.exit(1)

    def authenticate_kimai(self):
        """Authenticate with Kimai API."""
        try:
            # Try API token authentication first (preferred)
            if KIMAI_TOKEN:
                self.kimai_session.headers.update({
                    'Authorization': f'Bearer {KIMAI_TOKEN}',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                })
                logger.info("Using Kimai API token authentication")
            # Fall back to HTTP Basic Auth
            elif KIMAI_USERNAME and KIMAI_PASSWORD:
                self.kimai_session.auth = (KIMAI_USERNAME, KIMAI_PASSWORD)
                self.kimai_session.headers.update({
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                })
                logger.info("Using Kimai HTTP Basic authentication")
            else:
                logger.error("No Kimai authentication method configured")
                sys.exit(1)

            # Test authentication
            response = self.kimai_session.get(f"{KIMAI_URL}/api/version", timeout=10)
            response.raise_for_status()

            version_info = response.json()
            logger.info(f"Kimai authentication successful - Version: {version_info.get('version', 'unknown')}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Kimai authentication failed: {e}")
            sys.exit(1)

    def load_existing_kimai_data(self):
        """Load existing projects and activities from Kimai."""
        try:
            # Load projects
            response = self.kimai_session.get(f"{KIMAI_URL}/api/projects", timeout=10)
            response.raise_for_status()
            projects = response.json()

            for project in projects:
                self.existing_projects[project['id']] = project
                logger.debug(f"Loaded project: {project['name']} (ID: {project['id']})")

            # Load activities
            response = self.kimai_session.get(f"{KIMAI_URL}/api/activities", timeout=10)
            response.raise_for_status()
            activities = response.json()

            for activity in activities:
                self.existing_activities[activity['id']] = activity
                logger.debug(f"Loaded activity: {activity['name']} (ID: {activity['id']})")

            logger.info(f"Loaded {len(self.existing_projects)} projects and {len(self.existing_activities)} activities from Kimai")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to load existing Kimai data: {e}")
            sys.exit(1)

    def get_gitea_issues(self, repo: str) -> List[Dict]:
        """Fetch issues from a Gitea repository."""
        try:
            headers = {
                'Authorization': f'token {GITEA_TOKEN}',
                'Accept': 'application/json'
            }

            # Sanitize repository name
            safe_repo = self._sanitize_path_component(repo)
            safe_org = self._sanitize_path_component(GITEA_ORGANIZATION)
            url = f"{GITEA_URL}/api/v1/repos/{safe_org}/{safe_repo}/issues"
            params = {
                'state': 'all',
                'sort': 'updated',
                'order': 'desc',
                'limit': 100
            }

            response = self.session.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()

            all_items = response.json()
            if SYNC_PULL_REQUESTS:
                logger.info(f"Retrieved {len(all_items)} items (issues + PRs) from {repo}")
            else:
                actual_issues = [item for item in all_items if 'pull_request' not in item]
                logger.info(f"Retrieved {len(actual_issues)} issues from {repo} (skipping {len(all_items) - len(actual_issues)} PRs)")
            return all_items

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch issues from {repo}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status code: {e.response.status_code}")
                try:
                    error_details = e.response.json()
                    logger.error(f"Error details: {error_details}")
                except:
                    logger.error(f"Response text: {e.response.text[:200]}")
            return []

    def get_kimai_project_id(self, repo_name: str) -> Optional[int]:
        """Get Kimai project ID for a repository."""
        # First check manual mapping
        if repo_name in self.project_mapping:
            project_id = self.project_mapping[repo_name]
            if project_id in self.existing_projects:
                project = self.existing_projects[project_id]
                logger.info(f"Using mapped project for {repo_name}: {project['name']} (ID: {project_id})")
                return project_id
            else:
                logger.warning(f"Mapped project ID {project_id} for {repo_name} not found in Kimai")

        # Try to find project with matching name
        for project_id, project in self.existing_projects.items():
            if project['name'].lower() == repo_name.lower():
                logger.info(f"Found matching project for {repo_name}: {project['name']} (ID: {project_id})")
                return project_id

        # If read-only mode, we can't create projects
        if self.read_only:
            logger.warning(f"No project found for {repo_name} and running in read-only mode")
            return None

        # Try to create new project
        try:
            project_data = {
                'name': repo_name,
                'comment': f'Auto-created project for Gitea repository: {repo_name}',
                'visible': True
            }

            response = self.kimai_session.post(f"{KIMAI_URL}/api/projects", json=project_data, timeout=10)
            response.raise_for_status()

            new_project = response.json()
            project_id = new_project['id']
            self.existing_projects[project_id] = new_project
            logger.info(f"Created new project: {repo_name} (ID: {project_id})")
            return project_id

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create project for {repo_name}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                logger.error(f"Response status code: {status_code}")

                if status_code == 403:
                    logger.error("Permission denied: Your API token may not have project creation rights")
                elif status_code == 400:
                    try:
                        error_details = e.response.json()
                        logger.error(f"Validation error: {error_details}")
                    except:
                        logger.error(f"Response text: {e.response.text[:200]}")
            return None

    def get_existing_sync_record(self, gitea_issue_id: int, repository_name: str) -> Optional[Tuple]:
        """Check if issue is already synced."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT kimai_activity_id, kimai_project_id FROM activity_sync WHERE gitea_issue_id = ? AND repository_name = ?",
            (gitea_issue_id, repository_name)
        )
        return cursor.fetchone()

    def create_or_update_kimai_activity(self, issue: Dict, project_id: int, repository_name: str) -> bool:
        """Create or update a Kimai activity based on Gitea issue."""
        try:
            gitea_issue_id = issue['id']
            existing_record = self.get_existing_sync_record(gitea_issue_id, repository_name)

            # Prepare activity data
            item_type = "PR" if 'pull_request' in issue else "Issue"
            # Sanitize title and create activity name
            safe_title = self._sanitize_activity_name(issue.get('title', 'Untitled'))
            activity_name = f"[{item_type}] #{issue['number']}: {safe_title}"

            # Sanitize body content
            activity_comment = issue.get('body', '')
            if not activity_comment:
                activity_comment = "No description provided."
            elif len(activity_comment) > 500:
                activity_comment = activity_comment[:497] + "..."

            activity_data = {
                'name': activity_name,
                'comment': activity_comment,
                'project': project_id,
                'visible': True
            }

            if existing_record:
                # Update existing activity
                kimai_activity_id = existing_record[0]

                if self.read_only:
                    logger.info(f"[READ-ONLY] Would update activity: {activity_name} (ID: {kimai_activity_id})")
                    return True

                response = self.kimai_session.patch(
                    f"{KIMAI_URL}/api/activities/{kimai_activity_id}",
                    json=activity_data,
                    timeout=10
                )
                response.raise_for_status()

                # Update sync record
                cursor = self.conn.cursor()
                cursor.execute(
                    """UPDATE activity_sync SET
                       issue_title = ?, issue_state = ?, updated_at = CURRENT_TIMESTAMP
                       WHERE gitea_issue_id = ? AND repository_name = ?""",
                    (issue['title'], issue['state'], gitea_issue_id, repository_name)
                )
                self.conn.commit()

                logger.info(f"Updated activity: {activity_name} (ID: {kimai_activity_id})")
                return True
            else:
                # Create new activity
                if self.read_only:
                    logger.info(f"[READ-ONLY] Would create activity: {activity_name}")
                    return True

                response = self.kimai_session.post(
                    f"{KIMAI_URL}/api/activities",
                    json=activity_data,
                    timeout=10
                )
                response.raise_for_status()

                new_activity = response.json()
                kimai_activity_id = new_activity['id']

                # Insert sync record
                cursor = self.conn.cursor()
                cursor.execute(
                    """INSERT INTO activity_sync
                       (gitea_issue_id, kimai_activity_id, kimai_project_id, project_name,
                        repository_name, issue_number, issue_title, issue_state)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (gitea_issue_id, kimai_activity_id, project_id,
                     self.existing_projects[project_id]['name'], repository_name,
                     issue['number'], issue['title'], issue['state'])
                )
                self.conn.commit()

                logger.info(f"Created new activity: {activity_name} (ID: {kimai_activity_id})")
                return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create/update activity for issue #{issue['number']}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                logger.error(f"Response status code: {status_code}")

                if status_code == 403:
                    logger.error("Permission denied: Your API token may not have activity creation/modification rights")
                elif status_code == 400:
                    try:
                        error_details = e.response.json()
                        logger.error(f"Validation error: {error_details}")
                    except:
                        logger.error(f"Response text: {e.response.text[:200]}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error for issue #{issue['number']}: {e}")
            # Add specific handling for common SQLite errors
            if 'UNIQUE constraint failed' in str(e):
                logger.error("This appears to be a duplicate record issue. The database constraint prevented duplicate entries.")
            return False

    def sync_repository(self, repo: str) -> Tuple[int, int]:
        """Sync all issues from a repository to Kimai activities."""
        logger.info(f"Starting sync for repository: {repo}")

        # Get Kimai project ID
        project_id = self.get_kimai_project_id(repo)
        if not project_id:
            logger.error(f"Could not get/create project for repository: {repo}")
            return 0, 0

        # Get Gitea issues
        issues = self.get_gitea_issues(repo)
        if not issues:
            logger.warning(f"No issues found for repository: {repo}")
            return 0, 0

        # Sync each issue
        created_count = 0
        updated_count = 0

        for issue in issues:
            # Skip pull requests unless specifically enabled
            if 'pull_request' in issue and not SYNC_PULL_REQUESTS:
                continue

            existing_record = self.get_existing_sync_record(issue['id'], repo)

            success = self.create_or_update_kimai_activity(issue, project_id, repo)
            if success:
                if existing_record:
                    updated_count += 1
                else:
                    created_count += 1

        logger.info(f"Completed sync for {repo}: {created_count} created, {updated_count} updated")
        return created_count, updated_count

    def sync_all(self):
        """Sync all configured repositories."""
        logger.info("Starting full synchronization")
        if self.read_only:
            logger.info("=" * 60)
            logger.info("RUNNING IN READ-ONLY MODE - NO CHANGES WILL BE MADE")
            logger.info("=" * 60)

        total_created = 0
        total_updated = 0

        for repo in REPOS_TO_SYNC:
            repo = repo.strip()
            if repo and self._is_valid_repo_name(repo):
                try:
                    created, updated = self.sync_repository(repo)
                    total_created += created
                    total_updated += updated
                except Exception as e:
                    logger.error(f"Failed to sync repository {repo}: {e}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")

        logger.info("=" * 60)
        if self.read_only:
            logger.info(f"READ-ONLY SIMULATION: {total_created} activities would be created, {total_updated} would be updated")
        else:
            logger.info(f"Synchronization complete: {total_created} activities created, {total_updated} activities updated")
        logger.info("=" * 60)

        return total_created, total_updated

    def get_sync_statistics(self):
        """Get synchronization statistics from the database."""
        cursor = self.conn.cursor()

        # Total synced issues
        cursor.execute("SELECT COUNT(*) FROM activity_sync")
        total_synced = cursor.fetchone()[0]

        # Synced by repository
        cursor.execute("""
            SELECT repository_name, COUNT(*) as count, MAX(updated_at) as last_sync
            FROM activity_sync
            GROUP BY repository_name
            ORDER BY count DESC
        """)
        repo_stats = cursor.fetchall()

        # Synced by state
        cursor.execute("""
            SELECT issue_state, COUNT(*) as count
            FROM activity_sync
            GROUP BY issue_state
        """)
        state_stats = cursor.fetchall()

        return {
            'total_synced': total_synced,
            'by_repository': repo_stats,
            'by_state': state_stats
        }

    def print_statistics(self):
        """Print synchronization statistics."""
        stats = self.get_sync_statistics()

        logger.info("=" * 60)
        logger.info("SYNCHRONIZATION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Total synced issues: {stats['total_synced']}")

        if stats['by_repository']:
            logger.info("\nBy repository:")
            for repo, count, last_sync in stats['by_repository']:
                logger.info(f"  {repo}: {count} issues (last sync: {last_sync})")

        if stats['by_state']:
            logger.info("\nBy state:")
            for state, count in stats['by_state']:
                logger.info(f"  {state}: {count} issues")

        logger.info("=" * 60)

    def close(self):
        """Clean up resources."""
        if hasattr(self, 'conn'):
            self.conn.close()
        if hasattr(self, 'session'):
            self.session.close()
        if hasattr(self, 'kimai_session'):
            self.kimai_session.close()

    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format."""
        if not url:
            return False
        pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'([a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?'  # domain
            r'(/[a-zA-Z0-9_.-~%&:=+,$#!*?()]*)*/?' # path
        )
        return bool(pattern.match(url))

    def _is_valid_repo_name(self, name: str) -> bool:
        """Validate repository name format."""
        # Allow alphanumeric, dashes, underscores, and dots
        pattern = re.compile(r'^[a-zA-Z0-9._-]+$')
        return bool(pattern.match(name))

    def _is_valid_file_path(self, path: str) -> bool:
        """Validate file path format."""
        # Basic check for directory traversal and unsafe characters
        return '..' not in path and not re.search(r'[<>:"|?*]', path)

    def _sanitize_path_component(self, component: str) -> str:
        """Sanitize a path component (repo name, org name, etc)."""
        if not component:
            return ""
        # Replace potentially dangerous characters with underscores
        return re.sub(r'[^a-zA-Z0-9._-]', '_', component)

    def _sanitize_activity_name(self, name: str) -> str:
        """Sanitize activity name to prevent injection or format issues."""
        if not name:
            return "Untitled"
        # Limit length and remove control characters
        safe_name = re.sub(r'[\x00-\x1F\x7F]', '', name)
        if len(safe_name) > 100:  # Reasonable limit for activity names
            safe_name = safe_name[:97] + "..."
        return safe_name

def main():
    """Main entry point."""
    sync = None
    try:
        print("Starting Enhanced Gitea to Kimai Sync...")
        print("=" * 60)

        sync = EnhancedGiteeKimaiSync()
        sync.sync_all()
        sync.print_statistics()

    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(f"Detailed error: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if sync:
            sync.close()

if __name__ == "__main__":
    main()
