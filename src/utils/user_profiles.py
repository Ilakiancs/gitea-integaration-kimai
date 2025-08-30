#!/usr/bin/env python3
"""
User Profile System for Gitea-Kimai Integration

This module provides user profile management capabilities for the Gitea-Kimai integration,
allowing administrators to configure different user roles, permissions, and preferences.

Usage:
  python user_profiles.py list
  python user_profiles.py add <username>
  python user_profiles.py edit <username>
  python user_profiles.py delete <username>
"""

import os
import sys
import json
import argparse
import sqlite3
import getpass
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('users.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')
PROFILES_FILE = os.getenv('PROFILES_FILE', 'user_profiles.json')

# User roles
ROLES = {
    'admin': {
        'description': 'Full access to all features',
        'permissions': [
            'manage_users',
            'manage_sync',
            'trigger_sync',
            'view_statistics',
            'access_api',
            'manage_backups',
            'manage_scheduler',
            'view_logs',
            'edit_config'
        ]
    },
    'sync_manager': {
        'description': 'Can manage and trigger synchronization',
        'permissions': [
            'manage_sync',
            'trigger_sync',
            'view_statistics',
            'access_api',
            'view_logs'
        ]
    },
    'api_user': {
        'description': 'Can access the API and view statistics',
        'permissions': [
            'access_api',
            'view_statistics'
        ]
    },
    'viewer': {
        'description': 'Can only view synchronization results',
        'permissions': [
            'view_statistics'
        ]
    }
}

class UserProfileManager:
    """Manage user profiles for the Gitea-Kimai integration."""

    def __init__(self, profiles_file=PROFILES_FILE):
        """Initialize with profiles file path."""
        self.profiles_file = profiles_file
        self.profiles = self._load_profiles()

    def _load_profiles(self) -> Dict[str, Any]:
        """Load user profiles from the JSON file."""
        if os.path.exists(self.profiles_file):
            try:
                with open(self.profiles_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing profiles file: {e}")
                return {'users': []}
        else:
            return {'users': []}

    def _save_profiles(self) -> bool:
        """Save user profiles to the JSON file."""
        try:
            with open(self.profiles_file, 'w') as f:
                json.dump(self.profiles, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving profiles: {e}")
            return False

    def get_users(self) -> List[Dict[str, Any]]:
        """Get all user profiles."""
        return self.profiles.get('users', [])

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Get a specific user profile."""
        for user in self.profiles.get('users', []):
            if user.get('username') == username:
                return user
        return None

    def add_user(self, username: str, role: str, email: str = '', gitea_token: str = '',
                 kimai_token: str = '', preferences: Dict[str, Any] = None) -> bool:
        """
        Add a new user profile.

        Args:
            username: The username
            role: User role (admin, sync_manager, api_user, viewer)
            email: User email address
            gitea_token: Gitea API token for this user
            kimai_token: Kimai API token for this user
            preferences: User preferences

        Returns:
            bool: True if successful, False otherwise
        """
        if self.get_user(username):
            logger.error(f"User {username} already exists")
            return False

        if role not in ROLES:
            logger.error(f"Invalid role: {role}. Valid roles: {', '.join(ROLES.keys())}")
            return False

        user = {
            'username': username,
            'role': role,
            'email': email,
            'gitea_token': gitea_token,
            'kimai_token': kimai_token,
            'preferences': preferences or {},
            'permissions': ROLES[role]['permissions'],
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        self.profiles.setdefault('users', []).append(user)

        success = self._save_profiles()
        if success:
            logger.info(f"Added user: {username} with role: {role}")
        return success

    def update_user(self, username: str, **kwargs) -> bool:
        """
        Update an existing user profile.

        Args:
            username: The username to update
            **kwargs: Fields to update

        Returns:
            bool: True if successful, False otherwise
        """
        user = self.get_user(username)
        if not user:
            logger.error(f"User {username} not found")
            return False

        # Handle role update separately to update permissions
        if 'role' in kwargs and kwargs['role'] in ROLES:
            user['role'] = kwargs['role']
            user['permissions'] = ROLES[kwargs['role']]['permissions']

        # Update other fields
        for key, value in kwargs.items():
            if key != 'role' and key != 'username':
                user[key] = value

        user['updated_at'] = datetime.now().isoformat()

        success = self._save_profiles()
        if success:
            logger.info(f"Updated user: {username}")
        return success

    def delete_user(self, username: str) -> bool:
        """
        Delete a user profile.

        Args:
            username: The username to delete

        Returns:
            bool: True if successful, False otherwise
        """
        users = self.profiles.get('users', [])
        for i, user in enumerate(users):
            if user.get('username') == username:
                users.pop(i)
                success = self._save_profiles()
                if success:
                    logger.info(f"Deleted user: {username}")
                return success

        logger.error(f"User {username} not found")
        return False

    def has_permission(self, username: str, permission: str) -> bool:
        """
        Check if a user has a specific permission.

        Args:
            username: The username
            permission: The permission to check

        Returns:
            bool: True if user has the permission, False otherwise
        """
        user = self.get_user(username)
        if not user:
            return False

        return permission in user.get('permissions', [])

    def print_user_details(self, username: str) -> None:
        """Print details of a specific user."""
        user = self.get_user(username)
        if not user:
            print(f"User {username} not found")
            return

        print(f"\nUser: {user['username']}")
        print(f"Role: {user['role']} ({ROLES[user['role']]['description']})")
        print(f"Email: {user.get('email', 'Not set')}")
        print(f"Created: {user.get('created_at', 'Unknown')}")
        print(f"Updated: {user.get('updated_at', 'Unknown')}")

        print("\nPermissions:")
        for perm in user.get('permissions', []):
            print(f"  - {perm}")

        if user.get('preferences'):
            print("\nPreferences:")
            for key, value in user['preferences'].items():
                print(f"  {key}: {value}")

    def print_users_table(self) -> None:
        """Print a table of all users."""
        users = self.get_users()
        if not users:
            print("No users found")
            return

        print("\nUser Profiles:")
        print(f"{'Username':<15} {'Role':<15} {'Email':<25} {'Created':<20}")
        print("-" * 75)

        for user in users:
            created = user.get('created_at', 'Unknown')
            if len(created) > 19:
                created = created[:19]  # Truncate to fit

            email = user.get('email', '')
            if len(email) > 24:
                email = email[:21] + "..."

            print(f"{user['username']:<15} {user['role']:<15} {email:<25} {created:<20}")


def interactive_add_user() -> Dict[str, Any]:
    """Interactive user creation."""
    print("\nCreate New User Profile")
    print("-----------------------")

    username = input("Username: ")

    print("\nAvailable roles:")
    for role, details in ROLES.items():
        print(f"  {role}: {details['description']}")

    role = input("\nRole: ")
    while role not in ROLES:
        print(f"Invalid role. Choose from: {', '.join(ROLES.keys())}")
        role = input("Role: ")

    email = input("Email (optional): ")

    gitea_token = getpass.getpass("Gitea API token (optional, input will be hidden): ")
    kimai_token = getpass.getpass("Kimai API token (optional, input will be hidden): ")

    # Basic preferences
    repositories = input("Default repositories to sync (comma-separated, optional): ")
    notify_on_sync = input("Notify on sync completion? (y/n, default: n): ").lower() == 'y'

    preferences = {
        'default_repositories': [r.strip() for r in repositories.split(',') if r.strip()],
        'notify_on_sync': notify_on_sync,
        'theme': 'light',
        'items_per_page': 50
    }

    return {
        'username': username,
        'role': role,
        'email': email,
        'gitea_token': gitea_token,
        'kimai_token': kimai_token,
        'preferences': preferences
    }

def interactive_edit_user(user: Dict[str, Any]) -> Dict[str, Any]:
    """Interactive user editing."""
    print("\nEdit User Profile")
    print("----------------")
    print(f"Username: {user['username']} (cannot be changed)")

    print("\nCurrent role:", user['role'])
    print("Available roles:")
    for role, details in ROLES.items():
        print(f"  {role}: {details['description']}")

    role = input(f"\nNew role [{user['role']}]: ")
    if not role:
        role = user['role']
    while role not in ROLES:
        print(f"Invalid role. Choose from: {', '.join(ROLES.keys())}")
        role = input(f"New role [{user['role']}]: ")

    email = input(f"New email [{user.get('email', '')}]: ")
    if not email:
        email = user.get('email', '')

    update_gitea = input("Update Gitea API token? (y/n): ").lower() == 'y'
    gitea_token = user.get('gitea_token', '')
    if update_gitea:
        gitea_token = getpass.getpass("New Gitea API token (input will be hidden): ")

    update_kimai = input("Update Kimai API token? (y/n): ").lower() == 'y'
    kimai_token = user.get('kimai_token', '')
    if update_kimai:
        kimai_token = getpass.getpass("New Kimai API token (input will be hidden): ")

    # Get existing preferences or initialize
    preferences = user.get('preferences', {})

    # Edit preferences
    print("\nPreferences:")

    repositories = input(f"Default repositories [{', '.join(preferences.get('default_repositories', []))}]: ")
    if repositories:
        preferences['default_repositories'] = [r.strip() for r in repositories.split(',') if r.strip()]

    notify = input(f"Notify on sync completion? (y/n) [{('y' if preferences.get('notify_on_sync', False) else 'n')}]: ")
    if notify:
        preferences['notify_on_sync'] = notify.lower() == 'y'

    theme = input(f"Theme (light/dark) [{preferences.get('theme', 'light')}]: ")
    if theme in ['light', 'dark']:
        preferences['theme'] = theme

    return {
        'role': role,
        'email': email,
        'gitea_token': gitea_token,
        'kimai_token': kimai_token,
        'preferences': preferences
    }

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="User Profile Management")
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # List command
    subparsers.add_parser('list', help='List all user profiles')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new user profile')
    add_parser.add_argument('username', nargs='?', help='Username to add')
    add_parser.add_argument('--role', choices=ROLES.keys(), help='User role')
    add_parser.add_argument('--email', help='User email')
    add_parser.add_argument('--non-interactive', action='store_true', help='Non-interactive mode')

    # Edit command
    edit_parser = subparsers.add_parser('edit', help='Edit an existing user profile')
    edit_parser.add_argument('username', help='Username to edit')
    edit_parser.add_argument('--role', choices=ROLES.keys(), help='New user role')
    edit_parser.add_argument('--email', help='New user email')
    edit_parser.add_argument('--non-interactive', action='store_true', help='Non-interactive mode')

    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete a user profile')
    delete_parser.add_argument('username', help='Username to delete')

    # Show command
    show_parser = subparsers.add_parser('show', help='Show user profile details')
    show_parser.add_argument('username', help='Username to show')

    # Permissions command
    perm_parser = subparsers.add_parser('check-permission', help='Check if user has a permission')
    perm_parser.add_argument('username', help='Username to check')
    perm_parser.add_argument('permission', help='Permission to check')

    args = parser.parse_args()

    manager = UserProfileManager()

    if args.command == 'list':
        manager.print_users_table()

    elif args.command == 'add':
        if args.non_interactive and args.username and args.role:
            success = manager.add_user(args.username, args.role, args.email or '')
            if success:
                print(f"User {args.username} added successfully")
            else:
                print(f"Failed to add user {args.username}")
        else:
            user_data = interactive_add_user()
            success = manager.add_user(**user_data)
            if success:
                print(f"\nUser {user_data['username']} added successfully")
            else:
                print(f"\nFailed to add user {user_data['username']}")

    elif args.command == 'edit':
        user = manager.get_user(args.username)
        if not user:
            print(f"User {args.username} not found")
            return 1

        if args.non_interactive:
            update_data = {}
            if args.role:
                update_data['role'] = args.role
            if args.email:
                update_data['email'] = args.email

            if update_data:
                success = manager.update_user(args.username, **update_data)
                if success:
                    print(f"User {args.username} updated successfully")
                else:
                    print(f"Failed to update user {args.username}")
            else:
                print("No changes specified")
        else:
            update_data = interactive_edit_user(user)
            success = manager.update_user(args.username, **update_data)
            if success:
                print(f"\nUser {args.username} updated successfully")
            else:
                print(f"\nFailed to update user {args.username}")

    elif args.command == 'delete':
        confirm = input(f"Are you sure you want to delete user {args.username}? (y/n): ")
        if confirm.lower() == 'y':
            success = manager.delete_user(args.username)
            if success:
                print(f"User {args.username} deleted successfully")
            else:
                print(f"Failed to delete user {args.username}")
        else:
            print("Delete operation cancelled")

    elif args.command == 'show':
        manager.print_user_details(args.username)

    elif args.command == 'check-permission':
        has_perm = manager.has_permission(args.username, args.permission)
        if has_perm:
            print(f"User {args.username} has permission: {args.permission}")
        else:
            print(f"User {args.username} does NOT have permission: {args.permission}")

    else:
        parser.print_help()

    return 0

if __name__ == "__main__":
    sys.exit(main())
