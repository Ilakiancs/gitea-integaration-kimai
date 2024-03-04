#!/usr/bin/env python3
"""
API Client for Gitea-Kimai Integration

This module provides a client for interacting with the Gitea-Kimai Integration API.
It handles authentication, request formatting, and response parsing.

Usage:
  from api_client import GiteaKimaiClient

  client = GiteaKimaiClient('http://localhost:8080', 'username', 'password')
  status = client.get_status()
"""

import os
import json
import time
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

class ApiClientError(Exception):
    """API client error."""
    def __init__(self, message, status_code=None, response=None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)

class GiteaKimaiClient:
    """Client for interacting with the Gitea-Kimai Integration API."""

    def __init__(self, base_url: str, username: Optional[str] = None, password: Optional[str] = None, token: Optional[str] = None):
        """
        Initialize the API client.

        Args:
            base_url: The base URL of the API (e.g., http://localhost:8080)
            username: Username for authentication
            password: Password for authentication
            token: JWT token (if already authenticated)
        """
        self.base_url = base_url.rstrip('/')
        self.api_prefix = '/api/v1'
        self.token = token
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.default_timeout = 30
        self.session.timeout = self.default_timeout
        self.timeout_retries = 3
        # Configure retry strategy for network issues
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # If token is provided, use it immediately
        if self.token:
            self.session.headers.update({
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
        else:
            # Set basic headers
            self.session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })

    def authenticate(self) -> bool:
        """
        Authenticate with the API using username and password.

        Returns:
            bool: True if authentication was successful

        Raises:
            ApiClientError: If authentication fails
        """
        if not self.username or not self.password:
            raise ApiClientError("Username and password are required for authentication")

        try:
            response = self.session.post(
                f"{self.base_url}{self.api_prefix}/auth",
                json={
                    'username': self.username,
                    'password': self.password
                }
            )

            if response.status_code != 200:
                raise ApiClientError("Authentication failed", response.status_code, response)

            data = response.json()
            self.token = data.get('token')

            if not self.token:
                raise ApiClientError("No token in authentication response")

            self.session.headers.update({
                'Authorization': f'Bearer {self.token}'
            })

            return True

        except requests.Timeout as e:
            raise ApiClientError(f"Authentication timeout after 30 seconds: {e}")
        except requests.ConnectionError as e:
            raise ApiClientError(f"Connection error during authentication: {e}")
        except requests.RequestException as e:
            raise ApiClientError(f"Request error during authentication: {e}")

    def _ensure_authenticated(self):
        """Ensure the client is authenticated."""
        if not self.token and self.username and self.password:
            self.authenticate()
        elif not self.token:
            raise ApiClientError("Not authenticated. Provide username/password or token.")

    def _make_request(self, method: str, endpoint: str, params=None, data=None) -> Dict:
        """
        Make a request to the API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (without base URL and API prefix)
            params: Query parameters
            data: Request body

        Returns:
            Dict: Response data

        Raises:
            ApiClientError: If the request fails
        """
        # Only authenticate for protected endpoints
        if endpoint not in ['/status', '/auth']:
            self._ensure_authenticated()

        url = f"{self.base_url}{self.api_prefix}{endpoint}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data
            )

            if response.status_code >= 400:
                error_msg = f"API request failed: {response.text}"
                if response.status_code == 408:
                    error_msg = "Request timeout - server took too long to respond"
                elif response.status_code == 429:
                    error_msg = "Rate limit exceeded - too many requests"
                elif response.status_code >= 500:
                    error_msg = f"Server error ({response.status_code}): {response.text}"

                raise ApiClientError(error_msg, response.status_code, response)

            return response.json()

        except requests.Timeout as e:
            raise ApiClientError(f"Request timeout after 30 seconds: {e}")
        except requests.ConnectionError as e:
            raise ApiClientError(f"Connection error - check network connectivity: {e}")
        except requests.RequestException as e:
            raise ApiClientError(f"Request error: {e}")

    def get_status(self) -> Dict:
        """
        Get the API status.

        Returns:
            Dict: Status information
        """
        return self._make_request('GET', '/status')

    def get_activities(self, limit: int = 100, offset: int = 0, repository: Optional[str] = None, state: Optional[str] = None) -> Dict:
        """
        Get synchronized activities.

        Args:
            limit: Maximum number of activities to return
            offset: Offset for pagination
            repository: Filter by repository name
            state: Filter by issue state

        Returns:
            Dict: Activities data
        """
        params = {
            'limit': limit,
            'offset': offset
        }

        if repository:
            params['repository'] = repository

        if state:
            params['state'] = state

        return self._make_request('GET', '/activities', params=params)

    def get_projects(self) -> Dict:
        """
        Get synchronized projects.

        Returns:
            Dict: Projects data
        """
        return self._make_request('GET', '/projects')

    def get_sync_history(self, limit: int = 100) -> Dict:
        """
        Get synchronization history.

        Args:
            limit: Maximum number of history entries to return

        Returns:
            Dict: Sync history data
        """
        return self._make_request('GET', '/sync/history', params={'limit': limit})

    def trigger_sync(self, repository: Optional[str] = None, dry_run: bool = False) -> Dict:
        """
        Trigger synchronization.

        Args:
            repository: Repository to sync (or all if None)
            dry_run: Whether to perform a dry run

        Returns:
            Dict: Sync trigger response
        """
        data = {
            'dry_run': dry_run
        }

        if repository:
            data['repository'] = repository

        return self._make_request('POST', '/sync/trigger', data=data)

# Command-line interface
if __name__ == "__main__":
    import argparse
    import sys
    from getpass import getpass

    parser = argparse.ArgumentParser(description="Gitea-Kimai API Client")
    parser.add_argument('--url', default='http://localhost:8080', help='API server URL')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--token', help='JWT token (if already authenticated)')

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Status command
    subparsers.add_parser('status', help='Get API status')

    # Activities command
    activities_parser = subparsers.add_parser('activities', help='Get synchronized activities')
    activities_parser.add_argument('--limit', type=int, default=100, help='Maximum number of activities')
    activities_parser.add_argument('--offset', type=int, default=0, help='Offset for pagination')
    activities_parser.add_argument('--repository', help='Filter by repository')
    activities_parser.add_argument('--state', help='Filter by issue state')

    # Projects command
    subparsers.add_parser('projects', help='Get synchronized projects')

    # History command
    history_parser = subparsers.add_parser('history', help='Get synchronization history')
    history_parser.add_argument('--limit', type=int, default=100, help='Maximum number of history entries')

    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Trigger synchronization')
    sync_parser.add_argument('--repository', help='Repository to sync')
    sync_parser.add_argument('--dry-run', action='store_true', help='Perform dry run')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Initialize client
    try:
        # If token is provided, use it
        if args.token:
            client = GiteaKimaiClient(args.url, token=args.token)
        # Otherwise use username/password
        elif args.username:
            password = getpass('Password: ')
            client = GiteaKimaiClient(args.url, args.username, password)
        # For status, no auth needed
        elif args.command == 'status':
            client = GiteaKimaiClient(args.url)
        else:
            print("Error: Authentication required. Provide --username or --token.")
            sys.exit(1)

        # Execute command
        if args.command == 'status':
            result = client.get_status()
        elif args.command == 'activities':
            result = client.get_activities(
                limit=args.limit,
                offset=args.offset,
                repository=args.repository,
                state=args.state
            )
        elif args.command == 'projects':
            result = client.get_projects()
        elif args.command == 'history':
            result = client.get_sync_history(limit=args.limit)
        elif args.command == 'sync':
            result = client.trigger_sync(
                repository=args.repository,
                dry_run=args.dry_run
            )

        # Print result
        print(json.dumps(result, indent=2))

    except ApiClientError as e:
        print(f"Error: {e.message}")
        if e.status_code:
            print(f"Status code: {e.status_code}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
