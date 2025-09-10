#!/usr/bin/env python3
"""
Async Synchronization Engine

Provides asynchronous processing capabilities for improved performance
when handling large numbers of repositories and concurrent API operations.
"""

import asyncio
import aiohttp
import logging
import sqlite3
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path

from ..config.config_manager import ConfigurationManager
from ..utils.error_handler import ErrorHandler
from ..data.data_validation import DataValidator
from ..monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

@dataclass
class SyncTask:
    """Represents a sync task."""
    id: str
    repository: str
    task_type: str  # 'issues', 'pull_requests', 'commits'
    priority: int = 1
    created_at: datetime = None
    status: str = 'pending'
    retry_count: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None

@dataclass
class SyncResult:
    """Represents the result of a sync operation."""
    task_id: str
    repository: str
    success: bool
    items_processed: int
    items_created: int
    items_updated: int
    duration: float
    error_message: Optional[str] = None
    timestamp: datetime = None

class AsyncRateLimiter:
    """Async-aware rate limiter."""

    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.request_times = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Acquire permission to make a request."""
        async with self._lock:
            now = time.time()
            # Remove requests older than 1 minute
            self.request_times = [t for t in self.request_times if now - t < 60]

            if len(self.request_times) >= self.requests_per_minute:
                sleep_time = 60 - (now - self.request_times[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    # Clean up again after sleeping
                    now = time.time()
                    self.request_times = [t for t in self.request_times if now - t < 60]

            self.request_times.append(now)

class AsyncApiClient:
    """Async API client for Gitea and Kimai."""

    def __init__(self, session: aiohttp.ClientSession, rate_limiter: AsyncRateLimiter):
        self.session = session
        self.rate_limiter = rate_limiter
        self.metrics = MetricsCollector()

    async def get(self, url: str, headers: Dict[str, str] = None, **kwargs) -> Dict[str, Any]:
        """Make async GET request with rate limiting."""
        await self.rate_limiter.acquire()

        start_time = time.time()
        try:
            async with self.session.get(url, headers=headers, **kwargs) as response:
                response.raise_for_status()
                data = await response.json()

                # Record metrics
                duration = time.time() - start_time
                self.metrics.record_api_call(url, response.status, duration)

                return data
        except Exception as e:
            duration = time.time() - start_time
            self.metrics.record_api_call(url, getattr(e, 'status', 0), duration, error=str(e))
            raise

    async def post(self, url: str, data: Dict[str, Any] = None, headers: Dict[str, str] = None, **kwargs) -> Dict[str, Any]:
        """Make async POST request with rate limiting."""
        await self.rate_limiter.acquire()

        start_time = time.time()
        try:
            async with self.session.post(url, json=data, headers=headers, **kwargs) as response:
                response.raise_for_status()
                result = await response.json()

                # Record metrics
                duration = time.time() - start_time
                self.metrics.record_api_call(url, response.status, duration)

                return result
        except Exception as e:
            duration = time.time() - start_time
            self.metrics.record_api_call(url, getattr(e, 'status', 0), duration, error=str(e))
            raise

    async def patch(self, url: str, data: Dict[str, Any] = None, headers: Dict[str, str] = None, **kwargs) -> Dict[str, Any]:
        """Make async PATCH request with rate limiting."""
        await self.rate_limiter.acquire()

        start_time = time.time()
        try:
            async with self.session.patch(url, json=data, headers=headers, **kwargs) as response:
                response.raise_for_status()
                result = await response.json()

                # Record metrics
                duration = time.time() - start_time
                self.metrics.record_api_call(url, response.status, duration)

                return result
        except Exception as e:
            duration = time.time() - start_time
            self.metrics.record_api_call(url, getattr(e, 'status', 0), duration, error=str(e))
            raise

class AsyncTaskQueue:
    """Async task queue for managing sync operations."""

    def __init__(self, max_workers: int = 10):
        self.queue = asyncio.Queue()
        self.results = {}
        self.active_tasks = {}
        self.max_workers = max_workers
        self.workers = []
        self.running = False
        self._lock = asyncio.Lock()

    async def add_task(self, task: SyncTask) -> None:
        """Add a task to the queue."""
        task.created_at = datetime.now()
        await self.queue.put(task)
        logger.info(f"Added task {task.id} for repository {task.repository}")

    async def start_workers(self, sync_engine) -> None:
        """Start worker coroutines."""
        self.running = True
        self.workers = [
            asyncio.create_task(self._worker(f"worker-{i}", sync_engine))
            for i in range(self.max_workers)
        ]
        logger.info(f"Started {self.max_workers} async workers")

    async def stop_workers(self) -> None:
        """Stop all worker coroutines."""
        self.running = False

        # Add sentinel values to wake up workers
        for _ in range(self.max_workers):
            await self.queue.put(None)

        # Wait for workers to finish
        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)

        logger.info("Stopped all async workers")

    async def _worker(self, name: str, sync_engine) -> None:
        """Worker coroutine that processes tasks from the queue."""
        logger.info(f"Started worker {name}")

        while self.running:
            try:
                task = await self.queue.get()
                if task is None:  # Sentinel value to stop worker
                    break

                async with self._lock:
                    self.active_tasks[task.id] = task

                logger.info(f"Worker {name} processing task {task.id}")

                # Process the task
                result = await self._process_task(task, sync_engine)

                async with self._lock:
                    self.results[task.id] = result
                    if task.id in self.active_tasks:
                        del self.active_tasks[task.id]

                self.queue.task_done()

            except Exception as e:
                logger.error(f"Worker {name} error: {e}")
                async with self._lock:
                    if task and task.id in self.active_tasks:
                        del self.active_tasks[task.id]

        logger.info(f"Worker {name} stopped")

    async def _process_task(self, task: SyncTask, sync_engine) -> SyncResult:
        """Process a single sync task."""
        start_time = time.time()

        try:
            if task.task_type == 'issues':
                result = await sync_engine.sync_repository_issues(task.repository)
            elif task.task_type == 'pull_requests':
                result = await sync_engine.sync_repository_pull_requests(task.repository)
            elif task.task_type == 'commits':
                result = await sync_engine.sync_repository_commits(task.repository)
            else:
                raise ValueError(f"Unknown task type: {task.task_type}")

            duration = time.time() - start_time

            return SyncResult(
                task_id=task.id,
                repository=task.repository,
                success=True,
                items_processed=result.get('processed', 0),
                items_created=result.get('created', 0),
                items_updated=result.get('updated', 0),
                duration=duration,
                timestamp=datetime.now()
            )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Task {task.id} failed: {e}")

            return SyncResult(
                task_id=task.id,
                repository=task.repository,
                success=False,
                items_processed=0,
                items_created=0,
                items_updated=0,
                duration=duration,
                error_message=str(e),
                timestamp=datetime.now()
            )

    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a specific task."""
        async with self._lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                return {
                    'id': task.id,
                    'repository': task.repository,
                    'status': 'running',
                    'created_at': task.created_at.isoformat()
                }
            elif task_id in self.results:
                result = self.results[task_id]
                return {
                    'id': result.task_id,
                    'repository': result.repository,
                    'status': 'completed' if result.success else 'failed',
                    'success': result.success,
                    'items_processed': result.items_processed,
                    'items_created': result.items_created,
                    'items_updated': result.items_updated,
                    'duration': result.duration,
                    'error_message': result.error_message,
                    'timestamp': result.timestamp.isoformat() if result.timestamp else None
                }

        return None

    async def get_queue_status(self) -> Dict[str, Any]:
        """Get overall queue status."""
        async with self._lock:
            return {
                'queue_size': self.queue.qsize(),
                'active_tasks': len(self.active_tasks),
                'completed_tasks': len(self.results),
                'workers': len(self.workers),
                'running': self.running
            }

class AsyncGiteaKimaiSync:
    """Async version of the Gitea-Kimai sync engine."""

    def __init__(self, config_path: str = None):
        self.config_manager = ConfigurationManager(config_path)
        self.config = self.config_manager.load_config()
        self.error_handler = ErrorHandler()
        self.validator = DataValidator()
        self.metrics = MetricsCollector()

        # Async components
        self.session = None
        self.gitea_client = None
        self.kimai_client = None
        self.task_queue = AsyncTaskQueue(max_workers=10)
        self.rate_limiter = AsyncRateLimiter(requests_per_minute=60)

        # Database connection (thread-safe)
        self.db_lock = threading.Lock()
        self.db_path = self.config['database'].path

        # Setup database
        self._setup_database()

    def _setup_database(self):
        """Setup SQLite database with thread safety."""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Create tables if they don't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sync_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repository TEXT NOT NULL,
                    gitea_issue_id INTEGER,
                    kimai_activity_id INTEGER,
                    kimai_project_id INTEGER,
                    sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS async_sync_results (
                    id TEXT PRIMARY KEY,
                    repository TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    success BOOLEAN NOT NULL,
                    items_processed INTEGER DEFAULT 0,
                    items_created INTEGER DEFAULT 0,
                    items_updated INTEGER DEFAULT 0,
                    duration REAL DEFAULT 0,
                    error_message TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            conn.commit()
            conn.close()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()

    async def initialize(self):
        """Initialize async components."""
        # Create aiohttp session
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)

        # Create API clients
        self.gitea_client = AsyncApiClient(self.session, self.rate_limiter)
        self.kimai_client = AsyncApiClient(self.session, self.rate_limiter)

        # Start task queue workers
        await self.task_queue.start_workers(self)

        logger.info("Async sync engine initialized")

    async def cleanup(self):
        """Cleanup async components."""
        # Stop task queue workers
        await self.task_queue.stop_workers()

        # Close aiohttp session
        if self.session:
            await self.session.close()

        logger.info("Async sync engine cleaned up")

    async def sync_repositories_async(self, repositories: List[str], include_prs: bool = False) -> Dict[str, Any]:
        """Sync multiple repositories asynchronously."""
        logger.info(f"Starting async sync for {len(repositories)} repositories")

        # Create tasks for each repository
        task_ids = []
        for repo in repositories:
            # Create issue sync task
            task_id = f"issues-{repo}-{int(time.time())}"
            task = SyncTask(
                id=task_id,
                repository=repo,
                task_type='issues'
            )
            await self.task_queue.add_task(task)
            task_ids.append(task_id)

            # Create PR sync task if requested
            if include_prs:
                pr_task_id = f"prs-{repo}-{int(time.time())}"
                pr_task = SyncTask(
                    id=pr_task_id,
                    repository=repo,
                    task_type='pull_requests'
                )
                await self.task_queue.add_task(pr_task)
                task_ids.append(pr_task_id)

        # Wait for all tasks to complete or timeout
        timeout = 300  # 5 minutes
        start_time = time.time()

        while time.time() - start_time < timeout:
            status = await self.task_queue.get_queue_status()
            if status['queue_size'] == 0 and status['active_tasks'] == 0:
                break
            await asyncio.sleep(1)

        # Collect results
        results = {}
        for task_id in task_ids:
            task_status = await self.task_queue.get_task_status(task_id)
            if task_status:
                results[task_id] = task_status

        return {
            'total_tasks': len(task_ids),
            'completed_tasks': len(results),
            'results': results,
            'queue_status': await self.task_queue.get_queue_status()
        }

    async def sync_repository_issues(self, repository: str) -> Dict[str, Any]:
        """Sync issues for a specific repository."""
        logger.info(f"Syncing issues for repository: {repository}")

        try:
            # Get Gitea issues
            gitea_url = f"{self.config['gitea'].url}/api/v1/repos/{self.config['gitea'].organization}/{repository}/issues"
            headers = {'Authorization': f'token {self.config['gitea'].token}'}

            issues = await self.gitea_client.get(gitea_url, headers=headers)

            processed = 0
            created = 0
            updated = 0

            # Process each issue
            for issue in issues:
                if await self._process_issue(repository, issue):
                    if await self._issue_exists(repository, issue['id']):
                        updated += 1
                    else:
                        created += 1
                processed += 1

            logger.info(f"Processed {processed} issues for {repository}")

            return {
                'processed': processed,
                'created': created,
                'updated': updated
            }

        except Exception as e:
            logger.error(f"Error syncing issues for {repository}: {e}")
            raise

    async def sync_repository_pull_requests(self, repository: str) -> Dict[str, Any]:
        """Sync pull requests for a specific repository."""
        logger.info(f"Syncing pull requests for repository: {repository}")

        try:
            # Get Gitea pull requests
            gitea_url = f"{self.config['gitea'].url}/api/v1/repos/{self.config['gitea'].organization}/{repository}/pulls"
            headers = {'Authorization': f'token {self.config['gitea'].token}'}

            pulls = await self.gitea_client.get(gitea_url, headers=headers)

            processed = 0
            created = 0
            updated = 0

            # Process each pull request
            for pr in pulls:
                if await self._process_pull_request(repository, pr):
                    if await self._pr_exists(repository, pr['id']):
                        updated += 1
                    else:
                        created += 1
                processed += 1

            logger.info(f"Processed {processed} pull requests for {repository}")

            return {
                'processed': processed,
                'created': created,
                'updated': updated
            }

        except Exception as e:
            logger.error(f"Error syncing pull requests for {repository}: {e}")
            raise

    async def sync_repository_commits(self, repository: str) -> Dict[str, Any]:
        """Sync commits for a specific repository."""
        logger.info(f"Syncing commits for repository: {repository}")

        try:
            # Get Gitea commits
            gitea_url = f"{self.config['gitea'].url}/api/v1/repos/{self.config['gitea'].organization}/{repository}/commits"
            headers = {'Authorization': f'token {self.config['gitea'].token}'}

            commits = await self.gitea_client.get(gitea_url, headers=headers)

            processed = len(commits)

            # Process commits (implementation depends on requirements)
            # This is a placeholder for commit processing logic

            logger.info(f"Processed {processed} commits for {repository}")

            return {
                'processed': processed,
                'created': 0,
                'updated': 0
            }

        except Exception as e:
            logger.error(f"Error syncing commits for {repository}: {e}")
            raise

    async def _process_issue(self, repository: str, issue: Dict[str, Any]) -> bool:
        """Process a single issue asynchronously."""
        try:
            # Validate issue data
            if not self.validator.validate_issue_data(issue):
                logger.warning(f"Invalid issue data for issue {issue.get('id')}")
                return False

            # Create or update Kimai activity
            activity_id = await self._create_or_update_kimai_activity(repository, issue)

            # Record sync in database
            await self._record_sync(repository, issue['id'], activity_id)

            return True

        except Exception as e:
            logger.error(f"Error processing issue {issue.get('id')}: {e}")
            return False

    async def _process_pull_request(self, repository: str, pr: Dict[str, Any]) -> bool:
        """Process a single pull request asynchronously."""
        try:
            # Validate PR data
            if not self.validator.validate_pr_data(pr):
                logger.warning(f"Invalid PR data for PR {pr.get('id')}")
                return False

            # Create or update Kimai activity for PR
            activity_id = await self._create_or_update_kimai_activity(repository, pr, is_pr=True)

            # Record sync in database
            await self._record_sync(repository, pr['id'], activity_id, is_pr=True)

            return True

        except Exception as e:
            logger.error(f"Error processing PR {pr.get('id')}: {e}")
            return False

    async def _create_or_update_kimai_activity(self, repository: str, item: Dict[str, Any], is_pr: bool = False) -> Optional[int]:
        """Create or update Kimai activity."""
        try:
            # Get or create project
            project_id = await self._get_or_create_kimai_project(repository)

            # Prepare activity data
            prefix = "PR" if is_pr else "Issue"
            activity_name = f"{prefix} #{item['number']}: {item['title']}"

            activity_data = {
                'project': project_id,
                'name': activity_name,
                'comment': item.get('body', ''),
                'visible': True
            }

            # Check if activity exists
            existing_activity = await self._find_existing_activity(repository, item['id'], is_pr)

            if existing_activity:
                # Update existing activity
                update_url = f"{self.config['kimai'].url}/api/activities/{existing_activity}"
                headers = self._get_kimai_headers()

                result = await self.kimai_client.patch(update_url, activity_data, headers=headers)
                return result['id']
            else:
                # Create new activity
                create_url = f"{self.config['kimai'].url}/api/activities"
                headers = self._get_kimai_headers()

                result = await self.kimai_client.post(create_url, activity_data, headers=headers)
                return result['id']

        except Exception as e:
            logger.error(f"Error creating/updating Kimai activity: {e}")
            return None

    async def _get_or_create_kimai_project(self, repository: str) -> int:
        """Get or create Kimai project for repository."""
        # Implementation for getting or creating Kimai project
        # This would involve checking existing projects and creating if needed
        project_name = f"Gitea: {repository}"

        # Check existing projects
        projects_url = f"{self.config['kimai'].url}/api/projects"
        headers = self._get_kimai_headers()

        projects = await self.kimai_client.get(projects_url, headers=headers)

        for project in projects:
            if project['name'] == project_name:
                return project['id']

        # Create new project
        project_data = {
            'name': project_name,
            'comment': f'Auto-created for Gitea repository: {repository}',
            'visible': True
        }

        result = await self.kimai_client.post(projects_url, project_data, headers=headers)
        return result['id']

    def _get_kimai_headers(self) -> Dict[str, str]:
        """Get headers for Kimai API requests."""
        if hasattr(self.config['kimai'], 'token') and self.config['kimai'].token:
            return {
                'Authorization': f'Bearer {self.config["kimai"].token}',
                'Content-Type': 'application/json'
            }
        else:
            # Use basic auth
            import base64
            credentials = f"{self.config['kimai'].username}:{self.config['kimai'].password}"
            encoded = base64.b64encode(credentials.encode()).decode()
            return {
                'Authorization': f'Basic {encoded}',
                'Content-Type': 'application/json'
            }

    async def _find_existing_activity(self, repository: str, item_id: int, is_pr: bool = False) -> Optional[int]:
        """Find existing activity in database."""
        # Use thread pool for database operations
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(
                executor,
                self._find_existing_activity_sync,
                repository, item_id, is_pr
            )

    def _find_existing_activity_sync(self, repository: str, item_id: int, is_pr: bool = False) -> Optional[int]:
        """Synchronous database lookup for existing activity."""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            if is_pr:
                cursor.execute(
                    "SELECT kimai_activity_id FROM sync_records WHERE repository = ? AND gitea_issue_id = ? AND task_type = 'pull_request'",
                    (repository, item_id)
                )
            else:
                cursor.execute(
                    "SELECT kimai_activity_id FROM sync_records WHERE repository = ? AND gitea_issue_id = ?",
                    (repository, item_id)
                )

            result = cursor.fetchone()
            conn.close()

            return result[0] if result else None

    async def _record_sync(self, repository: str, item_id: int, activity_id: int, is_pr: bool = False):
        """Record sync operation in database."""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(
                executor,
                self._record_sync_sync,
                repository, item_id, activity_id, is_pr
            )

    def _record_sync_sync(self, repository: str, item_id: int, activity_id: int, is_pr: bool = False):
        """Synchronous database recording."""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            task_type = 'pull_request' if is_pr else 'issue'

            cursor.execute('''
                INSERT OR REPLACE INTO sync_records
                (repository, gitea_issue_id, kimai_activity_id, task_type, sync_timestamp, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (repository, item_id, activity_id, task_type, datetime.now(), datetime.now()))

            conn.commit()
            conn.close()

    async def _issue_exists(self, repository: str, issue_id: int) -> bool:
        """Check if issue already exists in sync records."""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(
                executor,
                self._issue_exists_sync,
                repository, issue_id
            )

    def _issue_exists_sync(self, repository: str, issue_id: int) -> bool:
        """Synchronous check for existing issue."""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM sync_records WHERE repository = ? AND gitea_issue_id = ?",
                (repository, issue_id)
            )

            count = cursor.fetchone()[0]
            conn.close()

            return count > 0

    async def _pr_exists(self, repository: str, pr_id: int) -> bool:
        """Check if PR already exists in sync records."""
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(
                executor,
                self._pr_exists_sync,
                repository, pr_id
            )

    def _pr_exists_sync(self, repository: str, pr_id: int) -> bool:
        """Synchronous check for existing PR."""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT COUNT(*) FROM sync_records WHERE repository = ? AND gitea_issue_id = ? AND task_type = 'pull_request'",
                (repository, pr_id)
            )

            count = cursor.fetchone()[0]
            conn.close()

            return count > 0

    async def get_sync_status(self) -> Dict[str, Any]:
        """Get overall sync status."""
        queue_status = await self.task_queue.get_queue_status()
        metrics = self.metrics.get_summary()

        return {
            'queue_status': queue_status,
            'metrics': metrics,
            'timestamp': datetime.now().isoformat()
        }
