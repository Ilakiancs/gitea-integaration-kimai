#!/usr/bin/env python3
"""
Real-time Webhook System for Gitea-Kimai Integration

Provides real-time synchronization capabilities through webhook handling
for instant updates when issues, pull requests, or commits are created/modified.
"""

import asyncio
import json
import logging
import hashlib
import hmac
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
from pydantic import BaseModel
import aioredis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from ..core.async_sync import AsyncGiteaKimaiSync
from ..config.config_manager import ConfigurationManager
from ..utils.error_handler import ErrorHandler
from ..monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

@dataclass
class WebhookEvent:
    """Represents a webhook event."""
    id: str
    source: str  # 'gitea' or 'kimai'
    event_type: str  # 'issues', 'pull_request', 'push', etc.
    action: str  # 'opened', 'closed', 'edited', etc.
    repository: str
    payload: Dict[str, Any]
    timestamp: datetime
    processed: bool = False
    retry_count: int = 0
    max_retries: int = 3

class WebhookEventModel(BaseModel):
    """Pydantic model for webhook events."""
    event_type: str
    action: str
    repository: str
    number: Optional[int] = None
    payload: Dict[str, Any]

class WebhookQueue:
    """Redis-based webhook event queue for high availability."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis = None
        self.queue_name = "webhook_events"
        self.processing_queue = "webhook_events_processing"

    async def connect(self):
        """Connect to Redis."""
        try:
            self.redis = await aioredis.from_url(self.redis_url)
            logger.info("Connected to Redis for webhook queue")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            # Fallback to in-memory queue
            self.redis = None

    async def enqueue(self, event: WebhookEvent):
        """Add event to queue."""
        if self.redis:
            await self.redis.lpush(self.queue_name, json.dumps(asdict(event), default=str))
        else:
            # Fallback to direct processing if no Redis
            await self._process_event_direct(event)

    async def dequeue(self) -> Optional[WebhookEvent]:
        """Get next event from queue."""
        if not self.redis:
            return None

        # Move from main queue to processing queue atomically
        event_data = await self.redis.brpoplpush(
            self.queue_name,
            self.processing_queue,
            timeout=1
        )

        if event_data:
            try:
                event_dict = json.loads(event_data)
                # Convert timestamp string back to datetime
                if 'timestamp' in event_dict:
                    event_dict['timestamp'] = datetime.fromisoformat(event_dict['timestamp'])
                return WebhookEvent(**event_dict)
            except Exception as e:
                logger.error(f"Failed to deserialize webhook event: {e}")

        return None

    async def mark_processed(self, event: WebhookEvent):
        """Mark event as successfully processed."""
        if self.redis:
            # Remove from processing queue
            await self.redis.lrem(self.processing_queue, 1, json.dumps(asdict(event), default=str))

    async def mark_failed(self, event: WebhookEvent):
        """Mark event as failed and potentially retry."""
        if not self.redis:
            return

        event.retry_count += 1

        if event.retry_count < event.max_retries:
            # Re-queue for retry
            await self.redis.lpush(self.queue_name, json.dumps(asdict(event), default=str))
        else:
            logger.error(f"Event {event.id} failed after {event.max_retries} retries")

        # Remove from processing queue
        await self.redis.lrem(self.processing_queue, 1, json.dumps(asdict(event), default=str))

    async def get_queue_stats(self) -> Dict[str, int]:
        """Get queue statistics."""
        if not self.redis:
            return {"pending": 0, "processing": 0}

        pending = await self.redis.llen(self.queue_name)
        processing = await self.redis.llen(self.processing_queue)

        return {"pending": pending, "processing": processing}

class WebhookProcessor:
    """Processes webhook events in real-time."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sync_engine = None
        self.queue = WebhookQueue(config.get('redis_url', 'redis://localhost:6379'))
        self.metrics = MetricsCollector()
        self.error_handler = ErrorHandler()
        self.running = False
        self.workers = []
        self.max_workers = config.get('max_workers', 5)

    async def initialize(self):
        """Initialize the webhook processor."""
        await self.queue.connect()
        self.sync_engine = AsyncGiteaKimaiSync()
        await self.sync_engine.initialize()
        logger.info("Webhook processor initialized")

    async def start(self):
        """Start webhook processing workers."""
        self.running = True
        self.workers = [
            asyncio.create_task(self._worker(f"webhook-worker-{i}"))
            for i in range(self.max_workers)
        ]
        logger.info(f"Started {self.max_workers} webhook processing workers")

    async def stop(self):
        """Stop webhook processing workers."""
        self.running = False

        if self.workers:
            await asyncio.gather(*self.workers, return_exceptions=True)

        if self.sync_engine:
            await self.sync_engine.cleanup()

        logger.info("Stopped webhook processing workers")

    async def _worker(self, name: str):
        """Worker coroutine for processing webhook events."""
        logger.info(f"Started webhook worker: {name}")

        while self.running:
            try:
                event = await self.queue.dequeue()
                if not event:
                    continue

                logger.info(f"Worker {name} processing event {event.id}")

                # Process the event
                success = await self._process_event(event)

                if success:
                    await self.queue.mark_processed(event)
                    self.metrics.record_webhook_processed(event.source, event.event_type, True)
                else:
                    await self.queue.mark_failed(event)
                    self.metrics.record_webhook_processed(event.source, event.event_type, False)

            except Exception as e:
                logger.error(f"Worker {name} error: {e}")

        logger.info(f"Webhook worker {name} stopped")

    async def _process_event(self, event: WebhookEvent) -> bool:
        """Process a single webhook event."""
        try:
            if event.source == 'gitea':
                return await self._process_gitea_event(event)
            elif event.source == 'kimai':
                return await self._process_kimai_event(event)
            else:
                logger.warning(f"Unknown event source: {event.source}")
                return False

        except Exception as e:
            logger.error(f"Error processing event {event.id}: {e}")
            return False

    async def _process_gitea_event(self, event: WebhookEvent) -> bool:
        """Process Gitea webhook event."""
        payload = event.payload

        if event.event_type == 'issues':
            if event.action in ['opened', 'edited', 'closed', 'reopened']:
                issue = payload.get('issue', {})
                return await self._sync_issue_realtime(event.repository, issue)

        elif event.event_type == 'pull_request':
            if event.action in ['opened', 'edited', 'closed', 'merged']:
                pr = payload.get('pull_request', {})
                return await self._sync_pull_request_realtime(event.repository, pr)

        elif event.event_type == 'push':
            commits = payload.get('commits', [])
            return await self._sync_commits_realtime(event.repository, commits)

        return True

    async def _process_kimai_event(self, event: WebhookEvent) -> bool:
        """Process Kimai webhook event."""
        # Handle Kimai events (timesheet updates, project changes, etc.)
        payload = event.payload

        if event.event_type == 'timesheet':
            return await self._handle_timesheet_update(payload)
        elif event.event_type == 'project':
            return await self._handle_project_update(payload)

        return True

    async def _sync_issue_realtime(self, repository: str, issue: Dict[str, Any]) -> bool:
        """Sync single issue in real-time."""
        try:
            result = await self.sync_engine._process_issue(repository, issue)
            logger.info(f"Real-time sync completed for issue #{issue.get('number')} in {repository}")
            return result
        except Exception as e:
            logger.error(f"Failed to sync issue #{issue.get('number')} in {repository}: {e}")
            return False

    async def _sync_pull_request_realtime(self, repository: str, pr: Dict[str, Any]) -> bool:
        """Sync single pull request in real-time."""
        try:
            result = await self.sync_engine._process_pull_request(repository, pr)
            logger.info(f"Real-time sync completed for PR #{pr.get('number')} in {repository}")
            return result
        except Exception as e:
            logger.error(f"Failed to sync PR #{pr.get('number')} in {repository}: {e}")
            return False

    async def _sync_commits_realtime(self, repository: str, commits: List[Dict[str, Any]]) -> bool:
        """Sync commits in real-time."""
        try:
            for commit in commits:
                await self.sync_engine._process_commit(repository, commit)
            logger.info(f"Real-time sync completed for {len(commits)} commits in {repository}")
            return True
        except Exception as e:
            logger.error(f"Failed to sync commits in {repository}: {e}")
            return False

    async def _handle_timesheet_update(self, payload: Dict[str, Any]) -> bool:
        """Handle Kimai timesheet updates."""
        # Update corresponding Gitea issue/PR with time tracking info
        logger.info("Processing Kimai timesheet update")
        return True

    async def _handle_project_update(self, payload: Dict[str, Any]) -> bool:
        """Handle Kimai project updates."""
        # Update project mappings
        logger.info("Processing Kimai project update")
        return True

    async def enqueue_event(self, event: WebhookEvent):
        """Add event to processing queue."""
        await self.queue.enqueue(event)

    async def get_status(self) -> Dict[str, Any]:
        """Get processor status."""
        queue_stats = await self.queue.get_queue_stats()
        return {
            "running": self.running,
            "workers": len(self.workers),
            "queue_stats": queue_stats,
            "metrics": self.metrics.get_webhook_stats()
        }

class WebhookServer:
    """FastAPI server for receiving webhooks."""

    def __init__(self, processor: WebhookProcessor, config: Dict[str, Any]):
        self.processor = processor
        self.config = config
        self.app = FastAPI(title="Gitea-Kimai Webhook Server")
        self.secret_key = config.get('webhook_secret', '')

        self._setup_routes()

    def _setup_routes(self):
        """Setup webhook routes."""

        @self.app.post("/webhooks/gitea")
        async def gitea_webhook(request: Request, background_tasks: BackgroundTasks):
            """Handle Gitea webhooks."""
            return await self._handle_gitea_webhook(request, background_tasks)

        @self.app.post("/webhooks/kimai")
        async def kimai_webhook(request: Request, background_tasks: BackgroundTasks):
            """Handle Kimai webhooks."""
            return await self._handle_kimai_webhook(request, background_tasks)

        @self.app.get("/webhooks/status")
        async def webhook_status():
            """Get webhook processing status."""
            return await self.processor.get_status()

        @self.app.get("/health")
        async def health_check():
            """Health check endpoint."""
            return {"status": "healthy", "timestamp": datetime.now().isoformat()}

    async def _handle_gitea_webhook(self, request: Request, background_tasks: BackgroundTasks):
        """Handle incoming Gitea webhook."""
        try:
            # Verify signature if secret is configured
            if self.secret_key:
                await self._verify_gitea_signature(request)

            # Parse webhook payload
            payload = await request.json()

            # Extract event information
            event_type = request.headers.get('X-Gitea-Event', 'unknown')
            action = payload.get('action', 'unknown')
            repository = payload.get('repository', {}).get('name', 'unknown')

            # Create webhook event
            event = WebhookEvent(
                id=f"gitea-{event_type}-{repository}-{int(time.time())}",
                source='gitea',
                event_type=event_type,
                action=action,
                repository=repository,
                payload=payload,
                timestamp=datetime.now()
            )

            # Enqueue for processing
            background_tasks.add_task(self.processor.enqueue_event, event)

            logger.info(f"Received Gitea webhook: {event_type}/{action} for {repository}")

            return JSONResponse({"status": "accepted", "event_id": event.id})

        except Exception as e:
            logger.error(f"Error handling Gitea webhook: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    async def _handle_kimai_webhook(self, request: Request, background_tasks: BackgroundTasks):
        """Handle incoming Kimai webhook."""
        try:
            # Parse webhook payload
            payload = await request.json()

            # Extract event information
            event_type = payload.get('event_type', 'unknown')
            action = payload.get('action', 'unknown')

            # Create webhook event
            event = WebhookEvent(
                id=f"kimai-{event_type}-{int(time.time())}",
                source='kimai',
                event_type=event_type,
                action=action,
                repository='',  # Kimai events don't have repositories
                payload=payload,
                timestamp=datetime.now()
            )

            # Enqueue for processing
            background_tasks.add_task(self.processor.enqueue_event, event)

            logger.info(f"Received Kimai webhook: {event_type}/{action}")

            return JSONResponse({"status": "accepted", "event_id": event.id})

        except Exception as e:
            logger.error(f"Error handling Kimai webhook: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    async def _verify_gitea_signature(self, request: Request):
        """Verify Gitea webhook signature."""
        signature = request.headers.get('X-Gitea-Signature')
        if not signature:
            raise HTTPException(status_code=401, detail="Missing signature")

        body = await request.body()
        expected_signature = hmac.new(
            self.secret_key.encode(),
            body,
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")

class RealtimeSync:
    """Main class for real-time synchronization system."""

    def __init__(self, config_path: str = None):
        self.config_manager = ConfigurationManager(config_path)
        self.config = self.config_manager.load_config()

        # Real-time specific configuration
        self.realtime_config = self.config.get('realtime', {
            'enabled': True,
            'webhook_port': 8090,
            'webhook_host': '0.0.0.0',
            'webhook_secret': '',
            'redis_url': 'redis://localhost:6379',
            'max_workers': 5
        })

        self.processor = WebhookProcessor(self.realtime_config)
        self.server = WebhookServer(self.processor, self.realtime_config)

    async def start(self):
        """Start the real-time sync system."""
        logger.info("Starting real-time sync system")

        # Initialize processor
        await self.processor.initialize()

        # Start processing workers
        await self.processor.start()

        # Start webhook server
        config = uvicorn.Config(
            self.server.app,
            host=self.realtime_config['webhook_host'],
            port=self.realtime_config['webhook_port'],
            log_level="info"
        )
        server = uvicorn.Server(config)

        # Run server in background
        await server.serve()

    async def stop(self):
        """Stop the real-time sync system."""
        logger.info("Stopping real-time sync system")
        await self.processor.stop()

    async def get_status(self) -> Dict[str, Any]:
        """Get real-time sync status."""
        return await self.processor.get_status()

# Global real-time sync instance
realtime_sync = None

async def start_realtime_sync(config_path: str = None):
    """Start the real-time sync system."""
    global realtime_sync
    realtime_sync = RealtimeSync(config_path)
    await realtime_sync.start()

async def stop_realtime_sync():
    """Stop the real-time sync system."""
    global realtime_sync
    if realtime_sync:
        await realtime_sync.stop()
        realtime_sync = None
