#!/usr/bin/env python3
"""
Webhook Handler

Provides webhook handling capabilities for processing incoming webhooks
from Gitea and triggering sync operations.
"""

import json
import hmac
import hashlib
import logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class WebhookEvent(Enum):
    """Webhook event types."""
    PUSH = "push"
    PULL_REQUEST = "pull_request"
    ISSUES = "issues"
    COMMIT_COMMENT = "commit_comment"
    PULL_REQUEST_REVIEW = "pull_request_review"
    REPOSITORY = "repository"
    RELEASE = "release"

@dataclass
class WebhookPayload:
    """Webhook payload data."""
    event_type: WebhookEvent
    repository: Dict[str, Any]
    sender: Dict[str, Any]
    timestamp: datetime
    data: Dict[str, Any]
    signature: Optional[str] = None

@dataclass
class WebhookConfig:
    """Webhook configuration."""
    secret: str
    enabled_events: List[WebhookEvent]
    endpoint_url: str
    timeout: int = 30
    retry_count: int = 3

class WebhookHandler:
    """Main webhook handler."""
    
    def __init__(self, config: WebhookConfig):
        self.config = config
        self.event_handlers: Dict[WebhookEvent, List[Callable]] = {}
        self._setup_default_handlers()
    
    def _setup_default_handlers(self):
        """Setup default event handlers."""
        self.register_handler(WebhookEvent.PUSH, self._handle_push_event)
        self.register_handler(WebhookEvent.PULL_REQUEST, self._handle_pull_request_event)
        self.register_handler(WebhookEvent.ISSUES, self._handle_issues_event)
        self.register_handler(WebhookEvent.REPOSITORY, self._handle_repository_event)
    
    def register_handler(self, event_type: WebhookEvent, handler: Callable):
        """Register a handler for a specific event type."""
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)
        logger.info(f"Registered handler for event: {event_type.value}")
    
    def process_webhook(self, payload: str, signature: str = None) -> bool:
        """Process incoming webhook payload."""
        try:
            # Validate signature if provided
            if signature and not self._validate_signature(payload, signature):
                logger.warning("Invalid webhook signature")
                return False
            
            # Parse payload
            data = json.loads(payload)
            
            # Extract event type
            event_type = self._extract_event_type(data)
            if not event_type:
                logger.warning("Unknown webhook event type")
                return False
            
            # Check if event is enabled
            if event_type not in self.config.enabled_events:
                logger.info(f"Event {event_type.value} is not enabled")
                return True  # Not an error, just ignored
            
            # Create webhook payload object
            webhook_payload = WebhookPayload(
                event_type=event_type,
                repository=data.get('repository', {}),
                sender=data.get('sender', {}),
                timestamp=datetime.now(),
                data=data,
                signature=signature
            )
            
            # Process event
            return self._process_event(webhook_payload)
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON payload: {e}")
            return False
        except Exception as e:
            logger.error(f"Error processing webhook: {e}")
            return False
    
    def _validate_signature(self, payload: str, signature: str) -> bool:
        """Validate webhook signature."""
        if not self.config.secret:
            return True  # No secret configured, skip validation
        
        expected_signature = hmac.new(
            self.config.secret.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(f"sha256={expected_signature}", signature)
    
    def _extract_event_type(self, data: Dict[str, Any]) -> Optional[WebhookEvent]:
        """Extract event type from webhook data."""
        # Check for Gitea-specific event types
        if 'push' in data:
            return WebhookEvent.PUSH
        elif 'pull_request' in data:
            return WebhookEvent.PULL_REQUEST
        elif 'issue' in data:
            return WebhookEvent.ISSUES
        elif 'comment' in data and 'commit' in data:
            return WebhookEvent.COMMIT_COMMENT
        elif 'review' in data:
            return WebhookEvent.PULL_REQUEST_REVIEW
        elif 'repository' in data:
            return WebhookEvent.REPOSITORY
        elif 'release' in data:
            return WebhookEvent.RELEASE
        
        return None
    
    def _process_event(self, webhook_payload: WebhookPayload) -> bool:
        """Process a webhook event."""
        event_type = webhook_payload.event_type
        
        if event_type not in self.event_handlers:
            logger.info(f"No handlers registered for event: {event_type.value}")
            return True
        
        success = True
        for handler in self.event_handlers[event_type]:
            try:
                result = handler(webhook_payload)
                if not result:
                    success = False
                    logger.warning(f"Handler failed for event: {event_type.value}")
            except Exception as e:
                success = False
                logger.error(f"Handler error for event {event_type.value}: {e}")
        
        return success
    
    def _handle_push_event(self, webhook_payload: WebhookPayload) -> bool:
        """Handle push events."""
        logger.info(f"Processing push event for repository: {webhook_payload.repository.get('name', 'unknown')}")
        
        # Extract relevant information
        repository = webhook_payload.repository.get('name', '')
        branch = webhook_payload.data.get('ref', '').replace('refs/heads/', '')
        commits = webhook_payload.data.get('commits', [])
        
        logger.info(f"Push to {repository}:{branch} with {len(commits)} commits")
        
        # Trigger sync for this repository
        # This would typically call the sync engine
        return True
    
    def _handle_pull_request_event(self, webhook_payload: WebhookPayload) -> bool:
        """Handle pull request events."""
        logger.info(f"Processing pull request event for repository: {webhook_payload.repository.get('name', 'unknown')}")
        
        # Extract relevant information
        pr_data = webhook_payload.data.get('pull_request', {})
        action = webhook_payload.data.get('action', '')
        pr_number = pr_data.get('number', 0)
        title = pr_data.get('title', '')
        
        logger.info(f"Pull request #{pr_number} {action}: {title}")
        
        # Handle different PR actions
        if action in ['opened', 'synchronize', 'reopened']:
            # Trigger sync for this PR
            pass
        elif action == 'closed':
            # Handle PR closure
            pass
        
        return True
    
    def _handle_issues_event(self, webhook_payload: WebhookPayload) -> bool:
        """Handle issues events."""
        logger.info(f"Processing issues event for repository: {webhook_payload.repository.get('name', 'unknown')}")
        
        # Extract relevant information
        issue_data = webhook_payload.data.get('issue', {})
        action = webhook_payload.data.get('action', '')
        issue_number = issue_data.get('number', 0)
        title = issue_data.get('title', '')
        
        logger.info(f"Issue #{issue_number} {action}: {title}")
        
        # Handle different issue actions
        if action in ['opened', 'edited', 'reopened']:
            # Trigger sync for this issue
            pass
        elif action == 'closed':
            # Handle issue closure
            pass
        
        return True
    
    def _handle_repository_event(self, webhook_payload: WebhookPayload) -> bool:
        """Handle repository events."""
        logger.info(f"Processing repository event: {webhook_payload.repository.get('name', 'unknown')}")
        
        # Extract relevant information
        action = webhook_payload.data.get('action', '')
        repository = webhook_payload.repository.get('name', '')
        
        logger.info(f"Repository {action}: {repository}")
        
        # Handle different repository actions
        if action == 'created':
            # New repository created
            pass
        elif action == 'deleted':
            # Repository deleted
            pass
        
        return True
    
    def get_webhook_url(self) -> str:
        """Get the webhook endpoint URL."""
        return self.config.endpoint_url
    
    def get_enabled_events(self) -> List[str]:
        """Get list of enabled event types."""
        return [event.value for event in self.config.enabled_events]

def create_webhook_handler(secret: str = None, enabled_events: List[str] = None) -> WebhookHandler:
    """Create a webhook handler with default configuration."""
    if enabled_events is None:
        enabled_events = ['push', 'pull_request', 'issues', 'repository']
    
    # Convert string event names to WebhookEvent enum
    events = []
    for event_name in enabled_events:
        try:
            events.append(WebhookEvent(event_name))
        except ValueError:
            logger.warning(f"Unknown event type: {event_name}")
    
    config = WebhookConfig(
        secret=secret or "",
        enabled_events=events,
        endpoint_url="/webhook/gitea"
    )
    
    return WebhookHandler(config)

if __name__ == "__main__":
    # Example usage
    handler = create_webhook_handler(
        secret="your_webhook_secret",
        enabled_events=['push', 'pull_request', 'issues']
    )
    
    # Sample webhook payload
    sample_payload = {
        "ref": "refs/heads/main",
        "repository": {
            "name": "test-repo",
            "full_name": "user/test-repo"
        },
        "sender": {
            "login": "testuser"
        },
        "commits": [
            {
                "id": "abc123",
                "message": "Update README"
            }
        ]
    }
    
    # Process webhook
    success = handler.process_webhook(json.dumps(sample_payload))
    print(f"Webhook processing: {'Success' if success else 'Failed'}")
