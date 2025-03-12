#!/usr/bin/env python3
"""
Webhook Validator for Gitea-Kimai Integration

This module provides validation for incoming webhooks from Gitea and Kimai.
It handles signature verification, payload validation, and security checks.
"""

import hmac
import hashlib
import json
import time
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class WebhookValidationError(Exception):
    """Webhook validation error."""
    pass

class WebhookValidator:
    """Validates incoming webhook requests."""

    def __init__(self, gitea_secret: Optional[str] = None, kimai_secret: Optional[str] = None):
        """
        Initialize webhook validator.

        Args:
            gitea_secret: Secret key for Gitea webhook validation
            kimai_secret: Secret key for Kimai webhook validation
        """
        self.gitea_secret = gitea_secret
        self.kimai_secret = kimai_secret
        self.max_timestamp_diff = 300  # 5 minutes

    def validate_gitea_webhook(self, headers: Dict[str, str], body: bytes) -> bool:
        """
        Validate Gitea webhook signature and headers.

        Args:
            headers: Request headers
            body: Raw request body

        Returns:
            bool: True if webhook is valid

        Raises:
            WebhookValidationError: If validation fails
        """
        if not self.gitea_secret:
            logger.warning("No Gitea secret configured, skipping signature validation")
            return True

        # Check required headers
        signature = headers.get('X-Gitea-Signature')
        if not signature:
            raise WebhookValidationError("Missing X-Gitea-Signature header")

        event_type = headers.get('X-Gitea-Event')
        if not event_type:
            raise WebhookValidationError("Missing X-Gitea-Event header")

        # Validate signature
        if not self._verify_gitea_signature(signature, body):
            raise WebhookValidationError("Invalid Gitea webhook signature")

        # Validate event type
        allowed_events = [
            'issues', 'issue_comment', 'pull_request', 'pull_request_comment',
            'push', 'repository', 'create', 'delete', 'release'
        ]

        if event_type not in allowed_events:
            raise WebhookValidationError(f"Unsupported event type: {event_type}")

        return True

    def validate_kimai_webhook(self, headers: Dict[str, str], body: bytes) -> bool:
        """
        Validate Kimai webhook signature and headers.

        Args:
            headers: Request headers
            body: Raw request body

        Returns:
            bool: True if webhook is valid

        Raises:
            WebhookValidationError: If validation fails
        """
        if not self.kimai_secret:
            logger.warning("No Kimai secret configured, skipping signature validation")
            return True

        # Check required headers
        signature = headers.get('X-Kimai-Signature')
        if not signature:
            raise WebhookValidationError("Missing X-Kimai-Signature header")

        event_type = headers.get('X-Kimai-Event')
        if not event_type:
            raise WebhookValidationError("Missing X-Kimai-Event header")

        # Validate signature
        if not self._verify_kimai_signature(signature, body):
            raise WebhookValidationError("Invalid Kimai webhook signature")

        # Validate event type
        allowed_events = [
            'timesheet.create', 'timesheet.update', 'timesheet.delete',
            'project.create', 'project.update', 'activity.create', 'activity.update'
        ]

        if event_type not in allowed_events:
            raise WebhookValidationError(f"Unsupported event type: {event_type}")

        return True

    def _verify_gitea_signature(self, signature: str, body: bytes) -> bool:
        """
        Verify Gitea webhook signature.

        Args:
            signature: Signature from X-Gitea-Signature header
            body: Raw request body

        Returns:
            bool: True if signature is valid
        """
        try:
            # Gitea uses sha256 hmac
            expected_signature = hmac.new(
                self.gitea_secret.encode('utf-8'),
                body,
                hashlib.sha256
            ).hexdigest()

            # Compare signatures securely
            return hmac.compare_digest(signature, expected_signature)

        except Exception as e:
            logger.error(f"Error verifying Gitea signature: {e}")
            return False

    def _verify_kimai_signature(self, signature: str, body: bytes) -> bool:
        """
        Verify Kimai webhook signature.

        Args:
            signature: Signature from X-Kimai-Signature header
            body: Raw request body

        Returns:
            bool: True if signature is valid
        """
        try:
            # Kimai uses sha256 hmac with 'sha256=' prefix
            if not signature.startswith('sha256='):
                return False

            signature_hash = signature[7:]  # Remove 'sha256=' prefix

            expected_signature = hmac.new(
                self.kimai_secret.encode('utf-8'),
                body,
                hashlib.sha256
            ).hexdigest()

            # Compare signatures securely
            return hmac.compare_digest(signature_hash, expected_signature)

        except Exception as e:
            logger.error(f"Error verifying Kimai signature: {e}")
            return False

    def validate_payload_structure(self, payload: Dict[str, Any], webhook_type: str) -> bool:
        """
        Validate webhook payload structure.

        Args:
            payload: Parsed webhook payload
            webhook_type: Type of webhook ('gitea' or 'kimai')

        Returns:
            bool: True if payload structure is valid

        Raises:
            WebhookValidationError: If payload structure is invalid
        """
        if webhook_type == 'gitea':
            return self._validate_gitea_payload(payload)
        elif webhook_type == 'kimai':
            return self._validate_kimai_payload(payload)
        else:
            raise WebhookValidationError(f"Unknown webhook type: {webhook_type}")

    def _validate_gitea_payload(self, payload: Dict[str, Any]) -> bool:
        """
        Validate Gitea webhook payload structure.

        Args:
            payload: Gitea webhook payload

        Returns:
            bool: True if payload is valid
        """
        required_fields = ['repository']

        for field in required_fields:
            if field not in payload:
                raise WebhookValidationError(f"Missing required field: {field}")

        # Validate repository structure
        repo = payload.get('repository', {})
        repo_required = ['id', 'name', 'full_name', 'owner']

        for field in repo_required:
            if field not in repo:
                raise WebhookValidationError(f"Missing repository field: {field}")

        return True

    def _validate_kimai_payload(self, payload: Dict[str, Any]) -> bool:
        """
        Validate Kimai webhook payload structure.

        Args:
            payload: Kimai webhook payload

        Returns:
            bool: True if payload is valid
        """
        required_fields = ['event', 'data']

        for field in required_fields:
            if field not in payload:
                raise WebhookValidationError(f"Missing required field: {field}")

        # Validate event data structure
        data = payload.get('data', {})
        if not isinstance(data, dict):
            raise WebhookValidationError("Invalid data structure")

        return True

    def validate_timestamp(self, headers: Dict[str, str]) -> bool:
        """
        Validate webhook timestamp to prevent replay attacks.

        Args:
            headers: Request headers

        Returns:
            bool: True if timestamp is valid

        Raises:
            WebhookValidationError: If timestamp is invalid
        """
        timestamp_header = headers.get('X-Timestamp') or headers.get('X-Hub-Timestamp')

        if not timestamp_header:
            logger.warning("No timestamp header found, skipping timestamp validation")
            return True

        try:
            timestamp = float(timestamp_header)
            current_time = time.time()

            if abs(current_time - timestamp) > self.max_timestamp_diff:
                raise WebhookValidationError("Webhook timestamp too old or too far in future")

            return True

        except ValueError:
            raise WebhookValidationError("Invalid timestamp format")

    def extract_event_info(self, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant event information from webhook.

        Args:
            headers: Request headers
            payload: Webhook payload

        Returns:
            Dict containing extracted event info
        """
        event_info = {
            'timestamp': datetime.now().isoformat(),
            'source': 'unknown',
            'event_type': 'unknown',
            'repository': None,
            'user': None,
            'action': None
        }

        # Determine source from headers
        if 'X-Gitea-Event' in headers:
            event_info['source'] = 'gitea'
            event_info['event_type'] = headers['X-Gitea-Event']

            # Extract Gitea specific info
            if 'repository' in payload:
                repo = payload['repository']
                event_info['repository'] = repo.get('full_name')

            if 'sender' in payload:
                event_info['user'] = payload['sender'].get('login')

            event_info['action'] = payload.get('action')

        elif 'X-Kimai-Event' in headers:
            event_info['source'] = 'kimai'
            event_info['event_type'] = headers['X-Kimai-Event']

            # Extract Kimai specific info
            data = payload.get('data', {})
            if 'user' in data:
                event_info['user'] = data['user'].get('username')

            if 'project' in data:
                event_info['repository'] = data['project'].get('name')

        return event_info

    def is_duplicate_webhook(self, headers: Dict[str, str], payload: Dict[str, Any]) -> bool:
        """
        Check if webhook is a duplicate based on delivery ID.

        Args:
            headers: Request headers
            payload: Webhook payload

        Returns:
            bool: True if webhook appears to be duplicate
        """
        delivery_id = headers.get('X-Delivery-ID') or headers.get('X-GitHub-Delivery')

        if not delivery_id:
            return False

        # In a real implementation, you would check against a cache/database
        # For now, just log the delivery ID
        logger.info(f"Processing webhook with delivery ID: {delivery_id}")

        return False

    def sanitize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize webhook payload by removing sensitive information.

        Args:
            payload: Raw webhook payload

        Returns:
            Dict with sanitized payload
        """
        # Fields to remove for security
        sensitive_fields = [
            'token', 'password', 'secret', 'key', 'auth',
            'authorization', 'x-api-key'
        ]

        def _sanitize_dict(data):
            if isinstance(data, dict):
                sanitized = {}
                for key, value in data.items():
                    if key.lower() in sensitive_fields:
                        sanitized[key] = '[REDACTED]'
                    else:
                        sanitized[key] = _sanitize_dict(value)
                return sanitized
            elif isinstance(data, list):
                return [_sanitize_dict(item) for item in data]
            else:
                return data

        return _sanitize_dict(payload)
