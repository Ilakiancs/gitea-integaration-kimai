#!/usr/bin/env python3
"""
Data Validation Module for Gitea-Kimai Sync

Provides comprehensive data validation and sanitization for issues,
pull requests, and user data before syncing to Kimai.
"""

import re
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import html

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates and sanitizes data for sync operations."""
    
    def __init__(self):
        self.max_title_length = 255
        self.max_description_length = 10000
        self.allowed_html_tags = ['b', 'i', 'u', 'strong', 'em', 'code', 'pre', 'br']
        
    def validate_issue_data(self, issue_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and sanitize issue data."""
        validated = {}
        
        # Validate title
        if 'title' in issue_data:
            validated['title'] = self._sanitize_title(issue_data['title'])
        
        # Validate description
        if 'body' in issue_data:
            validated['body'] = self._sanitize_description(issue_data['body'])
        
        # Validate labels
        if 'labels' in issue_data:
            validated['labels'] = self._validate_labels(issue_data['labels'])
        
        # Validate assignees
        if 'assignees' in issue_data:
            validated['assignees'] = self._validate_assignees(issue_data['assignees'])
        
        # Validate milestone
        if 'milestone' in issue_data:
            validated['milestone'] = self._validate_milestone(issue_data['milestone'])
        
        return validated
    
    def _sanitize_title(self, title: str) -> str:
        """Sanitize issue title."""
        if not title:
            return "Untitled Issue"
        
        # Remove HTML tags
        title = re.sub(r'<[^>]+>', '', title)
        
        # Truncate if too long
        if len(title) > self.max_title_length:
            title = title[:self.max_title_length-3] + "..."
        
        return title.strip()
    
    def _sanitize_description(self, description: str) -> str:
        """Sanitize issue description."""
        if not description:
            return ""
        
        # Remove potentially dangerous HTML
        description = html.escape(description)
        
        # Allow only safe HTML tags
        for tag in self.allowed_html_tags:
            description = re.sub(f'&lt;/{tag}&gt;', f'</{tag}>', description)
            description = re.sub(f'&lt;{tag}&gt;', f'<{tag}>', description)
        
        # Truncate if too long
        if len(description) > self.max_description_length:
            description = description[:self.max_description_length-3] + "..."
        
        return description
    
    def _validate_labels(self, labels: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate issue labels."""
        if not labels:
            return []
        
        validated_labels = []
        for label in labels:
            if isinstance(label, dict) and 'name' in label:
                validated_labels.append({
                    'name': str(label['name'])[:50],  # Limit label name length
                    'color': label.get('color', '#000000')
                })
        
        return validated_labels
    
    def _validate_assignees(self, assignees: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate issue assignees."""
        if not assignees:
            return []
        
        validated_assignees = []
        for assignee in assignees:
            if isinstance(assignee, dict) and 'login' in assignee:
                validated_assignees.append({
                    'login': str(assignee['login']),
                    'id': assignee.get('id'),
                    'avatar_url': assignee.get('avatar_url', '')
                })
        
        return validated_assignees
    
    def _validate_milestone(self, milestone: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Validate milestone data."""
        if not milestone:
            return None
        
        if isinstance(milestone, dict) and 'title' in milestone:
            return {
                'title': str(milestone['title'])[:100],
                'id': milestone.get('id'),
                'due_on': milestone.get('due_on')
            }
        
        return None
    
    def validate_repository_data(self, repo_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate repository data."""
        validated = {}
        
        if 'name' in repo_data:
            validated['name'] = str(repo_data['name'])[:100]
        
        if 'full_name' in repo_data:
            validated['full_name'] = str(repo_data['full_name'])[:200]
        
        if 'description' in repo_data:
            validated['description'] = self._sanitize_description(repo_data['description'])
        
        return validated
    
    def validate_user_data(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate user data."""
        validated = {}
        
        if 'login' in user_data:
            validated['login'] = str(user_data['login'])[:50]
        
        if 'email' in user_data:
            email = str(user_data['email'])
            if self._is_valid_email(email):
                validated['email'] = email
        
        if 'name' in user_data:
            validated['name'] = str(user_data['name'])[:100]
        
        return validated
    
    def _is_valid_email(self, email: str) -> bool:
        """Check if email is valid."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def validate_sync_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Validate sync configuration."""
        validated = {}
        
        # Validate URLs
        if 'gitea_url' in config:
            if self._is_valid_url(config['gitea_url']):
                validated['gitea_url'] = config['gitea_url']
        
        if 'kimai_url' in config:
            if self._is_valid_url(config['kimai_url']):
                validated['kimai_url'] = config['kimai_url']
        
        # Validate tokens
        if 'gitea_token' in config:
            validated['gitea_token'] = str(config['gitea_token'])
        
        if 'kimai_token' in config:
            validated['kimai_token'] = str(config['kimai_token'])
        
        return validated
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid."""
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, url))
