#!/usr/bin/env python3
"""
Issue Classifier Plugin

A sample data processor plugin that automatically classifies issues
based on their title and content, adding appropriate labels and priority.
"""

import re
import logging
from typing import Dict, List, Any
from datetime import datetime

from .. import DataProcessorPlugin, PluginMetadata

logger = logging.getLogger(__name__)

class IssueClassifierPlugin(DataProcessorPlugin):
    """Plugin that automatically classifies issues based on content."""

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        # Configuration for classification rules
        self.bug_keywords = self.config.get('bug_keywords', [
            'bug', 'error', 'exception', 'crash', 'fail', 'broken', 'issue',
            'problem', 'incorrect', 'wrong', 'not working', 'malfunction'
        ])

        self.feature_keywords = self.config.get('feature_keywords', [
            'feature', 'enhancement', 'improvement', 'add', 'implement',
            'support', 'new', 'request', 'suggestion', 'proposal'
        ])

        self.urgent_keywords = self.config.get('urgent_keywords', [
            'urgent', 'critical', 'blocking', 'production', 'outage',
            'down', 'security', 'vulnerability', 'data loss'
        ])

        self.documentation_keywords = self.config.get('documentation_keywords', [
            'docs', 'documentation', 'readme', 'guide', 'tutorial',
            'help', 'instructions', 'manual', 'wiki'
        ])

    def get_metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="issue_classifier",
            version="1.0.0",
            description="Automatically classifies issues and adds appropriate labels",
            author="Gitea-Kimai Integration Team",
            dependencies=[],
            enabled=True
        )

    async def initialize(self) -> bool:
        """Initialize the plugin."""
        self.logger.info("Initializing Issue Classifier plugin")

        # Compile regex patterns for better performance
        self.bug_pattern = re.compile(
            r'\b(' + '|'.join(self.bug_keywords) + r')\b',
            re.IGNORECASE
        )

        self.feature_pattern = re.compile(
            r'\b(' + '|'.join(self.feature_keywords) + r')\b',
            re.IGNORECASE
        )

        self.urgent_pattern = re.compile(
            r'\b(' + '|'.join(self.urgent_keywords) + r')\b',
            re.IGNORECASE
        )

        self.docs_pattern = re.compile(
            r'\b(' + '|'.join(self.documentation_keywords) + r')\b',
            re.IGNORECASE
        )

        return True

    async def cleanup(self) -> None:
        """Cleanup plugin resources."""
        self.logger.info("Cleaning up Issue Classifier plugin")

    async def process_issue(self, repository: str, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Process issue data and add classification."""
        try:
            title = issue.get('title', '').lower()
            body = issue.get('body', '').lower()
            content = f"{title} {body}"

            # Get existing labels
            existing_labels = issue.get('labels', [])
            if isinstance(existing_labels, list):
                labels = [label['name'] if isinstance(label, dict) else str(label)
                         for label in existing_labels]
            else:
                labels = []

            # Classify issue type
            issue_type = self._classify_issue_type(content)
            if issue_type and issue_type not in labels:
                labels.append(issue_type)

            # Determine priority
            priority = self._determine_priority(content, issue.get('assignees', []))
            if priority and priority not in labels:
                labels.append(priority)

            # Add component classification
            component = self._classify_component(content, repository)
            if component and component not in labels:
                labels.append(component)

            # Add estimated effort
            effort = self._estimate_effort(content)
            if effort and effort not in labels:
                labels.append(effort)

            # Update issue with new labels
            issue['labels'] = labels

            # Add metadata about the classification
            if 'metadata' not in issue:
                issue['metadata'] = {}

            issue['metadata']['classified_by'] = 'issue_classifier_plugin'
            issue['metadata']['classification_timestamp'] = datetime.now().isoformat()
            issue['metadata']['classification_confidence'] = self._calculate_confidence(content)

            self.logger.info(f"Classified issue #{issue.get('number')} in {repository}: {labels}")

            return issue

        except Exception as e:
            self.logger.error(f"Error classifying issue #{issue.get('number')}: {e}")
            return issue

    async def process_pull_request(self, repository: str, pr: Dict[str, Any]) -> Dict[str, Any]:
        """Process pull request data (minimal classification for PRs)."""
        try:
            title = pr.get('title', '').lower()
            body = pr.get('body', '').lower()
            content = f"{title} {body}"

            # Get existing labels
            existing_labels = pr.get('labels', [])
            if isinstance(existing_labels, list):
                labels = [label['name'] if isinstance(label, dict) else str(label)
                         for label in existing_labels]
            else:
                labels = []

            # Simple PR classification
            if self.bug_pattern.search(content):
                if 'bugfix' not in labels:
                    labels.append('bugfix')
            elif self.feature_pattern.search(content):
                if 'feature' not in labels:
                    labels.append('feature')
            elif self.docs_pattern.search(content):
                if 'documentation' not in labels:
                    labels.append('documentation')

            # Estimate PR size based on title/description
            size = self._estimate_pr_size(content)
            if size and size not in labels:
                labels.append(size)

            pr['labels'] = labels

            self.logger.info(f"Classified PR #{pr.get('number')} in {repository}: {labels}")

            return pr

        except Exception as e:
            self.logger.error(f"Error classifying PR #{pr.get('number')}: {e}")
            return pr

    async def process_commit(self, repository: str, commit: Dict[str, Any]) -> Dict[str, Any]:
        """Process commit data (add commit type classification)."""
        try:
            message = commit.get('message', '').lower()

            # Classify commit type based on conventional commits
            commit_type = self._classify_commit_type(message)

            if 'metadata' not in commit:
                commit['metadata'] = {}

            commit['metadata']['commit_type'] = commit_type
            commit['metadata']['classified_by'] = 'issue_classifier_plugin'

            return commit

        except Exception as e:
            self.logger.error(f"Error classifying commit {commit.get('sha', 'unknown')}: {e}")
            return commit

    def _classify_issue_type(self, content: str) -> str:
        """Classify the type of issue."""
        if self.bug_pattern.search(content):
            return 'bug'
        elif self.feature_pattern.search(content):
            return 'enhancement'
        elif self.docs_pattern.search(content):
            return 'documentation'
        elif any(word in content for word in ['question', 'help', 'how to']):
            return 'question'
        else:
            return 'task'

    def _determine_priority(self, content: str, assignees: List[Any]) -> str:
        """Determine issue priority."""
        if self.urgent_pattern.search(content):
            return 'priority-critical'
        elif any(word in content for word in ['important', 'asap', 'soon']):
            return 'priority-high'
        elif len(assignees) > 1:  # Multiple assignees might indicate importance
            return 'priority-medium'
        else:
            return 'priority-low'

    def _classify_component(self, content: str, repository: str) -> str:
        """Classify which component the issue relates to."""
        # Simple component classification based on keywords
        components = {
            'frontend': ['ui', 'frontend', 'interface', 'css', 'javascript', 'react', 'vue'],
            'backend': ['api', 'backend', 'server', 'database', 'python', 'node'],
            'infrastructure': ['deploy', 'docker', 'kubernetes', 'ci/cd', 'infrastructure'],
            'security': ['security', 'auth', 'authentication', 'authorization', 'vulnerability'],
            'performance': ['performance', 'slow', 'optimization', 'speed', 'memory'],
            'integration': ['integration', 'webhook', 'sync', 'external']
        }

        for component, keywords in components.items():
            if any(keyword in content for keyword in keywords):
                return f'component-{component}'

        return 'component-general'

    def _estimate_effort(self, content: str) -> str:
        """Estimate effort required based on content."""
        # Simple heuristic based on content length and complexity indicators
        content_length = len(content)
        complexity_words = ['complex', 'difficult', 'challenging', 'refactor', 'redesign']

        if any(word in content for word in complexity_words) or content_length > 1000:
            return 'effort-large'
        elif content_length > 500:
            return 'effort-medium'
        else:
            return 'effort-small'

    def _estimate_pr_size(self, content: str) -> str:
        """Estimate PR size based on description."""
        if any(word in content for word in ['refactor', 'rewrite', 'major']):
            return 'size-large'
        elif any(word in content for word in ['feature', 'enhancement', 'improvement']):
            return 'size-medium'
        else:
            return 'size-small'

    def _classify_commit_type(self, message: str) -> str:
        """Classify commit type using conventional commits."""
        conventional_patterns = {
            'feat': r'^feat(\(.+\))?\s*:',
            'fix': r'^fix(\(.+\))?\s*:',
            'docs': r'^docs(\(.+\))?\s*:',
            'style': r'^style(\(.+\))?\s*:',
            'refactor': r'^refactor(\(.+\))?\s*:',
            'test': r'^test(\(.+\))?\s*:',
            'chore': r'^chore(\(.+\))?\s*:'
        }

        for commit_type, pattern in conventional_patterns.items():
            if re.match(pattern, message):
                return commit_type

        # Fallback classification based on keywords
        if any(word in message for word in ['fix', 'bug', 'error']):
            return 'fix'
        elif any(word in message for word in ['add', 'implement', 'feature']):
            return 'feat'
        elif any(word in message for word in ['update', 'change', 'modify']):
            return 'refactor'
        elif any(word in message for word in ['test', 'spec']):
            return 'test'
        else:
            return 'chore'

    def _calculate_confidence(self, content: str) -> float:
        """Calculate confidence score for the classification."""
        # Simple confidence calculation based on keyword matches
        total_patterns = 4  # bug, feature, urgent, docs
        matches = 0

        if self.bug_pattern.search(content):
            matches += 1
        if self.feature_pattern.search(content):
            matches += 1
        if self.urgent_pattern.search(content):
            matches += 1
        if self.docs_pattern.search(content):
            matches += 1

        # Base confidence on pattern matches and content length
        pattern_confidence = matches / total_patterns
        length_confidence = min(len(content) / 500, 1.0)  # More content = higher confidence

        return (pattern_confidence + length_confidence) / 2

    def validate_config(self) -> bool:
        """Validate plugin configuration."""
        required_keys = ['bug_keywords', 'feature_keywords', 'urgent_keywords', 'documentation_keywords']

        for key in required_keys:
            if key in self.config and not isinstance(self.config[key], list):
                self.logger.error(f"Configuration key '{key}' must be a list")
                return False

        return True
