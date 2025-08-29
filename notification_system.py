#!/usr/bin/env python3
"""
Notification System

Provides a comprehensive notification system supporting multiple channels
(email, webhook, Slack, Discord) with templating and scheduling capabilities.
"""

import os
import json
import smtplib
import logging
import threading
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass, asdict
from pathlib import Path
import requests
from jinja2 import Template, Environment, FileSystemLoader

logger = logging.getLogger(__name__)

@dataclass
class NotificationTemplate:
    """Notification template configuration."""
    name: str
    subject: str
    body: str
    channel: str
    variables: List[str]
    enabled: bool = True

@dataclass
class NotificationConfig:
    """Notification configuration."""
    email: Dict[str, Any] = None
    webhook: Dict[str, Any] = None
    slack: Dict[str, Any] = None
    discord: Dict[str, Any] = None
    templates: List[NotificationTemplate] = None

@dataclass
class Notification:
    """Notification message."""
    id: str
    template_name: str
    channel: str
    subject: str
    body: str
    recipients: List[str]
    data: Dict[str, Any]
    priority: str = "normal"
    scheduled_at: Optional[datetime] = None
    sent_at: Optional[datetime] = None
    status: str = "pending"
    retry_count: int = 0
    max_retries: int = 3

class NotificationChannel:
    """Base class for notification channels."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.enabled = config.get('enabled', True)
    
    def send(self, notification: Notification) -> bool:
        """Send notification. Override in subclasses."""
        raise NotImplementedError
    
    def validate_config(self) -> bool:
        """Validate channel configuration. Override in subclasses."""
        return True

class EmailNotificationChannel(NotificationChannel):
    """Email notification channel."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.smtp_server = config.get('smtp_server', 'localhost')
        self.smtp_port = config.get('smtp_port', 587)
        self.username = config.get('username')
        self.password = config.get('password')
        self.from_email = config.get('from_email')
        self.use_tls = config.get('use_tls', True)
    
    def validate_config(self) -> bool:
        """Validate email configuration."""
        required_fields = ['username', 'password', 'from_email']
        return all(self.config.get(field) for field in required_fields)
    
    def send(self, notification: Notification) -> bool:
        """Send email notification."""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(notification.recipients)
            msg['Subject'] = notification.subject
            
            msg.attach(MIMEText(notification.body, 'html'))
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"Email notification sent to {notification.recipients}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False

class WebhookNotificationChannel(NotificationChannel):
    """Webhook notification channel."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.webhook_url = config.get('webhook_url')
        self.headers = config.get('headers', {})
        self.timeout = config.get('timeout', 30)
    
    def validate_config(self) -> bool:
        """Validate webhook configuration."""
        return bool(self.webhook_url)
    
    def send(self, notification: Notification) -> bool:
        """Send webhook notification."""
        try:
            payload = {
                'subject': notification.subject,
                'body': notification.body,
                'recipients': notification.recipients,
                'data': notification.data,
                'timestamp': datetime.now().isoformat()
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            logger.info(f"Webhook notification sent to {self.webhook_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send webhook notification: {e}")
            return False

class SlackNotificationChannel(NotificationChannel):
    """Slack notification channel."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.webhook_url = config.get('webhook_url')
        self.channel = config.get('channel', '#general')
        self.username = config.get('username', 'Gitea-Kimai Sync')
        self.icon_emoji = config.get('icon_emoji', ':robot_face:')
    
    def validate_config(self) -> bool:
        """Validate Slack configuration."""
        return bool(self.webhook_url)
    
    def send(self, notification: Notification) -> bool:
        """Send Slack notification."""
        try:
            payload = {
                'channel': self.channel,
                'username': self.username,
                'icon_emoji': self.icon_emoji,
                'text': f"*{notification.subject}*\n{notification.body}",
                'attachments': [
                    {
                        'fields': [
                            {
                                'title': key.title(),
                                'value': str(value),
                                'short': True
                            }
                            for key, value in notification.data.items()
                        ]
                    }
                ]
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Slack notification sent to {self.channel}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False

class DiscordNotificationChannel(NotificationChannel):
    """Discord notification channel."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.webhook_url = config.get('webhook_url')
        self.username = config.get('username', 'Gitea-Kimai Sync')
        self.avatar_url = config.get('avatar_url')
    
    def validate_config(self) -> bool:
        """Validate Discord configuration."""
        return bool(self.webhook_url)
    
    def send(self, notification: Notification) -> bool:
        """Send Discord notification."""
        try:
            payload = {
                'username': self.username,
                'content': f"**{notification.subject}**\n{notification.body}",
                'embeds': [
                    {
                        'title': notification.subject,
                        'description': notification.body,
                        'color': 0x00ff00,  # Green
                        'fields': [
                            {
                                'name': key.title(),
                                'value': str(value),
                                'inline': True
                            }
                            for key, value in notification.data.items()
                        ],
                        'timestamp': datetime.now().isoformat()
                    }
                ]
            }
            
            if self.avatar_url:
                payload['avatar_url'] = self.avatar_url
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"Discord notification sent")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")
            return False

class NotificationTemplateManager:
    """Manages notification templates."""
    
    def __init__(self, templates_dir: str = "templates"):
        self.templates_dir = Path(templates_dir)
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self.templates: Dict[str, NotificationTemplate] = {}
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            autoescape=True
        )
        self._load_templates()
    
    def _load_templates(self):
        """Load templates from directory."""
        template_files = self.templates_dir.glob("*.json")
        
        for template_file in template_files:
            try:
                with open(template_file, 'r') as f:
                    data = json.load(f)
                    template = NotificationTemplate(**data)
                    self.templates[template.name] = template
            except Exception as e:
                logger.error(f"Failed to load template {template_file}: {e}")
    
    def get_template(self, name: str) -> Optional[NotificationTemplate]:
        """Get template by name."""
        return self.templates.get(name)
    
    def create_template(self, template: NotificationTemplate) -> bool:
        """Create a new template."""
        try:
            template_file = self.templates_dir / f"{template.name}.json"
            with open(template_file, 'w') as f:
                json.dump(asdict(template), f, indent=2)
            
            self.templates[template.name] = template
            logger.info(f"Created template: {template.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create template {template.name}: {e}")
            return False
    
    def update_template(self, name: str, updates: Dict[str, Any]) -> bool:
        """Update an existing template."""
        template = self.templates.get(name)
        if not template:
            return False
        
        try:
            for key, value in updates.items():
                if hasattr(template, key):
                    setattr(template, key, value)
            
            template_file = self.templates_dir / f"{name}.json"
            with open(template_file, 'w') as f:
                json.dump(asdict(template), f, indent=2)
            
            logger.info(f"Updated template: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update template {name}: {e}")
            return False
    
    def delete_template(self, name: str) -> bool:
        """Delete a template."""
        try:
            template_file = self.templates_dir / f"{name}.json"
            if template_file.exists():
                template_file.unlink()
            
            if name in self.templates:
                del self.templates[name]
            
            logger.info(f"Deleted template: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete template {name}: {e}")
            return False
    
    def render_template(self, template_name: str, data: Dict[str, Any]) -> tuple[str, str]:
        """Render template with data."""
        template = self.get_template(template_name)
        if not template:
            raise ValueError(f"Template not found: {template_name}")
        
        try:
            subject_template = Template(template.subject)
            body_template = Template(template.body)
            
            subject = subject_template.render(**data)
            body = body_template.render(**data)
            
            return subject, body
            
        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {e}")
            raise

class NotificationScheduler:
    """Handles scheduled notifications."""
    
    def __init__(self):
        self.scheduled_notifications: Dict[str, Notification] = {}
        self.scheduler_thread = None
        self.running = False
    
    def schedule_notification(self, notification: Notification, delay_seconds: int = 0):
        """Schedule a notification for later delivery."""
        if delay_seconds > 0:
            notification.scheduled_at = datetime.now() + timedelta(seconds=delay_seconds)
            self.scheduled_notifications[notification.id] = notification
            logger.info(f"Scheduled notification {notification.id} for {notification.scheduled_at}")
    
    def start(self):
        """Start the scheduler thread."""
        if not self.running:
            self.running = True
            self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.scheduler_thread.start()
            logger.info("Notification scheduler started")
    
    def stop(self):
        """Stop the scheduler thread."""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
            logger.info("Notification scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        while self.running:
            try:
                current_time = datetime.now()
                ready_notifications = []
                
                for notification_id, notification in self.scheduled_notifications.items():
                    if notification.scheduled_at and notification.scheduled_at <= current_time:
                        ready_notifications.append(notification_id)
                
                for notification_id in ready_notifications:
                    notification = self.scheduled_notifications.pop(notification_id)
                    # The notification system will handle sending
                    logger.info(f"Notification {notification_id} ready for delivery")
                
                time.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(5)

class NotificationSystem:
    """Main notification system."""
    
    def __init__(self, config: NotificationConfig):
        self.config = config
        self.channels: Dict[str, NotificationChannel] = {}
        self.template_manager = NotificationTemplateManager()
        self.scheduler = NotificationScheduler()
        self.notification_queue: List[Notification] = []
        self.worker_thread = None
        self.running = False
        
        self._setup_channels()
        self._create_default_templates()
    
    def _setup_channels(self):
        """Setup notification channels."""
        if self.config.email:
            self.channels['email'] = EmailNotificationChannel(self.config.email)
        
        if self.config.webhook:
            self.channels['webhook'] = WebhookNotificationChannel(self.config.webhook)
        
        if self.config.slack:
            self.channels['slack'] = SlackNotificationChannel(self.config.slack)
        
        if self.config.discord:
            self.channels['discord'] = DiscordNotificationChannel(self.config.discord)
    
    def _create_default_templates(self):
        """Create default notification templates."""
        default_templates = [
            NotificationTemplate(
                name="sync_started",
                subject="Sync Started - {{repository}}",
                body="""
                <h2>Sync Operation Started</h2>
                <p><strong>Repository:</strong> {{repository}}</p>
                <p><strong>Started at:</strong> {{started_at}}</p>
                <p><strong>User:</strong> {{user}}</p>
                """,
                channel="email",
                variables=["repository", "started_at", "user"]
            ),
            NotificationTemplate(
                name="sync_completed",
                subject="Sync Completed - {{repository}}",
                body="""
                <h2>Sync Operation Completed</h2>
                <p><strong>Repository:</strong> {{repository}}</p>
                <p><strong>Items synced:</strong> {{items_synced}}</p>
                <p><strong>Duration:</strong> {{duration}}</p>
                <p><strong>Status:</strong> {{status}}</p>
                """,
                channel="email",
                variables=["repository", "items_synced", "duration", "status"]
            ),
            NotificationTemplate(
                name="sync_failed",
                subject="Sync Failed - {{repository}}",
                body="""
                <h2>Sync Operation Failed</h2>
                <p><strong>Repository:</strong> {{repository}}</p>
                <p><strong>Error:</strong> {{error}}</p>
                <p><strong>Failed at:</strong> {{failed_at}}</p>
                """,
                channel="email",
                variables=["repository", "error", "failed_at"]
            ),
            NotificationTemplate(
                name="system_alert",
                subject="System Alert - {{alert_type}}",
                body="""
                <h2>System Alert</h2>
                <p><strong>Type:</strong> {{alert_type}}</p>
                <p><strong>Message:</strong> {{message}}</p>
                <p><strong>Severity:</strong> {{severity}}</p>
                """,
                channel="webhook",
                variables=["alert_type", "message", "severity"]
            )
        ]
        
        for template in default_templates:
            if not self.template_manager.get_template(template.name):
                self.template_manager.create_template(template)
    
    def start(self):
        """Start the notification system."""
        self.running = True
        self.scheduler.start()
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        logger.info("Notification system started")
    
    def stop(self):
        """Stop the notification system."""
        self.running = False
        self.scheduler.stop()
        if self.worker_thread:
            self.worker_thread.join()
        logger.info("Notification system stopped")
    
    def _worker_loop(self):
        """Main worker loop for processing notifications."""
        while self.running:
            try:
                if self.notification_queue:
                    notification = self.notification_queue.pop(0)
                    self._send_notification(notification)
                else:
                    time.sleep(0.1)  # Small delay when queue is empty
                    
            except Exception as e:
                logger.error(f"Error in notification worker loop: {e}")
                time.sleep(1)
    
    def _send_notification(self, notification: Notification):
        """Send a notification through the appropriate channel."""
        channel = self.channels.get(notification.channel)
        if not channel:
            logger.error(f"Channel not found: {notification.channel}")
            notification.status = "failed"
            return
        
        if not channel.enabled:
            logger.warning(f"Channel {notification.channel} is disabled")
            notification.status = "skipped"
            return
        
        try:
            success = channel.send(notification)
            if success:
                notification.status = "sent"
                notification.sent_at = datetime.now()
            else:
                notification.status = "failed"
                notification.retry_count += 1
                
                if notification.retry_count < notification.max_retries:
                    # Re-queue for retry
                    self.notification_queue.append(notification)
                    logger.info(f"Re-queued notification {notification.id} for retry")
                
        except Exception as e:
            logger.error(f"Failed to send notification {notification.id}: {e}")
            notification.status = "failed"
    
    def send_notification(self, template_name: str, data: Dict[str, Any], 
                         recipients: List[str], channel: str = None,
                         priority: str = "normal", delay_seconds: int = 0) -> str:
        """Send a notification using a template."""
        try:
            # Render template
            subject, body = self.template_manager.render_template(template_name, data)
            
            # Determine channel
            if not channel:
                template = self.template_manager.get_template(template_name)
                channel = template.channel if template else "email"
            
            # Create notification
            notification = Notification(
                id=f"notif_{int(time.time() * 1000)}",
                template_name=template_name,
                channel=channel,
                subject=subject,
                body=body,
                recipients=recipients,
                data=data,
                priority=priority
            )
            
            # Schedule or queue
            if delay_seconds > 0:
                self.scheduler.schedule_notification(notification, delay_seconds)
            else:
                self.notification_queue.append(notification)
            
            logger.info(f"Queued notification {notification.id} using template {template_name}")
            return notification.id
            
        except Exception as e:
            logger.error(f"Failed to create notification: {e}")
            raise
    
    def send_direct_notification(self, subject: str, body: str, recipients: List[str],
                                channel: str = "email", data: Dict[str, Any] = None,
                                priority: str = "normal") -> str:
        """Send a direct notification without using a template."""
        notification = Notification(
            id=f"notif_{int(time.time() * 1000)}",
            template_name="direct",
            channel=channel,
            subject=subject,
            body=body,
            recipients=recipients,
            data=data or {},
            priority=priority
        )
        
        self.notification_queue.append(notification)
        logger.info(f"Queued direct notification {notification.id}")
        return notification.id
    
    def get_notification_status(self, notification_id: str) -> Optional[Dict[str, Any]]:
        """Get notification status."""
        # This would typically query a database
        # For now, return basic info
        return {
            'id': notification_id,
            'status': 'unknown'
        }
    
    def get_channel_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all channels."""
        status = {}
        for name, channel in self.channels.items():
            status[name] = {
                'enabled': channel.enabled,
                'valid_config': channel.validate_config()
            }
        return status

def create_notification_system(config_file: str = "notification_config.json") -> NotificationSystem:
    """Create notification system from config file."""
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)
    else:
        # Default configuration
        config_data = {
            'email': {
                'enabled': True,
                'smtp_server': 'localhost',
                'smtp_port': 587,
                'use_tls': True,
                'username': os.getenv('EMAIL_USERNAME'),
                'password': os.getenv('EMAIL_PASSWORD'),
                'from_email': os.getenv('EMAIL_FROM')
            },
            'webhook': {
                'enabled': False,
                'webhook_url': os.getenv('WEBHOOK_URL')
            },
            'slack': {
                'enabled': False,
                'webhook_url': os.getenv('SLACK_WEBHOOK_URL'),
                'channel': '#general'
            },
            'discord': {
                'enabled': False,
                'webhook_url': os.getenv('DISCORD_WEBHOOK_URL')
            }
        }
    
    config = NotificationConfig(**config_data)
    return NotificationSystem(config)

if __name__ == "__main__":
    # Example usage
    notification_system = create_notification_system()
    notification_system.start()
    
    try:
        # Send a test notification
        notification_system.send_notification(
            template_name="system_alert",
            data={
                'alert_type': 'Test Alert',
                'message': 'This is a test notification',
                'severity': 'info'
            },
            recipients=['admin@example.com']
        )
        
        time.sleep(5)  # Wait for processing
        
    finally:
        notification_system.stop()
