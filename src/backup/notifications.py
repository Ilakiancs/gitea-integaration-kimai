#!/usr/bin/env python3
"""
Backup Notifications Module

Notification system for backup alerts, status updates, and
integration with various notification channels.
"""

import os
import logging
import smtplib
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

logger = logging.getLogger(__name__)

@dataclass
class NotificationConfig:
    """Configuration for notification channels."""
    email_enabled: bool = False
    email_smtp_server: str = ""
    email_smtp_port: int = 587
    email_username: str = ""
    email_password: str = ""
    email_from: str = ""
    email_to: List[str] = None
    
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    slack_channel: str = "#backups"
    
    webhook_enabled: bool = False
    webhook_url: str = ""
    webhook_headers: Dict[str, str] = None
    
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

class BackupNotifier:
    """Notification system for backup operations."""
    
    def __init__(self, config: NotificationConfig):
        self.config = config
        self.notification_handlers = {
            'email': self._send_email_notification,
            'slack': self._send_slack_notification,
            'webhook': self._send_webhook_notification,
            'telegram': self._send_telegram_notification
        }
    
    def send_notification(self, title: str, message: str, 
                         channels: List[str] = None, level: str = "info") -> Dict[str, bool]:
        """Send notification through specified channels."""
        if channels is None:
            channels = self._get_enabled_channels()
        
        results = {}
        
        for channel in channels:
            if channel in self.notification_handlers:
                try:
                    success = self.notification_handlers[channel](title, message, level)
                    results[channel] = success
                    if success:
                        logger.info(f"Notification sent via {channel}")
                    else:
                        logger.error(f"Failed to send notification via {channel}")
                except Exception as e:
                    logger.error(f"Error sending notification via {channel}: {e}")
                    results[channel] = False
            else:
                logger.warning(f"Unknown notification channel: {channel}")
                results[channel] = False
        
        return results
    
    def _get_enabled_channels(self) -> List[str]:
        """Get list of enabled notification channels."""
        channels = []
        
        if self.config.email_enabled:
            channels.append('email')
        if self.config.slack_enabled:
            channels.append('slack')
        if self.config.webhook_enabled:
            channels.append('webhook')
        if self.config.telegram_enabled:
            channels.append('telegram')
        
        return channels
    
    def _send_email_notification(self, title: str, message: str, level: str) -> bool:
        """Send email notification."""
        if not self.config.email_enabled:
            return False
        
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.config.email_from
            msg['To'] = ', '.join(self.config.email_to or [])
            msg['Subject'] = f"[Backup] {title}"
            
            # Create HTML body
            html_body = f"""
            <html>
            <body>
                <h2>Backup Notification</h2>
                <p><strong>Title:</strong> {title}</p>
                <p><strong>Level:</strong> {level.upper()}</p>
                <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <hr>
                <p>{message}</p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            with smtplib.SMTP(self.config.email_smtp_server, self.config.email_smtp_port) as server:
                server.starttls()
                server.login(self.config.email_username, self.config.email_password)
                server.send_message(msg)
            
            return True
            
        except Exception as e:
            logger.error(f"Email notification failed: {e}")
            return False
    
    def _send_slack_notification(self, title: str, message: str, level: str) -> bool:
        """Send Slack notification."""
        if not self.config.slack_enabled:
            return False
        
        try:
            # Determine color based on level
            color_map = {
                'info': '#36a64f',    # Green
                'warning': '#ff8c00',  # Orange
                'error': '#ff0000'     # Red
            }
            color = color_map.get(level, '#36a64f')
            
            # Create Slack message
            slack_data = {
                "channel": self.config.slack_channel,
                "attachments": [
                    {
                        "color": color,
                        "title": title,
                        "text": message,
                        "fields": [
                            {
                                "title": "Level",
                                "value": level.upper(),
                                "short": True
                            },
                            {
                                "title": "Time",
                                "value": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                "short": True
                            }
                        ],
                        "footer": "Backup System"
                    }
                ]
            }
            
            # Send to Slack
            response = requests.post(
                self.config.slack_webhook_url,
                data=json.dumps(slack_data),
                headers={'Content-Type': 'application/json'}
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Slack notification failed: {e}")
            return False
    
    def _send_webhook_notification(self, title: str, message: str, level: str) -> bool:
        """Send webhook notification."""
        if not self.config.webhook_enabled:
            return False
        
        try:
            # Create webhook payload
            payload = {
                "title": title,
                "message": message,
                "level": level,
                "timestamp": datetime.now().isoformat(),
                "source": "backup_system"
            }
            
            # Send webhook
            headers = self.config.webhook_headers or {'Content-Type': 'application/json'}
            response = requests.post(
                self.config.webhook_url,
                data=json.dumps(payload),
                headers=headers
            )
            
            return response.status_code in [200, 201, 202]
            
        except Exception as e:
            logger.error(f"Webhook notification failed: {e}")
            return False
    
    def _send_telegram_notification(self, title: str, message: str, level: str) -> bool:
        """Send Telegram notification."""
        if not self.config.telegram_enabled:
            return False
        
        try:
            # Create Telegram message
            telegram_message = f"""
🔔 *Backup Notification*

*Title:* {title}
*Level:* {level.upper()}
*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{message}
            """.strip()
            
            # Send to Telegram
            telegram_url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
            telegram_data = {
                "chat_id": self.config.telegram_chat_id,
                "text": telegram_message,
                "parse_mode": "Markdown"
            }
            
            response = requests.post(telegram_url, data=telegram_data)
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Telegram notification failed: {e}")
            return False
    
    def notify_backup_success(self, backup_name: str, size: int, duration: float):
        """Send backup success notification."""
        title = "Backup Completed Successfully"
        message = f"""
Backup '{backup_name}' completed successfully.

- Size: {size / (1024*1024):.1f} MB
- Duration: {duration:.2f} seconds
- Status: ✅ Success
        """.strip()
        
        return self.send_notification(title, message, level="info")
    
    def notify_backup_failure(self, backup_name: str, error: str):
        """Send backup failure notification."""
        title = "Backup Failed"
        message = f"""
Backup '{backup_name}' failed.

- Error: {error}
- Status: ❌ Failed
- Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """.strip()
        
        return self.send_notification(title, message, level="error")
    
    def notify_backup_warning(self, backup_name: str, warning: str):
        """Send backup warning notification."""
        title = "Backup Warning"
        message = f"""
Backup '{backup_name}' completed with warnings.

- Warning: {warning}
- Status: ⚠️ Warning
- Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """.strip()
        
        return self.send_notification(title, message, level="warning")
    
    def notify_system_alert(self, alert_type: str, message: str):
        """Send system alert notification."""
        title = f"System Alert: {alert_type}"
        message = f"""
System alert detected.

- Type: {alert_type}
- Message: {message}
- Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """.strip()
        
        return self.send_notification(title, message, level="error")
    
    def test_notifications(self) -> Dict[str, bool]:
        """Test all enabled notification channels."""
        title = "Backup System Test"
        message = "This is a test notification from the backup system."
        
        return self.send_notification(title, message, level="info")

def create_notifier(config: NotificationConfig) -> BackupNotifier:
    """Create and return a backup notifier instance."""
    return BackupNotifier(config)
