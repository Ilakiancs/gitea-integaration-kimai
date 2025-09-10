#!/usr/bin/env python3
"""
Notification Module for Gitea-Kimai Integration

This module provides notification capabilities for the Gitea-Kimai integration,
allowing it to send alerts via email, Slack, and other channels when
synchronization events occur.

Usage:
  Import this module and use the NotificationManager class to send notifications.
"""

import os
import json
import smtplib
import requests
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logger configuration
logger = logging.getLogger(__name__)

# Notification configuration from environment
NOTIFICATIONS_ENABLED = os.getenv('NOTIFICATIONS_ENABLED', 'false').lower() == 'true'
NOTIFICATION_LEVEL = os.getenv('NOTIFICATION_LEVEL', 'ERROR')  # INFO, WARNING, ERROR
NOTIFICATION_CHANNELS = os.getenv('NOTIFICATION_CHANNELS', 'console').split(',')

# Email settings
EMAIL_ENABLED = 'email' in NOTIFICATION_CHANNELS
EMAIL_SERVER = os.getenv('EMAIL_SERVER', '')
EMAIL_PORT = int(os.getenv('EMAIL_PORT', '587'))
EMAIL_USERNAME = os.getenv('EMAIL_USERNAME', '')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')
EMAIL_FROM = os.getenv('EMAIL_FROM', '')
EMAIL_TO = os.getenv('EMAIL_TO', '').split(',')
EMAIL_USE_TLS = os.getenv('EMAIL_USE_TLS', 'true').lower() == 'true'

# Slack settings
SLACK_ENABLED = 'slack' in NOTIFICATION_CHANNELS
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', '#general')
SLACK_USERNAME = os.getenv('SLACK_USERNAME', 'Gitea-Kimai Bot')

# Teams settings
TEAMS_ENABLED = 'teams' in NOTIFICATION_CHANNELS
TEAMS_WEBHOOK_URL = os.getenv('TEAMS_WEBHOOK_URL', '')

# Discord settings
DISCORD_ENABLED = 'discord' in NOTIFICATION_CHANNELS
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL', '')

# Notification levels
LEVELS = {
    'INFO': 0,
    'WARNING': 1,
    'ERROR': 2
}

class NotificationManager:
    """
    Manages notifications for the Gitea-Kimai integration.
    """

    def __init__(self, enabled=None):
        """Initialize the notification manager."""
        self.enabled = NOTIFICATIONS_ENABLED if enabled is None else enabled
        self.channels = NOTIFICATION_CHANNELS
        self.min_level = NOTIFICATION_LEVEL

        if self.enabled:
            logger.info(f"Notifications enabled. Channels: {', '.join(self.channels)}")
            logger.info(f"Minimum notification level: {self.min_level}")
        else:
            logger.info("Notifications disabled")

    def should_notify(self, level):
        """Determine if a notification should be sent based on its level."""
        if not self.enabled:
            return False

        return LEVELS.get(level, 0) >= LEVELS.get(self.min_level, 0)

    def format_message(self, title, message, level, details=None):
        """Format a notification message."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        formatted = {
            'title': title,
            'message': message,
            'level': level,
            'timestamp': timestamp
        }

        if details:
            formatted['details'] = details

        return formatted

    def notify(self, title, message, level='INFO', details=None):
        """
        Send a notification through all configured channels.

        Args:
            title (str): The notification title
            message (str): The notification message
            level (str): The notification level (INFO, WARNING, ERROR)
            details (dict): Additional details for the notification
        """
        if not self.should_notify(level):
            return

        formatted = self.format_message(title, message, level, details)

        for channel in self.channels:
            try:
                if channel == 'console':
                    self._notify_console(formatted)
                elif channel == 'email' and EMAIL_ENABLED:
                    self._notify_email(formatted)
                elif channel == 'slack' and SLACK_ENABLED:
                    self._notify_slack(formatted)
                elif channel == 'teams' and TEAMS_ENABLED:
                    self._notify_teams(formatted)
                elif channel == 'discord' and DISCORD_ENABLED:
                    self._notify_discord(formatted)
            except Exception as e:
                logger.error(f"Failed to send notification via {channel}: {e}")

    def _notify_console(self, data):
        """Send a notification to the console."""
        level = data['level']
        title = data['title']
        message = data['message']
        timestamp = data['timestamp']

        if level == 'ERROR':
            prefix = "ERROR"
        elif level == 'WARNING':
            prefix = "WARNING"
        else:
            prefix = "INFO"

        print(f"\n{prefix} - {timestamp}")
        print(f"Title: {title}")
        print(f"Message: {message}")

        if 'details' in data:
            print("Details:")
            for key, value in data['details'].items():
                print(f"  {key}: {value}")

    def _notify_email(self, data):
        """Send a notification via email."""
        if not EMAIL_SERVER or not EMAIL_FROM or not EMAIL_TO:
            logger.warning("Email notification failed: Missing configuration")
            return

        subject = f"[{data['level']}] {data['title']}"

        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = ', '.join(EMAIL_TO)
        msg['Subject'] = subject

        # Create email body
        body = f"""
        <html>
        <body>
            <h2>{data['title']}</h2>
            <p><strong>Time:</strong> {data['timestamp']}</p>
            <p><strong>Level:</strong> {data['level']}</p>
            <p><strong>Message:</strong> {data['message']}</p>
        """

        if 'details' in data:
            body += "<h3>Details:</h3><ul>"
            for key, value in data['details'].items():
                body += f"<li><strong>{key}:</strong> {value}</li>"
            body += "</ul>"

        body += """
        </body>
        </html>
        """

        msg.attach(MIMEText(body, 'html'))

        # Send email
        try:
            server = smtplib.SMTP(EMAIL_SERVER, EMAIL_PORT)
            if EMAIL_USE_TLS:
                server.starttls()
            if EMAIL_USERNAME and EMAIL_PASSWORD:
                server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.send_message(msg)
            server.quit()
            logger.info(f"Email notification sent to {', '.join(EMAIL_TO)}")
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")

    def _notify_slack(self, data):
        """Send a notification to Slack."""
        if not SLACK_WEBHOOK_URL:
            logger.warning("Slack notification failed: Missing webhook URL")
            return

        # Choose color based on level
        if data['level'] == 'ERROR':
            color = "#FF0000"  # Red
        elif data['level'] == 'WARNING':
            color = "#FFA500"  # Orange
        else:
            color = "#0000FF"  # Blue

        # Build attachment fields
        fields = [
            {
                "title": "Message",
                "value": data['message'],
                "short": False
            },
            {
                "title": "Level",
                "value": data['level'],
                "short": True
            },
            {
                "title": "Time",
                "value": data['timestamp'],
                "short": True
            }
        ]

        # Add details if available
        if 'details' in data:
            for key, value in data['details'].items():
                fields.append({
                    "title": key,
                    "value": str(value),
                    "short": True
                })

        # Create payload
        payload = {
            "channel": SLACK_CHANNEL,
            "username": SLACK_USERNAME,
            "attachments": [
                {
                    "fallback": f"{data['level']}: {data['title']}",
                    "color": color,
                    "title": data['title'],
                    "fields": fields
                }
            ]
        }

        # Send to Slack
        try:
            response = requests.post(
                SLACK_WEBHOOK_URL,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            logger.info("Slack notification sent")
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")

    def _notify_teams(self, data):
        """Send a notification to Microsoft Teams."""
        if not TEAMS_WEBHOOK_URL:
            logger.warning("Teams notification failed: Missing webhook URL")
            return

        # Choose color based on level
        if data['level'] == 'ERROR':
            theme_color = "FF0000"  # Red
        elif data['level'] == 'WARNING':
            theme_color = "FFA500"  # Orange
        else:
            theme_color = "0000FF"  # Blue

        # Create facts from details
        facts = []
        if 'details' in data:
            for key, value in data['details'].items():
                facts.append({
                    "name": key,
                    "value": str(value)
                })

        # Create payload
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": theme_color,
            "summary": data['title'],
            "sections": [
                {
                    "activityTitle": data['title'],
                    "activitySubtitle": f"{data['level']} - {data['timestamp']}",
                    "text": data['message'],
                    "facts": facts
                }
            ]
        }

        # Send to Teams
        try:
            response = requests.post(
                TEAMS_WEBHOOK_URL,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            logger.info("Teams notification sent")
        except Exception as e:
            logger.error(f"Failed to send Teams notification: {e}")

    def _notify_discord(self, data):
        """Send a notification to Discord."""
        if not DISCORD_WEBHOOK_URL:
            logger.warning("Discord notification failed: Missing webhook URL")
            return

        # Choose color based on level
        if data['level'] == 'ERROR':
            color = 0xFF0000  # Red
        elif data['level'] == 'WARNING':
            color = 0xFFA500  # Orange
        else:
            color = 0x0000FF  # Blue

        # Create fields from details
        fields = []
        if 'details' in data:
            for key, value in data['details'].items():
                fields.append({
                    "name": key,
                    "value": str(value),
                    "inline": True
                })

        # Create payload
        payload = {
            "embeds": [
                {
                    "title": data['title'],
                    "description": data['message'],
                    "color": color,
                    "fields": fields,
                    "footer": {
                        "text": f"{data['level']} - {data['timestamp']}"
                    }
                }
            ]
        }

        # Send to Discord
        try:
            response = requests.post(
                DISCORD_WEBHOOK_URL,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            logger.info("Discord notification sent")
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")

def send_notification(title, message, level='INFO', details=None):
    """
    Convenience function to send a notification.

    Args:
        title (str): The notification title
        message (str): The notification message
        level (str): The notification level (INFO, WARNING, ERROR)
        details (dict): Additional details for the notification
    """
    manager = NotificationManager()
    manager.notify(title, message, level, details)

# Example usage
if __name__ == "__main__":
    # Enable logging
    logging.basicConfig(level=logging.INFO)

    # Create notification manager
    manager = NotificationManager(enabled=True)

    # Send test notifications
    manager.notify(
        title="Test Notification",
        message="This is a test notification",
        level="INFO",
        details={
            "source": "Test script",
            "environment": "Development",
            "test_id": 12345
        }
    )

    manager.notify(
        title="Warning Notification",
        message="This is a warning notification",
        level="WARNING",
        details={
            "source": "Test script",
            "issue": "Resource usage high"
        }
    )

    manager.notify(
        title="Error Notification",
        message="This is an error notification",
        level="ERROR",
        details={
            "source": "Test script",
            "error_code": "E123",
            "function": "process_data"
        }
    )
