#!/usr/bin/env python3
"""
Webhooks Module for Gitea-Kimai Integration

This module provides webhook support for the Gitea-Kimai Integration,
allowing the synchronization to be triggered by external events such as
Gitea issue/PR changes via webhooks.

Usage:
  python webhooks.py start
  python webhooks.py stop
  python webhooks.py status
"""

import os
import sys
import json
import time
import signal
import logging
import sqlite3
import hashlib
import argparse
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhooks.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
WEBHOOK_ENABLED = os.getenv('WEBHOOK_ENABLED', 'false').lower() == 'true'
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST', 'localhost')
WEBHOOK_PORT = int(os.getenv('WEBHOOK_PORT', '9000'))
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '')
WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', '/webhook')
WEBHOOK_PID_FILE = os.getenv('WEBHOOK_PID_FILE', 'webhook.pid')
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')

# Supported Gitea event types
SUPPORTED_EVENTS = [
    'issue',               # Issue created, edited, closed, etc.
    'pull_request',        # PR opened, closed, merged, etc.
    'repository',          # Repository created, deleted, etc.
    'push',                # Push to repository
    'release',             # Release created
    'ping'                 # Test webhook
]

class WebhookHandler(BaseHTTPRequestHandler):
    """HTTP handler for webhooks."""

    def log_message(self, format, *args):
        """Override to use our logger instead of built-in logging."""
        logger.info(format % args)

    def _send_response(self, status_code=200, message="OK"):
        """Send a JSON response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': message}).encode())

    def _verify_signature(self, payload_body):
        """Verify the webhook signature if a secret is configured."""
        if not WEBHOOK_SECRET:
            # No secret configured, skip verification
            return True

        signature_header = self.headers.get('X-Gitea-Signature')
        if not signature_header:
            logger.warning("No X-Gitea-Signature header in request")
            return False

        # Calculate expected signature
        expected_signature = 'sha256=' + hmac.new(
            WEBHOOK_SECRET.encode(),
            payload_body,
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature_header, expected_signature)

    def _get_payload(self):
        """Get and parse the webhook payload."""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            return None, None

        payload_body = self.rfile.read(content_length)

        # Verify signature
        if not self._verify_signature(payload_body):
            return None, payload_body

        try:
            payload = json.loads(payload_body.decode('utf-8'))
            return payload, payload_body
        except json.JSONDecodeError:
            logger.error("Failed to parse webhook payload as JSON")
            return None, payload_body

    def do_POST(self):
        """Handle POST requests (webhook events)."""
        if self.path != WEBHOOK_PATH:
            self._send_response(404, "Not Found")
            return

        # Get event type from header
        event_type = self.headers.get('X-Gitea-Event')
        if not event_type or event_type not in SUPPORTED_EVENTS:
            logger.warning(f"Unsupported or missing event type: {event_type}")
            self._send_response(400, "Unsupported event type")
            return

        # Get payload
        payload, raw_payload = self._get_payload()
        if not payload:
            self._send_response(400, "Invalid payload")
            return

        # Log the event
        logger.info(f"Received {event_type} webhook event")

        # Process the webhook
        try:
            self._process_webhook(event_type, payload)
            self._send_response(200, "Webhook processed successfully")
        except Exception as e:
            logger.error(f"Error processing webhook: {e}")
            self._send_response(500, "Error processing webhook")

    def _process_webhook(self, event_type, payload):
        """Process the webhook based on the event type."""
        # Record webhook in database
        self._record_webhook(event_type, payload)

        # Handle specific event types
        if event_type == 'issue':
            self._handle_issue_event(payload)
        elif event_type == 'pull_request':
            self._handle_pull_request_event(payload)
        elif event_type == 'repository':
            self._handle_repository_event(payload)
        elif event_type == 'push':
            self._handle_push_event(payload)
        elif event_type == 'ping':
            logger.info("Received ping event - webhook configured correctly")

    def _record_webhook(self, event_type, payload):
        """Record webhook event in the database."""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()

            # Create webhook_events table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS webhook_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    processed BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Insert the webhook event
            cursor.execute(
                "INSERT INTO webhook_events (event_type, payload) VALUES (?, ?)",
                (event_type, json.dumps(payload))
            )

            conn.commit()
            conn.close()
        except sqlite3.Error as e:
            logger.error(f"Database error recording webhook: {e}")

    def _handle_issue_event(self, payload):
        """Handle issue events (created, edited, closed, etc.)."""
        action = payload.get('action')
        issue = payload.get('issue', {})
        repository = payload.get('repository', {})

        if not issue or not repository:
            logger.warning("Missing issue or repository data in payload")
            return

        repo_name = repository.get('name')
        issue_number = issue.get('number')
        issue_title = issue.get('title')

        logger.info(f"Issue #{issue_number} {action}: {issue_title} in {repo_name}")

        # Trigger a sync for this repository
        self._trigger_sync(repo_name)

    def _handle_pull_request_event(self, payload):
        """Handle pull request events (opened, closed, merged, etc.)."""
        action = payload.get('action')
        pull_request = payload.get('pull_request', {})
        repository = payload.get('repository', {})

        if not pull_request or not repository:
            logger.warning("Missing pull request or repository data in payload")
            return

        repo_name = repository.get('name')
        pr_number = pull_request.get('number')
        pr_title = pull_request.get('title')

        logger.info(f"PR #{pr_number} {action}: {pr_title} in {repo_name}")

        # Only trigger sync if SYNC_PULL_REQUESTS is enabled
        if os.getenv('SYNC_PULL_REQUESTS', 'false').lower() == 'true':
            self._trigger_sync(repo_name)
        else:
            logger.info("Pull request syncing is disabled - not triggering sync")

    def _handle_repository_event(self, payload):
        """Handle repository events (created, deleted, etc.)."""
        action = payload.get('action')
        repository = payload.get('repository', {})

        if not repository:
            logger.warning("Missing repository data in payload")
            return

        repo_name = repository.get('name')
        logger.info(f"Repository {repo_name} {action}")

        # Only trigger sync for certain actions
        if action in ['created', 'edited', 'renamed']:
            self._trigger_sync(repo_name)

    def _handle_push_event(self, payload):
        """Handle push events."""
        repository = payload.get('repository', {})
        ref = payload.get('ref', '')

        if not repository:
            logger.warning("Missing repository data in payload")
            return

        repo_name = repository.get('name')
        branch = ref.replace('refs/heads/', '')

        logger.info(f"Push to {repo_name}/{branch}")

        # Typically don't trigger sync on push events unless configured to do so
        if os.getenv('SYNC_ON_PUSH', 'false').lower() == 'true':
            self._trigger_sync(repo_name)

    def _trigger_sync(self, repository):
        """Trigger a synchronization for the specified repository."""
        logger.info(f"Triggering sync for repository: {repository}")

        try:
            # Run sync.py in a separate process
            command = ['python', 'sync.py', '--repos', repository]

            # Run in background
            subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            logger.info(f"Sync started for {repository}")
        except Exception as e:
            logger.error(f"Failed to trigger sync: {e}")


class WebhookServer:
    """Webhook server for handling Gitea events."""

    def __init__(self, host=WEBHOOK_HOST, port=WEBHOOK_PORT):
        """Initialize with host and port."""
        self.host = host
        self.port = port
        self.server = None
        self.running = False

    def start(self, foreground=False):
        """Start the webhook server."""
        if not WEBHOOK_ENABLED:
            logger.error("Webhooks are disabled. Set WEBHOOK_ENABLED=true to enable.")
            return False

        try:
            self.server = HTTPServer((self.host, self.port), WebhookHandler)
            self.running = True

            logger.info(f"Webhook server starting on {self.host}:{self.port} (path: {WEBHOOK_PATH})")

            if foreground:
                # Run in foreground
                try:
                    self._write_pid()
                    self.server.serve_forever()
                except KeyboardInterrupt:
                    logger.info("Webhook server stopped by user")
                finally:
                    self.stop()
            else:
                # Run as daemon
                pid = os.fork()
                if pid > 0:
                    # Parent process
                    logger.info(f"Webhook server started with PID {pid}")
                    return True

                # Child process
                os.setsid()  # Create new session

                # Fork again
                pid = os.fork()
                if pid > 0:
                    # Exit first child
                    sys.exit(0)

                # Second child
                self._write_pid()

                # Close standard file descriptors
                sys.stdin.close()
                sys.stdout.close()
                sys.stderr.close()

                # Run server
                try:
                    self.server.serve_forever()
                except Exception as e:
                    logger.error(f"Webhook server error: {e}")
                    return False

            return True

        except Exception as e:
            logger.error(f"Failed to start webhook server: {e}")
            return False

    def stop(self):
        """Stop the webhook server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.running = False

            logger.info("Webhook server stopped")

            # Remove PID file
            if os.path.exists(WEBHOOK_PID_FILE):
                os.remove(WEBHOOK_PID_FILE)

            return True

        return False

    def _write_pid(self):
        """Write the current PID to the PID file."""
        with open(WEBHOOK_PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

def start_server(foreground=False):
    """Start the webhook server."""
    server = WebhookServer(WEBHOOK_HOST, WEBHOOK_PORT)
    return server.start(foreground)

def stop_server():
    """Stop the webhook server."""
    if not os.path.exists(WEBHOOK_PID_FILE):
        logger.error("PID file not found - webhook server not running?")
        return False

    try:
        with open(WEBHOOK_PID_FILE, 'r') as f:
            pid = int(f.read().strip())

        # Send SIGTERM to the process
        os.kill(pid, signal.SIGTERM)

        # Wait a bit for the server to shutdown
        time.sleep(1)

        # Check if process is still running
        try:
            os.kill(pid, 0)  # Signal 0 checks if process exists
            logger.warning("Process still running, sending SIGKILL")
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            # Process is gone, which is what we want
            pass

        # Remove PID file
        if os.path.exists(WEBHOOK_PID_FILE):
            os.remove(WEBHOOK_PID_FILE)

        logger.info("Webhook server stopped")
        return True

    except Exception as e:
        logger.error(f"Error stopping webhook server: {e}")
        return False

def check_status():
    """Check if the webhook server is running."""
    if not os.path.exists(WEBHOOK_PID_FILE):
        return "stopped"

    try:
        with open(WEBHOOK_PID_FILE, 'r') as f:
            pid = int(f.read().strip())

        # Check if process is running
        try:
            os.kill(pid, 0)  # Signal 0 checks if process exists
            return "running"
        except ProcessLookupError:
            # Process is not running
            os.remove(WEBHOOK_PID_FILE)
            return "stopped"

    except Exception as e:
        logger.error(f"Error checking webhook server status: {e}")
        return "unknown"

def get_webhook_url():
    """Get the full webhook URL for configuration."""
    host = WEBHOOK_HOST
    if host == '0.0.0.0' or host == 'localhost':
        # Try to get the actual machine hostname
        import socket
        try:
            host = socket.gethostname()
        except:
            host = 'localhost'

    protocol = "http"  # In production, you might want to use HTTPS
    return f"{protocol}://{host}:{WEBHOOK_PORT}{WEBHOOK_PATH}"

def generate_webhook_secret():
    """Generate a random secret for webhook security."""
    return hashlib.sha256(os.urandom(32)).hexdigest()

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Webhook server for Gitea-Kimai integration")

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Start command
    start_parser = subparsers.add_parser('start', help='Start the webhook server')
    start_parser.add_argument('--foreground', action='store_true', help='Run in foreground')

    # Stop command
    subparsers.add_parser('stop', help='Stop the webhook server')

    # Status command
    subparsers.add_parser('status', help='Check webhook server status')

    # Generate secret command
    subparsers.add_parser('generate-secret', help='Generate a new webhook secret')

    # URL command
    subparsers.add_parser('url', help='Show the webhook URL to use in Gitea')

    args = parser.parse_args()

    if args.command == 'start':
        if start_server(args.foreground):
            if not args.foreground:
                print("Webhook server started in the background")
        else:
            print("Failed to start webhook server")
            return 1

    elif args.command == 'stop':
        if stop_server():
            print("Webhook server stopped")
        else:
            print("Failed to stop webhook server")
            return 1

    elif args.command == 'status':
        status = check_status()
        print(f"Webhook server is {status}")

        if status == "running":
            print(f"Webhook URL: {get_webhook_url()}")

    elif args.command == 'generate-secret':
        secret = generate_webhook_secret()
        print(f"Generated webhook secret: {secret}")
        print("Add this to your .env file as WEBHOOK_SECRET=<secret>")

    elif args.command == 'url':
        print(f"Webhook URL: {get_webhook_url()}")
        print("\nIn Gitea, create a webhook with:")
        print(f"  - Target URL: {get_webhook_url()}")
        print("  - Content Type: application/json")
        if WEBHOOK_SECRET:
            print("  - Secret: [Your configured secret]")
        else:
            print("  - Secret: [No secret configured - consider adding one for security]")
        print("  - Trigger On: Issues, Pull Requests, Repository")

    else:
        parser.print_help()

    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
