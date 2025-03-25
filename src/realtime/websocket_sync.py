#!/usr/bin/env python3
"""
Real-time WebSocket Sync for Gitea-Kimai Integration

This module provides real-time synchronization capabilities using WebSocket
connections to enable instant updates between Gitea and Kimai systems.
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Dict, List, Any, Optional, Callable, Set
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
import websockets
from websockets.server import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosed, InvalidState

logger = logging.getLogger(__name__)

class SyncEventType(Enum):
    """Types of sync events for real-time updates."""
    ISSUE_CREATED = "issue_created"
    ISSUE_UPDATED = "issue_updated"
    ISSUE_CLOSED = "issue_closed"
    TIMESHEET_CREATED = "timesheet_created"
    TIMESHEET_UPDATED = "timesheet_updated"
    TIMESHEET_DELETED = "timesheet_deleted"
    PROJECT_CREATED = "project_created"
    PROJECT_UPDATED = "project_updated"
    SYNC_STARTED = "sync_started"
    SYNC_COMPLETED = "sync_completed"
    SYNC_ERROR = "sync_error"
    HEARTBEAT = "heartbeat"

class ClientType(Enum):
    """Types of WebSocket clients."""
    DASHBOARD = "dashboard"
    API_CLIENT = "api_client"
    MOBILE_APP = "mobile_app"
    WEBHOOK_LISTENER = "webhook_listener"
    ADMIN_PANEL = "admin_panel"

@dataclass
class SyncEvent:
    """Real-time sync event structure."""
    event_id: str
    event_type: SyncEventType
    timestamp: datetime
    source_system: str
    target_system: Optional[str] = None
    resource_id: Optional[str] = None
    resource_type: Optional[str] = None
    user_id: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class WebSocketClient:
    """WebSocket client information."""
    client_id: str
    websocket: WebSocketServerProtocol
    client_type: ClientType
    user_id: Optional[str] = None
    subscriptions: Set[str] = None
    last_heartbeat: Optional[datetime] = None
    connected_at: datetime = None

    def __post_init__(self):
        if self.subscriptions is None:
            self.subscriptions = set()
        if self.connected_at is None:
            self.connected_at = datetime.now()

class WebSocketSyncManager:
    """Manages real-time WebSocket synchronization."""

    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.clients: Dict[str, WebSocketClient] = {}
        self.event_handlers: Dict[SyncEventType, List[Callable]] = {}
        self.server = None
        self.running = False
        self.event_queue = asyncio.Queue()
        self.heartbeat_interval = 30  # seconds
        self.max_message_size = 1024 * 1024  # 1MB

        # Initialize event handlers
        self._setup_default_handlers()

    def _setup_default_handlers(self):
        """Setup default event handlers."""
        for event_type in SyncEventType:
            self.event_handlers[event_type] = []

    async def start_server(self):
        """Start the WebSocket server."""
        if self.running:
            logger.warning("WebSocket server already running")
            return

        logger.info(f"Starting WebSocket server on {self.host}:{self.port}")

        try:
            self.server = await websockets.serve(
                self._handle_client,
                self.host,
                self.port,
                max_size=self.max_message_size,
                ping_interval=20,
                ping_timeout=10
            )

            self.running = True

            # Start background tasks
            asyncio.create_task(self._process_events())
            asyncio.create_task(self._heartbeat_monitor())

            logger.info(f"WebSocket server started successfully")

        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")
            raise

    async def stop_server(self):
        """Stop the WebSocket server."""
        if not self.running:
            return

        logger.info("Stopping WebSocket server")
        self.running = False

        # Close all client connections
        if self.clients:
            await self._disconnect_all_clients()

        # Stop the server
        if self.server:
            self.server.close()
            await self.server.wait_closed()

        logger.info("WebSocket server stopped")

    async def _handle_client(self, websocket: WebSocketServerProtocol, path: str):
        """Handle new WebSocket client connection."""
        client_id = str(uuid.uuid4())
        client = WebSocketClient(
            client_id=client_id,
            websocket=websocket,
            client_type=ClientType.API_CLIENT,  # Default type
            last_heartbeat=datetime.now()
        )

        self.clients[client_id] = client
        logger.info(f"New WebSocket client connected: {client_id}")

        try:
            # Send welcome message
            await self._send_to_client(client, {
                "type": "connection_established",
                "client_id": client_id,
                "server_time": datetime.now().isoformat()
            })

            # Handle messages from client
            async for message in websocket:
                await self._handle_client_message(client, message)

        except ConnectionClosed:
            logger.info(f"Client {client_id} disconnected")
        except Exception as e:
            logger.error(f"Error handling client {client_id}: {e}")
        finally:
            # Clean up client
            if client_id in self.clients:
                del self.clients[client_id]

    async def _handle_client_message(self, client: WebSocketClient, message: str):
        """Handle message from WebSocket client."""
        try:
            data = json.loads(message)
            message_type = data.get("type")

            if message_type == "authenticate":
                await self._handle_authentication(client, data)
            elif message_type == "subscribe":
                await self._handle_subscription(client, data)
            elif message_type == "unsubscribe":
                await self._handle_unsubscription(client, data)
            elif message_type == "heartbeat":
                await self._handle_heartbeat(client, data)
            elif message_type == "sync_request":
                await self._handle_sync_request(client, data)
            else:
                logger.warning(f"Unknown message type from client {client.client_id}: {message_type}")

        except json.JSONDecodeError:
            logger.error(f"Invalid JSON from client {client.client_id}")
            await self._send_error(client, "Invalid JSON format")
        except Exception as e:
            logger.error(f"Error processing message from client {client.client_id}: {e}")
            await self._send_error(client, f"Message processing error: {e}")

    async def _handle_authentication(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle client authentication."""
        user_id = data.get("user_id")
        client_type = data.get("client_type", "api_client")
        token = data.get("token")

        # Simple token validation (in production, use proper JWT validation)
        if self._validate_token(token, user_id):
            client.user_id = user_id
            client.client_type = ClientType(client_type)

            await self._send_to_client(client, {
                "type": "authentication_success",
                "user_id": user_id,
                "client_type": client_type
            })

            logger.info(f"Client {client.client_id} authenticated as {user_id}")
        else:
            await self._send_error(client, "Authentication failed")

    async def _handle_subscription(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle client subscription to events."""
        subscriptions = data.get("subscriptions", [])

        for subscription in subscriptions:
            client.subscriptions.add(subscription)

        await self._send_to_client(client, {
            "type": "subscription_success",
            "subscriptions": list(client.subscriptions)
        })

        logger.debug(f"Client {client.client_id} subscribed to: {subscriptions}")

    async def _handle_unsubscription(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle client unsubscription from events."""
        subscriptions = data.get("subscriptions", [])

        for subscription in subscriptions:
            client.subscriptions.discard(subscription)

        await self._send_to_client(client, {
            "type": "unsubscription_success",
            "remaining_subscriptions": list(client.subscriptions)
        })

    async def _handle_heartbeat(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle client heartbeat."""
        client.last_heartbeat = datetime.now()

        await self._send_to_client(client, {
            "type": "heartbeat_ack",
            "server_time": datetime.now().isoformat()
        })

    async def _handle_sync_request(self, client: WebSocketClient, data: Dict[str, Any]):
        """Handle manual sync request from client."""
        sync_type = data.get("sync_type", "incremental")
        repository = data.get("repository")

        # Create sync event
        event = SyncEvent(
            event_id=str(uuid.uuid4()),
            event_type=SyncEventType.SYNC_STARTED,
            timestamp=datetime.now(),
            source_system="manual",
            user_id=client.user_id,
            data={
                "sync_type": sync_type,
                "repository": repository,
                "requested_by": client.client_id
            }
        )

        await self.publish_event(event)

    def _validate_token(self, token: str, user_id: str) -> bool:
        """Validate authentication token (simplified)."""
        # In production, implement proper JWT token validation
        return token is not None and len(token) > 10

    async def _send_to_client(self, client: WebSocketClient, message: Dict[str, Any]):
        """Send message to a specific client."""
        try:
            if client.websocket.open:
                await client.websocket.send(json.dumps(message))
        except (ConnectionClosed, InvalidState):
            logger.debug(f"Client {client.client_id} connection closed")
        except Exception as e:
            logger.error(f"Error sending message to client {client.client_id}: {e}")

    async def _send_error(self, client: WebSocketClient, error_message: str):
        """Send error message to client."""
        await self._send_to_client(client, {
            "type": "error",
            "message": error_message,
            "timestamp": datetime.now().isoformat()
        })

    async def _disconnect_all_clients(self):
        """Disconnect all clients gracefully."""
        disconnect_tasks = []

        for client in self.clients.values():
            disconnect_tasks.append(self._disconnect_client(client))

        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)

    async def _disconnect_client(self, client: WebSocketClient):
        """Disconnect a specific client."""
        try:
            await self._send_to_client(client, {
                "type": "server_shutdown",
                "message": "Server is shutting down"
            })
            await client.websocket.close()
        except Exception as e:
            logger.error(f"Error disconnecting client {client.client_id}: {e}")

    async def _process_events(self):
        """Process events from the event queue."""
        while self.running:
            try:
                # Wait for event with timeout
                event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                await self._broadcast_event(event)
                self.event_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing event: {e}")

    async def _broadcast_event(self, event: SyncEvent):
        """Broadcast event to subscribed clients."""
        event_data = {
            "type": "sync_event",
            "event": asdict(event)
        }

        # Convert datetime objects to ISO format
        event_data["event"]["timestamp"] = event.timestamp.isoformat()

        broadcast_tasks = []

        for client in self.clients.values():
            # Check if client is subscribed to this event type
            if (event.event_type.value in client.subscriptions or
                "all" in client.subscriptions):
                broadcast_tasks.append(self._send_to_client(client, event_data))

        if broadcast_tasks:
            await asyncio.gather(*broadcast_tasks, return_exceptions=True)

        # Execute event handlers
        handlers = self.event_handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Error in event handler: {e}")

    async def _heartbeat_monitor(self):
        """Monitor client heartbeats and disconnect stale clients."""
        while self.running:
            try:
                current_time = datetime.now()
                stale_clients = []

                for client in self.clients.values():
                    if client.last_heartbeat:
                        time_diff = (current_time - client.last_heartbeat).total_seconds()
                        if time_diff > self.heartbeat_interval * 2:  # 2x interval tolerance
                            stale_clients.append(client)

                # Disconnect stale clients
                for client in stale_clients:
                    logger.info(f"Disconnecting stale client: {client.client_id}")
                    await self._disconnect_client(client)
                    if client.client_id in self.clients:
                        del self.clients[client.client_id]

                await asyncio.sleep(self.heartbeat_interval)

            except Exception as e:
                logger.error(f"Error in heartbeat monitor: {e}")
                await asyncio.sleep(self.heartbeat_interval)

    async def publish_event(self, event: SyncEvent):
        """Publish a sync event to all subscribers."""
        await self.event_queue.put(event)

    def add_event_handler(self, event_type: SyncEventType, handler: Callable):
        """Add an event handler for specific event type."""
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)

    def remove_event_handler(self, event_type: SyncEventType, handler: Callable):
        """Remove an event handler."""
        if event_type in self.event_handlers:
            try:
                self.event_handlers[event_type].remove(handler)
            except ValueError:
                pass

    def get_client_stats(self) -> Dict[str, Any]:
        """Get statistics about connected clients."""
        stats = {
            "total_clients": len(self.clients),
            "clients_by_type": {},
            "authenticated_clients": 0,
            "total_subscriptions": 0
        }

        for client in self.clients.values():
            client_type = client.client_type.value
            stats["clients_by_type"][client_type] = stats["clients_by_type"].get(client_type, 0) + 1

            if client.user_id:
                stats["authenticated_clients"] += 1

            stats["total_subscriptions"] += len(client.subscriptions)

        return stats

    async def send_notification(self, message: str, level: str = "info",
                              target_users: List[str] = None,
                              target_types: List[ClientType] = None):
        """Send notification to specific users or client types."""
        notification = {
            "type": "notification",
            "message": message,
            "level": level,
            "timestamp": datetime.now().isoformat()
        }

        send_tasks = []

        for client in self.clients.values():
            should_send = True

            # Filter by user
            if target_users and client.user_id not in target_users:
                should_send = False

            # Filter by client type
            if target_types and client.client_type not in target_types:
                should_send = False

            if should_send:
                send_tasks.append(self._send_to_client(client, notification))

        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)

# Global WebSocket sync manager
_global_websocket_manager = None

def get_websocket_manager() -> WebSocketSyncManager:
    """Get global WebSocket sync manager instance."""
    global _global_websocket_manager

    if _global_websocket_manager is None:
        _global_websocket_manager = WebSocketSyncManager()

    return _global_websocket_manager

async def start_websocket_server(host: str = "localhost", port: int = 8765):
    """Start the WebSocket server."""
    manager = get_websocket_manager()
    manager.host = host
    manager.port = port
    await manager.start_server()

    # Keep server running
    try:
        while manager.running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await manager.stop_server()

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    try:
        asyncio.run(start_websocket_server())
    except KeyboardInterrupt:
        logger.info("WebSocket server stopped by user")
