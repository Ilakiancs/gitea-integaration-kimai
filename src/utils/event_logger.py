#!/usr/bin/env python3
"""
Event Logging System

Provides a comprehensive event logging system for tracking system events,
user actions, and system state changes with different severity levels.
"""

import json
import logging
import sqlite3
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

class EventSeverity(Enum):
    """Event severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class EventCategory(Enum):
    """Event categories."""
    SYSTEM = "system"
    USER = "user"
    SYNC = "sync"
    API = "api"
    SECURITY = "security"
    PERFORMANCE = "performance"
    DATABASE = "database"
    NETWORK = "network"

@dataclass
class Event:
    """An event log entry."""
    id: str
    timestamp: datetime
    severity: EventSeverity
    category: EventCategory
    message: str
    details: Dict[str, Any]
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None

class EventLogger:
    """Main event logging system."""

    def __init__(self, db_path: str = "events.db"):
        self.db_path = db_path
        self.deduplication_window = 300  # 5 minutes in seconds
        self._init_database()

    def _init_database(self):
        """Initialize the event database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    category TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT,
                    user_id TEXT,
                    session_id TEXT,
                    ip_address TEXT
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_timestamp
                ON events(timestamp)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_severity
                ON events(severity)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_category
                ON events(category)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_events_user_id
                ON events(user_id)
            """)

            conn.commit()

    def log_event(self, severity: EventSeverity, category: EventCategory,
                  message: str, details: Dict[str, Any] = None,
                  user_id: str = None, session_id: str = None,
                  ip_address: str = None) -> str:
        """Log an event."""
        import time

        # Check for duplicate events
        if self._is_duplicate_event(category, message, user_id):
            return None

        event_id = f"event_{int(time.time() * 1000)}"
        event = Event(
            id=event_id,
            timestamp=datetime.now(),
            severity=severity,
            category=category,
            message=message,
            details=details or {},
            user_id=user_id,
            session_id=session_id,
            ip_address=ip_address
        )

        self._save_event(event)

        # Also log to standard logging
        log_message = f"[{category.value.upper()}] {message}"
        if severity == EventSeverity.CRITICAL:
            logger.critical(log_message)
        elif severity == EventSeverity.ERROR:
            logger.error(log_message)
        elif severity == EventSeverity.WARNING:
            logger.warning(log_message)
        elif severity == EventSeverity.INFO:
            logger.info(log_message)
        else:
            logger.debug(log_message)

        return event_id

    def _is_duplicate_event(self, category: EventCategory, message: str,
                           user_id: str = None) -> bool:
        """Check if this event is a duplicate within the deduplication window."""
        cutoff_time = datetime.now().replace(second=datetime.now().second - self.deduplication_window)

        query = """
            SELECT COUNT(*) FROM events
            WHERE category = ? AND message = ? AND timestamp >= ?
        """
        params = [category.value, message, cutoff_time.isoformat()]

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            count = cursor.fetchone()[0]
            return count > 0

    def _save_event(self, event: Event):
        """Save event to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO events
                (id, timestamp, severity, category, message, details, user_id, session_id, ip_address)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event.id,
                event.timestamp.isoformat(),
                event.severity.value,
                event.category.value,
                event.message,
                json.dumps(event.details),
                event.user_id,
                event.session_id,
                event.ip_address
            ))
            conn.commit()

    def get_events(self, hours: int = 24, severity: EventSeverity = None,
                   category: EventCategory = None, user_id: str = None) -> List[Event]:
        """Get events with optional filtering."""
        cutoff_time = datetime.now().replace(hour=datetime.now().hour - hours)

        query = "SELECT * FROM events WHERE timestamp >= ?"
        params = [cutoff_time.isoformat()]

        if severity:
            query += " AND severity = ?"
            params.append(severity.value)

        if category:
            query += " AND category = ?"
            params.append(category.value)

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        query += " ORDER BY timestamp DESC"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)

            events = []
            for row in cursor.fetchall():
                events.append(Event(
                    id=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    severity=EventSeverity(row[2]),
                    category=EventCategory(row[3]),
                    message=row[4],
                    details=json.loads(row[5]) if row[5] else {},
                    user_id=row[6],
                    session_id=row[7],
                    ip_address=row[8]
                ))

            return events

    def get_event_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get event summary statistics."""
        events = self.get_events(hours)

        summary = {
            'total_events': len(events),
            'by_severity': {},
            'by_category': {},
            'recent_events': []
        }

        for event in events:
            # Count by severity
            severity = event.severity.value
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1

            # Count by category
            category = event.category.value
            summary['by_category'][category] = summary['by_category'].get(category, 0) + 1

        # Get recent events (last 10)
        summary['recent_events'] = [
            {
                'timestamp': event.timestamp.isoformat(),
                'severity': event.severity.value,
                'category': event.category.value,
                'message': event.message
            }
            for event in events[:10]
        ]

        return summary

    def cleanup_old_events(self, days: int = 30):
        """Clean up events older than specified days."""
        cutoff_date = datetime.now().replace(day=datetime.now().day - days)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                DELETE FROM events WHERE timestamp < ?
            """, (cutoff_date.isoformat(),))
            conn.commit()

# Convenience methods for common event types
def log_system_event(message: str, details: Dict[str, Any] = None):
    """Log a system event."""
    event_logger = EventLogger()
    return event_logger.log_event(
        EventSeverity.INFO,
        EventCategory.SYSTEM,
        message,
        details
    )

def log_user_action(user_id: str, action: str, details: Dict[str, Any] = None,
                   session_id: str = None, ip_address: str = None):
    """Log a user action."""
    event_logger = EventLogger()
    return event_logger.log_event(
        EventSeverity.INFO,
        EventCategory.USER,
        f"User action: {action}",
        details,
        user_id,
        session_id,
        ip_address
    )

def log_sync_event(message: str, details: Dict[str, Any] = None):
    """Log a sync event."""
    event_logger = EventLogger()
    return event_logger.log_event(
        EventSeverity.INFO,
        EventCategory.SYNC,
        message,
        details
    )

def log_security_event(severity: EventSeverity, message: str,
                      details: Dict[str, Any] = None, user_id: str = None):
    """Log a security event."""
    event_logger = EventLogger()
    return event_logger.log_event(
        severity,
        EventCategory.SECURITY,
        message,
        details,
        user_id
    )

if __name__ == "__main__":
    # Example usage
    event_logger = EventLogger()

    # Log some sample events
    event_logger.log_event(
        EventSeverity.INFO,
        EventCategory.SYSTEM,
        "System started",
        {"version": "1.0.0"}
    )

    event_logger.log_event(
        EventSeverity.INFO,
        EventCategory.USER,
        "User logged in",
        {"method": "password"},
        user_id="user123",
        session_id="session456",
        ip_address="192.168.1.1"
    )

    event_logger.log_event(
        EventSeverity.WARNING,
        EventCategory.SYNC,
        "Sync operation delayed",
        {"delay_seconds": 30}
    )

    # Get event summary
    summary = event_logger.get_event_summary()
    print("Event Summary:", json.dumps(summary, indent=2, default=str))
