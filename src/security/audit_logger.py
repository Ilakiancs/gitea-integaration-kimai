#!/usr/bin/env python3
"""
Security Audit Logger for Gitea-Kimai Integration

This module provides comprehensive audit logging for security-related events,
user actions, and system activities to ensure compliance and security monitoring.
"""

import os
import json
import time
import hashlib
import sqlite3
import logging
import threading
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

class AuditEventType(Enum):
    """Types of audit events."""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    CONFIGURATION_CHANGE = "configuration_change"
    SYSTEM_EVENT = "system_event"
    SECURITY_VIOLATION = "security_violation"
    API_ACCESS = "api_access"
    WEBHOOK_EVENT = "webhook_event"
    SYNC_OPERATION = "sync_operation"

class AuditSeverity(Enum):
    """Severity levels for audit events."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AuditOutcome(Enum):
    """Outcome of audited actions."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL = "partial"
    UNKNOWN = "unknown"

@dataclass
class AuditEvent:
    """Audit event data structure."""
    event_id: str
    timestamp: datetime
    event_type: AuditEventType
    severity: AuditSeverity
    outcome: AuditOutcome
    user_id: Optional[str] = None
    user_ip: Optional[str] = None
    user_agent: Optional[str] = None
    resource: Optional[str] = None
    action: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    source_system: Optional[str] = None
    target_system: Optional[str] = None
    data_classification: Optional[str] = None
    compliance_tags: Optional[List[str]] = None

class AuditDatabase:
    """Manages audit event storage and retrieval."""

    def __init__(self, db_path: str = "audit.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_database()

    def _init_database(self):
        """Initialize the audit database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id TEXT PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    event_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    user_id TEXT,
                    user_ip TEXT,
                    user_agent TEXT,
                    resource TEXT,
                    action TEXT,
                    details TEXT,
                    request_id TEXT,
                    session_id TEXT,
                    source_system TEXT,
                    target_system TEXT,
                    data_classification TEXT,
                    compliance_tags TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for performance
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_timestamp
                ON audit_events(timestamp)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_user_id
                ON audit_events(user_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_event_type
                ON audit_events(event_type)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_severity
                ON audit_events(severity)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_outcome
                ON audit_events(outcome)
            """)

            conn.commit()

    def save_event(self, event: AuditEvent):
        """Save audit event to database."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO audit_events
                    (event_id, timestamp, event_type, severity, outcome, user_id, user_ip,
                     user_agent, resource, action, details, request_id, session_id,
                     source_system, target_system, data_classification, compliance_tags)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event.event_id,
                    event.timestamp.isoformat(),
                    event.event_type.value,
                    event.severity.value,
                    event.outcome.value,
                    event.user_id,
                    event.user_ip,
                    event.user_agent,
                    event.resource,
                    event.action,
                    json.dumps(event.details) if event.details else None,
                    event.request_id,
                    event.session_id,
                    event.source_system,
                    event.target_system,
                    event.data_classification,
                    json.dumps(event.compliance_tags) if event.compliance_tags else None
                ))
                conn.commit()

    def get_events(self, start_time: datetime = None, end_time: datetime = None,
                   event_type: AuditEventType = None, user_id: str = None,
                   severity: AuditSeverity = None, limit: int = 1000) -> List[AuditEvent]:
        """Retrieve audit events with filtering."""
        query = "SELECT * FROM audit_events WHERE 1=1"
        params = []

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())

        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())

        if event_type:
            query += " AND event_type = ?"
            params.append(event_type.value)

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        if severity:
            query += " AND severity = ?"
            params.append(severity.value)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)
            events = []

            for row in cursor.fetchall():
                events.append(AuditEvent(
                    event_id=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    event_type=AuditEventType(row[2]),
                    severity=AuditSeverity(row[3]),
                    outcome=AuditOutcome(row[4]),
                    user_id=row[5],
                    user_ip=row[6],
                    user_agent=row[7],
                    resource=row[8],
                    action=row[9],
                    details=json.loads(row[10]) if row[10] else None,
                    request_id=row[11],
                    session_id=row[12],
                    source_system=row[13],
                    target_system=row[14],
                    data_classification=row[15],
                    compliance_tags=json.loads(row[16]) if row[16] else None
                ))

            return events

    def get_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get audit statistics for the specified time period."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        with sqlite3.connect(self.db_path) as conn:
            # Total events
            cursor = conn.execute("""
                SELECT COUNT(*) FROM audit_events WHERE timestamp >= ?
            """, (cutoff_time.isoformat(),))
            total_events = cursor.fetchone()[0]

            # Events by type
            cursor = conn.execute("""
                SELECT event_type, COUNT(*) FROM audit_events
                WHERE timestamp >= ?
                GROUP BY event_type
            """, (cutoff_time.isoformat(),))
            events_by_type = dict(cursor.fetchall())

            # Events by severity
            cursor = conn.execute("""
                SELECT severity, COUNT(*) FROM audit_events
                WHERE timestamp >= ?
                GROUP BY severity
            """, (cutoff_time.isoformat(),))
            events_by_severity = dict(cursor.fetchall())

            # Events by outcome
            cursor = conn.execute("""
                SELECT outcome, COUNT(*) FROM audit_events
                WHERE timestamp >= ?
                GROUP BY outcome
            """, (cutoff_time.isoformat(),))
            events_by_outcome = dict(cursor.fetchall())

            # Top users
            cursor = conn.execute("""
                SELECT user_id, COUNT(*) FROM audit_events
                WHERE timestamp >= ? AND user_id IS NOT NULL
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """, (cutoff_time.isoformat(),))
            top_users = dict(cursor.fetchall())

            return {
                'total_events': total_events,
                'events_by_type': events_by_type,
                'events_by_severity': events_by_severity,
                'events_by_outcome': events_by_outcome,
                'top_users': top_users,
                'time_period_hours': hours
            }

    def cleanup_old_events(self, days: int = 90):
        """Clean up old audit events."""
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    DELETE FROM audit_events WHERE timestamp < ?
                """, (cutoff_date.isoformat(),))
                deleted_count = cursor.rowcount
                conn.commit()

        logger.info(f"Cleaned up {deleted_count} audit events older than {days} days")

class AuditLogger:
    """Main audit logging system."""

    def __init__(self, database: AuditDatabase = None, enable_file_logging: bool = True,
                 log_file_path: str = "audit.log"):
        self.database = database or AuditDatabase()
        self.enable_file_logging = enable_file_logging
        self.log_file_path = log_file_path
        self.lock = threading.Lock()

        # Setup file logging if enabled
        if self.enable_file_logging:
            self._setup_file_logging()

    def _setup_file_logging(self):
        """Setup file logging for audit events."""
        audit_logger = logging.getLogger('audit')
        audit_logger.setLevel(logging.INFO)

        if not audit_logger.handlers:
            handler = logging.FileHandler(self.log_file_path)
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            audit_logger.addHandler(handler)

    def _generate_event_id(self) -> str:
        """Generate unique event ID."""
        timestamp = str(int(time.time() * 1000000))
        random_data = os.urandom(8).hex()
        return hashlib.sha256(f"{timestamp}{random_data}".encode()).hexdigest()[:16]

    def log_event(self, event_type: AuditEventType, action: str,
                  outcome: AuditOutcome = AuditOutcome.SUCCESS,
                  severity: AuditSeverity = AuditSeverity.LOW,
                  user_id: str = None, user_ip: str = None, user_agent: str = None,
                  resource: str = None, details: Dict[str, Any] = None,
                  request_id: str = None, session_id: str = None,
                  source_system: str = None, target_system: str = None,
                  data_classification: str = None, compliance_tags: List[str] = None):
        """Log an audit event."""

        event = AuditEvent(
            event_id=self._generate_event_id(),
            timestamp=datetime.now(),
            event_type=event_type,
            severity=severity,
            outcome=outcome,
            user_id=user_id,
            user_ip=user_ip,
            user_agent=user_agent,
            resource=resource,
            action=action,
            details=details,
            request_id=request_id,
            session_id=session_id,
            source_system=source_system,
            target_system=target_system,
            data_classification=data_classification,
            compliance_tags=compliance_tags
        )

        # Save to database
        try:
            self.database.save_event(event)
        except Exception as e:
            logger.error(f"Failed to save audit event to database: {e}")

        # Log to file if enabled
        if self.enable_file_logging:
            try:
                audit_logger = logging.getLogger('audit')
                log_message = self._format_log_message(event)
                audit_logger.info(log_message)
            except Exception as e:
                logger.error(f"Failed to write audit event to file: {e}")

    def _format_log_message(self, event: AuditEvent) -> str:
        """Format audit event for file logging."""
        message_parts = [
            f"ID={event.event_id}",
            f"TYPE={event.event_type.value}",
            f"ACTION={event.action}",
            f"OUTCOME={event.outcome.value}",
            f"SEVERITY={event.severity.value}"
        ]

        if event.user_id:
            message_parts.append(f"USER={event.user_id}")

        if event.user_ip:
            message_parts.append(f"IP={event.user_ip}")

        if event.resource:
            message_parts.append(f"RESOURCE={event.resource}")

        if event.source_system:
            message_parts.append(f"SOURCE={event.source_system}")

        if event.target_system:
            message_parts.append(f"TARGET={event.target_system}")

        return " | ".join(message_parts)

    def log_authentication(self, user_id: str, outcome: AuditOutcome,
                          user_ip: str = None, user_agent: str = None,
                          details: Dict[str, Any] = None):
        """Log authentication event."""
        severity = AuditSeverity.MEDIUM if outcome == AuditOutcome.FAILURE else AuditSeverity.LOW

        self.log_event(
            event_type=AuditEventType.AUTHENTICATION,
            action="user_login",
            outcome=outcome,
            severity=severity,
            user_id=user_id,
            user_ip=user_ip,
            user_agent=user_agent,
            details=details,
            compliance_tags=["authentication", "access_control"]
        )

    def log_authorization(self, user_id: str, resource: str, action: str,
                         outcome: AuditOutcome, user_ip: str = None,
                         details: Dict[str, Any] = None):
        """Log authorization event."""
        severity = AuditSeverity.HIGH if outcome == AuditOutcome.FAILURE else AuditSeverity.LOW

        self.log_event(
            event_type=AuditEventType.AUTHORIZATION,
            action=action,
            outcome=outcome,
            severity=severity,
            user_id=user_id,
            user_ip=user_ip,
            resource=resource,
            details=details,
            compliance_tags=["authorization", "access_control"]
        )

    def log_data_access(self, user_id: str, resource: str, action: str,
                       data_classification: str = None, user_ip: str = None,
                       details: Dict[str, Any] = None):
        """Log data access event."""
        severity = AuditSeverity.MEDIUM if data_classification == "sensitive" else AuditSeverity.LOW

        self.log_event(
            event_type=AuditEventType.DATA_ACCESS,
            action=action,
            outcome=AuditOutcome.SUCCESS,
            severity=severity,
            user_id=user_id,
            user_ip=user_ip,
            resource=resource,
            data_classification=data_classification,
            details=details,
            compliance_tags=["data_access", "privacy"]
        )

    def log_data_modification(self, user_id: str, resource: str, action: str,
                            outcome: AuditOutcome, data_classification: str = None,
                            user_ip: str = None, details: Dict[str, Any] = None):
        """Log data modification event."""
        severity = AuditSeverity.HIGH if data_classification == "sensitive" else AuditSeverity.MEDIUM

        self.log_event(
            event_type=AuditEventType.DATA_MODIFICATION,
            action=action,
            outcome=outcome,
            severity=severity,
            user_id=user_id,
            user_ip=user_ip,
            resource=resource,
            data_classification=data_classification,
            details=details,
            compliance_tags=["data_modification", "integrity"]
        )

    def log_configuration_change(self, user_id: str, resource: str, action: str,
                                outcome: AuditOutcome, user_ip: str = None,
                                details: Dict[str, Any] = None):
        """Log configuration change event."""
        self.log_event(
            event_type=AuditEventType.CONFIGURATION_CHANGE,
            action=action,
            outcome=outcome,
            severity=AuditSeverity.HIGH,
            user_id=user_id,
            user_ip=user_ip,
            resource=resource,
            details=details,
            compliance_tags=["configuration", "system_integrity"]
        )

    def log_security_violation(self, event_description: str, user_id: str = None,
                             user_ip: str = None, severity: AuditSeverity = AuditSeverity.HIGH,
                             details: Dict[str, Any] = None):
        """Log security violation event."""
        self.log_event(
            event_type=AuditEventType.SECURITY_VIOLATION,
            action=event_description,
            outcome=AuditOutcome.FAILURE,
            severity=severity,
            user_id=user_id,
            user_ip=user_ip,
            details=details,
            compliance_tags=["security_violation", "incident"]
        )

    def log_api_access(self, endpoint: str, method: str, user_id: str = None,
                      user_ip: str = None, user_agent: str = None,
                      outcome: AuditOutcome = AuditOutcome.SUCCESS,
                      request_id: str = None, details: Dict[str, Any] = None):
        """Log API access event."""
        self.log_event(
            event_type=AuditEventType.API_ACCESS,
            action=f"{method} {endpoint}",
            outcome=outcome,
            severity=AuditSeverity.LOW,
            user_id=user_id,
            user_ip=user_ip,
            user_agent=user_agent,
            resource=endpoint,
            request_id=request_id,
            details=details,
            compliance_tags=["api_access"]
        )

    def log_sync_operation(self, operation_type: str, source_system: str,
                          target_system: str, outcome: AuditOutcome,
                          user_id: str = None, details: Dict[str, Any] = None):
        """Log synchronization operation event."""
        self.log_event(
            event_type=AuditEventType.SYNC_OPERATION,
            action=operation_type,
            outcome=outcome,
            severity=AuditSeverity.MEDIUM,
            user_id=user_id,
            source_system=source_system,
            target_system=target_system,
            details=details,
            compliance_tags=["sync_operation", "data_flow"]
        )

    def get_recent_events(self, hours: int = 24, event_type: AuditEventType = None,
                         user_id: str = None, severity: AuditSeverity = None) -> List[AuditEvent]:
        """Get recent audit events."""
        start_time = datetime.now() - timedelta(hours=hours)
        return self.database.get_events(
            start_time=start_time,
            event_type=event_type,
            user_id=user_id,
            severity=severity
        )

    def get_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get audit statistics."""
        return self.database.get_statistics(hours)

    def export_events(self, output_file: str, start_time: datetime = None,
                     end_time: datetime = None, format_type: str = "json"):
        """Export audit events to file."""
        events = self.database.get_events(start_time=start_time, end_time=end_time)

        if format_type.lower() == "json":
            with open(output_file, 'w') as f:
                json.dump([asdict(event) for event in events], f, indent=2, default=str)
        elif format_type.lower() == "csv":
            import csv
            with open(output_file, 'w', newline='') as f:
                if events:
                    writer = csv.DictWriter(f, fieldnames=asdict(events[0]).keys())
                    writer.writeheader()
                    for event in events:
                        writer.writerow(asdict(event))

        logger.info(f"Exported {len(events)} audit events to {output_file}")

# Global audit logger instance
_global_audit_logger = None

def get_audit_logger() -> AuditLogger:
    """Get global audit logger instance."""
    global _global_audit_logger

    if _global_audit_logger is None:
        _global_audit_logger = AuditLogger()

    return _global_audit_logger

def audit_decorator(event_type: AuditEventType, action: str,
                   severity: AuditSeverity = AuditSeverity.LOW):
    """Decorator for automatic audit logging."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            audit_logger = get_audit_logger()
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                outcome = AuditOutcome.SUCCESS
                details = {
                    'function': func.__name__,
                    'duration_ms': round((time.time() - start_time) * 1000, 2)
                }

                audit_logger.log_event(
                    event_type=event_type,
                    action=action,
                    outcome=outcome,
                    severity=severity,
                    details=details
                )

                return result

            except Exception as e:
                outcome = AuditOutcome.FAILURE
                details = {
                    'function': func.__name__,
                    'error': str(e),
                    'duration_ms': round((time.time() - start_time) * 1000, 2)
                }

                audit_logger.log_event(
                    event_type=event_type,
                    action=action,
                    outcome=outcome,
                    severity=AuditSeverity.HIGH,
                    details=details
                )

                raise

        return wrapper
    return decorator
