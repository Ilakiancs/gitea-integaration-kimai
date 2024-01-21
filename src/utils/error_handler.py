#!/usr/bin/env python3
"""
Error Handling and Recovery System

Provides comprehensive error handling, categorization, automatic recovery strategies,
and detailed error reporting for the sync system.
Includes retry mechanisms and graceful degradation.
"""

import os
import json
import logging
import traceback
import threading
import time
from typing import Dict, List, Optional, Any, Union, Callable, Type
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import sqlite3
from pathlib import Path
import sys
import signal
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    """Error severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class ErrorCategory(Enum):
    """Error categories."""
    NETWORK = "network"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    RATE_LIMIT = "rate_limit"
    VALIDATION = "validation"
    CONFIGURATION = "configuration"
    DATABASE = "database"
    API = "api"
    SYSTEM = "system"
    UNKNOWN = "unknown"

class RecoveryStrategy(Enum):
    """Recovery strategies."""
    RETRY = "retry"
    BACKOFF = "backoff"
    FALLBACK = "fallback"
    CIRCUIT_BREAKER = "circuit_breaker"
    MANUAL_INTERVENTION = "manual_intervention"
    IGNORE = "ignore"
    LOG_CONTEXT = "log_context"

@dataclass
class ErrorInfo:
    """Detailed error information."""
    id: str
    timestamp: datetime
    error_type: str
    error_message: str
    error_category: ErrorCategory
    severity: ErrorSeverity
    stack_trace: str
    context: Dict[str, Any]
    recovery_strategy: RecoveryStrategy
    retry_count: int = 0
    max_retries: int = 3
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    resolution_notes: str = ""

@dataclass
class ErrorRule:
    """Error handling rule."""
    name: str
    error_patterns: List[str]
    category: ErrorCategory
    severity: ErrorSeverity
    recovery_strategy: RecoveryStrategy
    max_retries: int = 3
    retry_delay: int = 60
    enabled: bool = True

@dataclass
class ErrorConfig:
    """Error handling configuration."""
    enabled: bool = True
    log_errors: bool = True
    store_errors: bool = True
    alert_on_critical: bool = True
    max_error_history: int = 1000
    cleanup_interval_hours: int = 24

class ErrorDatabase:
    """Manages error storage and retrieval."""

    def __init__(self, db_path: str = "errors.db"):
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize the error database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS errors (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    error_category TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    stack_trace TEXT,
                    context TEXT,
                    recovery_strategy TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    resolved INTEGER DEFAULT 0,
                    resolved_at TEXT,
                    resolution_notes TEXT
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS error_rules (
                    name TEXT PRIMARY KEY,
                    error_patterns TEXT NOT NULL,
                    category TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    recovery_strategy TEXT NOT NULL,
                    max_retries INTEGER DEFAULT 3,
                    retry_delay INTEGER DEFAULT 60,
                    enabled INTEGER DEFAULT 1
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_errors_timestamp
                ON errors(timestamp)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_errors_category
                ON errors(error_category)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_errors_severity
                ON errors(severity)
            """)

            conn.commit()

    def save_error(self, error: ErrorInfo):
        """Save error to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO errors
                (id, timestamp, error_type, error_message, error_category, severity,
                 stack_trace, context, recovery_strategy, retry_count, max_retries,
                 resolved, resolved_at, resolution_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                error.id,
                error.timestamp.isoformat(),
                error.error_type,
                error.error_message,
                error.error_category.value,
                error.severity.value,
                error.stack_trace,
                json.dumps(error.context) if error.context else None,
                error.recovery_strategy.value,
                error.retry_count,
                error.max_retries,
                1 if error.resolved else 0,
                error.resolved_at.isoformat() if error.resolved_at else None,
                error.resolution_notes
            ))
            conn.commit()

    def get_error(self, error_id: str) -> Optional[ErrorInfo]:
        """Get error by ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM errors WHERE id = ?
            """, (error_id,))
            row = cursor.fetchone()

            if row:
                return ErrorInfo(
                    id=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    error_type=row[2],
                    error_message=row[3],
                    error_category=ErrorCategory(row[4]),
                    severity=ErrorSeverity(row[5]),
                    stack_trace=row[6],
                    context=json.loads(row[7]) if row[7] else {},
                    recovery_strategy=RecoveryStrategy(row[8]),
                    retry_count=row[9],
                    max_retries=row[10],
                    resolved=bool(row[11]),
                    resolved_at=datetime.fromisoformat(row[12]) if row[12] else None,
                    resolution_notes=row[13]
                )
            return None

    def get_recent_errors(self, hours: int = 24, category: ErrorCategory = None,
                         severity: ErrorSeverity = None) -> List[ErrorInfo]:
        """Get recent errors with optional filtering."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        query = """
            SELECT * FROM errors
            WHERE timestamp >= ?
        """
        params = [cutoff_time.isoformat()]

        if category:
            query += " AND error_category = ?"
            params.append(category.value)

        if severity:
            query += " AND severity = ?"
            params.append(severity.value)

        query += " ORDER BY timestamp DESC"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query, params)

            errors = []
            for row in cursor.fetchall():
                errors.append(ErrorInfo(
                    id=row[0],
                    timestamp=datetime.fromisoformat(row[1]),
                    error_type=row[2],
                    error_message=row[3],
                    error_category=ErrorCategory(row[4]),
                    severity=ErrorSeverity(row[5]),
                    stack_trace=row[6],
                    context=json.loads(row[7]) if row[7] else {},
                    recovery_strategy=RecoveryStrategy(row[8]),
                    retry_count=row[9],
                    max_retries=row[10],
                    resolved=bool(row[11]),
                    resolved_at=datetime.fromisoformat(row[12]) if row[12] else None,
                    resolution_notes=row[13]
                ))

            return errors

    def mark_error_resolved(self, error_id: str, resolution_notes: str = ""):
        """Mark an error as resolved."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE errors
                SET resolved = 1, resolved_at = ?, resolution_notes = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), resolution_notes, error_id))
            conn.commit()

    def cleanup_old_errors(self, days: int):
        """Clean up errors older than specified days."""
        cutoff_date = datetime.now() - timedelta(days=days)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                DELETE FROM errors WHERE timestamp < ?
            """, (cutoff_date.isoformat(),))
            conn.commit()

class ErrorRuleManager:
    """Manages error handling rules."""

    def __init__(self, database: ErrorDatabase):
        self.database = database
        self.rules: Dict[str, ErrorRule] = {}
        self._load_rules()
        self._setup_default_rules()

    def _load_rules(self):
        """Load rules from database."""
        with sqlite3.connect(self.database.db_path) as conn:
            cursor = conn.execute("SELECT * FROM error_rules")

            for row in cursor.fetchall():
                rule = ErrorRule(
                    name=row[0],
                    error_patterns=json.loads(row[1]),
                    category=ErrorCategory(row[2]),
                    severity=ErrorSeverity(row[3]),
                    recovery_strategy=RecoveryStrategy(row[4]),
                    max_retries=row[5],
                    retry_delay=row[6],
                    enabled=bool(row[7])
                )
                self.rules[rule.name] = rule

    def _setup_default_rules(self):
        """Setup default error handling rules."""
        default_rules = [
            ErrorRule(
                name="network_timeout",
                error_patterns=["timeout", "ConnectionError", "socket.timeout"],
                category=ErrorCategory.NETWORK,
                severity=ErrorSeverity.WARNING,
                recovery_strategy=RecoveryStrategy.RETRY,
                max_retries=3,
                retry_delay=30
            ),
            ErrorRule(
                name="rate_limit_exceeded",
                error_patterns=["rate limit", "429", "Too Many Requests"],
                category=ErrorCategory.RATE_LIMIT,
                severity=ErrorSeverity.WARNING,
                recovery_strategy=RecoveryStrategy.BACKOFF,
                max_retries=5,
                retry_delay=300
            ),
            ErrorRule(
                name="authentication_failed",
                error_patterns=["401", "Unauthorized", "authentication failed"],
                category=ErrorCategory.AUTHENTICATION,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=RecoveryStrategy.MANUAL_INTERVENTION,
                max_retries=1
            ),
            ErrorRule(
                name="authorization_failed",
                error_patterns=["403", "Forbidden", "permission denied"],
                category=ErrorCategory.AUTHORIZATION,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=RecoveryStrategy.MANUAL_INTERVENTION,
                max_retries=1
            ),
            ErrorRule(
                name="validation_error",
                error_patterns=["validation", "invalid", "400", "Bad Request"],
                category=ErrorCategory.VALIDATION,
                severity=ErrorSeverity.WARNING,
                recovery_strategy=RecoveryStrategy.IGNORE,
                max_retries=1
            ),
            ErrorRule(
                name="database_error",
                error_patterns=["database", "sqlite", "connection", "deadlock"],
                category=ErrorCategory.DATABASE,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=RecoveryStrategy.RETRY,
                max_retries=3,
                retry_delay=10
            ),
            ErrorRule(
                name="api_error",
                error_patterns=["500", "Internal Server Error", "api error"],
                category=ErrorCategory.API,
                severity=ErrorSeverity.ERROR,
                recovery_strategy=RecoveryStrategy.BACKOFF,
                max_retries=3,
                retry_delay=60
            )
        ]

        for rule in default_rules:
            if rule.name not in self.rules:
                self.add_rule(rule)

    def add_rule(self, rule: ErrorRule):
        """Add a new error rule."""
        self.rules[rule.name] = rule

        with sqlite3.connect(self.database.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO error_rules
                (name, error_patterns, category, severity, recovery_strategy, max_retries, retry_delay, enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.name,
                json.dumps(rule.error_patterns),
                rule.category.value,
                rule.severity.value,
                rule.recovery_strategy.value,
                rule.max_retries,
                rule.retry_delay,
                1 if rule.enabled else 0
            ))
            conn.commit()

    def match_error(self, error_message: str, error_type: str) -> Optional[ErrorRule]:
        """Match error against rules."""
        for rule in self.rules.values():
            if not rule.enabled:
                continue

            for pattern in rule.error_patterns:
                if (pattern.lower() in error_message.lower() or
                    pattern.lower() in error_type.lower()):
                    return rule

        return None

class RecoveryManager:
    """Manages error recovery strategies."""

    def __init__(self, error_handler: 'ErrorHandler'):
        self.error_handler = error_handler
        self.recovery_handlers: Dict[RecoveryStrategy, Callable] = {}
        self._setup_recovery_handlers()

    def _setup_recovery_handlers(self):
        """Setup recovery strategy handlers."""
        self.recovery_handlers[RecoveryStrategy.RETRY] = self._handle_retry
        self.recovery_handlers[RecoveryStrategy.BACKOFF] = self._handle_backoff
        self.recovery_handlers[RecoveryStrategy.FALLBACK] = self._handle_fallback
        self.recovery_handlers[RecoveryStrategy.CIRCUIT_BREAKER] = self._handle_circuit_breaker
        self.recovery_handlers[RecoveryStrategy.MANUAL_INTERVENTION] = self._handle_manual_intervention
        self.recovery_handlers[RecoveryStrategy.IGNORE] = self._handle_ignore

    def _handle_retry(self, error: ErrorInfo, func: Callable, *args, **kwargs):
        """Handle retry recovery strategy."""
        if error.retry_count < error.max_retries:
            logger.info(f"Retrying operation after error: {error.error_message}")
            time.sleep(1)  # Simple delay
            return func(*args, **kwargs)
        else:
            logger.error(f"Max retries exceeded for error: {error.error_message}")
            return None

    def _handle_backoff(self, error: ErrorInfo, func: Callable, *args, **kwargs):
        """Handle exponential backoff recovery strategy."""
        if error.retry_count < error.max_retries:
            delay = min(60 * (2 ** error.retry_count), 300)  # Max 5 minutes
            logger.info(f"Backing off for {delay} seconds before retry")
            time.sleep(delay)
            return func(*args, **kwargs)
        else:
            logger.error(f"Max backoff retries exceeded for error: {error.error_message}")
            return None

    def _handle_fallback(self, error: ErrorInfo, func: Callable, *args, **kwargs):
        """Handle fallback recovery strategy."""
        logger.info(f"Using fallback strategy for error: {error.error_message}")
        # This would typically call an alternative function
        # For now, just return None
        return None

    def _handle_circuit_breaker(self, error: ErrorInfo, func: Callable, *args, **kwargs):
        """Handle circuit breaker recovery strategy."""
        logger.warning(f"Circuit breaker activated for error: {error.error_message}")
        # This would typically check circuit breaker state
        # For now, just return None
        return None

    def _handle_manual_intervention(self, error: ErrorInfo, func: Callable, *args, **kwargs):
        """Handle manual intervention recovery strategy."""
        logger.critical(f"Manual intervention required for error: {error.error_message}")
        # This would typically trigger alerts or notifications
        return None

    def _handle_ignore(self, error: ErrorInfo, func: Callable, *args, **kwargs):
        """Handle ignore recovery strategy."""
        logger.info(f"Ignoring error: {error.error_message}")
        return None

    def attempt_recovery(self, error: ErrorInfo, func: Callable, *args, **kwargs):
        """Attempt to recover from an error."""
        handler = self.recovery_handlers.get(error.recovery_strategy)
        if handler:
            return handler(error, func, *args, **kwargs)
        else:
            logger.error(f"No recovery handler for strategy: {error.recovery_strategy}")
            return None

class ErrorHandler:
    """Main error handling system."""

    def __init__(self, config: ErrorConfig):
        self.config = config
        self.database = ErrorDatabase()
        self.rule_manager = ErrorRuleManager(self.database)
        self.recovery_manager = RecoveryManager(self)

        self.error_callbacks: List[Callable[[ErrorInfo], None]] = []
        self.critical_error_callbacks: List[Callable[[ErrorInfo], None]] = []

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully")
        sys.exit(0)

    def handle_error(self, exception: Exception, context: Dict[str, Any] = None,
                    func: Callable = None, *args, **kwargs) -> Optional[Any]:
        """Handle an exception with recovery."""
        error_info = self._create_error_info(exception, context)

        # Log error
        if self.config.log_errors:
            self._log_error(error_info)

        # Store error
        if self.config.store_errors:
            self.database.save_error(error_info)

        # Trigger callbacks
        self._trigger_callbacks(error_info)

        # Attempt recovery
        if func and error_info.retry_count < error_info.max_retries:
            error_info.retry_count += 1
            return self.recovery_manager.attempt_recovery(error_info, func, *args, **kwargs)

        return None

    def _create_error_info(self, exception: Exception, context: Dict[str, Any] = None) -> ErrorInfo:
        """Create error information from exception."""
        error_message = str(exception)
        error_type = type(exception).__name__

        # Match error against rules
        rule = self.rule_manager.match_error(error_message, error_type)

        if rule:
            category = rule.category
            severity = rule.severity
            recovery_strategy = rule.recovery_strategy
            max_retries = rule.max_retries
        else:
            category = ErrorCategory.UNKNOWN
            severity = ErrorSeverity.ERROR
            recovery_strategy = RecoveryStrategy.IGNORE
            max_retries = 1

        return ErrorInfo(
            id=f"error_{int(time.time() * 1000)}",
            timestamp=datetime.now(),
            error_type=error_type,
            error_message=error_message,
            error_category=category,
            severity=severity,
            stack_trace=traceback.format_exc(),
            context=context or {},
            recovery_strategy=recovery_strategy,
            max_retries=max_retries
        )

    def _log_error(self, error: ErrorInfo):
        """Log error with appropriate level."""
        log_message = f"Error [{error.error_category.value}/{error.severity.value}]: {error.error_message}"

        if error.severity == ErrorSeverity.CRITICAL:
            logger.critical(log_message)
        elif error.severity == ErrorSeverity.ERROR:
            logger.error(log_message)
        elif error.severity == ErrorSeverity.WARNING:
            logger.warning(log_message)
        elif error.severity == ErrorSeverity.INFO:
            logger.info(log_message)
        else:
            logger.debug(log_message)

    def _trigger_callbacks(self, error: ErrorInfo):
        """Trigger error callbacks."""
        for callback in self.error_callbacks:
            try:
                callback(error)
            except Exception as e:
                logger.error(f"Error in error callback: {e}")

        if error.severity == ErrorSeverity.CRITICAL:
            for callback in self.critical_error_callbacks:
                try:
                    callback(error)
                except Exception as e:
                    logger.error(f"Error in critical error callback: {e}")

    def add_error_callback(self, callback: Callable[[ErrorInfo], None]):
        """Add a callback for all errors."""
        self.error_callbacks.append(callback)

    def add_critical_error_callback(self, callback: Callable[[ErrorInfo], None]):
        """Add a callback for critical errors only."""
        self.critical_error_callbacks.append(callback)

    def get_recent_errors(self, hours: int = 24, category: ErrorCategory = None,
                         severity: ErrorSeverity = None) -> List[ErrorInfo]:
        """Get recent errors."""
        return self.database.get_recent_errors(hours, category, severity)

    def mark_error_resolved(self, error_id: str, resolution_notes: str = ""):
        """Mark an error as resolved."""
        self.database.mark_error_resolved(error_id, resolution_notes)

    def get_error_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get error summary statistics."""
        errors = self.get_recent_errors(hours)

        summary = {
            'total_errors': len(errors),
            'resolved_errors': len([e for e in errors if e.resolved]),
            'unresolved_errors': len([e for e in errors if not e.resolved]),
            'by_category': {},
            'by_severity': {},
            'by_recovery_strategy': {}
        }

        for error in errors:
            # Count by category
            category = error.error_category.value
            summary['by_category'][category] = summary['by_category'].get(category, 0) + 1

            # Count by severity
            severity = error.severity.value
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1

            # Count by recovery strategy
            strategy = error.recovery_strategy.value
            summary['by_recovery_strategy'][strategy] = summary['by_recovery_strategy'].get(strategy, 0) + 1

        return summary

    def cleanup_old_errors(self):
        """Clean up old error data."""
        self.database.cleanup_old_errors(30)  # Keep 30 days

@contextmanager
def error_context(error_handler: ErrorHandler, context: Dict[str, Any] = None):
    """Context manager for error handling."""
    try:
        yield
    except Exception as e:
        error_handler.handle_error(e, context)
        raise

def handle_errors(error_handler: ErrorHandler, context: Dict[str, Any] = None):
    """Decorator for automatic error handling."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                return error_handler.handle_error(e, context, func, *args, **kwargs)
        return wrapper
    return decorator

def create_error_handler(config_file: str = "error_config.json") -> ErrorHandler:
    """Create error handler from configuration."""
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)
    else:
        config_data = {
            'enabled': True,
            'log_errors': True,
            'store_errors': True,
            'alert_on_critical': True,
            'max_error_history': 1000,
            'cleanup_interval_hours': 24
        }

    config = ErrorConfig(**config_data)
    return ErrorHandler(config)

if __name__ == "__main__":
    # Example usage
    error_handler = create_error_handler()

    # Add error callbacks
    def log_error_to_file(error: ErrorInfo):
        with open("error_log.txt", "a") as f:
            f.write(f"{error.timestamp}: {error.error_message}\n")

    def send_critical_alert(error: ErrorInfo):
        print(f"CRITICAL ALERT: {error.error_message}")

    error_handler.add_error_callback(log_error_to_file)
    error_handler.add_critical_error_callback(send_critical_alert)

    # Example function with error handling
    @handle_errors(error_handler, {"operation": "test_function"})
    def test_function():
        raise ValueError("This is a test error")

    # Test error handling
    try:
        test_function()
    except Exception as e:
        print(f"Caught exception: {e}")

    # Get error summary
    summary = error_handler.get_error_summary()
    print("Error Summary:", json.dumps(summary, indent=2, default=str))
