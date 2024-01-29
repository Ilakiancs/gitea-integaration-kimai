#!/usr/bin/env python3
"""
API Rate Limiting Module

Provides intelligent rate limiting for API calls to prevent throttling
and ensure optimal performance when interacting with external APIs.
"""

import time
import logging
import threading
from typing import Dict, Optional, Callable, Any
from datetime import datetime, timedelta
from collections import defaultdict, deque
import sqlite3
import json

logger = logging.getLogger(__name__)

class RateLimiter:
    """Base rate limiter class."""

    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window  # in seconds
        self.requests = deque()
        self.lock = threading.RLock()
        self.burst_threshold = max_requests * 0.8  # 80% of limit triggers burst protection

    def can_proceed(self, identifier: str = "default") -> bool:
        """Check if request can proceed."""
        raise NotImplementedError

    def record_request(self, identifier: str = "default"):
        """Record a request."""
        raise NotImplementedError

    def wait_if_needed(self, identifier: str = "default"):
        """Wait if rate limit is exceeded."""
        raise NotImplementedError

class SimpleRateLimiter(RateLimiter):
    """Simple in-memory rate limiter."""

    def __init__(self, max_requests: int, time_window: int):
        super().__init__(max_requests, time_window)
        self.request_times = defaultdict(deque)

    def can_proceed(self, identifier: str = "default") -> bool:
        """Check if request can proceed."""
        with self.lock:
            current_time = time.time()
            request_times = self.request_times[identifier]

            # Remove old requests outside the time window
            while request_times and current_time - request_times[0] > self.time_window:
                request_times.popleft()

            return len(request_times) < self.max_requests

    def record_request(self, identifier: str = "default"):
        """Record a request."""
        with self.lock:
            current_time = time.time()
            self.request_times[identifier].append(current_time)

    def wait_if_needed(self, identifier: str = "default"):
        """Wait if rate limit is exceeded."""
        while not self.can_proceed(identifier):
            # Calculate wait time
            request_times = self.request_times[identifier]
            if request_times:
                oldest_request = request_times[0]
                wait_time = self.time_window - (time.time() - oldest_request)
                if wait_time > 0:
                    logger.info(f"Rate limit exceeded for {identifier}, waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)

            # Clean up old requests
            self.can_proceed(identifier)

    def get_remaining_requests(self, identifier: str = "default") -> int:
        """Get remaining requests allowed."""
        with self.lock:
            current_time = time.time()
            request_times = self.request_times[identifier]

            # Remove old requests
            while request_times and current_time - request_times[0] > self.time_window:
                request_times.popleft()

            return max(0, self.max_requests - len(request_times))

    def get_reset_time(self, identifier: str = "default") -> Optional[float]:
        """Get time until rate limit resets."""
        with self.lock:
            request_times = self.request_times[identifier]
            if not request_times:
                return None

            oldest_request = request_times[0]
            reset_time = oldest_request + self.time_window
            return max(0, reset_time - time.time())

class TokenBucketRateLimiter(RateLimiter):
    """Token bucket rate limiter implementation."""

    def __init__(self, max_requests: int, time_window: int, burst_size: Optional[int] = None):
        super().__init__(max_requests, time_window)
        self.burst_size = burst_size or max_requests
        self.tokens = defaultdict(lambda: self.burst_size)
        self.last_refill = defaultdict(lambda: time.time())
        self.refill_rate = max_requests / time_window  # tokens per second

    def can_proceed(self, identifier: str = "default") -> bool:
        """Check if request can proceed."""
        with self.lock:
            self._refill_tokens(identifier)
            return self.tokens[identifier] >= 1

    def record_request(self, identifier: str = "default"):
        """Record a request."""
        with self.lock:
            self._refill_tokens(identifier)
            if self.tokens[identifier] >= 1:
                self.tokens[identifier] -= 1

    def wait_if_needed(self, identifier: str = "default"):
        """Wait if rate limit is exceeded."""
        while not self.can_proceed(identifier):
            # Calculate wait time for next token
            wait_time = 1.0 / self.refill_rate
            logger.info(f"Token bucket empty for {identifier}, waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)

    def _refill_tokens(self, identifier: str):
        """Refill tokens based on time passed."""
        current_time = time.time()
        time_passed = current_time - self.last_refill[identifier]
        tokens_to_add = time_passed * self.refill_rate

        self.tokens[identifier] = min(
            self.burst_size,
            self.tokens[identifier] + tokens_to_add
        )
        self.last_refill[identifier] = current_time

    def get_remaining_tokens(self, identifier: str = "default") -> float:
        """Get remaining tokens."""
        with self.lock:
            self._refill_tokens(identifier)
            return self.tokens[identifier]

class AdaptiveRateLimiter(RateLimiter):
    """Adaptive rate limiter that adjusts based on API responses."""

    def __init__(self, initial_max_requests: int, time_window: int,
                 min_requests: int = 1, max_requests: int = 1000):
        super().__init__(initial_max_requests, time_window)
        self.current_max_requests = initial_max_requests
        self.min_requests = min_requests
        self.max_requests_limit = max_requests
        self.success_count = 0
        self.failure_count = 0
        self.last_adjustment = time.time()
        self.adjustment_interval = 60  # seconds
        self.request_times = deque()

    def can_proceed(self, identifier: str = "default") -> bool:
        """Check if request can proceed."""
        with self.lock:
            current_time = time.time()

            # Remove old requests
            while self.request_times and current_time - self.request_times[0] > self.time_window:
                self.request_times.popleft()

            return len(self.request_times) < self.current_max_requests

    def record_request(self, identifier: str = "default"):
        """Record a request."""
        with self.lock:
            current_time = time.time()
            self.request_times.append(current_time)

    def wait_if_needed(self, identifier: str = "default"):
        """Wait if rate limit is exceeded."""
        while not self.can_proceed(identifier):
            # Calculate wait time
            if self.request_times:
                oldest_request = self.request_times[0]
                wait_time = self.time_window - (time.time() - oldest_request)
                if wait_time > 0:
                    logger.info(f"Adaptive rate limit exceeded, waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)

            # Clean up old requests
            self.can_proceed(identifier)

    def record_success(self):
        """Record a successful request."""
        with self.lock:
            self.success_count += 1
            self._maybe_adjust_rate()

    def record_failure(self, status_code: int = None):
        """Record a failed request."""
        with self.lock:
            self.failure_count += 1

            # Immediate rate reduction for certain status codes
            if status_code in [429, 503]:  # Rate limited or service unavailable
                self._reduce_rate_immediately()

            self._maybe_adjust_rate()

    def _maybe_adjust_rate(self):
        """Adjust rate based on success/failure ratio."""
        current_time = time.time()
        if current_time - self.last_adjustment < self.adjustment_interval:
            return

        total_requests = self.success_count + self.failure_count
        if total_requests < 10:  # Need minimum sample size
            return

        success_rate = self.success_count / total_requests

        if success_rate > 0.95:  # High success rate, can increase
            new_rate = min(self.max_requests_limit, int(self.current_max_requests * 1.1))
            if new_rate != self.current_max_requests:
                logger.info(f"Increasing rate limit from {self.current_max_requests} to {new_rate}")
                self.current_max_requests = new_rate
        elif success_rate < 0.8:  # Low success rate, should decrease
            new_rate = max(self.min_requests, int(self.current_max_requests * 0.8))
            if new_rate != self.current_max_requests:
                logger.info(f"Decreasing rate limit from {self.current_max_requests} to {new_rate}")
                self.current_max_requests = new_rate

        # Reset counters
        self.success_count = 0
        self.failure_count = 0
        self.last_adjustment = current_time

    def _reduce_rate_immediately(self):
        """Immediately reduce rate due to rate limiting response."""
        new_rate = max(self.min_requests, int(self.current_max_requests * 0.5))
        if new_rate != self.current_max_requests:
            logger.warning(f"Rate limit hit, reducing rate from {self.current_max_requests} to {new_rate}")
            self.current_max_requests = new_rate

class DatabaseRateLimiter(RateLimiter):
    """Database-backed rate limiter for persistence across restarts."""

    def __init__(self, max_requests: int, time_window: int, db_path: str = "rate_limits.db"):
        super().__init__(max_requests, time_window)
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize rate limiting database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rate_limits (
                    identifier TEXT,
                    request_time REAL,
                    PRIMARY KEY (identifier, request_time)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rate_limits_time ON rate_limits(request_time)")
            conn.commit()

    def can_proceed(self, identifier: str = "default") -> bool:
        """Check if request can proceed."""
        with sqlite3.connect(self.db_path) as conn:
            current_time = time.time()
            cutoff_time = current_time - self.time_window

            cursor = conn.execute("""
                SELECT COUNT(*) FROM rate_limits
                WHERE identifier = ? AND request_time > ?
            """, (identifier, cutoff_time))

            current_requests = cursor.fetchone()[0]
            return current_requests < self.max_requests

    def record_request(self, identifier: str = "default"):
        """Record a request."""
        with sqlite3.connect(self.db_path) as conn:
            current_time = time.time()
            conn.execute("""
                INSERT INTO rate_limits (identifier, request_time)
                VALUES (?, ?)
            """, (identifier, current_time))
            conn.commit()

    def wait_if_needed(self, identifier: str = "default"):
        """Wait if rate limit is exceeded."""
        while not self.can_proceed(identifier):
            # Get oldest request time
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT MIN(request_time) FROM rate_limits
                    WHERE identifier = ?
                """, (identifier,))
                oldest_time = cursor.fetchone()[0]

                if oldest_time:
                    wait_time = self.time_window - (time.time() - oldest_time)
                    if wait_time > 0:
                        logger.info(f"Database rate limit exceeded for {identifier}, waiting {wait_time:.2f} seconds")
                        time.sleep(wait_time)

            # Clean up old requests
            self._cleanup_old_requests()

    def _cleanup_old_requests(self):
        """Clean up old request records."""
        with sqlite3.connect(self.db_path) as conn:
            cutoff_time = time.time() - self.time_window
            conn.execute("DELETE FROM rate_limits WHERE request_time <= ?", (cutoff_time,))
            conn.commit()

    def get_remaining_requests(self, identifier: str = "default") -> int:
        """Get remaining requests allowed."""
        with sqlite3.connect(self.db_path) as conn:
            current_time = time.time()
            cutoff_time = current_time - self.time_window

            cursor = conn.execute("""
                SELECT COUNT(*) FROM rate_limits
                WHERE identifier = ? AND request_time > ?
            """, (identifier, cutoff_time))

            current_requests = cursor.fetchone()[0]
            return max(0, self.max_requests - current_requests)

class RateLimitManager:
    """Manages multiple rate limiters for different APIs and endpoints."""

    def __init__(self):
        self.limiters: Dict[str, RateLimiter] = {}
        self.default_limits = {
            'gitea': {'max_requests': 60, 'time_window': 60},  # 60 requests per minute
            'kimai': {'max_requests': 100, 'time_window': 60},  # 100 requests per minute
            'default': {'max_requests': 30, 'time_window': 60}  # 30 requests per minute
        }

    def add_limiter(self, name: str, limiter: RateLimiter):
        """Add a rate limiter."""
        self.limiters[name] = limiter
        logger.info(f"Added rate limiter: {name}")

    def get_limiter(self, name: str) -> RateLimiter:
        """Get a rate limiter by name."""
        if name not in self.limiters:
            # Create default limiter
            limits = self.default_limits.get(name, self.default_limits['default'])
            limiter = SimpleRateLimiter(limits['max_requests'], limits['time_window'])
            self.limiters[name] = limiter
            logger.info(f"Created default rate limiter for: {name}")

        return self.limiters[name]

    def can_proceed(self, limiter_name: str, identifier: str = "default") -> bool:
        """Check if request can proceed."""
        limiter = self.get_limiter(limiter_name)
        return limiter.can_proceed(identifier)

    def record_request(self, limiter_name: str, identifier: str = "default"):
        """Record a request."""
        limiter = self.get_limiter(limiter_name)
        limiter.record_request(identifier)

    def wait_if_needed(self, limiter_name: str, identifier: str = "default"):
        """Wait if rate limit is exceeded."""
        limiter = self.get_limiter(limiter_name)
        limiter.wait_if_needed(identifier)

    def record_success(self, limiter_name: str):
        """Record a successful request (for adaptive limiters)."""
        limiter = self.get_limiter(limiter_name)
        if hasattr(limiter, 'record_success'):
            limiter.record_success()

    def record_failure(self, limiter_name: str, status_code: int = None):
        """Record a failed request (for adaptive limiters)."""
        limiter = self.get_limiter(limiter_name)
        if hasattr(limiter, 'record_failure'):
            limiter.record_failure(status_code)

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        stats = {}
        for name, limiter in self.limiters.items():
            if hasattr(limiter, 'get_remaining_requests'):
                stats[name] = {
                    'remaining_requests': limiter.get_remaining_requests(),
                    'max_requests': limiter.max_requests,
                    'time_window': limiter.time_window
                }
            elif hasattr(limiter, 'get_remaining_tokens'):
                stats[name] = {
                    'remaining_tokens': limiter.get_remaining_tokens(),
                    'max_requests': limiter.max_requests,
                    'time_window': limiter.time_window
                }

        return stats

def rate_limit_decorator(limiter_name: str, identifier_func: Callable = None):
    """Decorator for rate limiting function calls."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Get rate limiter
            manager = getattr(func, '_rate_limit_manager', None)
            if not manager:
                manager = RateLimitManager()
                func._rate_limit_manager = manager

            # Determine identifier
            if identifier_func:
                identifier = identifier_func(*args, **kwargs)
            else:
                identifier = "default"

            # Wait if needed
            manager.wait_if_needed(limiter_name, identifier)

            # Record request
            manager.record_request(limiter_name, identifier)

            # Execute function
            try:
                result = func(*args, **kwargs)
                manager.record_success(limiter_name)
                return result
            except Exception as e:
                # Record failure if it's an HTTP error
                if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                    manager.record_failure(limiter_name, e.response.status_code)
                raise

        return wrapper
    return decorator
