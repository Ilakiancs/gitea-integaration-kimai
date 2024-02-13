#!/usr/bin/env python3
"""
Retry Handler for API Calls

Provides robust retry mechanisms with exponential backoff for handling
API call failures and network issues.
"""

import time
import logging
import random
from typing import Callable, Any, Optional, Dict
from functools import wraps
import requests

logger = logging.getLogger(__name__)

class RetryHandler:
    """Handles retry logic for API calls with exponential backoff."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def retry_on_failure(self, func: Callable) -> Callable:
        """Decorator to retry function on failure."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(self.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ConnectionError, TimeoutError) as e:
                    last_exception = e

                    if attempt == self.max_retries:
                        logger.error(f"Max retries ({self.max_retries}) exceeded for {func.__name__}: {e}")
                        raise

                    delay = self._calculate_delay(attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}, retrying in {delay:.2f}s: {e}")
                    time.sleep(delay)

            raise last_exception
        return wrapper

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and jitter."""
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        jitter = random.uniform(0, 0.1 * delay)
        return delay + jitter

    def retry_api_call(self, api_call: Callable, *args, **kwargs) -> Any:
        """Execute API call with retry logic."""
        return self.retry_on_failure(api_call)(*args, **kwargs)

class SmartRetryHandler(RetryHandler):
    """Enhanced retry handler with status code awareness."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        super().__init__(max_retries, base_delay, max_delay)
        self.retryable_status_codes = {408, 429, 500, 502, 503, 504}

    def should_retry(self, response: requests.Response) -> bool:
        """Determine if response should trigger a retry."""
        return response.status_code in self.retryable_status_codes

    def retry_on_response(self, func: Callable) -> Callable:
        """Decorator to retry based on response status codes."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(self.max_retries + 1):
                try:
                    response = func(*args, **kwargs)

                    if not self.should_retry(response):
                        return response

                    if attempt == self.max_retries:
                        logger.error(f"Max retries ({self.max_retries}) exceeded for {func.__name__}, status: {response.status_code}")
                        return response

                    delay = self._calculate_delay(attempt)
                    logger.warning(f"Attempt {attempt + 1} returned status {response.status_code} for {func.__name__}, retrying in {delay:.2f}s")
                    time.sleep(delay)

                except Exception as e:
                    last_exception = e

                    if attempt == self.max_retries:
                        logger.error(f"Max retries ({self.max_retries}) exceeded for {func.__name__}: {e}")
                        raise

                    delay = self._calculate_delay(attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}, retrying in {delay:.2f}s: {e}")
                    time.sleep(delay)

            if last_exception:
                raise last_exception

        return wrapper

class RateLimitRetryHandler(SmartRetryHandler):
    """Retry handler with rate limit awareness."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        super().__init__(max_retries, base_delay, max_delay)

    def should_retry(self, response: requests.Response) -> bool:
        """Enhanced retry logic with rate limit handling."""
        if response.status_code == 429:
            # Rate limited - check for Retry-After header
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    delay = int(retry_after)
                    logger.info(f"Rate limited, waiting {delay} seconds as requested")
                    time.sleep(delay)
                    return True
                except ValueError:
                    pass
            return True

        return super().should_retry(response)

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay with rate limit consideration."""
        if attempt == 0:
            return 1.0  # Immediate retry for rate limits

        return super()._calculate_delay(attempt)

def retry_decorator(max_retries: int = 3, base_delay: float = 1.0):
    """Simple retry decorator for quick use."""
    def decorator(func: Callable) -> Callable:
        handler = RetryHandler(max_retries, base_delay)
        return handler.retry_on_failure(func)
    return decorator

class CircuitBreakerRetryHandler(RateLimitRetryHandler):
    """Retry handler with circuit breaker pattern."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        super().__init__(max_retries, base_delay, max_delay)
        self.failure_count = 0
        self.success_count = 0
        self.failure_threshold = 5
        self.circuit_open = False
        self.last_failure_time = 0
        self.circuit_timeout = 60.0  # seconds

    def is_circuit_open(self) -> bool:
        """Check if circuit breaker is open."""
        if self.circuit_open:
            if time.time() - self.last_failure_time > self.circuit_timeout:
                self.circuit_open = False
                self.failure_count = 0
                logger.info("Circuit breaker reset - attempting to close")
                return False
            return True
        return False

    def record_success(self):
        """Record successful operation."""
        self.success_count += 1
        if self.circuit_open and self.success_count >= 3:
            self.circuit_open = False
            self.failure_count = 0
            logger.info("Circuit breaker closed after successful operations")

    def record_failure(self):
        """Record failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.circuit_open = True
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

def smart_retry_decorator(max_retries: int = 3, base_delay: float = 1.0):
    """Smart retry decorator with status code awareness."""
    def decorator(func: Callable) -> Callable:
        handler = SmartRetryHandler(max_retries, base_delay)
        return handler.retry_on_response(func)
    return decorator
