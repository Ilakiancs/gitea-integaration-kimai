#!/usr/bin/env python3
"""
Database Connection Pool for Gitea-Kimai Integration

This module provides connection pooling for database operations to improve
performance and handle concurrent access efficiently.
"""

import sqlite3
import threading
import time
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from queue import Queue, Empty, Full
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class PoolStats:
    """Statistics for connection pool monitoring."""
    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    created_connections: int = 0
    closed_connections: int = 0
    failed_connections: int = 0
    peak_connections: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    average_wait_time: float = 0.0
    health_check_count: int = 0
    health_check_failures: int = 0

class PooledConnection:
    """Wrapper for database connections with metadata."""

    def __init__(self, connection: sqlite3.Connection, pool: 'ConnectionPool'):
        self.connection = connection
        self.pool = pool
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        self.use_count = 0
        self.is_active = False
        self.thread_id = None

    def execute(self, sql: str, parameters=None):
        """Execute SQL with connection tracking."""
        self.last_used = datetime.now()
        self.use_count += 1
        if parameters:
            return self.connection.execute(sql, parameters)
        return self.connection.execute(sql)

    def executemany(self, sql: str, parameters):
        """Execute many SQL statements with connection tracking."""
        self.last_used = datetime.now()
        self.use_count += 1
        return self.connection.executemany(sql, parameters)

    def commit(self):
        """Commit transaction."""
        self.connection.commit()

    def rollback(self):
        """Rollback transaction."""
        self.connection.rollback()

    def close(self):
        """Close the underlying connection."""
        try:
            self.connection.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

class ConnectionPool:
    """Thread-safe database connection pool."""

    def __init__(self, database_path: str, min_connections: int = 2,
                 max_connections: int = 10, max_idle_time: int = 300,
                 connection_timeout: int = 30):
        """
        Initialize connection pool.

        Args:
            database_path: Path to SQLite database
            min_connections: Minimum number of connections to maintain
            max_connections: Maximum number of connections allowed
            max_idle_time: Maximum time (seconds) connection can be idle
            connection_timeout: Timeout (seconds) for getting connection
        """
        self.database_path = database_path
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.max_idle_time = max_idle_time
        self.connection_timeout = connection_timeout

        self._pool = Queue(maxsize=max_connections)
        self._all_connections: List[PooledConnection] = []
        self._lock = threading.RLock()
        self._shutdown = False
        self._stats = PoolStats()
        self._cleanup_thread = None
        self._health_check_interval = 30  # seconds

        # Initialize minimum connections
        self._initialize_pool()

        # Start cleanup thread
        self._start_cleanup_thread()

    def _initialize_pool(self):
        """Initialize pool with minimum connections."""
        for _ in range(self.min_connections):
            try:
                conn = self._create_connection()
                self._pool.put(conn, block=False)
            except Exception as e:
                logger.error(f"Failed to create initial connection: {e}")
                self._stats.failed_connections += 1

    def _create_connection(self) -> PooledConnection:
        """Create a new database connection."""
        try:
            conn = sqlite3.connect(
                self.database_path,
                timeout=30,
                check_same_thread=False,
                isolation_level=None  # Autocommit mode
            )

            # Configure connection
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size = -2000")  # 2MB cache
            conn.execute("PRAGMA temp_store = memory")

            pooled_conn = PooledConnection(conn, self)

            with self._lock:
                self._all_connections.append(pooled_conn)
                self._stats.created_connections += 1
                self._stats.total_connections += 1
                self._stats.peak_connections = max(
                    self._stats.peak_connections,
                    self._stats.total_connections
                )

            logger.debug(f"Created new connection. Total: {self._stats.total_connections}")
            return pooled_conn

        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            self._stats.failed_connections += 1
            raise

    def get_connection(self, timeout: Optional[int] = None) -> PooledConnection:
        """
        Get a connection from the pool.

        Args:
            timeout: Timeout in seconds (uses pool default if None)

        Returns:
            PooledConnection: Database connection

        Raises:
            Exception: If no connection available within timeout
        """
        if self._shutdown:
            raise RuntimeError("Connection pool is shutdown")

        timeout = timeout or self.connection_timeout
        start_time = time.time()

        with self._lock:
            self._stats.total_requests += 1

        try:
            # Try to get existing connection from pool
            try:
                conn = self._pool.get(block=True, timeout=timeout)
                conn.is_active = True
                conn.thread_id = threading.current_thread().ident

                # Test connection
                if self._test_connection(conn):
                    with self._lock:
                        self._stats.active_connections += 1
                        self._stats.idle_connections = max(0, self._stats.idle_connections - 1)
                        wait_time = time.time() - start_time
                        self._update_average_wait_time(wait_time)

                    logger.debug(f"Retrieved connection from pool. Active: {self._stats.active_connections}")
                    return conn
                else:
                    # Connection is stale, remove it
                    self._remove_connection(conn)

            except Empty:
                pass

            # Create new connection if under limit
            with self._lock:
                if self._stats.total_connections < self.max_connections:
                    conn = self._create_connection()
                    conn.is_active = True
                    conn.thread_id = threading.current_thread().ident
                    self._stats.active_connections += 1

                    wait_time = time.time() - start_time
                    self._update_average_wait_time(wait_time)

                    logger.debug(f"Created new connection. Active: {self._stats.active_connections}")
                    return conn

            # Pool is full, wait for available connection
            raise Exception(f"No database connection available within {timeout} seconds")

        except Exception as e:
            with self._lock:
                self._stats.failed_requests += 1
            logger.error(f"Failed to get database connection: {e}")
            raise

    def return_connection(self, conn: PooledConnection):
        """
        Return a connection to the pool.

        Args:
            conn: Connection to return
        """
        if not conn or self._shutdown:
            return

        conn.is_active = False
        conn.thread_id = None
        conn.last_used = datetime.now()

        try:
            # Test connection before returning to pool
            if self._test_connection(conn):
                self._pool.put(conn, block=False)

                with self._lock:
                    self._stats.active_connections = max(0, self._stats.active_connections - 1)
                    self._stats.idle_connections += 1

                logger.debug(f"Returned connection to pool. Active: {self._stats.active_connections}")
            else:
                # Connection is bad, remove it
                self._remove_connection(conn)

        except Full:
            # Pool is full, close excess connection
            self._remove_connection(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")
            self._remove_connection(conn)

    def _test_connection(self, conn: PooledConnection) -> bool:
        """Test if connection is still valid."""
        try:
            with self._lock:
                self._stats.health_check_count += 1

            conn.execute("SELECT 1").fetchone()
            return True
        except Exception as e:
            with self._lock:
                self._stats.health_check_failures += 1
            logger.debug(f"Connection test failed: {e}")
            return False

    def _remove_connection(self, conn: PooledConnection):
        """Remove connection from pool and close it."""
        try:
            with self._lock:
                if conn in self._all_connections:
                    self._all_connections.remove(conn)
                    self._stats.total_connections -= 1
                    self._stats.closed_connections += 1

                if conn.is_active:
                    self._stats.active_connections = max(0, self._stats.active_connections - 1)
                else:
                    self._stats.idle_connections = max(0, self._stats.idle_connections - 1)

            conn.close()
            logger.debug(f"Removed connection. Total: {self._stats.total_connections}")

        except Exception as e:
            logger.error(f"Error removing connection: {e}")

    def _update_average_wait_time(self, wait_time: float):
        """Update average wait time statistic."""
        if self._stats.total_requests == 1:
            self._stats.average_wait_time = wait_time
        else:
            # Exponential moving average
            alpha = 0.1
            self._stats.average_wait_time = (
                alpha * wait_time + (1 - alpha) * self._stats.average_wait_time
            )

    def _start_cleanup_thread(self):
        """Start background thread for cleaning up idle connections."""
        def cleanup_worker():
            health_check_counter = 0
            while not self._shutdown:
                try:
                    self._cleanup_idle_connections()

                    # Perform health checks every few cleanup cycles
                    health_check_counter += 1
                    if health_check_counter >= 2:  # Every 2 minutes
                        self._perform_health_checks()
                        health_check_counter = 0

                    time.sleep(60)  # Check every minute
                except Exception as e:
                    logger.error(f"Error in cleanup thread: {e}")

        self._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        self._cleanup_thread.start()

    def _cleanup_idle_connections(self):
        """Remove connections that have been idle too long."""
        cutoff_time = datetime.now() - timedelta(seconds=self.max_idle_time)
        connections_to_remove = []

        with self._lock:
            for conn in self._all_connections:
                if (not conn.is_active and
                    conn.last_used < cutoff_time and
                    self._stats.total_connections > self.min_connections):
                    connections_to_remove.append(conn)

        for conn in connections_to_remove:
            try:
                # Try to remove from queue if it's there
                temp_queue = Queue()
                while True:
                    try:
                        queued_conn = self._pool.get_nowait()
                        if queued_conn != conn:
                            temp_queue.put(queued_conn)
                        else:
                            break
                    except Empty:
                        break

                # Put back the other connections
                while not temp_queue.empty():
                    self._pool.put(temp_queue.get())

                self._remove_connection(conn)
                logger.debug(f"Cleaned up idle connection")

            except Exception as e:
                logger.error(f"Error cleaning up connection: {e}")

    def _perform_health_checks(self):
        """Perform health checks on all idle connections."""
        unhealthy_connections = []

        with self._lock:
            idle_connections = [conn for conn in self._all_connections if not conn.is_active]

        for conn in idle_connections:
            if not self._test_connection(conn):
                unhealthy_connections.append(conn)

        # Remove unhealthy connections
        for conn in unhealthy_connections:
            self._remove_connection(conn)
            logger.debug(f"Removed unhealthy connection during health check")

    @contextmanager
    def get_connection_context(self):
        """Context manager for getting and returning connections."""
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        finally:
            if conn:
                self.return_connection(conn)

    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self._lock:
            health_check_success_rate = 0.0
            if self._stats.health_check_count > 0:
                health_check_success_rate = round(
                    ((self._stats.health_check_count - self._stats.health_check_failures) /
                     self._stats.health_check_count) * 100, 2
                )

            return {
                'total_connections': self._stats.total_connections,
                'active_connections': self._stats.active_connections,
                'idle_connections': self._stats.idle_connections,
                'created_connections': self._stats.created_connections,
                'closed_connections': self._stats.closed_connections,
                'failed_connections': self._stats.failed_connections,
                'peak_connections': self._stats.peak_connections,
                'total_requests': self._stats.total_requests,
                'failed_requests': self._stats.failed_requests,
                'average_wait_time_ms': round(self._stats.average_wait_time * 1000, 2),
                'pool_utilization': round(
                    (self._stats.active_connections / self.max_connections) * 100, 2
                ) if self.max_connections > 0 else 0,
                'health_check_count': self._stats.health_check_count,
                'health_check_failures': self._stats.health_check_failures,
                'health_check_success_rate': health_check_success_rate
            }

    def shutdown(self):
        """Shutdown the connection pool and close all connections."""
        logger.info("Shutting down connection pool")
        self._shutdown = True

        # Wait for cleanup thread to finish
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)

        # Close all connections
        with self._lock:
            for conn in self._all_connections.copy():
                self._remove_connection(conn)

        # Clear the queue
        while not self._pool.empty():
            try:
                self._pool.get_nowait()
            except Empty:
                break

        logger.info("Connection pool shutdown complete")

# Global pool instance
_global_pool: Optional[ConnectionPool] = None
_pool_lock = threading.Lock()

def get_global_pool(database_path: str = "sync.db", **kwargs) -> ConnectionPool:
    """Get or create global connection pool instance."""
    global _global_pool

    with _pool_lock:
        if _global_pool is None:
            _global_pool = ConnectionPool(database_path, **kwargs)
        return _global_pool

def shutdown_global_pool():
    """Shutdown global connection pool."""
    global _global_pool

    with _pool_lock:
        if _global_pool:
            _global_pool.shutdown()
            _global_pool = None
