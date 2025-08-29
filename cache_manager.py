#!/usr/bin/env python3
"""
Cache Management System

Provides intelligent caching for API responses, database queries,
and frequently accessed data to improve performance.
"""

import os
import json
import time
import hashlib
import logging
import pickle
from typing import Any, Optional, Dict, List, Union
from datetime import datetime, timedelta
from pathlib import Path
import threading
import sqlite3
from collections import OrderedDict

logger = logging.getLogger(__name__)

class CacheEntry:
    """Represents a cache entry with metadata."""
    
    def __init__(self, key: str, value: Any, ttl: int = 3600):
        self.key = key
        self.value = value
        self.created_at = time.time()
        self.ttl = ttl
        self.access_count = 0
        self.last_accessed = self.created_at
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return time.time() > (self.created_at + self.ttl)
    
    def access(self):
        """Mark the entry as accessed."""
        self.access_count += 1
        self.last_accessed = time.time()
    
    def get_age(self) -> float:
        """Get the age of the entry in seconds."""
        return time.time() - self.created_at
    
    def get_time_to_live(self) -> float:
        """Get remaining time to live in seconds."""
        return max(0, (self.created_at + self.ttl) - time.time())

class MemoryCache:
    """In-memory cache with LRU eviction policy."""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from cache."""
        with self.lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            
            if entry.is_expired():
                del self.cache[key]
                return None
            
            entry.access()
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return entry.value
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Set a value in cache."""
        with self.lock:
            if ttl is None:
                ttl = self.default_ttl
            
            # Remove if exists
            if key in self.cache:
                del self.cache[key]
            
            # Evict if cache is full
            if len(self.cache) >= self.max_size:
                self._evict_lru()
            
            entry = CacheEntry(key, value, ttl)
            self.cache[key] = entry
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
    
    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if self.cache:
            # Remove first item (least recently used)
            self.cache.popitem(last=False)
    
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count of removed entries."""
        with self.lock:
            expired_keys = [key for key, entry in self.cache.items() if entry.is_expired()]
            for key in expired_keys:
                del self.cache[key]
            return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_entries = len(self.cache)
            expired_entries = sum(1 for entry in self.cache.values() if entry.is_expired())
            total_access_count = sum(entry.access_count for entry in self.cache.values())
            
            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'valid_entries': total_entries - expired_entries,
                'total_access_count': total_access_count,
                'max_size': self.max_size,
                'utilization': (total_entries / self.max_size) * 100 if self.max_size > 0 else 0
            }

class DiskCache:
    """Disk-based cache with SQLite backend."""
    
    def __init__(self, cache_dir: str = ".cache", default_ttl: int = 3600):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.cache_dir / "cache.db"
        self.default_ttl = default_ttl
        self.lock = threading.RLock()
        self._init_database()
    
    def _init_database(self):
        """Initialize the cache database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_entries (
                    key TEXT PRIMARY KEY,
                    value BLOB,
                    created_at REAL,
                    ttl INTEGER,
                    access_count INTEGER DEFAULT 0,
                    last_accessed REAL
                )
            """)
            conn.commit()
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from disk cache."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT value, created_at, ttl, access_count
                    FROM cache_entries WHERE key = ?
                """, (key,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                value_blob, created_at, ttl, access_count = row
                
                # Check if expired
                if time.time() > (created_at + ttl):
                    self.delete(key)
                    return None
                
                # Update access statistics
                conn.execute("""
                    UPDATE cache_entries 
                    SET access_count = ?, last_accessed = ?
                    WHERE key = ?
                """, (access_count + 1, time.time(), key))
                conn.commit()
                
                try:
                    return pickle.loads(value_blob)
                except Exception as e:
                    logger.error(f"Failed to deserialize cached value for key {key}: {e}")
                    self.delete(key)
                    return None
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Set a value in disk cache."""
        if ttl is None:
            ttl = self.default_ttl
        
        with self.lock:
            try:
                value_blob = pickle.dumps(value)
                created_at = time.time()
                
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        INSERT OR REPLACE INTO cache_entries 
                        (key, value, created_at, ttl, access_count, last_accessed)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (key, value_blob, created_at, ttl, 0, created_at))
                    conn.commit()
            except Exception as e:
                logger.error(f"Failed to cache value for key {key}: {e}")
    
    def delete(self, key: str) -> bool:
        """Delete a key from disk cache."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("DELETE FROM cache_entries WHERE key = ?", (key,))
                conn.commit()
                return cursor.rowcount > 0
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM cache_entries")
                conn.commit()
    
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count of removed entries."""
        with self.lock:
            current_time = time.time()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    DELETE FROM cache_entries 
                    WHERE (created_at + ttl) < ?
                """, (current_time,))
                conn.commit()
                return cursor.rowcount
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM cache_entries")
            total_entries = cursor.fetchone()[0]
            
            cursor = conn.execute("""
                SELECT COUNT(*) FROM cache_entries 
                WHERE (created_at + ttl) < ?
            """, (time.time(),))
            expired_entries = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT SUM(access_count) FROM cache_entries")
            total_access_count = cursor.fetchone()[0] or 0
            
            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'valid_entries': total_entries - expired_entries,
                'total_access_count': total_access_count,
                'cache_size_mb': self._get_cache_size()
            }
    
    def _get_cache_size(self) -> float:
        """Get cache size in megabytes."""
        try:
            size_bytes = self.db_path.stat().st_size
            return size_bytes / (1024 * 1024)
        except Exception:
            return 0.0

class CacheManager:
    """Unified cache manager with multiple cache layers."""
    
    def __init__(self, memory_cache_size: int = 1000, disk_cache_dir: str = ".cache"):
        self.memory_cache = MemoryCache(memory_cache_size)
        self.disk_cache = DiskCache(disk_cache_dir)
        self.enabled = True
    
    def get(self, key: str, use_disk_cache: bool = True) -> Optional[Any]:
        """Get value from cache, checking memory first, then disk."""
        if not self.enabled:
            return None
        
        # Try memory cache first
        value = self.memory_cache.get(key)
        if value is not None:
            return value
        
        # Try disk cache if enabled
        if use_disk_cache:
            value = self.disk_cache.get(key)
            if value is not None:
                # Store in memory cache for faster access
                self.memory_cache.set(key, value)
                return value
        
        return None
    
    def set(self, key: str, value: Any, ttl: int = 3600, use_disk_cache: bool = True) -> None:
        """Set value in cache layers."""
        if not self.enabled:
            return
        
        # Set in memory cache
        self.memory_cache.set(key, value, ttl)
        
        # Set in disk cache if enabled
        if use_disk_cache:
            self.disk_cache.set(key, value, ttl)
    
    def delete(self, key: str) -> bool:
        """Delete key from all cache layers."""
        memory_deleted = self.memory_cache.delete(key)
        disk_deleted = self.disk_cache.delete(key)
        return memory_deleted or disk_deleted
    
    def clear(self) -> None:
        """Clear all cache layers."""
        self.memory_cache.clear()
        self.disk_cache.clear()
    
    def cleanup(self) -> Dict[str, int]:
        """Clean up expired entries from all cache layers."""
        memory_cleaned = self.memory_cache.cleanup_expired()
        disk_cleaned = self.disk_cache.cleanup_expired()
        
        return {
            'memory_cleaned': memory_cleaned,
            'disk_cleaned': disk_cleaned,
            'total_cleaned': memory_cleaned + disk_cleaned
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from all cache layers."""
        memory_stats = self.memory_cache.get_stats()
        disk_stats = self.disk_cache.get_stats()
        
        return {
            'memory_cache': memory_stats,
            'disk_cache': disk_stats,
            'enabled': self.enabled
        }
    
    def enable(self) -> None:
        """Enable caching."""
        self.enabled = True
    
    def disable(self) -> None:
        """Disable caching."""
        self.enabled = False

class APICache:
    """Specialized cache for API responses."""
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
    
    def get_cached_response(self, url: str, params: Dict = None, headers: Dict = None) -> Optional[Dict]:
        """Get cached API response."""
        cache_key = self._generate_cache_key(url, params, headers)
        return self.cache_manager.get(cache_key)
    
    def cache_response(self, url: str, response: Dict, params: Dict = None, 
                      headers: Dict = None, ttl: int = 300) -> None:
        """Cache API response."""
        cache_key = self._generate_cache_key(url, params, headers)
        self.cache_manager.set(cache_key, response, ttl)
    
    def invalidate_url_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching URL pattern."""
        # This is a simplified implementation
        # In a real scenario, you might want to use regex patterns
        # or maintain a separate index of URL patterns
        return 0
    
    def _generate_cache_key(self, url: str, params: Dict = None, headers: Dict = None) -> str:
        """Generate cache key for API request."""
        key_parts = [url]
        
        if params:
            # Sort params for consistent key generation
            sorted_params = sorted(params.items())
            key_parts.append(str(sorted_params))
        
        if headers:
            # Include relevant headers (exclude auth headers)
            relevant_headers = {k: v for k, v in headers.items() 
                              if k.lower() not in ['authorization', 'cookie']}
            if relevant_headers:
                sorted_headers = sorted(relevant_headers.items())
                key_parts.append(str(sorted_headers))
        
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()

def cache_decorator(ttl: int = 3600, key_prefix: str = ""):
    """Decorator for caching function results."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Generate cache key
            key_parts = [key_prefix, func.__name__]
            if args:
                key_parts.append(str(args))
            if kwargs:
                key_parts.append(str(sorted(kwargs.items())))
            
            cache_key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
            
            # Try to get from cache
            cache_manager = getattr(func, '_cache_manager', None)
            if cache_manager:
                cached_result = cache_manager.get(cache_key)
                if cached_result is not None:
                    return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            
            if cache_manager:
                cache_manager.set(cache_key, result, ttl)
            
            return result
        return wrapper
    return decorator
