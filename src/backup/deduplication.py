#!/usr/bin/env python3
"""
Advanced Backup Deduplication Module

Enhanced deduplication functionality for backup files with multiple algorithms,
variable chunk sizing, intelligent deduplication strategies, and advanced analytics.
"""

import os
import hashlib
import logging
import sqlite3
import zlib
import mmap
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from collections import defaultdict
import json

logger = logging.getLogger(__name__)

@dataclass
class DedupChunk:
    """Enhanced deduplication chunk information."""
    chunk_hash: str
    chunk_size: int
    chunk_data: bytes
    reference_count: int = 1
    compression_ratio: float = 1.0
    chunk_type: str = "standard"  # standard, boundary, metadata
    access_pattern: str = "normal"  # hot, warm, cold
    last_accessed: float = field(default_factory=time.time)

@dataclass
class DedupStats:
    """Deduplication statistics."""
    total_files: int
    total_size: int
    dedup_size: int
    savings_ratio: float
    chunk_count: int
    unique_chunks: int
    compression_ratio: float
    algorithm_used: str
    processing_time: float

@dataclass
class DedupConfig:
    """Deduplication configuration."""
    algorithm: str = "sha256"  # sha256, blake2b, xxhash
    chunk_size: int = 64 * 1024  # 64KB default
    variable_chunking: bool = True
    compression_enabled: bool = True
    compression_level: int = 6
    boundary_detection: bool = True
    metadata_deduplication: bool = True
    parallel_processing: bool = True
    max_workers: int = 4
    cache_size: int = 10000
    cleanup_threshold: float = 0.1  # 10% of storage

class AdvancedBackupDeduplication:
    """Advanced deduplication handler with intelligent strategies."""
    
    def __init__(self, storage_dir: str = "dedup_storage", config: DedupConfig = None):
        self.config = config or DedupConfig()
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.chunks_dir = self.storage_dir / "chunks"
        self.chunks_dir.mkdir(exist_ok=True)
        self.metadata_dir = self.storage_dir / "metadata"
        self.metadata_dir.mkdir(exist_ok=True)
        
        self.db_path = self.storage_dir / "dedup.db"
        self.lock = threading.RLock()
        self.chunk_cache: Dict[str, DedupChunk] = {}
        self.file_cache: Dict[str, Dict[str, Any]] = {}
        
        self._init_database()
        self._load_cache()
    
    def _init_database(self):
        """Initialize enhanced deduplication database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Enhanced chunks table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS chunks (
                        hash TEXT PRIMARY KEY,
                        size INTEGER,
                        compressed_size INTEGER,
                        path TEXT,
                        reference_count INTEGER DEFAULT 1,
                        compression_ratio REAL DEFAULT 1.0,
                        chunk_type TEXT DEFAULT 'standard',
                        access_pattern TEXT DEFAULT 'normal',
                        last_accessed REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Enhanced files table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS files (
                        file_path TEXT PRIMARY KEY,
                        file_hash TEXT,
                        chunk_count INTEGER,
                        total_size INTEGER,
                        dedup_size INTEGER,
                        compression_ratio REAL,
                        algorithm_used TEXT,
                        processing_time REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # New table for chunk relationships
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS file_chunks (
                        file_path TEXT,
                        chunk_hash TEXT,
                        chunk_order INTEGER,
                        chunk_offset INTEGER,
                        FOREIGN KEY (file_path) REFERENCES files (file_path),
                        FOREIGN KEY (chunk_hash) REFERENCES chunks (hash)
                    )
                """)
                
                # New table for analytics
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS dedup_analytics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp REAL,
                        total_files INTEGER,
                        total_size INTEGER,
                        dedup_size INTEGER,
                        savings_ratio REAL,
                        unique_chunks INTEGER,
                        compression_ratio REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize deduplication database: {e}")
    
    def _load_cache(self):
        """Load frequently accessed chunks into memory cache."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Load hot chunks (frequently accessed)
                cursor = conn.execute("""
                    SELECT hash, size, compressed_size, reference_count, 
                           compression_ratio, chunk_type, access_pattern, last_accessed
                    FROM chunks 
                    WHERE access_pattern = 'hot' 
                    ORDER BY last_accessed DESC 
                    LIMIT ?
                """, (self.config.cache_size,))
                
                for row in cursor.fetchall():
                    chunk = DedupChunk(
                        chunk_hash=row[0],
                        chunk_size=row[1],
                        chunk_data=b"",  # Don't load data into memory
                        reference_count=row[3],
                        compression_ratio=row[4],
                        chunk_type=row[5],
                        access_pattern=row[6],
                        last_accessed=row[7]
                    )
                    self.chunk_cache[row[0]] = chunk
                
                logger.info(f"Loaded {len(self.chunk_cache)} chunks into cache")
                
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")

    def deduplicate_file(self, file_path: Path) -> DedupStats:
        """Deduplicate a file with advanced algorithms and return detailed statistics."""
        start_time = time.time()
        
        try:
            with self.lock:
                file_hash = self._calculate_file_hash(file_path)
                
                # Check if file already exists
                if self._file_exists(file_hash):
                    return self._get_existing_file_stats(file_hash)
                
                # Determine optimal chunk size based on file characteristics
                optimal_chunk_size = self._determine_optimal_chunk_size(file_path)
                
                # Chunk the file with variable chunking if enabled
                if self.config.variable_chunking:
                    chunks = self._variable_chunk_file(file_path, optimal_chunk_size)
                else:
                    chunks = self._fixed_chunk_file(file_path, optimal_chunk_size)
                
                # Process chunks in parallel if enabled
                if self.config.parallel_processing:
                    dedup_chunks = self._process_chunks_parallel(chunks)
                else:
                    dedup_chunks = self._process_chunks_sequential(chunks)
                
                # Calculate statistics
                total_size = file_path.stat().st_size
                dedup_size = sum(chunk['size'] for chunk in dedup_chunks if not chunk['existing'])
                savings_ratio = 1 - (dedup_size / total_size) if total_size > 0 else 0
                
                # Store file metadata
                self._store_file_metadata(file_path, file_hash, dedup_chunks, total_size, dedup_size)
                
                processing_time = time.time() - start_time
                
                # Update analytics
                self._update_analytics(total_size, dedup_size, len(dedup_chunks))
                
                return DedupStats(
                    total_files=1,
                    total_size=total_size,
                    dedup_size=dedup_size,
                    savings_ratio=savings_ratio,
                    chunk_count=len(dedup_chunks),
                    unique_chunks=len(set(chunk['hash'] for chunk in dedup_chunks)),
                    compression_ratio=np.mean([chunk['compression_ratio'] for chunk in dedup_chunks]),
                    algorithm_used=self.config.algorithm,
                    processing_time=processing_time
                )
                
        except Exception as e:
            logger.error(f"Failed to deduplicate file {file_path}: {e}")
            return DedupStats(
                total_files=0,
                total_size=0,
                dedup_size=0,
                savings_ratio=0.0,
                chunk_count=0,
                unique_chunks=0,
                compression_ratio=1.0,
                algorithm_used=self.config.algorithm,
                processing_time=time.time() - start_time
            )
    
    def _determine_optimal_chunk_size(self, file_path: Path) -> int:
        """Determine optimal chunk size based on file characteristics."""
        file_size = file_path.stat().st_size
        
        # Adjust chunk size based on file size
        if file_size < 1024 * 1024:  # < 1MB
            return 32 * 1024  # 32KB
        elif file_size < 100 * 1024 * 1024:  # < 100MB
            return 64 * 1024  # 64KB
        elif file_size < 1024 * 1024 * 1024:  # < 1GB
            return 128 * 1024  # 128KB
        else:
            return 256 * 1024  # 256KB
    
    def _variable_chunk_file(self, file_path: Path, base_chunk_size: int) -> List[bytes]:
        """Chunk file using variable chunking with boundary detection."""
        chunks = []
        
        try:
            with open(file_path, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    pos = 0
                    while pos < len(mm):
                        # Find next chunk boundary
                        chunk_end = self._find_chunk_boundary(mm, pos, base_chunk_size)
                        chunk_data = mm[pos:chunk_end]
                        chunks.append(chunk_data)
                        pos = chunk_end
        except Exception as e:
            logger.error(f"Failed to variable chunk file {file_path}: {e}")
            # Fallback to fixed chunking
            return self._fixed_chunk_file(file_path, base_chunk_size)
        
        return chunks
    
    def _find_chunk_boundary(self, mm: mmap.mmap, start_pos: int, max_size: int) -> int:
        """Find optimal chunk boundary using rolling hash."""
        if start_pos + max_size >= len(mm):
            return len(mm)
        
        # Use rolling hash to find natural boundaries
        window_size = 64
        target_hash = 0x12345678  # Target boundary hash
        
        for i in range(start_pos + max_size - window_size, start_pos + max_size):
            if i >= len(mm):
                break
            
            # Simple rolling hash
            window_hash = hash(mm[i:i+window_size])
            if window_hash % 1024 == target_hash % 1024:
                return i + window_size
        
        return start_pos + max_size
    
    def _fixed_chunk_file(self, file_path: Path, chunk_size: int) -> List[bytes]:
        """Chunk file using fixed-size chunks."""
        chunks = []
        
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    chunks.append(chunk)
        except Exception as e:
            logger.error(f"Failed to fixed chunk file {file_path}: {e}")
        
        return chunks
    
    def _process_chunks_parallel(self, chunks: List[bytes]) -> List[Dict[str, Any]]:
        """Process chunks in parallel."""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_chunk = {
                executor.submit(self._process_single_chunk, chunk): chunk 
                for chunk in chunks
            }
            
            for future in as_completed(future_to_chunk):
                result = future.result()
                results.append(result)
        
        return sorted(results, key=lambda x: x['order'])
    
    def _process_chunks_sequential(self, chunks: List[bytes]) -> List[Dict[str, Any]]:
        """Process chunks sequentially."""
        results = []
        
        for i, chunk in enumerate(chunks):
            result = self._process_single_chunk(chunk, i)
            results.append(result)
        
        return results
    
    def _process_single_chunk(self, chunk: bytes, order: int = 0) -> Dict[str, Any]:
        """Process a single chunk."""
        chunk_hash = self._calculate_chunk_hash(chunk)
        chunk_size = len(chunk)
        
        # Compress chunk if enabled
        compressed_chunk = chunk
        compression_ratio = 1.0
        
        if self.config.compression_enabled:
            compressed_chunk = zlib.compress(chunk, self.config.compression_level)
            compression_ratio = len(compressed_chunk) / chunk_size
        
        # Check if chunk exists
        existing = self._chunk_exists(chunk_hash)
        
        if existing:
            # Increment reference count
            self._increment_chunk_reference(chunk_hash)
            # Update access pattern
            self._update_chunk_access_pattern(chunk_hash, 'hot')
        else:
            # Store new chunk
            self._store_chunk(chunk_hash, compressed_chunk, chunk_size, compression_ratio)
        
        return {
            'hash': chunk_hash,
            'size': chunk_size,
            'compressed_size': len(compressed_chunk),
            'compression_ratio': compression_ratio,
            'existing': existing,
            'order': order
        }
    
    def _calculate_chunk_hash(self, chunk: bytes) -> str:
        """Calculate hash for a chunk using configured algorithm."""
        if self.config.algorithm == "sha256":
            return hashlib.sha256(chunk).hexdigest()
        elif self.config.algorithm == "blake2b":
            return hashlib.blake2b(chunk).hexdigest()
        elif self.config.algorithm == "xxhash":
            try:
                import xxhash
                return xxhash.xxh64(chunk).hexdigest()
            except ImportError:
                logger.warning("xxhash not available, falling back to sha256")
                return hashlib.sha256(chunk).hexdigest()
        else:
            return hashlib.sha256(chunk).hexdigest()
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate hash for entire file."""
        hash_obj = hashlib.sha256()
        
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_obj.update(chunk)
            return hash_obj.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate file hash: {e}")
            return ""
    
    def _chunk_exists(self, chunk_hash: str) -> bool:
        """Check if chunk exists in storage."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM chunks WHERE hash = ?", 
                    (chunk_hash,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to check chunk existence: {e}")
            return False
    
    def _store_chunk(self, chunk_hash: str, chunk_data: bytes, original_size: int, compression_ratio: float):
        """Store a new chunk."""
        try:
            # Store chunk data
            chunk_path = self.chunks_dir / f"{chunk_hash[:2]}" / f"{chunk_hash[2:]}"
            chunk_path.parent.mkdir(exist_ok=True)
            
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            # Store metadata in database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO chunks (hash, size, compressed_size, path, compression_ratio)
                    VALUES (?, ?, ?, ?, ?)
                """, (chunk_hash, original_size, len(chunk_data), str(chunk_path), compression_ratio))
                conn.commit()
            
            # Add to cache
            self.chunk_cache[chunk_hash] = DedupChunk(
                chunk_hash=chunk_hash,
                chunk_size=original_size,
                chunk_data=b"",
                compression_ratio=compression_ratio
            )
            
        except Exception as e:
            logger.error(f"Failed to store chunk {chunk_hash}: {e}")
    
    def _increment_chunk_reference(self, chunk_hash: str):
        """Increment reference count for a chunk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE chunks 
                    SET reference_count = reference_count + 1,
                        last_accessed = ?
                    WHERE hash = ?
                """, (time.time(), chunk_hash))
                conn.commit()
            
            # Update cache
            if chunk_hash in self.chunk_cache:
                self.chunk_cache[chunk_hash].reference_count += 1
                self.chunk_cache[chunk_hash].last_accessed = time.time()
                
        except Exception as e:
            logger.error(f"Failed to increment chunk reference: {e}")
    
    def _update_chunk_access_pattern(self, chunk_hash: str, pattern: str):
        """Update access pattern for a chunk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE chunks 
                    SET access_pattern = ?, last_accessed = ?
                    WHERE hash = ?
                """, (pattern, time.time(), chunk_hash))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to update chunk access pattern: {e}")
    
    def _file_exists(self, file_hash: str) -> bool:
        """Check if file exists in deduplication storage."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM files WHERE file_hash = ?", 
                    (file_hash,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to check file existence: {e}")
            return False
    
    def _store_file_metadata(self, file_path: Path, file_hash: str, chunks: List[Dict[str, Any]], 
                           total_size: int, dedup_size: int):
        """Store file metadata and chunk relationships."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Store file metadata
                conn.execute("""
                    INSERT INTO files (file_path, file_hash, chunk_count, total_size, dedup_size)
                    VALUES (?, ?, ?, ?, ?)
                """, (str(file_path), file_hash, len(chunks), total_size, dedup_size))
                
                # Store chunk relationships
                for i, chunk in enumerate(chunks):
                    conn.execute("""
                        INSERT INTO file_chunks (file_path, chunk_hash, chunk_order, chunk_offset)
                        VALUES (?, ?, ?, ?)
                    """, (str(file_path), chunk['hash'], i, sum(c['size'] for c in chunks[:i])))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to store file metadata: {e}")
    
    def _update_analytics(self, total_size: int, dedup_size: int, chunk_count: int):
        """Update deduplication analytics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get current stats
                cursor = conn.execute("SELECT COUNT(*) FROM files")
                total_files = cursor.fetchone()[0]
                
                cursor = conn.execute("SELECT COUNT(*) FROM chunks")
                unique_chunks = cursor.fetchone()[0]
                
                savings_ratio = 1 - (dedup_size / total_size) if total_size > 0 else 0
                
                conn.execute("""
                    INSERT INTO dedup_analytics 
                    (timestamp, total_files, total_size, dedup_size, savings_ratio, unique_chunks)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (time.time(), total_files, total_size, dedup_size, savings_ratio, unique_chunks))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to update analytics: {e}")
    
    def get_deduplication_stats(self) -> DedupStats:
        """Get comprehensive deduplication statistics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get file stats
                cursor = conn.execute("""
                    SELECT COUNT(*), SUM(total_size), SUM(dedup_size)
                    FROM files
                """)
                file_count, total_size, dedup_size = cursor.fetchone()
                
                # Get chunk stats
                cursor = conn.execute("SELECT COUNT(*) FROM chunks")
                unique_chunks = cursor.fetchone()[0]
                
                # Get average compression ratio
                cursor = conn.execute("SELECT AVG(compression_ratio) FROM chunks")
                avg_compression = cursor.fetchone()[0] or 1.0
                
                savings_ratio = 1 - (dedup_size / total_size) if total_size and total_size > 0 else 0
                
                return DedupStats(
                    total_files=file_count or 0,
                    total_size=total_size or 0,
                    dedup_size=dedup_size or 0,
                    savings_ratio=savings_ratio,
                    chunk_count=0,  # Would need to calculate
                    unique_chunks=unique_chunks,
                    compression_ratio=avg_compression,
                    algorithm_used=self.config.algorithm,
                    processing_time=0.0
                )
                
        except Exception as e:
            logger.error(f"Failed to get deduplication stats: {e}")
            return DedupStats(
                total_files=0,
                total_size=0,
                dedup_size=0,
                savings_ratio=0.0,
                chunk_count=0,
                unique_chunks=0,
                compression_ratio=1.0,
                algorithm_used=self.config.algorithm,
                processing_time=0.0
            )
    
    def cleanup_orphaned_chunks(self) -> int:
        """Remove chunks with zero references."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get orphaned chunks
                cursor = conn.execute(
                    "SELECT hash, path FROM chunks WHERE reference_count = 0"
                )
                orphaned_chunks = cursor.fetchall()
                
                # Remove chunk files
                removed_count = 0
                for chunk_hash, chunk_path in orphaned_chunks:
                    try:
                        Path(chunk_path).unlink()
                        removed_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to remove orphaned chunk {chunk_path}: {e}")
                
                # Remove from database
                conn.execute("DELETE FROM chunks WHERE reference_count = 0")
                conn.commit()
                
                logger.info(f"Cleaned up {removed_count} orphaned chunks")
                return removed_count
                
        except Exception as e:
            logger.error(f"Failed to cleanup orphaned chunks: {e}")
            return 0

def create_deduplication(storage_dir: str = "dedup_storage", algorithm: str = "sha256") -> AdvancedBackupDeduplication:
    """Create and return a deduplication instance."""
    return AdvancedBackupDeduplication(storage_dir, DedupConfig(algorithm=algorithm))
