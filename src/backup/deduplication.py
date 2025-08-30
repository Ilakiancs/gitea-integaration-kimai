#!/usr/bin/env python3
"""
Backup Deduplication Module

Deduplication functionality for backup files with multiple algorithms
and efficient storage management.
"""

import os
import hashlib
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import threading

logger = logging.getLogger(__name__)

@dataclass
class DedupChunk:
    """Deduplication chunk information."""
    chunk_hash: str
    chunk_size: int
    chunk_data: bytes
    reference_count: int = 1

class BackupDeduplication:
    """Deduplication handler for backup files."""
    
    def __init__(self, storage_dir: str = "dedup_storage", algorithm: str = "sha256"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.algorithm = algorithm
        self.chunk_size = 64 * 1024  # 64KB chunks
        self.db_path = self.storage_dir / "dedup.db"
        self.lock = threading.RLock()
        self._init_database()
    
    def _init_database(self):
        """Initialize deduplication database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS chunks (
                        hash TEXT PRIMARY KEY,
                        size INTEGER,
                        path TEXT,
                        reference_count INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS files (
                        file_path TEXT PRIMARY KEY,
                        chunk_count INTEGER,
                        total_size INTEGER,
                        dedup_size INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize deduplication database: {e}")
    
    def deduplicate_file(self, file_path: Path) -> Dict[str, Any]:
        """Deduplicate a file and return statistics."""
        try:
            with self.lock:
                file_hash = self._calculate_file_hash(file_path)
                
                # Check if file already exists
                if self._file_exists(file_hash):
                    return self._get_existing_file_stats(file_hash)
                
                # Chunk the file
                chunks = self._chunk_file(file_path)
                
                # Process chunks
                dedup_chunks = []
                total_size = 0
                dedup_size = 0
                
                for chunk in chunks:
                    chunk_hash = self._calculate_chunk_hash(chunk)
                    chunk_size = len(chunk)
                    total_size += chunk_size
                    
                    if self._chunk_exists(chunk_hash):
                        # Increment reference count
                        self._increment_chunk_reference(chunk_hash)
                        dedup_size += 0  # No additional storage needed
                    else:
                        # Store new chunk
                        self._store_chunk(chunk_hash, chunk)
                        dedup_size += chunk_size
                    
                    dedup_chunks.append(chunk_hash)
                
                # Store file metadata
                self._store_file_metadata(str(file_path), len(dedup_chunks), total_size, dedup_size)
                
                stats = {
                    'original_size': total_size,
                    'dedup_size': dedup_size,
                    'chunk_count': len(dedup_chunks),
                    'space_saved': total_size - dedup_size,
                    'compression_ratio': (total_size - dedup_size) / total_size if total_size > 0 else 0
                }
                
                logger.info(f"File deduplicated: {file_path} - {stats['compression_ratio']*100:.1f}% space saved")
                return stats
                
        except Exception as e:
            logger.error(f"Deduplication failed for {file_path}: {e}")
            return {}
    
    def _chunk_file(self, file_path: Path) -> List[bytes]:
        """Split file into chunks."""
        chunks = []
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    chunks.append(chunk)
        except Exception as e:
            logger.error(f"Failed to chunk file {file_path}: {e}")
        
        return chunks
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate hash of entire file."""
        hash_func = hashlib.new(self.algorithm)
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_func.update(chunk)
        except Exception as e:
            logger.error(f"Failed to calculate file hash: {e}")
        
        return hash_func.hexdigest()
    
    def _calculate_chunk_hash(self, chunk: bytes) -> str:
        """Calculate hash of a chunk."""
        hash_func = hashlib.new(self.algorithm)
        hash_func.update(chunk)
        return hash_func.hexdigest()
    
    def _chunk_exists(self, chunk_hash: str) -> bool:
        """Check if chunk exists in storage."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM chunks WHERE hash = ?",
                    (chunk_hash,)
                )
                return cursor.fetchone()[0] > 0
        except Exception as e:
            logger.error(f"Failed to check chunk existence: {e}")
            return False
    
    def _file_exists(self, file_hash: str) -> bool:
        """Check if file exists in storage."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM files WHERE file_path = ?",
                    (file_hash,)
                )
                return cursor.fetchone()[0] > 0
        except Exception as e:
            logger.error(f"Failed to check file existence: {e}")
            return False
    
    def _store_chunk(self, chunk_hash: str, chunk_data: bytes):
        """Store a chunk in the deduplication storage."""
        try:
            # Store chunk data
            chunk_path = self.storage_dir / f"{chunk_hash[:2]}" / f"{chunk_hash[2:]}"
            chunk_path.parent.mkdir(exist_ok=True)
            
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            # Store metadata
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO chunks (hash, size, path) VALUES (?, ?, ?)",
                    (chunk_hash, len(chunk_data), str(chunk_path))
                )
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to store chunk: {e}")
    
    def _increment_chunk_reference(self, chunk_hash: str):
        """Increment reference count for a chunk."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE chunks SET reference_count = reference_count + 1 WHERE hash = ?",
                    (chunk_hash,)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to increment chunk reference: {e}")
    
    def _store_file_metadata(self, file_path: str, chunk_count: int, total_size: int, dedup_size: int):
        """Store file metadata."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO files (file_path, chunk_count, total_size, dedup_size) VALUES (?, ?, ?, ?)",
                    (file_path, chunk_count, total_size, dedup_size)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to store file metadata: {e}")
    
    def _get_existing_file_stats(self, file_hash: str) -> Dict[str, Any]:
        """Get statistics for existing file."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT chunk_count, total_size, dedup_size FROM files WHERE file_path = ?",
                    (file_hash,)
                )
                row = cursor.fetchone()
                if row:
                    chunk_count, total_size, dedup_size = row
                    return {
                        'original_size': total_size,
                        'dedup_size': dedup_size,
                        'chunk_count': chunk_count,
                        'space_saved': total_size - dedup_size,
                        'compression_ratio': (total_size - dedup_size) / total_size if total_size > 0 else 0
                    }
        except Exception as e:
            logger.error(f"Failed to get existing file stats: {e}")
        
        return {}
    
    def get_deduplication_stats(self) -> Dict[str, Any]:
        """Get overall deduplication statistics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get chunk statistics
                cursor = conn.execute("SELECT COUNT(*), SUM(size) FROM chunks")
                chunk_count, total_chunk_size = cursor.fetchone()
                
                # Get file statistics
                cursor = conn.execute("SELECT COUNT(*), SUM(total_size), SUM(dedup_size) FROM files")
                file_count, total_file_size, total_dedup_size = cursor.fetchone()
                
                # Get reference count statistics
                cursor = conn.execute("SELECT AVG(reference_count), MAX(reference_count) FROM chunks")
                avg_refs, max_refs = cursor.fetchone()
                
                return {
                    'total_files': file_count or 0,
                    'total_chunks': chunk_count or 0,
                    'total_original_size': total_file_size or 0,
                    'total_dedup_size': total_dedup_size or 0,
                    'total_space_saved': (total_file_size or 0) - (total_dedup_size or 0),
                    'average_compression_ratio': ((total_file_size or 0) - (total_dedup_size or 0)) / (total_file_size or 1),
                    'average_references': avg_refs or 0,
                    'max_references': max_refs or 0
                }
                
        except Exception as e:
            logger.error(f"Failed to get deduplication stats: {e}")
            return {}
    
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

def create_deduplication(storage_dir: str = "dedup_storage", algorithm: str = "sha256") -> BackupDeduplication:
    """Create and return a deduplication instance."""
    return BackupDeduplication(storage_dir, algorithm)
