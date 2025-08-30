#!/usr/bin/env python3
"""
Data Backup Manager Module

Provides comprehensive backup and recovery functionality for the sync system,
including automated backups, data compression, and restoration capabilities.
"""

import os
import shutil
import sqlite3
import logging
import json
import gzip
import zipfile
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from pathlib import Path
import threading
import time
import hashlib

logger = logging.getLogger(__name__)

class BackupManager:
    """Manages backup operations for the sync system."""
    
    def __init__(self, backup_dir: str = "backups", retention_days: int = 30):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days
        self.backup_history: List[Dict[str, Any]] = []
        self.lock = threading.RLock()
        self._load_backup_history()
    
    def _load_backup_history(self):
        """Load backup history from file."""
        history_file = self.backup_dir / "backup_history.json"
        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    self.backup_history = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load backup history: {e}")
                self.backup_history = []
    
    def _save_backup_history(self):
        """Save backup history to file."""
        history_file = self.backup_dir / "backup_history.json"
        try:
            with open(history_file, 'w') as f:
                json.dump(self.backup_history, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save backup history: {e}")
    
    def create_backup(self, source_paths: List[str], backup_name: str = None, 
                     compression: bool = True, include_logs: bool = True) -> Dict[str, Any]:
        """Create a backup of specified files and directories."""
        if backup_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"backup_{timestamp}"
        
        backup_path = self.backup_dir / f"{backup_name}.zip"
        
        with self.lock:
            try:
                # Create backup
                backup_info = self._create_backup_archive(source_paths, backup_path, compression, include_logs)
                
                # Calculate checksum
                backup_info['checksum'] = self._calculate_checksum(backup_path)
                
                # Add to history
                self.backup_history.append(backup_info)
                self._save_backup_history()
                
                logger.info(f"Backup created successfully: {backup_path}")
                return backup_info
                
            except Exception as e:
                logger.error(f"Backup creation failed: {e}")
                # Clean up failed backup
                if backup_path.exists():
                    backup_path.unlink()
                raise
    
    def _create_backup_archive(self, source_paths: List[str], backup_path: Path, 
                              compression: bool, include_logs: bool) -> Dict[str, Any]:
        """Create backup archive."""
        start_time = time.time()
        total_size = 0
        file_count = 0
        
        # Add log files if requested
        if include_logs:
            log_files = list(Path(".").glob("*.log"))
            source_paths.extend([str(f) for f in log_files])
        
        with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED if compression else zipfile.ZIP_STORED) as zipf:
            for source_path in source_paths:
                source = Path(source_path)
                if not source.exists():
                    logger.warning(f"Source path does not exist: {source_path}")
                    continue
                
                if source.is_file():
                    # Add single file
                    arcname = source.name
                    zipf.write(source, arcname)
                    total_size += source.stat().st_size
                    file_count += 1
                    
                elif source.is_dir():
                    # Add directory recursively
                    for file_path in source.rglob("*"):
                        if file_path.is_file():
                            arcname = file_path.relative_to(source)
                            zipf.write(file_path, arcname)
                            total_size += file_path.stat().st_size
                            file_count += 1
        
        duration = time.time() - start_time
        
        return {
            'name': backup_path.stem,
            'path': str(backup_path),
            'created_at': datetime.now().isoformat(),
            'size_bytes': total_size,
            'size_mb': round(total_size / (1024 * 1024), 2),
            'file_count': file_count,
            'compression': compression,
            'duration_seconds': round(duration, 2),
            'source_paths': source_paths
        }
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of backup file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def restore_backup(self, backup_name: str, restore_path: str = ".", 
                      overwrite: bool = False) -> Dict[str, Any]:
        """Restore a backup."""
        backup_path = self.backup_dir / f"{backup_name}.zip"
        
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup not found: {backup_name}")
        
        with self.lock:
            try:
                start_time = time.time()
                restore_info = self._restore_backup_archive(backup_path, restore_path, overwrite)
                
                # Verify checksum
                if not self._verify_backup_checksum(backup_path, backup_name):
                    raise ValueError("Backup checksum verification failed")
                
                duration = time.time() - start_time
                restore_info['duration_seconds'] = round(duration, 2)
                
                logger.info(f"Backup restored successfully: {backup_name}")
                return restore_info
                
            except Exception as e:
                logger.error(f"Backup restoration failed: {e}")
                raise
    
    def _restore_backup_archive(self, backup_path: Path, restore_path: str, 
                               overwrite: bool) -> Dict[str, Any]:
        """Restore backup archive."""
        restore_dir = Path(restore_path)
        restore_dir.mkdir(parents=True, exist_ok=True)
        
        extracted_files = []
        total_size = 0
        
        with zipfile.ZipFile(backup_path, 'r') as zipf:
            # Check for conflicts if not overwriting
            if not overwrite:
                for info in zipf.infolist():
                    target_path = restore_dir / info.filename
                    if target_path.exists():
                        raise FileExistsError(f"File already exists: {target_path}")
            
            # Extract files
            for info in zipf.infolist():
                zipf.extract(info, restore_dir)
                extracted_files.append(info.filename)
                total_size += info.file_size
        
        return {
            'backup_name': backup_path.stem,
            'restore_path': str(restore_dir),
            'extracted_files': extracted_files,
            'file_count': len(extracted_files),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'restored_at': datetime.now().isoformat()
        }
    
    def _verify_backup_checksum(self, backup_path: Path, backup_name: str) -> bool:
        """Verify backup checksum."""
        # Find backup in history
        backup_info = None
        for backup in self.backup_history:
            if backup['name'] == backup_name:
                backup_info = backup
                break
        
        if not backup_info or 'checksum' not in backup_info:
            logger.warning(f"No checksum found for backup: {backup_name}")
            return True  # Skip verification if no checksum
        
        current_checksum = self._calculate_checksum(backup_path)
        return current_checksum == backup_info['checksum']
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List all available backups."""
        with self.lock:
            return self.backup_history.copy()
    
    def get_backup_info(self, backup_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific backup."""
        with self.lock:
            for backup in self.backup_history:
                if backup['name'] == backup_name:
                    return backup.copy()
        return None
    
    def delete_backup(self, backup_name: str) -> bool:
        """Delete a backup."""
        with self.lock:
            # Find backup in history
            backup_info = None
            for backup in self.backup_history:
                if backup['name'] == backup_name:
                    backup_info = backup
                    break
            
            if not backup_info:
                logger.warning(f"Backup not found in history: {backup_name}")
                return False
            
            # Delete backup file
            backup_path = Path(backup_info['path'])
            if backup_path.exists():
                backup_path.unlink()
                logger.info(f"Deleted backup file: {backup_path}")
            
            # Remove from history
            self.backup_history = [b for b in self.backup_history if b['name'] != backup_name]
            self._save_backup_history()
            
            logger.info(f"Deleted backup: {backup_name}")
            return True
    
    def cleanup_old_backups(self) -> int:
        """Remove backups older than retention period."""
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        deleted_count = 0
        
        with self.lock:
            backups_to_delete = []
            
            for backup in self.backup_history:
                backup_date = datetime.fromisoformat(backup['created_at'])
                if backup_date < cutoff_date:
                    backups_to_delete.append(backup['name'])
            
            for backup_name in backups_to_delete:
                if self.delete_backup(backup_name):
                    deleted_count += 1
        
        logger.info(f"Cleaned up {deleted_count} old backups")
        return deleted_count
    
    def create_database_backup(self, db_path: str, backup_name: str = None) -> Dict[str, Any]:
        """Create a specialized database backup."""
        if backup_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"db_backup_{timestamp}"
        
        backup_path = self.backup_dir / f"{backup_name}.db"
        
        with self.lock:
            try:
                start_time = time.time()
                
                # Create database backup
                self._backup_database(db_path, backup_path)
                
                # Compress backup
                compressed_path = backup_path.with_suffix('.db.gz')
                self._compress_file(backup_path, compressed_path)
                
                # Remove uncompressed backup
                backup_path.unlink()
                
                # Calculate checksum
                checksum = self._calculate_checksum(compressed_path)
                
                duration = time.time() - start_time
                size_bytes = compressed_path.stat().st_size
                
                backup_info = {
                    'name': backup_name,
                    'path': str(compressed_path),
                    'created_at': datetime.now().isoformat(),
                    'size_bytes': size_bytes,
                    'size_mb': round(size_bytes / (1024 * 1024), 2),
                    'file_count': 1,
                    'compression': True,
                    'duration_seconds': round(duration, 2),
                    'source_paths': [db_path],
                    'checksum': checksum,
                    'type': 'database'
                }
                
                # Add to history
                self.backup_history.append(backup_info)
                self._save_backup_history()
                
                logger.info(f"Database backup created: {compressed_path}")
                return backup_info
                
            except Exception as e:
                logger.error(f"Database backup failed: {e}")
                # Clean up failed backup
                if backup_path.exists():
                    backup_path.unlink()
                if compressed_path.exists():
                    compressed_path.unlink()
                raise
    
    def _backup_database(self, source_db: str, backup_db: Path):
        """Create a backup of SQLite database."""
        with sqlite3.connect(source_db) as source_conn:
            with sqlite3.connect(backup_db) as backup_conn:
                source_conn.backup(backup_conn)
    
    def _compress_file(self, source_path: Path, target_path: Path):
        """Compress a file using gzip."""
        with open(source_path, 'rb') as f_in:
            with gzip.open(target_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    
    def restore_database_backup(self, backup_name: str, target_db: str, 
                               overwrite: bool = False) -> Dict[str, Any]:
        """Restore a database backup."""
        backup_info = self.get_backup_info(backup_name)
        if not backup_info:
            raise FileNotFoundError(f"Backup not found: {backup_name}")
        
        backup_path = Path(backup_info['path'])
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_path}")
        
        # Check if target database exists
        target_path = Path(target_db)
        if target_path.exists() and not overwrite:
            raise FileExistsError(f"Target database already exists: {target_db}")
        
        with self.lock:
            try:
                start_time = time.time()
                
                # Decompress backup
                temp_backup = backup_path.with_suffix('.db.tmp')
                self._decompress_file(backup_path, temp_backup)
                
                # Restore database
                self._restore_database(temp_backup, target_db)
                
                # Clean up temporary file
                temp_backup.unlink()
                
                duration = time.time() - start_time
                
                restore_info = {
                    'backup_name': backup_name,
                    'target_database': target_db,
                    'restored_at': datetime.now().isoformat(),
                    'duration_seconds': round(duration, 2)
                }
                
                logger.info(f"Database backup restored: {backup_name} -> {target_db}")
                return restore_info
                
            except Exception as e:
                logger.error(f"Database restore failed: {e}")
                # Clean up temporary file
                if temp_backup.exists():
                    temp_backup.unlink()
                raise
    
    def _decompress_file(self, source_path: Path, target_path: Path):
        """Decompress a gzipped file."""
        with gzip.open(source_path, 'rb') as f_in:
            with open(target_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    
    def _restore_database(self, backup_db: Path, target_db: str):
        """Restore database from backup."""
        with sqlite3.connect(backup_db) as backup_conn:
            with sqlite3.connect(target_db) as target_conn:
                backup_conn.backup(target_conn)
    
    def get_backup_stats(self) -> Dict[str, Any]:
        """Get backup statistics."""
        with self.lock:
            if not self.backup_history:
                return {
                    'total_backups': 0,
                    'total_size_mb': 0,
                    'oldest_backup': None,
                    'newest_backup': None,
                    'average_size_mb': 0
                }
            
            total_backups = len(self.backup_history)
            total_size_mb = sum(b['size_mb'] for b in self.backup_history)
            average_size_mb = total_size_mb / total_backups
            
            # Find oldest and newest backups
            dates = [datetime.fromisoformat(b['created_at']) for b in self.backup_history]
            oldest_backup = min(dates).isoformat()
            newest_backup = max(dates).isoformat()
            
            return {
                'total_backups': total_backups,
                'total_size_mb': round(total_size_mb, 2),
                'oldest_backup': oldest_backup,
                'newest_backup': newest_backup,
                'average_size_mb': round(average_size_mb, 2)
            }
    
    def schedule_backup(self, source_paths: List[str], schedule: str, 
                       callback: Optional[Callable] = None):
        """Schedule automatic backups."""
        # This would integrate with a scheduler
        logger.info(f"Scheduled backup for paths: {source_paths}")
        logger.info(f"Schedule: {schedule}")
        
        if callback:
            callback()

class AutomatedBackupScheduler:
    """Scheduler for automated backups."""
    
    def __init__(self, backup_manager: BackupManager):
        self.backup_manager = backup_manager
        self.schedules: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self.thread = None
    
    def add_schedule(self, name: str, source_paths: List[str], 
                    interval_hours: int = 24, max_backups: int = 7):
        """Add a backup schedule."""
        self.schedules[name] = {
            'source_paths': source_paths,
            'interval_hours': interval_hours,
            'max_backups': max_backups,
            'last_backup': None,
            'next_backup': datetime.now() + timedelta(hours=interval_hours)
        }
        logger.info(f"Added backup schedule: {name}")
    
    def remove_schedule(self, name: str):
        """Remove a backup schedule."""
        if name in self.schedules:
            del self.schedules[name]
            logger.info(f"Removed backup schedule: {name}")
    
    def start(self):
        """Start the backup scheduler."""
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()
        logger.info("Backup scheduler started")
    
    def stop(self):
        """Stop the backup scheduler."""
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("Backup scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        while self.running:
            try:
                current_time = datetime.now()
                
                for name, schedule in self.schedules.items():
                    if schedule['next_backup'] <= current_time:
                        self._run_scheduled_backup(name, schedule)
                
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(300)  # Wait 5 minutes on error
    
    def _run_scheduled_backup(self, name: str, schedule: Dict[str, Any]):
        """Run a scheduled backup."""
        try:
            logger.info(f"Running scheduled backup: {name}")
            
            # Create backup
            backup_info = self.backup_manager.create_backup(
                schedule['source_paths'],
                backup_name=f"scheduled_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            # Update schedule
            schedule['last_backup'] = backup_info['created_at']
            schedule['next_backup'] = datetime.now() + timedelta(hours=schedule['interval_hours'])
            
            # Clean up old backups if needed
            self._cleanup_old_scheduled_backups(name, schedule['max_backups'])
            
            logger.info(f"Scheduled backup completed: {name}")
            
        except Exception as e:
            logger.error(f"Scheduled backup failed: {name} - {e}")
    
    def _cleanup_old_scheduled_backups(self, schedule_name: str, max_backups: int):
        """Clean up old scheduled backups."""
        backups = self.backup_manager.list_backups()
        scheduled_backups = [b for b in backups if b['name'].startswith(f"scheduled_{schedule_name}_")]
        
        if len(scheduled_backups) > max_backups:
            # Sort by creation date and remove oldest
            scheduled_backups.sort(key=lambda x: x['created_at'])
            backups_to_remove = scheduled_backups[:-max_backups]
            
            for backup in backups_to_remove:
                self.backup_manager.delete_backup(backup['name'])
                logger.info(f"Removed old scheduled backup: {backup['name']}")
