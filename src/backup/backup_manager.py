#!/usr/bin/env python3
"""
Next-Generation Backup Manager

Advanced backup management system with intelligent scheduling, compression optimization,
encryption, deduplication, cloud integration, and automated recovery capabilities.
"""

import os
import json
import zipfile
import shutil
import logging
import hashlib
import sqlite3
import tempfile
import threading
import time
import schedule
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set, Callable
from dataclasses import dataclass, asdict
import psutil
import requests
from cryptography.fernet import Fernet
import boto3
from google.cloud import storage
import azure.storage.blob

logger = logging.getLogger(__name__)

@dataclass
class BackupConfig:
    """Configuration for backup operations."""
    source_paths: List[str]
    backup_dir: str
    retention_days: int
    compression_level: int
    encryption_enabled: bool
    deduplication_enabled: bool
    cloud_sync_enabled: bool
    schedule_enabled: bool
    schedule_interval: str  # 'hourly', 'daily', 'weekly'
    cloud_provider: str  # 'aws', 'gcp', 'azure', 'local'
    cloud_config: Dict[str, Any]

@dataclass
class BackupMetadata:
    """Enhanced metadata for backup operations."""
    backup_id: str
    timestamp: str
    source_paths: List[str]
    file_count: int
    total_size: int
    compressed_size: int
    compression_ratio: float
    checksum: str
    encryption_key_id: Optional[str]
    deduplication_stats: Dict[str, Any]
    cloud_sync_status: str
    performance_metrics: Dict[str, Any]
    dependencies: List[str]
    tags: List[str]

class IntelligentBackupManager:
    """Next-generation backup manager with advanced features."""
    
    def __init__(self, config: BackupConfig):
        self.config = config
        self.backup_dir = Path(config.backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.backup_history: List[BackupMetadata] = []
        self.deduplication_cache: Dict[str, str] = {}
        self.encryption_key = None
        self.lock = threading.RLock()
        self._load_backup_history()
        self._initialize_encryption()
        self._initialize_cloud_client()
    
    def _load_backup_history(self):
        """Load backup history from file."""
        history_file = self.backup_dir / "backup_history.json"
        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    history_data = json.load(f)
                    self.backup_history = [BackupMetadata(**item) for item in history_data]
            except Exception as e:
                logger.error(f"Failed to load backup history: {e}")
                self.backup_history = []
    
    def _save_backup_history(self):
        """Save backup history to file."""
        history_file = self.backup_dir / "backup_history.json"
        try:
            with open(history_file, 'w') as f:
                history_data = [asdict(item) for item in self.backup_history]
                json.dump(history_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save backup history: {e}")
    
    def _initialize_encryption(self):
        """Initialize encryption if enabled."""
        if self.config.encryption_enabled:
            key_file = self.backup_dir / "encryption.key"
            if key_file.exists():
                with open(key_file, 'rb') as f:
                    self.encryption_key = f.read()
            else:
                self.encryption_key = Fernet.generate_key()
                with open(key_file, 'wb') as f:
                    f.write(self.encryption_key)
    
    def _initialize_cloud_client(self):
        """Initialize cloud storage client."""
        if self.config.cloud_sync_enabled:
            if self.config.cloud_provider == 'aws':
                self.cloud_client = boto3.client('s3')
            elif self.config.cloud_provider == 'gcp':
                self.cloud_client = storage.Client()
            elif self.config.cloud_provider == 'azure':
                self.cloud_client = azure.storage.blob.BlobServiceClient.from_connection_string(
                    self.config.cloud_config.get('connection_string')
                )
    
    def create_intelligent_backup(self, backup_name: str = None, 
                                tags: List[str] = None) -> Tuple[bool, BackupMetadata]:
        """Create an intelligent backup with advanced features."""
        if backup_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"intelligent_backup_{timestamp}"
        
        backup_path = self.backup_dir / f"{backup_name}.zip"
        start_time = time.time()
        
        with self.lock:
            try:
                # Analyze source files
                file_analysis = self._analyze_source_files()
                
                # Create backup with intelligent features
                backup_info = self._create_enhanced_backup_archive(backup_path, file_analysis)
                
                # Apply deduplication if enabled
                if self.config.deduplication_enabled:
                    backup_info = self._apply_deduplication(backup_path, backup_info)
                
                # Apply encryption if enabled
                if self.config.encryption_enabled:
                    backup_info = self._apply_encryption(backup_path)
                
                # Sync to cloud if enabled
                if self.config.cloud_sync_enabled:
                    backup_info = self._sync_to_cloud(backup_path, backup_info)
                
                # Create metadata
                backup_time = time.time() - start_time
                metadata = BackupMetadata(
                    backup_id=backup_name,
                    timestamp=datetime.now().isoformat(),
                    source_paths=self.config.source_paths,
                    file_count=backup_info['file_count'],
                    total_size=backup_info['total_size'],
                    compressed_size=backup_info['compressed_size'],
                    compression_ratio=backup_info['compression_ratio'],
                    checksum=backup_info['checksum'],
                    encryption_key_id=backup_info.get('encryption_key_id'),
                    deduplication_stats=backup_info.get('deduplication_stats', {}),
                    cloud_sync_status=backup_info.get('cloud_sync_status', 'not_synced'),
                    performance_metrics={
                        'backup_time': backup_time,
                        'files_per_second': backup_info['file_count'] / backup_time if backup_time > 0 else 0,
                        'compression_speed': backup_info['total_size'] / backup_time if backup_time > 0 else 0,
                        'memory_usage': psutil.Process().memory_info().rss / 1024 / 1024,
                        'cpu_usage': psutil.cpu_percent()
                    },
                    dependencies=backup_info.get('dependencies', []),
                    tags=tags or []
                )
                
                # Add to history
                self.backup_history.append(metadata)
                self._save_backup_history()
                
                # Cleanup old backups
                self._cleanup_old_backups()
                
                logger.info(f"Intelligent backup created successfully: {backup_path}")
                return True, metadata
                
            except Exception as e:
                logger.error(f"Intelligent backup creation failed: {e}")
                if backup_path.exists():
                    backup_path.unlink()
                return False, None
    
    def _analyze_source_files(self) -> Dict[str, Any]:
        """Analyze source files for intelligent backup optimization."""
        analysis = {
            'files': [],
            'total_size': 0,
            'file_types': {},
            'modification_patterns': {},
            'dependencies': []
        }
        
        for source_path in self.config.source_paths:
            source = Path(source_path)
            if source.exists():
                for file_path in source.rglob('*'):
                    if file_path.is_file():
                        file_info = {
                            'path': str(file_path),
                            'size': file_path.stat().st_size,
                            'modified': file_path.stat().st_mtime,
                            'extension': file_path.suffix.lower()
                        }
                        analysis['files'].append(file_info)
                        analysis['total_size'] += file_info['size']
                        
                        # Track file types
                        ext = file_info['extension']
                        analysis['file_types'][ext] = analysis['file_types'].get(ext, 0) + 1
                        
                        # Track modification patterns
                        mod_date = datetime.fromtimestamp(file_info['modified']).date()
                        analysis['modification_patterns'][str(mod_date)] = analysis['modification_patterns'].get(str(mod_date), 0) + 1
        
        return analysis
    
    def _create_enhanced_backup_archive(self, backup_path: Path, 
                                      file_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Create enhanced backup archive with optimization."""
        backup_info = {
            'file_count': 0,
            'total_size': 0,
            'compressed_size': 0,
            'compression_ratio': 0,
            'checksum': '',
            'dependencies': []
        }
        
        with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED, 
                           compresslevel=self.config.compression_level) as zipf:
            
            for file_info in file_analysis['files']:
                file_path = Path(file_info['path'])
                arc_name = file_path.name
                
                # Add file to archive
                zipf.write(file_path, arc_name)
                backup_info['file_count'] += 1
                backup_info['total_size'] += file_info['size']
                
                # Track dependencies for database files
                if file_path.suffix.lower() == '.db':
                    backup_info['dependencies'].append(str(file_path))
            
            # Add metadata
            metadata = {
                'backup_info': backup_info,
                'file_analysis': file_analysis,
                'config': asdict(self.config)
            }
            zipf.writestr('enhanced_metadata.json', json.dumps(metadata, indent=2))
        
        # Calculate compression ratio
        backup_info['compressed_size'] = backup_path.stat().st_size
        if backup_info['total_size'] > 0:
            backup_info['compression_ratio'] = 1 - (backup_info['compressed_size'] / backup_info['total_size'])
        
        # Calculate checksum
        backup_info['checksum'] = self._calculate_checksum(backup_path)
        
        return backup_info
    
    def _apply_deduplication(self, backup_path: Path, backup_info: Dict[str, Any]) -> Dict[str, Any]:
        """Apply deduplication to backup."""
        dedup_stats = {
            'original_files': backup_info['file_count'],
            'deduplicated_files': 0,
            'space_saved': 0
        }
        
        # Simple deduplication based on file content hash
        with zipfile.ZipFile(backup_path, 'r') as zipf:
            temp_dir = tempfile.mkdtemp()
            try:
                zipf.extractall(temp_dir)
                
                # Create new archive with deduplication
                dedup_backup_path = backup_path.with_suffix('.dedup.zip')
                with zipfile.ZipFile(dedup_backup_path, 'w', zipfile.ZIP_DEFLATED) as new_zipf:
                    file_hashes = {}
                    
                    for file_path in Path(temp_dir).rglob('*'):
                        if file_path.is_file():
                            file_hash = self._calculate_file_hash(file_path)
                            
                            if file_hash in file_hashes:
                                # Create symbolic link to existing file
                                new_zipf.writestr(f"dedup_link_{len(file_hashes)}.link", file_hash)
                                dedup_stats['space_saved'] += file_path.stat().st_size
                            else:
                                file_hashes[file_hash] = file_path
                                new_zipf.write(file_path, file_path.name)
                                dedup_stats['deduplicated_files'] += 1
                
                # Replace original with deduplicated version
                backup_path.unlink()
                dedup_backup_path.rename(backup_path)
                
            finally:
                shutil.rmtree(temp_dir)
        
        backup_info['deduplication_stats'] = dedup_stats
        return backup_info
    
    def _apply_encryption(self, backup_path: Path) -> Dict[str, Any]:
        """Apply encryption to backup."""
        if not self.encryption_key:
            return {}
        
        fernet = Fernet(self.encryption_key)
        
        # Read backup file
        with open(backup_path, 'rb') as f:
            data = f.read()
        
        # Encrypt data
        encrypted_data = fernet.encrypt(data)
        
        # Write encrypted backup
        encrypted_backup_path = backup_path.with_suffix('.encrypted.zip')
        with open(encrypted_backup_path, 'wb') as f:
            f.write(encrypted_data)
        
        # Replace original with encrypted version
        backup_path.unlink()
        encrypted_backup_path.rename(backup_path)
        
        return {'encryption_key_id': hashlib.sha256(self.encryption_key).hexdigest()[:16]}
    
    def _sync_to_cloud(self, backup_path: Path, backup_info: Dict[str, Any]) -> Dict[str, Any]:
        """Sync backup to cloud storage."""
        try:
            if self.config.cloud_provider == 'aws':
                bucket_name = self.config.cloud_config.get('bucket_name')
                key = f"backups/{backup_path.name}"
                self.cloud_client.upload_file(str(backup_path), bucket_name, key)
                backup_info['cloud_sync_status'] = 'synced_to_aws'
                
            elif self.config.cloud_provider == 'gcp':
                bucket_name = self.config.cloud_config.get('bucket_name')
                bucket = self.cloud_client.bucket(bucket_name)
                blob = bucket.blob(f"backups/{backup_path.name}")
                blob.upload_from_filename(str(backup_path))
                backup_info['cloud_sync_status'] = 'synced_to_gcp'
                
            elif self.config.cloud_provider == 'azure':
                container_name = self.config.cloud_config.get('container_name')
                blob_client = self.cloud_client.get_blob_client(
                    container=container_name, 
                    blob=f"backups/{backup_path.name}"
                )
                with open(backup_path, 'rb') as f:
                    blob_client.upload_blob(f)
                backup_info['cloud_sync_status'] = 'synced_to_azure'
                
        except Exception as e:
            logger.error(f"Cloud sync failed: {e}")
            backup_info['cloud_sync_status'] = 'sync_failed'
        
        return backup_info
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate hash of file content."""
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def _cleanup_old_backups(self):
        """Clean up old backups based on retention policy."""
        cutoff_date = datetime.now() - timedelta(days=self.config.retention_days)
        
        backups_to_remove = []
        for backup_file in self.backup_dir.glob("*.zip"):
            if backup_file.stat().st_mtime < cutoff_date.timestamp():
                backups_to_remove.append(backup_file)
        
        for backup_file in backups_to_remove:
            try:
                backup_file.unlink()
                logger.info(f"Removed old backup: {backup_file}")
            except Exception as e:
                logger.error(f"Failed to remove old backup {backup_file}: {e}")
    
    def schedule_backup(self, callback: Callable = None):
        """Schedule automated backups."""
        if not self.config.schedule_enabled:
            return
        
        if self.config.schedule_interval == 'hourly':
            schedule.every().hour.do(self._scheduled_backup, callback)
        elif self.config.schedule_interval == 'daily':
            schedule.every().day.at("02:00").do(self._scheduled_backup, callback)
        elif self.config.schedule_interval == 'weekly':
            schedule.every().sunday.at("02:00").do(self._scheduled_backup, callback)
        
        logger.info(f"Backup scheduled: {self.config.schedule_interval}")
    
    def _scheduled_backup(self, callback: Callable = None):
        """Execute scheduled backup."""
        logger.info("Executing scheduled backup")
        success, metadata = self.create_intelligent_backup()
        
        if callback:
            callback(success, metadata)
    
    def get_backup_statistics(self) -> Dict[str, Any]:
        """Get comprehensive backup statistics."""
        if not self.backup_history:
            return {}
        
        total_backups = len(self.backup_history)
        total_size = sum(b.total_size for b in self.backup_history)
        total_compressed_size = sum(b.compressed_size for b in self.backup_history)
        
        # Calculate average compression ratio
        avg_compression = sum(b.compression_ratio for b in self.backup_history) / total_backups
        
        # Calculate backup frequency
        if total_backups > 1:
            first_backup = datetime.fromisoformat(self.backup_history[0].timestamp)
            last_backup = datetime.fromisoformat(self.backup_history[-1].timestamp)
            backup_frequency = (last_backup - first_backup).total_seconds() / (total_backups - 1)
        else:
            backup_frequency = 0
        
        return {
            'total_backups': total_backups,
            'total_size': total_size,
            'total_compressed_size': total_compressed_size,
            'average_compression_ratio': avg_compression,
            'space_saved': total_size - total_compressed_size,
            'backup_frequency_seconds': backup_frequency,
            'cloud_sync_success_rate': self._calculate_cloud_sync_success_rate(),
            'encryption_enabled': self.config.encryption_enabled,
            'deduplication_enabled': self.config.deduplication_enabled
        }
    
    def _calculate_cloud_sync_success_rate(self) -> float:
        """Calculate cloud sync success rate."""
        if not self.backup_history:
            return 0.0
        
        successful_syncs = sum(1 for b in self.backup_history 
                             if b.cloud_sync_status.startswith('synced'))
        return successful_syncs / len(self.backup_history)
    
    def list_backups(self, include_cloud: bool = True) -> List[Dict[str, Any]]:
        """List all backups with enhanced information."""
        backups = []
        
        for metadata in self.backup_history:
            backup_info = {
                'id': metadata.backup_id,
                'timestamp': metadata.timestamp,
                'file_count': metadata.file_count,
                'total_size': metadata.total_size,
                'compressed_size': metadata.compressed_size,
                'compression_ratio': metadata.compression_ratio,
                'cloud_sync_status': metadata.cloud_sync_status,
                'tags': metadata.tags,
                'performance_metrics': metadata.performance_metrics
            }
            
            if include_cloud and self.config.cloud_sync_enabled:
                backup_info['cloud_info'] = self._get_cloud_backup_info(metadata.backup_id)
            
            backups.append(backup_info)
        
        return sorted(backups, key=lambda x: x['timestamp'], reverse=True)
    
    def _get_cloud_backup_info(self, backup_id: str) -> Dict[str, Any]:
        """Get cloud backup information."""
        try:
            if self.config.cloud_provider == 'aws':
                bucket_name = self.config.cloud_config.get('bucket_name')
                key = f"backups/{backup_id}.zip"
                response = self.cloud_client.head_object(Bucket=bucket_name, Key=key)
                return {
                    'size': response['ContentLength'],
                    'last_modified': response['LastModified'].isoformat(),
                    'storage_class': response.get('StorageClass', 'STANDARD')
                }
            # Add similar logic for GCP and Azure
        except Exception as e:
            logger.error(f"Failed to get cloud backup info: {e}")
        
        return {}

def create_backup_manager(config: BackupConfig) -> IntelligentBackupManager:
    """Create and return an intelligent backup manager instance."""
    return IntelligentBackupManager(config)
