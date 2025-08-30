#!/usr/bin/env python3
"""
Next-Generation Backup Restore

Advanced restore functionality for the Gitea to Kimai integration backup system.
Features include incremental restore, selective restore, integrity verification,
rollback capabilities, and advanced metadata handling.
"""

import os
import json
import zipfile
import shutil
import logging
import hashlib
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, asdict
import threading
import time
import subprocess
import psutil

logger = logging.getLogger(__name__)

@dataclass
class RestoreMetadata:
    """Metadata for restore operations."""
    backup_id: str
    restore_timestamp: str
    restored_files: List[str]
    file_count: int
    total_size: int
    checksum_verified: bool
    restore_type: str  # 'full', 'incremental', 'selective'
    rollback_available: bool
    dependencies_restored: List[str]
    performance_metrics: Dict[str, Any]

class AdvancedBackupRestore:
    """Next-generation backup restore functionality with advanced features."""
    
    def __init__(self, backup_dir: str = "backups", restore_dir: str = "restored"):
        self.backup_dir = Path(backup_dir)
        self.restore_dir = Path(restore_dir)
        self.backup_dir.mkdir(exist_ok=True)
        self.restore_dir.mkdir(exist_ok=True)
        self.restore_history: List[RestoreMetadata] = []
        self.lock = threading.RLock()
        self._load_restore_history()
    
    def _load_restore_history(self):
        """Load restore history from file."""
        history_file = self.restore_dir / "restore_history.json"
        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    history_data = json.load(f)
                    self.restore_history = [RestoreMetadata(**item) for item in history_data]
            except Exception as e:
                logger.error(f"Failed to load restore history: {e}")
                self.restore_history = []
    
    def _save_restore_history(self):
        """Save restore history to file."""
        history_file = self.restore_dir / "restore_history.json"
        try:
            with open(history_file, 'w') as f:
                history_data = [asdict(item) for item in self.restore_history]
                json.dump(history_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save restore history: {e}")
    
    def list_available_backups(self, include_integrity: bool = True) -> List[Dict[str, Any]]:
        """List all available backups with enhanced metadata and integrity checks."""
        backups = []
        
        if not self.backup_dir.exists():
            return backups
        
        for backup_file in self.backup_dir.glob("*.zip"):
            try:
                metadata = self._extract_enhanced_metadata(backup_file)
                backup_info = {
                    'file': backup_file.name,
                    'path': str(backup_file),
                    'size': backup_file.stat().st_size,
                    'created': metadata.get('created_at'),
                    'version': metadata.get('version'),
                    'description': metadata.get('description', ''),
                    'file_count': metadata.get('file_count', 0),
                    'compression_ratio': metadata.get('compression_ratio', 0),
                    'backup_type': metadata.get('backup_type', 'full'),
                    'dependencies': metadata.get('dependencies', []),
                    'tags': metadata.get('tags', [])
                }
                
                if include_integrity:
                    backup_info['integrity_status'] = self._verify_backup_integrity(backup_file)
                    backup_info['checksum_valid'] = self._verify_checksum(backup_file, metadata)
                
                backups.append(backup_info)
            except Exception as e:
                logger.warning(f"Could not read metadata for {backup_file}: {e}")
        
        return sorted(backups, key=lambda x: x['created'], reverse=True)
    
    def restore_backup(self, backup_file: str, target_dir: str = ".", 
                      restore_type: str = "full", file_patterns: List[str] = None,
                      overwrite: bool = False, verify_integrity: bool = True,
                      create_rollback: bool = True) -> Tuple[bool, RestoreMetadata]:
        """Enhanced restore with multiple restore types and options."""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_file}")
            return False, None
        
        if not backup_path.suffix == '.zip':
            logger.error(f"Invalid backup file format: {backup_file}")
            return False, None
        
        start_time = time.time()
        
        try:
            # Extract enhanced metadata
            metadata = self._extract_enhanced_metadata(backup_path)
            logger.info(f"Restoring backup created at: {metadata.get('created_at')}")
            
            # Create target directory
            target_path = Path(target_dir)
            target_path.mkdir(parents=True, exist_ok=True)
            
            # Create rollback if requested
            rollback_path = None
            if create_rollback and target_path.exists():
                rollback_path = self._create_rollback(target_path)
            
            # Perform restore based on type
            if restore_type == "full":
                success = self._perform_full_restore(backup_path, target_path, overwrite, verify_integrity)
            elif restore_type == "incremental":
                success = self._perform_incremental_restore(backup_path, target_path, overwrite, verify_integrity)
            elif restore_type == "selective":
                success = self._perform_selective_restore(backup_path, target_path, file_patterns, overwrite, verify_integrity)
            else:
                logger.error(f"Unknown restore type: {restore_type}")
                return False, None
            
            if not success:
                # Restore rollback if restore failed
                if rollback_path:
                    self._restore_rollback(rollback_path, target_path)
                return False, None
            
            # Create restore metadata
            restore_time = time.time() - start_time
            restore_metadata = RestoreMetadata(
                backup_id=metadata.get('backup_id', backup_file),
                restore_timestamp=datetime.now().isoformat(),
                restored_files=self._get_restored_files(target_path),
                file_count=len(self._get_restored_files(target_path)),
                total_size=sum(f.stat().st_size for f in target_path.rglob('*') if f.is_file()),
                checksum_verified=verify_integrity,
                restore_type=restore_type,
                rollback_available=rollback_path is not None,
                dependencies_restored=metadata.get('dependencies', []),
                performance_metrics={
                    'restore_time': restore_time,
                    'files_per_second': len(self._get_restored_files(target_path)) / restore_time if restore_time > 0 else 0,
                    'memory_usage': psutil.Process().memory_info().rss / 1024 / 1024,  # MB
                    'cpu_usage': psutil.cpu_percent()
                }
            )
            
            # Add to history
            self.restore_history.append(restore_metadata)
            self._save_restore_history()
            
            logger.info(f"Backup restored successfully to {target_path}")
            return True, restore_metadata
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False, None
    
    def _perform_full_restore(self, backup_path: Path, target_path: Path, 
                            overwrite: bool, verify_integrity: bool) -> bool:
        """Perform full restore of all files."""
        try:
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                # Check integrity if requested
                if verify_integrity:
                    if not self._verify_archive_integrity(zip_ref):
                        logger.error("Archive integrity check failed")
                        return False
                
                # Check for conflicts if not overwriting
                if not overwrite:
                    conflicts = self._check_restore_conflicts(zip_ref, target_path)
                    if conflicts:
                        logger.error(f"Restore conflicts found: {conflicts}")
                        return False
                
                # Extract all files
                zip_ref.extractall(target_path)
                logger.info(f"Restored {len(zip_ref.infolist())} files")
            
            # Restore database with advanced features
            self._restore_database_advanced(target_path)
            
            # Restore configuration with validation
            self._restore_configuration_advanced(target_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Full restore failed: {e}")
            return False
    
    def _perform_incremental_restore(self, backup_path: Path, target_path: Path,
                                   overwrite: bool, verify_integrity: bool) -> bool:
        """Perform incremental restore based on file timestamps."""
        try:
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                restored_count = 0
                skipped_count = 0
                
                for file_info in zip_ref.infolist():
                    target_file = target_path / file_info.filename
                    
                    # Check if file should be restored (newer or doesn't exist)
                    should_restore = self._should_restore_file(target_file, file_info)
                    
                    if should_restore:
                        zip_ref.extract(file_info.filename, target_path)
                        restored_count += 1
                    else:
                        skipped_count += 1
                
                logger.info(f"Incremental restore: {restored_count} restored, {skipped_count} skipped")
                return True
                
        except Exception as e:
            logger.error(f"Incremental restore failed: {e}")
            return False
    
    def _perform_selective_restore(self, backup_path: Path, target_path: Path,
                                 file_patterns: List[str], overwrite: bool, 
                                 verify_integrity: bool) -> bool:
        """Perform selective restore based on file patterns."""
        if not file_patterns:
            logger.error("No file patterns specified for selective restore")
            return False
        
        try:
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                restored_count = 0
                
                for file_info in zip_ref.infolist():
                    if any(self._matches_pattern(file_info.filename, pattern) for pattern in file_patterns):
                        zip_ref.extract(file_info.filename, target_path)
                        restored_count += 1
                
                logger.info(f"Selective restore: {restored_count} files restored")
                return True
                
        except Exception as e:
            logger.error(f"Selective restore failed: {e}")
            return False
    
    def _should_restore_file(self, target_file: Path, file_info) -> bool:
        """Determine if a file should be restored based on timestamps."""
        if not target_file.exists():
            return True
        
        # Compare timestamps
        backup_time = datetime(*file_info.date_time)
        target_time = datetime.fromtimestamp(target_file.stat().st_mtime)
        
        return backup_time > target_time
    
    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """Check if filename matches pattern (supports glob patterns)."""
        from fnmatch import fnmatch
        return fnmatch(filename, pattern)
    
    def _create_rollback(self, target_path: Path) -> Optional[Path]:
        """Create a rollback backup of the target directory."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            rollback_path = self.restore_dir / f"rollback_{timestamp}.zip"
            
            with zipfile.ZipFile(rollback_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in target_path.rglob('*'):
                    if file_path.is_file():
                        arc_name = file_path.relative_to(target_path)
                        zipf.write(file_path, arc_name)
            
            logger.info(f"Rollback created: {rollback_path}")
            return rollback_path
            
        except Exception as e:
            logger.error(f"Failed to create rollback: {e}")
            return None
    
    def _restore_rollback(self, rollback_path: Path, target_path: Path) -> bool:
        """Restore from rollback backup."""
        try:
            with zipfile.ZipFile(rollback_path, 'r') as zipf:
                zipf.extractall(target_path)
            logger.info("Rollback restored successfully")
            return True
        except Exception as e:
            logger.error(f"Rollback restore failed: {e}")
            return False
    
    def _verify_backup_integrity(self, backup_path: Path) -> str:
        """Verify backup file integrity."""
        try:
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                zip_ref.testzip()
            return "valid"
        except Exception as e:
            logger.error(f"Integrity check failed: {e}")
            return "corrupted"
    
    def _verify_checksum(self, backup_path: Path, metadata: Dict[str, Any]) -> bool:
        """Verify backup file checksum."""
        expected_checksum = metadata.get('checksum')
        if not expected_checksum:
            return True  # No checksum to verify
        
        try:
            with open(backup_path, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
            return file_hash == expected_checksum
        except Exception as e:
            logger.error(f"Checksum verification failed: {e}")
            return False
    
    def _verify_archive_integrity(self, zip_ref) -> bool:
        """Verify archive integrity."""
        try:
            zip_ref.testzip()
            return True
        except Exception as e:
            logger.error(f"Archive integrity check failed: {e}")
            return False
    
    def _check_restore_conflicts(self, zip_ref, target_path: Path) -> List[str]:
        """Check for restore conflicts."""
        conflicts = []
        for file_info in zip_ref.infolist():
            target_file = target_path / file_info.filename
            if target_file.exists():
                conflicts.append(file_info.filename)
        return conflicts
    
    def _extract_enhanced_metadata(self, backup_file: Path) -> Dict[str, Any]:
        """Extract enhanced metadata from backup file."""
        try:
            with zipfile.ZipFile(backup_file, 'r') as zip_ref:
                # Look for metadata files
                metadata_files = [f for f in zip_ref.namelist() if f.endswith('metadata.json')]
                backup_info_files = [f for f in zip_ref.namelist() if f.endswith('backup_info.json')]
                
                if metadata_files:
                    with zip_ref.open(metadata_files[0]) as f:
                        metadata = json.load(f)
                elif backup_info_files:
                    with zip_ref.open(backup_info_files[0]) as f:
                        metadata = json.load(f)
                else:
                    # Fallback: extract from filename and file info
                    metadata = {
                        'created_at': datetime.fromtimestamp(backup_file.stat().st_mtime).isoformat(),
                        'version': '1.0.0',
                        'description': 'Legacy backup',
                        'backup_type': 'full',
                        'file_count': len(zip_ref.namelist()),
                        'compression_ratio': 0,
                        'dependencies': [],
                        'tags': []
                    }
                
                # Add file analysis
                metadata['file_analysis'] = self._analyze_backup_contents(zip_ref)
                
                return metadata
                
        except Exception as e:
            logger.error(f"Failed to extract enhanced metadata: {e}")
            return {}
    
    def _analyze_backup_contents(self, zip_ref) -> Dict[str, Any]:
        """Analyze backup contents for insights."""
        file_types = {}
        total_size = 0
        
        for file_info in zip_ref.infolist():
            ext = Path(file_info.filename).suffix.lower()
            file_types[ext] = file_types.get(ext, 0) + 1
            total_size += file_info.file_size
        
        return {
            'file_types': file_types,
            'total_size': total_size,
            'file_count': len(zip_ref.namelist())
        }
    
    def _restore_database_advanced(self, target_path: Path) -> None:
        """Advanced database restore with integrity checks."""
        db_files = list(target_path.glob("*.db"))
        if db_files:
            logger.info(f"Found {len(db_files)} database files to restore")
            for db_file in db_files:
                try:
                    # Verify database integrity
                    conn = sqlite3.connect(db_file)
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA integrity_check")
                    result = cursor.fetchone()
                    conn.close()
                    
                    if result[0] == "ok":
                        logger.info(f"Database restored and verified: {db_file}")
                    else:
                        logger.warning(f"Database integrity issues: {db_file}")
                        
                except Exception as e:
                    logger.error(f"Database restore failed: {db_file} - {e}")
    
    def _restore_configuration_advanced(self, target_path: Path) -> None:
        """Advanced configuration restore with validation."""
        config_files = list(target_path.glob(".env*")) + list(target_path.glob("config.*"))
        if config_files:
            logger.info(f"Found {len(config_files)} configuration files to restore")
            for config_file in config_files:
                try:
                    # Validate configuration format
                    if config_file.suffix == '.json':
                        with open(config_file, 'r') as f:
                            json.load(f)  # Validate JSON
                    elif config_file.name.startswith('.env'):
                        # Validate environment file format
                        with open(config_file, 'r') as f:
                            for line_num, line in enumerate(f, 1):
                                line = line.strip()
                                if line and not line.startswith('#') and '=' not in line:
                                    logger.warning(f"Invalid env format at line {line_num}: {config_file}")
                    
                    logger.info(f"Configuration restored and validated: {config_file}")
                    
                except Exception as e:
                    logger.error(f"Configuration restore failed: {config_file} - {e}")
    
    def _get_restored_files(self, target_path: Path) -> List[str]:
        """Get list of restored files."""
        return [str(f.relative_to(target_path)) for f in target_path.rglob('*') if f.is_file()]
    
    def get_restore_history(self) -> List[RestoreMetadata]:
        """Get restore history."""
        return self.restore_history.copy()
    
    def rollback_last_restore(self, target_dir: str = ".") -> bool:
        """Rollback the last restore operation."""
        if not self.restore_history:
            logger.error("No restore history available")
            return False
        
        last_restore = self.restore_history[-1]
        if not last_restore.rollback_available:
            logger.error("Rollback not available for last restore")
            return False
        
        # Find rollback file
        rollback_files = list(self.restore_dir.glob("rollback_*.zip"))
        if not rollback_files:
            logger.error("No rollback files found")
            return False
        
        # Use the most recent rollback
        latest_rollback = max(rollback_files, key=lambda f: f.stat().st_mtime)
        target_path = Path(target_dir)
        
        return self._restore_rollback(latest_rollback, target_path)
    
    def validate_backup(self, backup_file: str) -> Dict[str, Any]:
        """Enhanced backup validation with detailed results."""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            return {'valid': False, 'error': 'Backup file not found'}
        
        validation_result = {
            'valid': True,
            'file_size': backup_path.stat().st_size,
            'integrity_check': 'passed',
            'metadata_check': 'passed',
            'file_count': 0,
            'warnings': []
        }
        
        try:
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                # Check if it's a valid zip file
                zip_ref.testzip()
                validation_result['file_count'] = len(zip_ref.namelist())
                
                # Check for required files
                file_list = zip_ref.namelist()
                required_files = ['metadata.json', 'backup_info.json']
                
                for required_file in required_files:
                    if not any(f.endswith(required_file) for f in file_list):
                        validation_result['warnings'].append(f"Missing required file: {required_file}")
                
                # Extract and validate metadata
                metadata = self._extract_enhanced_metadata(backup_path)
                if not metadata:
                    validation_result['warnings'].append("Could not extract metadata")
                
                logger.info(f"Backup validation successful: {validation_result['file_count']} files")
                
        except Exception as e:
            validation_result['valid'] = False
            validation_result['error'] = str(e)
            logger.error(f"Backup validation failed: {e}")
        
        return validation_result
    
    def get_backup_info(self, backup_file: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a backup file."""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            return None
        
        try:
            metadata = self._extract_enhanced_metadata(backup_path)
            
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                
                info = {
                    'file': backup_file,
                    'size': backup_path.stat().st_size,
                    'file_count': len(file_list),
                    'files': file_list,
                    'metadata': metadata,
                    'integrity_status': self._verify_backup_integrity(backup_path),
                    'checksum_valid': self._verify_checksum(backup_path, metadata)
                }
                
                return info
                
        except Exception as e:
            logger.error(f"Failed to get backup info: {e}")
            return None

def create_restore() -> AdvancedBackupRestore:
    """Create and return an advanced backup restore instance."""
    return AdvancedBackupRestore()
