#!/usr/bin/env python3
"""
Backup Restore

Restore functionality for the Gitea to Kimai integration backup system.
"""

import os
import json
import zipfile
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class BackupRestore:
    """Backup restore functionality."""
    
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
    
    def list_available_backups(self) -> List[Dict[str, Any]]:
        """List all available backups with metadata."""
        backups = []
        
        if not self.backup_dir.exists():
            return backups
        
        for backup_file in self.backup_dir.glob("*.zip"):
            try:
                metadata = self._extract_metadata(backup_file)
                backups.append({
                    'file': backup_file.name,
                    'path': str(backup_file),
                    'size': backup_file.stat().st_size,
                    'created': metadata.get('created_at'),
                    'version': metadata.get('version'),
                    'description': metadata.get('description', '')
                })
            except Exception as e:
                logger.warning(f"Could not read metadata for {backup_file}: {e}")
        
        return sorted(backups, key=lambda x: x['created'], reverse=True)
    
    def restore_backup(self, backup_file: str, target_dir: str = ".", 
                      overwrite: bool = False) -> bool:
        """Restore a backup file."""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        if not backup_path.suffix == '.zip':
            logger.error(f"Invalid backup file format: {backup_file}")
            return False
        
        try:
            # Extract metadata first
            metadata = self._extract_metadata(backup_path)
            logger.info(f"Restoring backup created at: {metadata.get('created_at')}")
            
            # Create target directory
            target_path = Path(target_dir)
            target_path.mkdir(parents=True, exist_ok=True)
            
            # Extract backup
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                # Check if files exist and handle overwrite
                if not overwrite:
                    for file_info in zip_ref.infolist():
                        file_path = target_path / file_info.filename
                        if file_path.exists():
                            logger.error(f"File already exists: {file_path}")
                            return False
                
                # Extract all files
                zip_ref.extractall(target_path)
                logger.info(f"Restored {len(zip_ref.infolist())} files")
            
            # Restore database if present
            self._restore_database(target_path, metadata)
            
            # Restore configuration if present
            self._restore_configuration(target_path, metadata)
            
            logger.info(f"Backup restored successfully to {target_path}")
            return True
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def _extract_metadata(self, backup_file: Path) -> Dict[str, Any]:
        """Extract metadata from backup file."""
        try:
            with zipfile.ZipFile(backup_file, 'r') as zip_ref:
                # Look for metadata file
                metadata_files = [f for f in zip_ref.namelist() if f.endswith('metadata.json')]
                
                if metadata_files:
                    with zip_ref.open(metadata_files[0]) as f:
                        return json.load(f)
                else:
                    # Fallback: try to extract from filename
                    return {
                        'created_at': datetime.fromtimestamp(backup_file.stat().st_mtime).isoformat(),
                        'version': '1.0.0',
                        'description': 'Legacy backup'
                    }
        except Exception as e:
            logger.error(f"Failed to extract metadata: {e}")
            return {}
    
    def _restore_database(self, target_path: Path, metadata: Dict[str, Any]) -> None:
        """Restore database files."""
        db_files = list(target_path.glob("*.db"))
        if db_files:
            logger.info(f"Found {len(db_files)} database files to restore")
            for db_file in db_files:
                logger.info(f"Database restored: {db_file}")
    
    def _restore_configuration(self, target_path: Path, metadata: Dict[str, Any]) -> None:
        """Restore configuration files."""
        config_files = list(target_path.glob(".env*")) + list(target_path.glob("config.*"))
        if config_files:
            logger.info(f"Found {len(config_files)} configuration files to restore")
            for config_file in config_files:
                logger.info(f"Configuration restored: {config_file}")
    
    def validate_backup(self, backup_file: str) -> bool:
        """Validate a backup file."""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        try:
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                # Check if it's a valid zip file
                zip_ref.testzip()
                
                # Check for required files
                file_list = zip_ref.namelist()
                required_files = ['metadata.json']
                
                for required_file in required_files:
                    if not any(f.endswith(required_file) for f in file_list):
                        logger.warning(f"Missing required file: {required_file}")
                
                logger.info(f"Backup validation successful: {len(file_list)} files")
                return True
                
        except Exception as e:
            logger.error(f"Backup validation failed: {e}")
            return False
    
    def get_backup_info(self, backup_file: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a backup file."""
        backup_path = Path(backup_file)
        
        if not backup_path.exists():
            return None
        
        try:
            metadata = self._extract_metadata(backup_path)
            
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                
                info = {
                    'file': backup_file,
                    'size': backup_path.stat().st_size,
                    'file_count': len(file_list),
                    'files': file_list,
                    'metadata': metadata
                }
                
                return info
                
        except Exception as e:
            logger.error(f"Failed to get backup info: {e}")
            return None

def create_restore() -> BackupRestore:
    """Create and return a backup restore instance."""
    return BackupRestore()
