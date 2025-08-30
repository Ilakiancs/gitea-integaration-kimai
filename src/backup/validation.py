#!/usr/bin/env python3
"""
Backup Validation Module

Comprehensive validation system for backup integrity checks,
data validation, and consistency verification.
"""

import os
import hashlib
import logging
import sqlite3
import json
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import threading
import time

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of validation operation."""
    valid: bool
    errors: List[str]
    warnings: List[str]
    checks_performed: List[str]
    validation_time: float
    file_count: int
    total_size: int

class BackupValidator:
    """Comprehensive backup validation system."""
    
    def __init__(self):
        self.validation_checks = {
            'integrity': self._check_integrity,
            'metadata': self._check_metadata,
            'database': self._check_database,
            'configuration': self._check_configuration,
            'file_structure': self._check_file_structure,
            'checksum': self._check_checksum
        }
        self.lock = threading.RLock()
    
    def validate_backup(self, backup_path: Path, checks: List[str] = None) -> ValidationResult:
        """Validate a backup file with specified checks."""
        if checks is None:
            checks = list(self.validation_checks.keys())
        
        start_time = time.time()
        errors = []
        warnings = []
        checks_performed = []
        
        try:
            # Basic file validation
            if not backup_path.exists():
                return ValidationResult(
                    valid=False,
                    errors=["Backup file does not exist"],
                    warnings=[],
                    checks_performed=[],
                    validation_time=0,
                    file_count=0,
                    total_size=0
                )
            
            # Perform requested checks
            for check_name in checks:
                if check_name in self.validation_checks:
                    try:
                        check_result = self.validation_checks[check_name](backup_path)
                        if check_result['valid']:
                            checks_performed.append(check_name)
                        else:
                            errors.extend(check_result.get('errors', []))
                        warnings.extend(check_result.get('warnings', []))
                    except Exception as e:
                        errors.append(f"Check '{check_name}' failed: {e}")
            
            validation_time = time.time() - start_time
            file_count, total_size = self._get_backup_stats(backup_path)
            
            result = ValidationResult(
                valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                checks_performed=checks_performed,
                validation_time=validation_time,
                file_count=file_count,
                total_size=total_size
            )
            
            logger.info(f"Backup validation completed: {len(errors)} errors, {len(warnings)} warnings")
            return result
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return ValidationResult(
                valid=False,
                errors=[f"Validation failed: {e}"],
                warnings=[],
                checks_performed=[],
                validation_time=time.time() - start_time,
                file_count=0,
                total_size=0
            )
    
    def _check_integrity(self, backup_path: Path) -> Dict[str, Any]:
        """Check backup file integrity."""
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        try:
            if backup_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                    # Test zip integrity
                    bad_file = zip_ref.testzip()
                    if bad_file:
                        result['valid'] = False
                        result['errors'].append(f"Corrupted file in archive: {bad_file}")
                    
                    # Check for empty archive
                    if len(zip_ref.infolist()) == 0:
                        result['warnings'].append("Backup archive is empty")
            
            else:
                result['warnings'].append(f"Unknown backup format: {backup_path.suffix}")
                
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Integrity check failed: {e}")
        
        return result
    
    def _check_metadata(self, backup_path: Path) -> Dict[str, Any]:
        """Check backup metadata."""
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        try:
            if backup_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                    # Look for metadata files
                    metadata_files = [f for f in zip_ref.namelist() if f.endswith('metadata.json')]
                    backup_info_files = [f for f in zip_ref.namelist() if f.endswith('backup_info.json')]
                    
                    if not metadata_files and not backup_info_files:
                        result['warnings'].append("No metadata files found")
                    else:
                        # Validate metadata content
                        for metadata_file in metadata_files + backup_info_files:
                            try:
                                with zip_ref.open(metadata_file) as f:
                                    metadata = json.load(f)
                                
                                # Check required fields
                                required_fields = ['created_at', 'version']
                                for field in required_fields:
                                    if field not in metadata:
                                        result['warnings'].append(f"Missing required field: {field}")
                                
                            except json.JSONDecodeError:
                                result['errors'].append(f"Invalid JSON in metadata file: {metadata_file}")
                            except Exception as e:
                                result['warnings'].append(f"Could not read metadata file {metadata_file}: {e}")
                                
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Metadata check failed: {e}")
        
        return result
    
    def _check_database(self, backup_path: Path) -> Dict[str, Any]:
        """Check database files in backup."""
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        try:
            if backup_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                    db_files = [f for f in zip_ref.namelist() if f.endswith('.db')]
                    
                    for db_file in db_files:
                        try:
                            # Extract and validate database
                            with zip_ref.open(db_file) as f:
                                # Create temporary file for validation
                                temp_db = Path(f"temp_{db_file}")
                                with open(temp_db, 'wb') as temp_f:
                                    temp_f.write(f.read())
                                
                                # Validate SQLite database
                                conn = sqlite3.connect(temp_db)
                                cursor = conn.cursor()
                                
                                # Check database integrity
                                cursor.execute("PRAGMA integrity_check")
                                integrity_result = cursor.fetchone()
                                
                                if integrity_result[0] != "ok":
                                    result['errors'].append(f"Database integrity issues in {db_file}: {integrity_result[0]}")
                                
                                # Check for tables
                                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                                tables = cursor.fetchall()
                                
                                if not tables:
                                    result['warnings'].append(f"Database {db_file} has no tables")
                                
                                conn.close()
                                temp_db.unlink()  # Clean up
                                
                        except Exception as e:
                            result['warnings'].append(f"Could not validate database {db_file}: {e}")
                            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Database check failed: {e}")
        
        return result
    
    def _check_configuration(self, backup_path: Path) -> Dict[str, Any]:
        """Check configuration files in backup."""
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        try:
            if backup_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                    config_files = [f for f in zip_ref.namelist() 
                                  if f.endswith('.json') or f.endswith('.env') or f.startswith('config.')]
                    
                    for config_file in config_files:
                        try:
                            with zip_ref.open(config_file) as f:
                                content = f.read().decode('utf-8')
                                
                                if config_file.endswith('.json'):
                                    # Validate JSON
                                    json.loads(content)
                                elif config_file.endswith('.env'):
                                    # Validate environment file format
                                    lines = content.split('\n')
                                    for line_num, line in enumerate(lines, 1):
                                        line = line.strip()
                                        if line and not line.startswith('#') and '=' not in line:
                                            result['warnings'].append(f"Invalid env format at line {line_num} in {config_file}")
                                
                        except json.JSONDecodeError:
                            result['errors'].append(f"Invalid JSON in configuration file: {config_file}")
                        except Exception as e:
                            result['warnings'].append(f"Could not validate configuration file {config_file}: {e}")
                            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Configuration check failed: {e}")
        
        return result
    
    def _check_file_structure(self, backup_path: Path) -> Dict[str, Any]:
        """Check backup file structure."""
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        try:
            if backup_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    
                    # Check for required directories
                    required_dirs = ['cache', 'exports', 'logs']
                    for required_dir in required_dirs:
                        if not any(f.startswith(required_dir + '/') for f in file_list):
                            result['warnings'].append(f"Missing required directory: {required_dir}")
                    
                    # Check for suspicious files
                    suspicious_extensions = ['.exe', '.bat', '.sh', '.pyc']
                    for file in file_list:
                        if any(file.endswith(ext) for ext in suspicious_extensions):
                            result['warnings'].append(f"Suspicious file found: {file}")
                    
                    # Check file permissions
                    for file_info in zip_ref.infolist():
                        if file_info.external_attr & 0o111:  # Executable
                            result['warnings'].append(f"Executable file found: {file_info.filename}")
                            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"File structure check failed: {e}")
        
        return result
    
    def _check_checksum(self, backup_path: Path) -> Dict[str, Any]:
        """Check backup file checksum."""
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        try:
            # Calculate current checksum
            current_checksum = self._calculate_file_checksum(backup_path)
            
            # Look for stored checksum in metadata
            if backup_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                    metadata_files = [f for f in zip_ref.namelist() if f.endswith('metadata.json')]
                    
                    for metadata_file in metadata_files:
                        try:
                            with zip_ref.open(metadata_file) as f:
                                metadata = json.load(f)
                            
                            stored_checksum = metadata.get('checksum')
                            if stored_checksum:
                                if stored_checksum != current_checksum:
                                    result['errors'].append("Checksum mismatch detected")
                                else:
                                    logger.info("Checksum validation passed")
                            else:
                                result['warnings'].append("No stored checksum found in metadata")
                                
                        except Exception as e:
                            result['warnings'].append(f"Could not verify checksum: {e}")
                            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Checksum check failed: {e}")
        
        return result
    
    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def _get_backup_stats(self, backup_path: Path) -> Tuple[int, int]:
        """Get backup file statistics."""
        try:
            if backup_path.suffix.lower() == '.zip':
                with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                    file_count = len(zip_ref.infolist())
                    total_size = sum(info.file_size for info in zip_ref.infolist())
                    return file_count, total_size
            else:
                return 1, backup_path.stat().st_size
        except Exception:
            return 0, 0
    
    def validate_backup_chain(self, backup_paths: List[Path]) -> Dict[str, Any]:
        """Validate a chain of backup files."""
        results = []
        chain_valid = True
        
        for backup_path in backup_paths:
            result = self.validate_backup(backup_path)
            results.append({
                'file': str(backup_path),
                'valid': result.valid,
                'errors': result.errors,
                'warnings': result.warnings
            })
            
            if not result.valid:
                chain_valid = False
        
        return {
            'chain_valid': chain_valid,
            'total_backups': len(backup_paths),
            'valid_backups': sum(1 for r in results if r['valid']),
            'results': results
        }

def create_validator() -> BackupValidator:
    """Create and return a backup validator instance."""
    return BackupValidator()
