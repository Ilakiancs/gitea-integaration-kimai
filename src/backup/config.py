#!/usr/bin/env python3
"""
Backup Configuration Management

Configuration management system for the backup and restore functionality
with validation, environment support, and dynamic configuration loading.
"""

import os
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict, field
from pydantic import BaseModel, validator, Field

logger = logging.getLogger(__name__)

class BackupConfigModel(BaseModel):
    """Pydantic model for backup configuration validation."""
    
    # Basic settings
    source_paths: List[str] = Field(default_factory=lambda: ['.', 'data', 'config'])
    backup_dir: str = Field(default='backups')
    retention_days: int = Field(default=30, ge=1, le=365)
    
    # Compression settings
    compression_level: int = Field(default=6, ge=1, le=9)
    compression_algorithm: str = Field(default='deflate', regex='^(deflate|lzma|bz2)$')
    
    # Security settings
    encryption_enabled: bool = Field(default=False)
    encryption_algorithm: str = Field(default='AES-256', regex='^(AES-256|ChaCha20)$')
    encryption_key_file: Optional[str] = Field(default=None)
    
    # Deduplication settings
    deduplication_enabled: bool = Field(default=True)
    deduplication_algorithm: str = Field(default='sha256', regex='^(sha256|blake2b|md5)$')
    
    # Cloud settings
    cloud_sync_enabled: bool = Field(default=False)
    cloud_provider: str = Field(default='local', regex='^(aws|gcp|azure|local)$')
    cloud_config: Dict[str, Any] = Field(default_factory=dict)
    
    # Scheduling settings
    schedule_enabled: bool = Field(default=False)
    schedule_interval: str = Field(default='daily', regex='^(hourly|daily|weekly|monthly)$')
    schedule_time: str = Field(default='02:00', regex='^([01]?[0-9]|2[0-3]):[0-5][0-9]$')
    
    # Performance settings
    max_concurrent_backups: int = Field(default=1, ge=1, le=10)
    max_backup_size_gb: int = Field(default=10, ge=1, le=1000)
    compression_memory_limit_mb: int = Field(default=512, ge=64, le=4096)
    
    # Monitoring settings
    enable_monitoring: bool = Field(default=True)
    monitoring_interval_seconds: int = Field(default=300, ge=60, le=3600)
    alert_on_failure: bool = Field(default=True)
    alert_on_success: bool = Field(default=False)
    
    # Logging settings
    log_level: str = Field(default='INFO', regex='^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$')
    log_file: Optional[str] = Field(default=None)
    log_rotation: bool = Field(default=True)
    log_retention_days: int = Field(default=7, ge=1, le=30)
    
    @validator('source_paths')
    def validate_source_paths(cls, v):
        """Validate source paths exist."""
        for path in v:
            if not Path(path).exists():
                logger.warning(f"Source path does not exist: {path}")
        return v
    
    @validator('cloud_config')
    def validate_cloud_config(cls, v, values):
        """Validate cloud configuration based on provider."""
        provider = values.get('cloud_provider', 'local')
        
        if provider == 'aws':
            required_keys = ['bucket_name', 'region']
            missing_keys = [key for key in required_keys if key not in v]
            if missing_keys:
                raise ValueError(f"Missing required AWS config keys: {missing_keys}")
        
        elif provider == 'gcp':
            required_keys = ['bucket_name', 'project_id']
            missing_keys = [key for key in required_keys if key not in v]
            if missing_keys:
                raise ValueError(f"Missing required GCP config keys: {missing_keys}")
        
        elif provider == 'azure':
            required_keys = ['container_name', 'connection_string']
            missing_keys = [key for key in required_keys if key not in v]
            if missing_keys:
                raise ValueError(f"Missing required Azure config keys: {missing_keys}")
        
        return v

@dataclass
class BackupConfigManager:
    """Configuration manager for backup system."""
    
    config_file: str = "backup_config.json"
    config: BackupConfigModel = None
    
    def __post_init__(self):
        """Initialize configuration."""
        if self.config is None:
            self.config = self.load_config()
    
    def load_config(self, config_file: str = None) -> BackupConfigModel:
        """Load configuration from file or environment."""
        config_file = config_file or self.config_file
        
        # Try to load from file
        if Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                        config_data = yaml.safe_load(f)
                    else:
                        config_data = json.load(f)
                
                return BackupConfigModel(**config_data)
            except Exception as e:
                logger.error(f"Failed to load config from {config_file}: {e}")
        
        # Load from environment variables
        config_data = self._load_from_environment()
        
        # Create default config
        return BackupConfigModel(**config_data)
    
    def _load_from_environment(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config_data = {}
        
        # Map environment variables to config fields
        env_mapping = {
            'BACKUP_SOURCE_PATHS': 'source_paths',
            'BACKUP_DIR': 'backup_dir',
            'BACKUP_RETENTION_DAYS': 'retention_days',
            'BACKUP_COMPRESSION_LEVEL': 'compression_level',
            'BACKUP_ENCRYPTION_ENABLED': 'encryption_enabled',
            'BACKUP_DEDUPLICATION_ENABLED': 'deduplication_enabled',
            'BACKUP_CLOUD_SYNC_ENABLED': 'cloud_sync_enabled',
            'BACKUP_CLOUD_PROVIDER': 'cloud_provider',
            'BACKUP_SCHEDULE_ENABLED': 'schedule_enabled',
            'BACKUP_SCHEDULE_INTERVAL': 'schedule_interval',
            'BACKUP_LOG_LEVEL': 'log_level'
        }
        
        for env_var, config_key in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                # Convert string values to appropriate types
                if config_key in ['source_paths']:
                    config_data[config_key] = value.split(',')
                elif config_key in ['retention_days', 'compression_level']:
                    config_data[config_key] = int(value)
                elif config_key in ['encryption_enabled', 'deduplication_enabled', 
                                  'cloud_sync_enabled', 'schedule_enabled']:
                    config_data[config_key] = value.lower() in ['true', '1', 'yes']
                else:
                    config_data[config_key] = value
        
        # Load cloud configuration from environment
        cloud_config = self._load_cloud_config_from_env()
        if cloud_config:
            config_data['cloud_config'] = cloud_config
        
        return config_data
    
    def _load_cloud_config_from_env(self) -> Dict[str, Any]:
        """Load cloud configuration from environment variables."""
        cloud_config = {}
        
        # AWS configuration
        if os.getenv('AWS_BUCKET_NAME'):
            cloud_config.update({
                'bucket_name': os.getenv('AWS_BUCKET_NAME'),
                'region': os.getenv('AWS_REGION', 'us-east-1'),
                'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
                'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY')
            })
        
        # GCP configuration
        elif os.getenv('GCP_BUCKET_NAME'):
            cloud_config.update({
                'bucket_name': os.getenv('GCP_BUCKET_NAME'),
                'project_id': os.getenv('GCP_PROJECT_ID'),
                'credentials_file': os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            })
        
        # Azure configuration
        elif os.getenv('AZURE_CONNECTION_STRING'):
            cloud_config.update({
                'container_name': os.getenv('AZURE_CONTAINER_NAME'),
                'connection_string': os.getenv('AZURE_CONNECTION_STRING')
            })
        
        return cloud_config
    
    def save_config(self, config_file: str = None) -> bool:
        """Save configuration to file."""
        config_file = config_file or self.config_file
        
        try:
            config_data = self.config.dict()
            
            with open(config_file, 'w') as f:
                if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                    yaml.dump(config_data, f, default_flow_style=False, indent=2)
                else:
                    json.dump(config_data, f, indent=2)
            
            logger.info(f"Configuration saved to {config_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return False
    
    def update_config(self, updates: Dict[str, Any]) -> bool:
        """Update configuration with new values."""
        try:
            # Create new config with updates
            current_data = self.config.dict()
            current_data.update(updates)
            
            # Validate new config
            new_config = BackupConfigModel(**current_data)
            self.config = new_config
            
            logger.info("Configuration updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update configuration: {e}")
            return False
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate current configuration."""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Validate using Pydantic
            self.config.dict()
            
            # Additional custom validations
            if self.config.cloud_sync_enabled:
                if not self.config.cloud_config:
                    validation_result['warnings'].append("Cloud sync enabled but no cloud config provided")
                
                if self.config.cloud_provider == 'local':
                    validation_result['warnings'].append("Cloud sync enabled with local provider")
            
            # Check source paths
            for path in self.config.source_paths:
                if not Path(path).exists():
                    validation_result['warnings'].append(f"Source path does not exist: {path}")
            
            # Check backup directory
            backup_dir = Path(self.config.backup_dir)
            if not backup_dir.exists():
                validation_result['warnings'].append(f"Backup directory does not exist: {self.config.backup_dir}")
            
        except Exception as e:
            validation_result['valid'] = False
            validation_result['errors'].append(str(e))
        
        return validation_result
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get configuration summary."""
        return {
            'source_paths': self.config.source_paths,
            'backup_dir': self.config.backup_dir,
            'retention_days': self.config.retention_days,
            'compression_level': self.config.compression_level,
            'encryption_enabled': self.config.encryption_enabled,
            'deduplication_enabled': self.config.deduplication_enabled,
            'cloud_sync_enabled': self.config.cloud_sync_enabled,
            'cloud_provider': self.config.cloud_provider,
            'schedule_enabled': self.config.schedule_enabled,
            'schedule_interval': self.config.schedule_interval,
            'log_level': self.config.log_level
        }
    
    def create_default_config(self, config_file: str = None) -> bool:
        """Create default configuration file."""
        config_file = config_file or self.config_file
        
        # Create default config
        default_config = BackupConfigModel()
        
        # Save to file
        try:
            config_data = default_config.dict()
            
            with open(config_file, 'w') as f:
                if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                    yaml.dump(config_data, f, default_flow_style=False, indent=2)
                else:
                    json.dump(config_data, f, indent=2)
            
            logger.info(f"Default configuration created: {config_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create default configuration: {e}")
            return False

def create_config_manager(config_file: str = "backup_config.json") -> BackupConfigManager:
    """Create and return a configuration manager instance."""
    return BackupConfigManager(config_file=config_file)

def load_backup_config(config_file: str = None) -> BackupConfigModel:
    """Load backup configuration from file or environment."""
    manager = create_config_manager(config_file or "backup_config.json")
    return manager.config
