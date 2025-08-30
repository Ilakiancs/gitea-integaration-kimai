"""
Next-Generation Backup Module

Advanced backup and restore functionality for the Gitea to Kimai integration.
Features include intelligent scheduling, compression optimization, encryption,
deduplication, cloud integration, and automated recovery capabilities.
"""

__version__ = "2.0.0"

from .restore import AdvancedBackupRestore, create_restore, RestoreMetadata
from .backup_manager import IntelligentBackupManager, BackupConfig, BackupMetadata, create_backup_manager
from .cli import BackupCLI, cli

__all__ = [
    'AdvancedBackupRestore',
    'IntelligentBackupManager', 
    'BackupConfig',
    'BackupMetadata',
    'RestoreMetadata',
    'create_restore',
    'create_backup_manager',
    'BackupCLI',
    'cli'
]
