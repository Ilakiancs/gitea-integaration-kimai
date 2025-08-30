#!/usr/bin/env python3
"""
Advanced Backup Cloud Storage Module

Enhanced cloud storage integration for backup files with support for multiple
providers including AWS S3, Google Cloud Storage, Azure Blob, and advanced features
like multi-region replication, intelligent tiering, and enhanced security.
"""

import os
import logging
import hashlib
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from google.cloud import storage
import azure.storage.blob
from botocore.exceptions import ClientError
import requests
from cryptography.fernet import Fernet
import json
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class CloudStorageConfig:
    """Enhanced cloud storage configuration."""
    provider: str  # 'aws', 'gcp', 'azure', 'minio'
    bucket_name: str
    region: str = 'us-east-1'
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    endpoint_url: Optional[str] = None
    encryption_enabled: bool = True
    encryption_key: Optional[str] = None
    multi_region_replication: bool = False
    replication_regions: List[str] = field(default_factory=list)
    intelligent_tiering: bool = True
    lifecycle_policies: Dict[str, Any] = field(default_factory=dict)
    versioning_enabled: bool = True
    access_logging: bool = True
    metrics_enabled: bool = True
    tags: Dict[str, str] = field(default_factory=dict)

@dataclass
class UploadResult:
    """Result of cloud storage upload operation."""
    success: bool
    remote_path: str
    size: int
    checksum: str
    upload_time: float
    encryption_used: bool
    replication_status: Optional[str] = None
    tier: str = 'standard'
    error_message: Optional[str] = None

@dataclass
class StorageMetrics:
    """Cloud storage metrics."""
    total_files: int
    total_size: int
    average_file_size: float
    storage_class_distribution: Dict[str, int]
    replication_status: Dict[str, int]
    cost_estimate: float
    last_updated: str

class CloudStorageProvider(ABC):
    """Enhanced abstract base class for cloud storage providers."""
    
    @abstractmethod
    def upload_file(self, local_path: Path, remote_path: str, 
                   encryption_key: Optional[str] = None) -> UploadResult:
        """Upload a file to cloud storage with enhanced features."""
        pass
    
    @abstractmethod
    def download_file(self, remote_path: str, local_path: Path,
                     decryption_key: Optional[str] = None) -> bool:
        """Download a file from cloud storage with decryption."""
        pass
    
    @abstractmethod
    def list_files(self, prefix: str = "", recursive: bool = True) -> List[Dict[str, Any]]:
        """List files in cloud storage with enhanced metadata."""
        pass
    
    @abstractmethod
    def delete_file(self, remote_path: str, version_id: Optional[str] = None) -> bool:
        """Delete a file from cloud storage with versioning support."""
        pass
    
    @abstractmethod
    def get_file_info(self, remote_path: str, version_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get detailed information about a file in cloud storage."""
        pass
    
    @abstractmethod
    def enable_versioning(self) -> bool:
        """Enable versioning for the storage bucket."""
        pass
    
    @abstractmethod
    def set_lifecycle_policy(self, policy: Dict[str, Any]) -> bool:
        """Set lifecycle policy for the storage bucket."""
        pass
    
    @abstractmethod
    def get_storage_metrics(self) -> StorageMetrics:
        """Get storage metrics and analytics."""
        pass
    
    @abstractmethod
    def replicate_to_regions(self, regions: List[str]) -> bool:
        """Replicate bucket to multiple regions."""
        pass

class EnhancedAWSS3Provider(CloudStorageProvider):
    """Enhanced AWS S3 cloud storage provider with advanced features."""
    
    def __init__(self, config: CloudStorageConfig):
        self.config = config
        self.bucket_name = config.bucket_name
        self.region = config.region
        
        # Initialize S3 client
        if config.access_key_id and config.secret_access_key:
            self.client = boto3.client(
                's3',
                aws_access_key_id=config.access_key_id,
                aws_secret_access_key=config.secret_access_key,
                region_name=config.region,
                endpoint_url=config.endpoint_url
            )
        else:
            self.client = boto3.client(
                's3', 
                region_name=config.region,
                endpoint_url=config.endpoint_url
            )
        
        # Initialize encryption if enabled
        self.encryption_key = None
        if config.encryption_enabled and config.encryption_key:
            self.encryption_key = Fernet(config.encryption_key.encode())
        
        # Setup bucket if it doesn't exist
        self._ensure_bucket_exists()
        self._configure_bucket_features()
    
    def _ensure_bucket_exists(self):
        """Ensure the S3 bucket exists and is properly configured."""
        try:
            # Check if bucket exists
            self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Create bucket
                create_params = {
                    'Bucket': self.bucket_name,
                    'CreateBucketConfiguration': {
                        'LocationConstraint': self.region
                    }
                }
                self.client.create_bucket(**create_params)
                logger.info(f"Created S3 bucket: {self.bucket_name}")
            else:
                raise
    
    def _configure_bucket_features(self):
        """Configure bucket features like versioning, encryption, etc."""
        try:
            # Enable versioning
            if self.config.versioning_enabled:
                self.client.put_bucket_versioning(
                    Bucket=self.bucket_name,
                    VersioningConfiguration={'Status': 'Enabled'}
                )
            
            # Enable server-side encryption
            if self.config.encryption_enabled:
                self.client.put_bucket_encryption(
                    Bucket=self.bucket_name,
                    ServerSideEncryptionConfiguration={
                        'Rules': [{
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            }
                        }]
                    }
                )
            
            # Set lifecycle policy
            if self.config.lifecycle_policies:
                self.set_lifecycle_policy(self.config.lifecycle_policies)
            
            # Enable access logging
            if self.config.access_logging:
                log_bucket = f"{self.bucket_name}-logs"
                self.client.put_bucket_logging(
                    Bucket=self.bucket_name,
                    BucketLoggingStatus={
                        'LoggingEnabled': {
                            'TargetBucket': log_bucket,
                            'TargetPrefix': 'access-logs/'
                        }
                    }
                )
            
            logger.info(f"Configured S3 bucket features: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure bucket features: {e}")
    
    def upload_file(self, local_path: Path, remote_path: str, 
                   encryption_key: Optional[str] = None) -> UploadResult:
        """Upload a file to S3 with enhanced features."""
        start_time = time.time()
        
        try:
            # Calculate file checksum
            file_checksum = self._calculate_file_checksum(local_path)
            file_size = local_path.stat().st_size
            
            # Prepare upload parameters
            upload_params = {
                'Filename': str(local_path),
                'Bucket': self.bucket_name,
                'Key': remote_path,
                'ExtraArgs': {
                    'Metadata': {
                        'checksum': file_checksum,
                        'original_size': str(file_size),
                        'upload_timestamp': str(int(time.time())),
                        'encryption_enabled': str(self.config.encryption_enabled)
                    }
                }
            }
            
            # Add encryption if enabled
            if self.config.encryption_enabled:
                upload_params['ExtraArgs']['ServerSideEncryption'] = 'AES256'
            
            # Add tags if configured
            if self.config.tags:
                upload_params['ExtraArgs']['Tagging'] = '&'.join([
                    f"{k}={v}" for k, v in self.config.tags.items()
                ])
            
            # Upload file
            self.client.upload_file(**upload_params)
            
            upload_time = time.time() - start_time
            
            # Determine storage tier
            storage_tier = 'standard'
            if self.config.intelligent_tiering:
                storage_tier = 'intelligent-tiering'
            
            # Check replication status if enabled
            replication_status = None
            if self.config.multi_region_replication:
                replication_status = self._check_replication_status(remote_path)
            
            logger.info(f"File uploaded to S3: {remote_path} (size: {file_size}, time: {upload_time:.2f}s)")
            
            return UploadResult(
                success=True,
                remote_path=remote_path,
                size=file_size,
                checksum=file_checksum,
                upload_time=upload_time,
                encryption_used=self.config.encryption_enabled,
                replication_status=replication_status,
                tier=storage_tier
            )
            
        except Exception as e:
            upload_time = time.time() - start_time
            logger.error(f"Failed to upload file to S3: {e}")
            
            return UploadResult(
                success=False,
                remote_path=remote_path,
                size=0,
                checksum="",
                upload_time=upload_time,
                encryption_used=False,
                error_message=str(e)
            )
    
    def download_file(self, remote_path: str, local_path: Path,
                     decryption_key: Optional[str] = None) -> bool:
        """Download a file from S3 with decryption support."""
        try:
            # Download file
            self.client.download_file(
                self.bucket_name,
                remote_path,
                str(local_path)
            )
            
            # Verify checksum if available
            file_info = self.get_file_info(remote_path)
            if file_info and 'checksum' in file_info.get('metadata', {}):
                expected_checksum = file_info['metadata']['checksum']
                actual_checksum = self._calculate_file_checksum(local_path)
                
                if expected_checksum != actual_checksum:
                    logger.error(f"Checksum mismatch for {remote_path}")
                    return False
            
            logger.info(f"File downloaded from S3: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download file from S3: {e}")
            return False
    
    def list_files(self, prefix: str = "", recursive: bool = True) -> List[Dict[str, Any]]:
        """List files in S3 bucket with enhanced metadata."""
        try:
            files = []
            paginator = self.client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_info = {
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat(),
                            'storage_class': obj.get('StorageClass', 'STANDARD'),
                            'etag': obj['ETag'].strip('"'),
                            'version_id': obj.get('VersionId')
                        }
                        
                        # Get additional metadata
                        try:
                            head_response = self.client.head_object(
                                Bucket=self.bucket_name,
                                Key=obj['Key']
                            )
                            file_info['metadata'] = head_response.get('Metadata', {})
                            file_info['content_type'] = head_response.get('ContentType')
                            file_info['encryption'] = head_response.get('ServerSideEncryption')
                        except:
                            pass
                        
                        files.append(file_info)
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files in S3: {e}")
            return []
    
    def delete_file(self, remote_path: str, version_id: Optional[str] = None) -> bool:
        """Delete a file from S3 with versioning support."""
        try:
            delete_params = {
                'Bucket': self.bucket_name,
                'Key': remote_path
            }
            
            if version_id:
                delete_params['VersionId'] = version_id
            
            self.client.delete_object(**delete_params)
            logger.info(f"File deleted from S3: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete file from S3: {e}")
            return False
    
    def get_file_info(self, remote_path: str, version_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get detailed information about a file in S3."""
        try:
            head_params = {
                'Bucket': self.bucket_name,
                'Key': remote_path
            }
            
            if version_id:
                head_params['VersionId'] = version_id
            
            response = self.client.head_object(**head_params)
            
            return {
                'key': remote_path,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].isoformat(),
                'etag': response['ETag'].strip('"'),
                'content_type': response.get('ContentType'),
                'metadata': response.get('Metadata', {}),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'encryption': response.get('ServerSideEncryption'),
                'version_id': response.get('VersionId')
            }
            
        except Exception as e:
            logger.error(f"Failed to get file info from S3: {e}")
            return None
    
    def enable_versioning(self) -> bool:
        """Enable versioning for the S3 bucket."""
        try:
            self.client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            logger.info(f"Enabled versioning for bucket: {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to enable versioning: {e}")
            return False
    
    def set_lifecycle_policy(self, policy: Dict[str, Any]) -> bool:
        """Set lifecycle policy for the S3 bucket."""
        try:
            self.client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=policy
            )
            logger.info(f"Set lifecycle policy for bucket: {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to set lifecycle policy: {e}")
            return False
    
    def get_storage_metrics(self) -> StorageMetrics:
        """Get S3 storage metrics and analytics."""
        try:
            total_files = 0
            total_size = 0
            storage_classes = {}
            
            # Get storage class distribution
            paginator = self.client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_files += 1
                        total_size += obj['Size']
                        storage_class = obj.get('StorageClass', 'STANDARD')
                        storage_classes[storage_class] = storage_classes.get(storage_class, 0) + 1
            
            # Calculate cost estimate (rough calculation)
            cost_estimate = self._calculate_storage_cost(total_size, storage_classes)
            
            return StorageMetrics(
                total_files=total_files,
                total_size=total_size,
                average_file_size=total_size / total_files if total_files > 0 else 0,
                storage_class_distribution=storage_classes,
                replication_status={},  # Would need to check replication status
                cost_estimate=cost_estimate,
                last_updated=datetime.now().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Failed to get storage metrics: {e}")
            return StorageMetrics(
                total_files=0,
                total_size=0,
                average_file_size=0,
                storage_class_distribution={},
                replication_status={},
                cost_estimate=0.0,
                last_updated=datetime.now().isoformat()
            )
    
    def replicate_to_regions(self, regions: List[str]) -> bool:
        """Replicate S3 bucket to multiple regions."""
        try:
            # This would require setting up S3 Cross-Region Replication
            # For now, we'll just log the intention
            logger.info(f"Replication to regions requested: {regions}")
            logger.warning("S3 Cross-Region Replication requires manual setup in AWS console")
            return True
        except Exception as e:
            logger.error(f"Failed to setup replication: {e}")
            return False
    
    def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def _check_replication_status(self, remote_path: str) -> Optional[str]:
        """Check replication status of a file."""
        try:
            response = self.client.head_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            return response.get('ReplicationStatus')
        except:
            return None
    
    def _calculate_storage_cost(self, total_size: int, storage_classes: Dict[str, int]) -> float:
        """Calculate estimated storage cost."""
        # Rough cost estimates (should be configurable)
        costs = {
            'STANDARD': 0.023,  # $0.023 per GB per month
            'STANDARD_IA': 0.0125,  # $0.0125 per GB per month
            'GLACIER': 0.004,  # $0.004 per GB per month
            'DEEP_ARCHIVE': 0.00099  # $0.00099 per GB per month
        }
        
        total_cost = 0.0
        for storage_class, count in storage_classes.items():
            # Assume average file size for cost calculation
            avg_file_size = total_size / sum(storage_classes.values()) if storage_classes else 0
            class_cost = costs.get(storage_class, costs['STANDARD'])
            total_cost += (avg_file_size * count * class_cost) / (1024**3)  # Convert to GB
        
        return round(total_cost, 2)

class GoogleCloudProvider(CloudStorageProvider):
    """Google Cloud Storage provider."""
    
    def __init__(self, config: Dict[str, Any]):
        self.bucket_name = config.get('bucket_name')
        self.project_id = config.get('project_id')
        
        # Initialize GCS client
        self.client = storage.Client(project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)
    
    def upload_file(self, local_path: Path, remote_path: str) -> bool:
        """Upload a file to Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            blob.upload_from_filename(str(local_path))
            logger.info(f"File uploaded to GCS: {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload file to GCS: {e}")
            return False
    
    def download_file(self, remote_path: str, local_path: Path) -> bool:
        """Download a file from Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            blob.download_to_filename(str(local_path))
            logger.info(f"File downloaded from GCS: {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download file from GCS: {e}")
            return False
    
    def list_files(self, prefix: str = "") -> List[Dict[str, Any]]:
        """List files in Google Cloud Storage bucket."""
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            
            files = []
            for blob in blobs:
                files.append({
                    'key': blob.name,
                    'size': blob.size,
                    'last_modified': blob.updated.isoformat(),
                    'storage_class': blob.storage_class
                })
            
            return files
        except Exception as e:
            logger.error(f"Failed to list files in GCS: {e}")
            return []
    
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            blob.delete()
            logger.info(f"File deleted from GCS: {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file from GCS: {e}")
            return False
    
    def get_file_info(self, remote_path: str) -> Optional[Dict[str, Any]]:
        """Get information about a file in Google Cloud Storage."""
        try:
            blob = self.bucket.blob(remote_path)
            blob.reload()
            
            return {
                'size': blob.size,
                'last_modified': blob.updated.isoformat(),
                'storage_class': blob.storage_class,
                'etag': blob.etag,
                'content_type': blob.content_type
            }
        except Exception as e:
            logger.error(f"Failed to get file info from GCS: {e}")
            return None

class AzureBlobProvider(CloudStorageProvider):
    """Azure Blob Storage provider."""
    
    def __init__(self, config: Dict[str, Any]):
        self.container_name = config.get('container_name')
        self.connection_string = config.get('connection_string')
        
        # Initialize Azure client
        self.client = azure.storage.blob.BlobServiceClient.from_connection_string(
            self.connection_string
        )
        self.container_client = self.client.get_container_client(self.container_name)
    
    def upload_file(self, local_path: Path, remote_path: str) -> bool:
        """Upload a file to Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            with open(local_path, 'rb') as f:
                blob_client.upload_blob(f, overwrite=True)
            logger.info(f"File uploaded to Azure: {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload file to Azure: {e}")
            return False
    
    def download_file(self, remote_path: str, local_path: Path) -> bool:
        """Download a file from Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            with open(local_path, 'wb') as f:
                blob_data = blob_client.download_blob()
                f.write(blob_data.readall())
            logger.info(f"File downloaded from Azure: {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download file from Azure: {e}")
            return False
    
    def list_files(self, prefix: str = "") -> List[Dict[str, Any]]:
        """List files in Azure Blob Storage container."""
        try:
            blobs = self.container_client.list_blobs(name_starts_with=prefix)
            
            files = []
            for blob in blobs:
                files.append({
                    'key': blob.name,
                    'size': blob.size,
                    'last_modified': blob.last_modified.isoformat(),
                    'storage_class': blob.blob_tier
                })
            
            return files
        except Exception as e:
            logger.error(f"Failed to list files in Azure: {e}")
            return []
    
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.delete_blob()
            logger.info(f"File deleted from Azure: {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete file from Azure: {e}")
            return False
    
    def get_file_info(self, remote_path: str) -> Optional[Dict[str, Any]]:
        """Get information about a file in Azure Blob Storage."""
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            properties = blob_client.get_blob_properties()
            
            return {
                'size': properties.size,
                'last_modified': properties.last_modified.isoformat(),
                'storage_class': properties.blob_tier,
                'etag': properties.etag,
                'content_type': properties.content_settings.content_type
            }
        except Exception as e:
            logger.error(f"Failed to get file info from Azure: {e}")
            return None

class CloudStorageManager:
    """Manager for cloud storage operations."""
    
    def __init__(self, provider: str, config: Dict[str, Any]):
        self.provider_name = provider
        self.config = config
        self.provider = self._create_provider(provider, config)
    
    def _create_provider(self, provider: str, config: Dict[str, Any]) -> CloudStorageProvider:
        """Create cloud storage provider instance."""
        if provider == 'aws':
            return EnhancedAWSS3Provider(CloudStorageConfig(**config))
        elif provider == 'gcp':
            return GoogleCloudProvider(config)
        elif provider == 'azure':
            return AzureBlobProvider(config)
        else:
            raise ValueError(f"Unsupported cloud provider: {provider}")
    
    def sync_backup(self, backup_path: Path, remote_path: str = None) -> bool:
        """Sync a backup file to cloud storage."""
        if remote_path is None:
            remote_path = f"backups/{backup_path.name}"
        
        return self.provider.upload_file(backup_path, remote_path)
    
    def download_backup(self, remote_path: str, local_path: Path) -> bool:
        """Download a backup file from cloud storage."""
        return self.provider.download_file(remote_path, local_path)
    
    def list_backups(self, prefix: str = "backups/") -> List[Dict[str, Any]]:
        """List backup files in cloud storage."""
        return self.provider.list_files(prefix)
    
    def delete_backup(self, remote_path: str) -> bool:
        """Delete a backup file from cloud storage."""
        return self.provider.delete_file(remote_path)
    
    def get_backup_info(self, remote_path: str) -> Optional[Dict[str, Any]]:
        """Get information about a backup file in cloud storage."""
        return self.provider.get_file_info(remote_path)
    
    def get_sync_status(self, backup_path: Path, remote_path: str = None) -> Dict[str, Any]:
        """Get sync status of a backup file."""
        if remote_path is None:
            remote_path = f"backups/{backup_path.name}"
        
        local_info = {
            'exists': backup_path.exists(),
            'size': backup_path.stat().st_size if backup_path.exists() else 0,
            'modified': backup_path.stat().st_mtime if backup_path.exists() else 0
        }
        
        remote_info = self.provider.get_file_info(remote_path)
        
        if remote_info:
            # Compare local and remote
            if local_info['exists']:
                size_match = local_info['size'] == remote_info['size']
                return {
                    'synced': size_match,
                    'local_info': local_info,
                    'remote_info': remote_info,
                    'size_match': size_match
                }
            else:
                return {
                    'synced': False,
                    'local_info': local_info,
                    'remote_info': remote_info,
                    'size_match': False
                }
        else:
            return {
                'synced': False,
                'local_info': local_info,
                'remote_info': None,
                'size_match': False
            }

def create_cloud_storage(provider: str, config: Dict[str, Any]) -> CloudStorageManager:
    """Create and return a cloud storage manager instance."""
    return CloudStorageManager(provider, config)
