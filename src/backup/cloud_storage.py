#!/usr/bin/env python3
"""
Backup Cloud Storage Module

Cloud storage integration for backup files with support for multiple
providers including AWS S3, Google Cloud Storage, and Azure Blob.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from abc import ABC, abstractmethod
import boto3
from google.cloud import storage
import azure.storage.blob
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class CloudStorageProvider(ABC):
    """Abstract base class for cloud storage providers."""
    
    @abstractmethod
    def upload_file(self, local_path: Path, remote_path: str) -> bool:
        """Upload a file to cloud storage."""
        pass
    
    @abstractmethod
    def download_file(self, remote_path: str, local_path: Path) -> bool:
        """Download a file from cloud storage."""
        pass
    
    @abstractmethod
    def list_files(self, prefix: str = "") -> List[Dict[str, Any]]:
        """List files in cloud storage."""
        pass
    
    @abstractmethod
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from cloud storage."""
        pass
    
    @abstractmethod
    def get_file_info(self, remote_path: str) -> Optional[Dict[str, Any]]:
        """Get information about a file in cloud storage."""
        pass

class AWSS3Provider(CloudStorageProvider):
    """AWS S3 cloud storage provider."""
    
    def __init__(self, config: Dict[str, Any]):
        self.bucket_name = config.get('bucket_name')
        self.region = config.get('region', 'us-east-1')
        self.access_key_id = config.get('access_key_id')
        self.secret_access_key = config.get('secret_access_key')
        
        # Initialize S3 client
        if self.access_key_id and self.secret_access_key:
            self.client = boto3.client(
                's3',
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region
            )
        else:
            self.client = boto3.client('s3', region_name=self.region)
    
    def upload_file(self, local_path: Path, remote_path: str) -> bool:
        """Upload a file to S3."""
        try:
            self.client.upload_file(
                str(local_path),
                self.bucket_name,
                remote_path
            )
            logger.info(f"File uploaded to S3: {remote_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            return False
    
    def download_file(self, remote_path: str, local_path: Path) -> bool:
        """Download a file from S3."""
        try:
            self.client.download_file(
                self.bucket_name,
                remote_path,
                str(local_path)
            )
            logger.info(f"File downloaded from S3: {remote_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download file from S3: {e}")
            return False
    
    def list_files(self, prefix: str = "") -> List[Dict[str, Any]]:
        """List files in S3 bucket."""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            for obj in response.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                })
            
            return files
        except ClientError as e:
            logger.error(f"Failed to list files in S3: {e}")
            return []
    
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from S3."""
        try:
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            logger.info(f"File deleted from S3: {remote_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete file from S3: {e}")
            return False
    
    def get_file_info(self, remote_path: str) -> Optional[Dict[str, Any]]:
        """Get information about a file in S3."""
        try:
            response = self.client.head_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].isoformat(),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'etag': response.get('ETag', ''),
                'content_type': response.get('ContentType', '')
            }
        except ClientError as e:
            logger.error(f"Failed to get file info from S3: {e}")
            return None

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
            return AWSS3Provider(config)
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
