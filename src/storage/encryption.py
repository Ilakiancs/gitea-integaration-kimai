#!/usr/bin/env python3
"""
Data Encryption Module

Provides encryption and decryption capabilities for sensitive data
such as API tokens, passwords, and configuration secrets.
"""

import os
import base64
import logging
from typing import Optional, Union
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import secrets

logger = logging.getLogger(__name__)

class DataEncryption:
    """Handles encryption and decryption of sensitive data."""
    
    def __init__(self, key: Optional[bytes] = None, salt: Optional[bytes] = None):
        self.key = key or self._generate_key()
        self.salt = salt or self._generate_salt()
        self.fernet = Fernet(self.key)
    
    def _generate_key(self) -> bytes:
        """Generate a new encryption key."""
        return Fernet.generate_key()
    
    def _generate_salt(self) -> bytes:
        """Generate a new salt for key derivation."""
        return secrets.token_bytes(16)
    
    def derive_key_from_password(self, password: str, salt: Optional[bytes] = None) -> bytes:
        """Derive encryption key from password using PBKDF2."""
        if salt is None:
            salt = self._generate_salt()
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key
    
    def encrypt_data(self, data: Union[str, bytes]) -> str:
        """Encrypt data and return base64 encoded string."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        encrypted_data = self.fernet.encrypt(data)
        return base64.urlsafe_b64encode(encrypted_data).decode('utf-8')
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt base64 encoded encrypted data."""
        try:
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode('utf-8'))
            decrypted_data = self.fernet.decrypt(encrypted_bytes)
            return decrypted_data.decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise ValueError("Invalid encrypted data or key")
    
    def encrypt_file(self, input_path: str, output_path: str):
        """Encrypt a file."""
        try:
            with open(input_path, 'rb') as f:
                data = f.read()
            
            encrypted_data = self.fernet.encrypt(data)
            
            with open(output_path, 'wb') as f:
                f.write(encrypted_data)
            
            logger.info(f"File encrypted: {input_path} -> {output_path}")
        except Exception as e:
            logger.error(f"Failed to encrypt file {input_path}: {e}")
            raise
    
    def decrypt_file(self, input_path: str, output_path: str):
        """Decrypt a file."""
        try:
            with open(input_path, 'rb') as f:
                encrypted_data = f.read()
            
            decrypted_data = self.fernet.decrypt(encrypted_data)
            
            with open(output_path, 'wb') as f:
                f.write(decrypted_data)
            
            logger.info(f"File decrypted: {input_path} -> {output_path}")
        except Exception as e:
            logger.error(f"Failed to decrypt file {input_path}: {e}")
            raise
    
    def get_key_b64(self) -> str:
        """Get the encryption key as base64 string."""
        return base64.urlsafe_b64encode(self.key).decode('utf-8')
    
    def get_salt_b64(self) -> str:
        """Get the salt as base64 string."""
        return base64.urlsafe_b64encode(self.salt).decode('utf-8')

class SecureConfigManager:
    """Manages secure storage of sensitive configuration data."""
    
    def __init__(self, encryption_key: Optional[bytes] = None):
        self.encryption = DataEncryption(encryption_key)
        self.secure_storage = {}
    
    def store_secure_value(self, key: str, value: str):
        """Store a value securely."""
        encrypted_value = self.encryption.encrypt_data(value)
        self.secure_storage[key] = encrypted_value
    
    def get_secure_value(self, key: str) -> Optional[str]:
        """Retrieve a securely stored value."""
        if key not in self.secure_storage:
            return None
        
        try:
            return self.encryption.decrypt_data(self.secure_storage[key])
        except Exception as e:
            logger.error(f"Failed to decrypt value for key {key}: {e}")
            return None
    
    def remove_secure_value(self, key: str):
        """Remove a securely stored value."""
        if key in self.secure_storage:
            del self.secure_storage[key]
    
    def list_secure_keys(self) -> list:
        """List all secure storage keys."""
        return list(self.secure_storage.keys())
    
    def export_secure_storage(self, file_path: str):
        """Export secure storage to file."""
        try:
            with open(file_path, 'w') as f:
                import json
                json.dump(self.secure_storage, f, indent=2)
            logger.info(f"Secure storage exported to {file_path}")
        except Exception as e:
            logger.error(f"Failed to export secure storage: {e}")
            raise
    
    def import_secure_storage(self, file_path: str):
        """Import secure storage from file."""
        try:
            with open(file_path, 'r') as f:
                import json
                self.secure_storage = json.load(f)
            logger.info(f"Secure storage imported from {file_path}")
        except Exception as e:
            logger.error(f"Failed to import secure storage: {e}")
            raise

class TokenManager:
    """Manages secure storage and rotation of API tokens."""
    
    def __init__(self, encryption_key: Optional[bytes] = None):
        self.secure_manager = SecureConfigManager(encryption_key)
        self.token_metadata = {}
    
    def store_token(self, service: str, token: str, expires_at: Optional[str] = None):
        """Store an API token securely."""
        self.secure_manager.store_secure_value(f"token_{service}", token)
        
        metadata = {
            'stored_at': self._get_current_timestamp(),
            'expires_at': expires_at
        }
        self.token_metadata[service] = metadata
    
    def get_token(self, service: str) -> Optional[str]:
        """Retrieve an API token."""
        return self.secure_manager.get_secure_value(f"token_{service}")
    
    def rotate_token(self, service: str, new_token: str, expires_at: Optional[str] = None):
        """Rotate an API token."""
        old_token = self.get_token(service)
        if old_token:
            logger.info(f"Rotating token for service: {service}")
        
        self.store_token(service, new_token, expires_at)
        logger.info(f"Token rotated for service: {service}")
    
    def is_token_expired(self, service: str) -> bool:
        """Check if a token is expired."""
        if service not in self.token_metadata:
            return False
        
        expires_at = self.token_metadata[service].get('expires_at')
        if not expires_at:
            return False
        
        try:
            from datetime import datetime
            expiry_time = datetime.fromisoformat(expires_at)
            return datetime.now() > expiry_time
        except Exception as e:
            logger.warning(f"Could not parse expiry time for {service}: {e}")
            return False
    
    def get_token_info(self, service: str) -> Optional[dict]:
        """Get token metadata."""
        if service not in self.token_metadata:
            return None
        
        return self.token_metadata[service].copy()
    
    def list_services(self) -> list:
        """List all services with stored tokens."""
        services = []
        for key in self.secure_manager.list_secure_keys():
            if key.startswith('token_'):
                service = key[6:]  # Remove 'token_' prefix
                services.append(service)
        return services
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()

class PasswordManager:
    """Manages secure storage of passwords."""
    
    def __init__(self, encryption_key: Optional[bytes] = None):
        self.secure_manager = SecureConfigManager(encryption_key)
    
    def store_password(self, service: str, username: str, password: str):
        """Store a password securely."""
        key = f"password_{service}_{username}"
        self.secure_manager.store_secure_value(key, password)
    
    def get_password(self, service: str, username: str) -> Optional[str]:
        """Retrieve a stored password."""
        key = f"password_{service}_{username}"
        return self.secure_manager.get_secure_value(key)
    
    def remove_password(self, service: str, username: str):
        """Remove a stored password."""
        key = f"password_{service}_{username}"
        self.secure_manager.remove_secure_value(key)
    
    def list_credentials(self) -> list:
        """List all stored credentials."""
        credentials = []
        for key in self.secure_manager.list_secure_keys():
            if key.startswith('password_'):
                parts = key.split('_', 2)
                if len(parts) == 3:
                    credentials.append({
                        'service': parts[1],
                        'username': parts[2]
                    })
        return credentials

def generate_encryption_key() -> str:
    """Generate a new encryption key and return as base64 string."""
    key = Fernet.generate_key()
    return base64.urlsafe_b64encode(key).decode('utf-8')

def create_encryption_from_password(password: str, salt: Optional[str] = None) -> DataEncryption:
    """Create encryption instance from password."""
    if salt:
        salt_bytes = base64.urlsafe_b64decode(salt.encode('utf-8'))
    else:
        salt_bytes = None
    
    encryption = DataEncryption()
    key = encryption.derive_key_from_password(password, salt_bytes)
    return DataEncryption(key, salt_bytes)
