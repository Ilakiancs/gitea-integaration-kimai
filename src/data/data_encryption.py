#!/usr/bin/env python3
"""
Data Encryption Utility

Provides utilities for encrypting and decrypting data using various
encryption algorithms and key management.
"""

import os
import base64
import hashlib
import logging
from typing import Dict, List, Optional, Any, Union, Bytes
from dataclasses import dataclass
from enum import Enum
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import json

logger = logging.getLogger(__name__)

class EncryptionType(Enum):
    """Types of encryption algorithms."""
    FERNET = "fernet"
    AES = "aes"
    CUSTOM = "custom"

@dataclass
class EncryptionResult:
    """Result of an encryption operation."""
    encrypted_data: bytes
    key: bytes
    salt: Optional[bytes] = None
    iv: Optional[bytes] = None
    algorithm: EncryptionType = EncryptionType.FERNET

class DataEncryptor:
    """Main data encryption utility."""
    
    def __init__(self):
        self.default_algorithm = EncryptionType.FERNET
        self.key_length = 32  # 256 bits
        self.salt_length = 16
        self.iv_length = 16
    
    def generate_key(self, algorithm: EncryptionType = EncryptionType.FERNET) -> bytes:
        """Generate a new encryption key."""
        if algorithm == EncryptionType.FERNET:
            return Fernet.generate_key()
        elif algorithm == EncryptionType.AES:
            return os.urandom(self.key_length)
        else:
            return os.urandom(self.key_length)
    
    def derive_key_from_password(self, password: str, salt: Optional[bytes] = None) -> tuple[bytes, bytes]:
        """Derive a key from a password using PBKDF2."""
        if salt is None:
            salt = os.urandom(self.salt_length)
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=self.key_length,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key, salt
    
    def encrypt(self, data: Union[str, bytes, dict, list], 
                key: Optional[bytes] = None,
                password: Optional[str] = None,
                algorithm: EncryptionType = EncryptionType.FERNET) -> EncryptionResult:
        """Encrypt data using the specified algorithm."""
        # Convert data to bytes if needed
        if isinstance(data, str):
            data_bytes = data.encode('utf-8')
        elif isinstance(data, (dict, list)):
            data_bytes = json.dumps(data).encode('utf-8')
        else:
            data_bytes = data
        
        # Generate or derive key
        if password:
            key, salt = self.derive_key_from_password(password)
        elif key is None:
            key = self.generate_key(algorithm)
            salt = None
        
        # Encrypt data
        if algorithm == EncryptionType.FERNET:
            encrypted_data = self._encrypt_fernet(data_bytes, key)
            return EncryptionResult(
                encrypted_data=encrypted_data,
                key=key,
                salt=salt,
                algorithm=algorithm
            )
        elif algorithm == EncryptionType.AES:
            encrypted_data, iv = self._encrypt_aes(data_bytes, key)
            return EncryptionResult(
                encrypted_data=encrypted_data,
                key=key,
                salt=salt,
                iv=iv,
                algorithm=algorithm
            )
        else:
            raise ValueError(f"Unsupported encryption algorithm: {algorithm}")
    
    def decrypt(self, encrypted_data: bytes, key: bytes,
                salt: Optional[bytes] = None,
                iv: Optional[bytes] = None,
                algorithm: EncryptionType = EncryptionType.FERNET) -> bytes:
        """Decrypt data using the specified algorithm."""
        if algorithm == EncryptionType.FERNET:
            return self._decrypt_fernet(encrypted_data, key)
        elif algorithm == EncryptionType.AES:
            if iv is None:
                raise ValueError("IV is required for AES decryption")
            return self._decrypt_aes(encrypted_data, key, iv)
        else:
            raise ValueError(f"Unsupported encryption algorithm: {algorithm}")
    
    def _encrypt_fernet(self, data: bytes, key: bytes) -> bytes:
        """Encrypt data using Fernet."""
        fernet = Fernet(key)
        return fernet.encrypt(data)
    
    def _decrypt_fernet(self, encrypted_data: bytes, key: bytes) -> bytes:
        """Decrypt data using Fernet."""
        fernet = Fernet(key)
        return fernet.decrypt(encrypted_data)
    
    def _encrypt_aes(self, data: bytes, key: bytes) -> tuple[bytes, bytes]:
        """Encrypt data using AES."""
        iv = os.urandom(self.iv_length)
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        
        # Pad data to block size
        padded_data = self._pad_data(data)
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
        
        return encrypted_data, iv
    
    def _decrypt_aes(self, encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
        """Decrypt data using AES."""
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        
        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
        
        # Remove padding
        return self._unpad_data(decrypted_data)
    
    def _pad_data(self, data: bytes) -> bytes:
        """Pad data to AES block size."""
        block_size = 16
        padding_length = block_size - (len(data) % block_size)
        padding = bytes([padding_length] * padding_length)
        return data + padding
    
    def _unpad_data(self, data: bytes) -> bytes:
        """Remove padding from data."""
        padding_length = data[-1]
        return data[:-padding_length]
    
    def encrypt_file(self, input_path: str, output_path: str,
                    key: Optional[bytes] = None,
                    password: Optional[str] = None,
                    algorithm: EncryptionType = EncryptionType.FERNET) -> EncryptionResult:
        """Encrypt a file."""
        with open(input_path, 'rb') as f:
            data = f.read()
        
        result = self.encrypt(data, key, password, algorithm)
        
        # Save encrypted data with metadata
        metadata = {
            'algorithm': result.algorithm.value,
            'salt': base64.b64encode(result.salt).decode() if result.salt else None,
            'iv': base64.b64encode(result.iv).decode() if result.iv else None
        }
        
        encrypted_file_data = {
            'metadata': metadata,
            'data': base64.b64encode(result.encrypted_data).decode()
        }
        
        with open(output_path, 'w') as f:
            json.dump(encrypted_file_data, f)
        
        logger.info(f"Encrypted {input_path} to {output_path}")
        return result
    
    def decrypt_file(self, input_path: str, output_path: str,
                    key: bytes,
                    algorithm: EncryptionType = EncryptionType.FERNET) -> bytes:
        """Decrypt a file."""
        with open(input_path, 'r') as f:
            encrypted_file_data = json.load(f)
        
        metadata = encrypted_file_data['metadata']
        encrypted_data = base64.b64decode(encrypted_file_data['data'])
        
        salt = base64.b64decode(metadata['salt']) if metadata['salt'] else None
        iv = base64.b64decode(metadata['iv']) if metadata['iv'] else None
        
        decrypted_data = self.decrypt(encrypted_data, key, salt, iv, algorithm)
        
        with open(output_path, 'wb') as f:
            f.write(decrypted_data)
        
        logger.info(f"Decrypted {input_path} to {output_path}")
        return decrypted_data
    
    def encrypt_to_base64(self, data: Union[str, bytes, dict, list],
                         key: Optional[bytes] = None,
                         password: Optional[str] = None,
                         algorithm: EncryptionType = EncryptionType.FERNET) -> str:
        """Encrypt data and encode as base64 string."""
        result = self.encrypt(data, key, password, algorithm)
        return base64.b64encode(result.encrypted_data).decode('utf-8')
    
    def decrypt_from_base64(self, base64_data: str,
                           key: bytes,
                           salt: Optional[bytes] = None,
                           iv: Optional[bytes] = None,
                           algorithm: EncryptionType = EncryptionType.FERNET) -> bytes:
        """Decrypt data from base64 string."""
        encrypted_data = base64.b64decode(base64_data)
        return self.decrypt(encrypted_data, key, salt, iv, algorithm)
    
    def hash_data(self, data: Union[str, bytes], algorithm: str = 'sha256') -> str:
        """Hash data using the specified algorithm."""
        if isinstance(data, str):
            data_bytes = data.encode('utf-8')
        else:
            data_bytes = data
        
        if algorithm == 'md5':
            return hashlib.md5(data_bytes).hexdigest()
        elif algorithm == 'sha1':
            return hashlib.sha1(data_bytes).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(data_bytes).hexdigest()
        elif algorithm == 'sha512':
            return hashlib.sha512(data_bytes).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    def verify_hash(self, data: Union[str, bytes], hash_value: str, algorithm: str = 'sha256') -> bool:
        """Verify data against a hash value."""
        computed_hash = self.hash_data(data, algorithm)
        return computed_hash == hash_value

def create_encryptor() -> DataEncryptor:
    """Create a data encryptor with default settings."""
    return DataEncryptor()

def encrypt_data(data: Union[str, bytes, dict, list], 
                key: Optional[bytes] = None,
                password: Optional[str] = None) -> EncryptionResult:
    """Convenience function to encrypt data."""
    encryptor = create_encryptor()
    return encryptor.encrypt(data, key, password)

def decrypt_data(encrypted_data: bytes, key: bytes) -> bytes:
    """Convenience function to decrypt data."""
    encryptor = create_encryptor()
    return encryptor.decrypt(encrypted_data, key)

if __name__ == "__main__":
    # Example usage
    encryptor = create_encryptor()
    
    # Sample data
    sample_data = {
        "username": "john_doe",
        "password": "secret_password",
        "email": "john@example.com"
    }
    
    # Test encryption with password
    print("=== Encryption Test with Password ===")
    password = "my_secret_password"
    result = encrypt_data(sample_data, password=password)
    
    print(f"Algorithm: {result.algorithm.value}")
    print(f"Salt: {base64.b64encode(result.salt).decode() if result.salt else 'None'}")
    print(f"IV: {base64.b64encode(result.iv).decode() if result.iv else 'None'}")
    print(f"Encrypted data length: {len(result.encrypted_data)} bytes")
    
    # Test decryption
    decrypted_data = decrypt_data(result.encrypted_data, result.key)
    decrypted_json = json.loads(decrypted_data.decode('utf-8'))
    
    print(f"Decryption successful: {sample_data == decrypted_json}")
    
    # Test hashing
    print("\n=== Hashing Test ===")
    hash_value = encryptor.hash_data("test_data", "sha256")
    print(f"SHA256 hash: {hash_value}")
    
    is_valid = encryptor.verify_hash("test_data", hash_value, "sha256")
    print(f"Hash verification: {is_valid}")
