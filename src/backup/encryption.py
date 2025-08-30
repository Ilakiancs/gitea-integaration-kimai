#!/usr/bin/env python3
"""
Backup Encryption Module

Encryption functionality for backup files with multiple algorithms
and secure key management.
"""

import os
import base64
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import hashlib

logger = logging.getLogger(__name__)

class BackupEncryption:
    """Encryption handler for backup files."""
    
    def __init__(self, key_file: str = "encryption.key"):
        self.key_file = Path(key_file)
        self.key = None
        self._load_or_generate_key()
    
    def _load_or_generate_key(self):
        """Load existing key or generate new one."""
        if self.key_file.exists():
            try:
                with open(self.key_file, 'rb') as f:
                    self.key = f.read()
                logger.info("Encryption key loaded from file")
            except Exception as e:
                logger.error(f"Failed to load encryption key: {e}")
                self._generate_new_key()
        else:
            self._generate_new_key()
    
    def _generate_new_key(self):
        """Generate a new encryption key."""
        self.key = Fernet.generate_key()
        try:
            with open(self.key_file, 'wb') as f:
                f.write(self.key)
            logger.info("New encryption key generated and saved")
        except Exception as e:
            logger.error(f"Failed to save encryption key: {e}")
    
    def encrypt_file(self, file_path: Path, algorithm: str = "AES-256") -> bool:
        """Encrypt a file using specified algorithm."""
        try:
            if algorithm == "AES-256":
                return self._encrypt_aes256(file_path)
            elif algorithm == "ChaCha20":
                return self._encrypt_chacha20(file_path)
            else:
                logger.error(f"Unsupported encryption algorithm: {algorithm}")
                return False
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return False
    
    def decrypt_file(self, file_path: Path, algorithm: str = "AES-256") -> bool:
        """Decrypt a file using specified algorithm."""
        try:
            if algorithm == "AES-256":
                return self._decrypt_aes256(file_path)
            elif algorithm == "ChaCha20":
                return self._decrypt_chacha20(file_path)
            else:
                logger.error(f"Unsupported encryption algorithm: {algorithm}")
                return False
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return False
    
    def _encrypt_aes256(self, file_path: Path) -> bool:
        """Encrypt file using AES-256."""
        from cryptography.hazmat.primitives import padding
        
        # Read file data
        with open(file_path, 'rb') as f:
            data = f.read()
        
        # Generate IV
        iv = os.urandom(16)
        
        # Create cipher
        cipher = Cipher(
            algorithms.AES(self.key[:32]),  # Use first 32 bytes as key
            modes.CBC(iv),
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        
        # Pad data
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data) + padder.finalize()
        
        # Encrypt
        encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
        
        # Write encrypted file
        encrypted_path = file_path.with_suffix('.encrypted')
        with open(encrypted_path, 'wb') as f:
            f.write(iv + encrypted_data)
        
        # Remove original file
        file_path.unlink()
        
        logger.info(f"File encrypted with AES-256: {encrypted_path}")
        return True
    
    def _decrypt_aes256(self, file_path: Path) -> bool:
        """Decrypt file using AES-256."""
        from cryptography.hazmat.primitives import padding
        
        # Read encrypted data
        with open(file_path, 'rb') as f:
            data = f.read()
        
        # Extract IV and encrypted data
        iv = data[:16]
        encrypted_data = data[16:]
        
        # Create cipher
        cipher = Cipher(
            algorithms.AES(self.key[:32]),
            modes.CBC(iv),
            backend=default_backend()
        )
        decryptor = cipher.decryptor()
        
        # Decrypt
        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
        
        # Unpad data
        unpadder = padding.PKCS7(128).unpadder()
        original_data = unpadder.update(decrypted_data) + unpadder.finalize()
        
        # Write decrypted file
        decrypted_path = file_path.with_suffix('').with_suffix('')
        with open(decrypted_path, 'wb') as f:
            f.write(original_data)
        
        # Remove encrypted file
        file_path.unlink()
        
        logger.info(f"File decrypted with AES-256: {decrypted_path}")
        return True
    
    def _encrypt_chacha20(self, file_path: Path) -> bool:
        """Encrypt file using ChaCha20."""
        # Read file data
        with open(file_path, 'rb') as f:
            data = f.read()
        
        # Generate nonce
        nonce = os.urandom(12)
        
        # Create cipher
        cipher = Cipher(
            algorithms.ChaCha20(self.key[:32], nonce),
            mode=None,
            backend=default_backend()
        )
        encryptor = cipher.encryptor()
        
        # Encrypt
        encrypted_data = encryptor.update(data) + encryptor.finalize()
        
        # Write encrypted file
        encrypted_path = file_path.with_suffix('.encrypted')
        with open(encrypted_path, 'wb') as f:
            f.write(nonce + encrypted_data)
        
        # Remove original file
        file_path.unlink()
        
        logger.info(f"File encrypted with ChaCha20: {encrypted_path}")
        return True
    
    def _decrypt_chacha20(self, file_path: Path) -> bool:
        """Decrypt file using ChaCha20."""
        # Read encrypted data
        with open(file_path, 'rb') as f:
            data = f.read()
        
        # Extract nonce and encrypted data
        nonce = data[:12]
        encrypted_data = data[12:]
        
        # Create cipher
        cipher = Cipher(
            algorithms.ChaCha20(self.key[:32], nonce),
            mode=None,
            backend=default_backend()
        )
        decryptor = cipher.decryptor()
        
        # Decrypt
        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
        
        # Write decrypted file
        decrypted_path = file_path.with_suffix('').with_suffix('')
        with open(decrypted_path, 'wb') as f:
            f.write(decrypted_data)
        
        # Remove encrypted file
        file_path.unlink()
        
        logger.info(f"File decrypted with ChaCha20: {decrypted_path}")
        return True
    
    def get_key_info(self) -> Dict[str, Any]:
        """Get information about the encryption key."""
        return {
            'key_file': str(self.key_file),
            'key_exists': self.key_file.exists(),
            'key_length': len(self.key) if self.key else 0,
            'key_hash': hashlib.sha256(self.key).hexdigest()[:16] if self.key else None
        }

def create_encryption() -> BackupEncryption:
    """Create and return an encryption instance."""
    return BackupEncryption()
