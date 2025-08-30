#!/usr/bin/env python3
"""
Data Checksum Utility

Provides utilities for calculating and verifying checksums to ensure
data integrity during storage and transfer operations.
"""

import hashlib
import os
import logging
from typing import Dict, List, Optional, Any, Union, Bytes
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class ChecksumType(Enum):
    """Types of checksum algorithms."""
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"
    BLAKE2B = "blake2b"
    BLAKE2S = "blake2s"

@dataclass
class ChecksumResult:
    """Result of a checksum calculation."""
    checksum: str
    algorithm: ChecksumType
    data_size: int
    calculation_time: float

class DataChecksum:
    """Main data checksum utility."""
    
    def __init__(self):
        self.supported_algorithms = {
            ChecksumType.MD5: hashlib.md5,
            ChecksumType.SHA1: hashlib.sha1,
            ChecksumType.SHA256: hashlib.sha256,
            ChecksumType.SHA512: hashlib.sha512,
            ChecksumType.BLAKE2B: hashlib.blake2b,
            ChecksumType.BLAKE2S: hashlib.blake2s
        }
    
    def calculate_checksum(self, data: Union[str, bytes, dict, list], 
                          algorithm: ChecksumType = ChecksumType.SHA256) -> ChecksumResult:
        """Calculate checksum for data."""
        import time
        
        start_time = time.time()
        
        # Convert data to bytes if needed
        if isinstance(data, str):
            data_bytes = data.encode('utf-8')
        elif isinstance(data, (dict, list)):
            data_bytes = json.dumps(data, sort_keys=True).encode('utf-8')
        else:
            data_bytes = data
        
        # Calculate checksum
        if algorithm in self.supported_algorithms:
            hash_func = self.supported_algorithms[algorithm]
            if algorithm in [ChecksumType.BLAKE2B, ChecksumType.BLAKE2S]:
                checksum = hash_func(data_bytes).hexdigest()
            else:
                checksum = hash_func(data_bytes).hexdigest()
        else:
            raise ValueError(f"Unsupported checksum algorithm: {algorithm}")
        
        calculation_time = time.time() - start_time
        
        return ChecksumResult(
            checksum=checksum,
            algorithm=algorithm,
            data_size=len(data_bytes),
            calculation_time=calculation_time
        )
    
    def calculate_file_checksum(self, file_path: str, 
                               algorithm: ChecksumType = ChecksumType.SHA256) -> ChecksumResult:
        """Calculate checksum for a file."""
        import time
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        start_time = time.time()
        
        hash_func = self.supported_algorithms[algorithm]
        hasher = hash_func()
        
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        
        checksum = hasher.hexdigest()
        calculation_time = time.time() - start_time
        
        file_size = os.path.getsize(file_path)
        
        return ChecksumResult(
            checksum=checksum,
            algorithm=algorithm,
            data_size=file_size,
            calculation_time=calculation_time
        )
    
    def verify_checksum(self, data: Union[str, bytes, dict, list], 
                       expected_checksum: str,
                       algorithm: ChecksumType = ChecksumType.SHA256) -> bool:
        """Verify data against an expected checksum."""
        result = self.calculate_checksum(data, algorithm)
        return result.checksum.lower() == expected_checksum.lower()
    
    def verify_file_checksum(self, file_path: str, 
                           expected_checksum: str,
                           algorithm: ChecksumType = ChecksumType.SHA256) -> bool:
        """Verify file against an expected checksum."""
        result = self.calculate_file_checksum(file_path, algorithm)
        return result.checksum.lower() == expected_checksum.lower()
    
    def calculate_multiple_checksums(self, data: Union[str, bytes, dict, list],
                                   algorithms: List[ChecksumType] = None) -> Dict[str, ChecksumResult]:
        """Calculate checksums using multiple algorithms."""
        if algorithms is None:
            algorithms = [ChecksumType.MD5, ChecksumType.SHA1, ChecksumType.SHA256]
        
        results = {}
        for algorithm in algorithms:
            try:
                result = self.calculate_checksum(data, algorithm)
                results[algorithm.value] = result
            except Exception as e:
                logger.warning(f"Failed to calculate {algorithm.value} checksum: {e}")
        
        return results
    
    def calculate_file_multiple_checksums(self, file_path: str,
                                        algorithms: List[ChecksumType] = None) -> Dict[str, ChecksumResult]:
        """Calculate checksums for a file using multiple algorithms."""
        if algorithms is None:
            algorithms = [ChecksumType.MD5, ChecksumType.SHA1, ChecksumType.SHA256]
        
        results = {}
        for algorithm in algorithms:
            try:
                result = self.calculate_file_checksum(file_path, algorithm)
                results[algorithm.value] = result
            except Exception as e:
                logger.warning(f"Failed to calculate {algorithm.value} checksum for {file_path}: {e}")
        
        return results
    
    def create_checksum_file(self, file_path: str, 
                           output_path: str,
                           algorithm: ChecksumType = ChecksumType.SHA256) -> ChecksumResult:
        """Create a checksum file in standard format."""
        result = self.calculate_file_checksum(file_path, algorithm)
        
        # Create checksum file content
        filename = os.path.basename(file_path)
        checksum_content = f"{result.checksum}  {filename}\n"
        
        with open(output_path, 'w') as f:
            f.write(checksum_content)
        
        logger.info(f"Created checksum file: {output_path}")
        return result
    
    def verify_checksum_file(self, checksum_file_path: str, 
                           data_file_path: str,
                           algorithm: ChecksumType = ChecksumType.SHA256) -> bool:
        """Verify a file against its checksum file."""
        if not os.path.exists(checksum_file_path):
            raise FileNotFoundError(f"Checksum file not found: {checksum_file_path}")
        
        # Read checksum from file
        with open(checksum_file_path, 'r') as f:
            checksum_line = f.readline().strip()
        
        # Parse checksum (format: checksum filename)
        parts = checksum_line.split()
        if len(parts) < 2:
            raise ValueError(f"Invalid checksum file format: {checksum_line}")
        
        expected_checksum = parts[0]
        return self.verify_file_checksum(data_file_path, expected_checksum, algorithm)
    
    def get_checksum_info(self, data: Union[str, bytes, dict, list]) -> Dict[str, Any]:
        """Get comprehensive checksum information for data."""
        info = {}
        
        for algorithm in ChecksumType:
            try:
                result = self.calculate_checksum(data, algorithm)
                info[algorithm.value] = {
                    'checksum': result.checksum,
                    'data_size': result.data_size,
                    'calculation_time': result.calculation_time
                }
            except Exception as e:
                logger.warning(f"Failed to calculate {algorithm.value} checksum: {e}")
                info[algorithm.value] = {'error': str(e)}
        
        return info
    
    def compare_checksums(self, checksum1: str, checksum2: str, 
                         algorithm: ChecksumType = ChecksumType.SHA256) -> bool:
        """Compare two checksums."""
        return checksum1.lower() == checksum2.lower()
    
    def validate_checksum_format(self, checksum: str, 
                               algorithm: ChecksumType = ChecksumType.SHA256) -> bool:
        """Validate checksum format."""
        if not checksum:
            return False
        
        # Check length based on algorithm
        expected_lengths = {
            ChecksumType.MD5: 32,
            ChecksumType.SHA1: 40,
            ChecksumType.SHA256: 64,
            ChecksumType.SHA512: 128,
            ChecksumType.BLAKE2B: 128,
            ChecksumType.BLAKE2S: 64
        }
        
        expected_length = expected_lengths.get(algorithm, 64)
        if len(checksum) != expected_length:
            return False
        
        # Check if it's a valid hexadecimal string
        try:
            int(checksum, 16)
            return True
        except ValueError:
            return False

def create_checksum() -> DataChecksum:
    """Create a data checksum utility with default settings."""
    return DataChecksum()

def calculate_checksum(data: Union[str, bytes, dict, list], 
                      algorithm: ChecksumType = ChecksumType.SHA256) -> ChecksumResult:
    """Convenience function to calculate checksum."""
    checksum = create_checksum()
    return checksum.calculate_checksum(data, algorithm)

def verify_checksum(data: Union[str, bytes, dict, list], 
                   expected_checksum: str,
                   algorithm: ChecksumType = ChecksumType.SHA256) -> bool:
    """Convenience function to verify checksum."""
    checksum = create_checksum()
    return checksum.verify_checksum(data, expected_checksum, algorithm)

if __name__ == "__main__":
    # Example usage
    checksum = create_checksum()
    
    # Sample data
    sample_data = {
        "users": [
            {"name": "John Doe", "email": "john@example.com"},
            {"name": "Jane Smith", "email": "jane@example.com"}
        ],
        "settings": {
            "theme": "dark",
            "notifications": True
        }
    }
    
    # Test different checksum algorithms
    print("=== Checksum Test ===")
    info = checksum.get_checksum_info(sample_data)
    
    for algorithm, details in info.items():
        if 'error' not in details:
            print(f"{algorithm.upper()}:")
            print(f"  Checksum: {details['checksum']}")
            print(f"  Data size: {details['data_size']} bytes")
            print(f"  Calculation time: {details['calculation_time']:.4f}s")
            print()
    
    # Test verification
    print("=== Verification Test ===")
    result = calculate_checksum(sample_data, ChecksumType.SHA256)
    is_valid = verify_checksum(sample_data, result.checksum, ChecksumType.SHA256)
    print(f"Checksum verification: {is_valid}")
    
    # Test invalid checksum
    is_valid = verify_checksum(sample_data, "invalid_checksum", ChecksumType.SHA256)
    print(f"Invalid checksum verification: {is_valid}")
