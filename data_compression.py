#!/usr/bin/env python3
"""
Data Compression Utility

Provides utilities for compressing and decompressing data using various
algorithms like gzip, bzip2, and lzma for efficient storage and transfer.
"""

import gzip
import bz2
import lzma
import zlib
import base64
import logging
from typing import Dict, List, Optional, Any, Union, Bytes
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class CompressionType(Enum):
    """Types of compression algorithms."""
    GZIP = "gzip"
    BZIP2 = "bzip2"
    LZMA = "lzma"
    ZLIB = "zlib"
    NONE = "none"

@dataclass
class CompressionResult:
    """Result of a compression operation."""
    compressed_data: bytes
    original_size: int
    compressed_size: int
    compression_ratio: float
    algorithm: CompressionType

class DataCompressor:
    """Main data compression utility."""
    
    def __init__(self):
        self.compression_levels = {
            CompressionType.GZIP: 6,
            CompressionType.BZIP2: 9,
            CompressionType.LZMA: 6,
            CompressionType.ZLIB: 6
        }
    
    def compress(self, data: Union[str, bytes, dict, list], 
                algorithm: CompressionType = CompressionType.GZIP,
                level: Optional[int] = None) -> CompressionResult:
        """Compress data using the specified algorithm."""
        # Convert data to bytes if needed
        if isinstance(data, str):
            data_bytes = data.encode('utf-8')
        elif isinstance(data, (dict, list)):
            data_bytes = json.dumps(data).encode('utf-8')
        else:
            data_bytes = data
        
        original_size = len(data_bytes)
        
        # Compress data
        if algorithm == CompressionType.GZIP:
            compressed_data = self._compress_gzip(data_bytes, level)
        elif algorithm == CompressionType.BZIP2:
            compressed_data = self._compress_bzip2(data_bytes, level)
        elif algorithm == CompressionType.LZMA:
            compressed_data = self._compress_lzma(data_bytes, level)
        elif algorithm == CompressionType.ZLIB:
            compressed_data = self._compress_zlib(data_bytes, level)
        elif algorithm == CompressionType.NONE:
            compressed_data = data_bytes
        else:
            raise ValueError(f"Unsupported compression algorithm: {algorithm}")
        
        compressed_size = len(compressed_data)
        compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
        
        return CompressionResult(
            compressed_data=compressed_data,
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=compression_ratio,
            algorithm=algorithm
        )
    
    def decompress(self, compressed_data: bytes, 
                   algorithm: CompressionType = CompressionType.GZIP) -> bytes:
        """Decompress data using the specified algorithm."""
        if algorithm == CompressionType.GZIP:
            return self._decompress_gzip(compressed_data)
        elif algorithm == CompressionType.BZIP2:
            return self._decompress_bzip2(compressed_data)
        elif algorithm == CompressionType.LZMA:
            return self._decompress_lzma(compressed_data)
        elif algorithm == CompressionType.ZLIB:
            return self._decompress_zlib(compressed_data)
        elif algorithm == CompressionType.NONE:
            return compressed_data
        else:
            raise ValueError(f"Unsupported compression algorithm: {algorithm}")
    
    def _compress_gzip(self, data: bytes, level: Optional[int] = None) -> bytes:
        """Compress data using gzip."""
        if level is None:
            level = self.compression_levels[CompressionType.GZIP]
        return gzip.compress(data, compresslevel=level)
    
    def _decompress_gzip(self, data: bytes) -> bytes:
        """Decompress data using gzip."""
        return gzip.decompress(data)
    
    def _compress_bzip2(self, data: bytes, level: Optional[int] = None) -> bytes:
        """Compress data using bzip2."""
        if level is None:
            level = self.compression_levels[CompressionType.BZIP2]
        return bz2.compress(data, compresslevel=level)
    
    def _decompress_bzip2(self, data: bytes) -> bytes:
        """Decompress data using bzip2."""
        return bz2.decompress(data)
    
    def _compress_lzma(self, data: bytes, level: Optional[int] = None) -> bytes:
        """Compress data using lzma."""
        if level is None:
            level = self.compression_levels[CompressionType.LZMA]
        return lzma.compress(data, preset=level)
    
    def _decompress_lzma(self, data: bytes) -> bytes:
        """Decompress data using lzma."""
        return lzma.decompress(data)
    
    def _compress_zlib(self, data: bytes, level: Optional[int] = None) -> bytes:
        """Compress data using zlib."""
        if level is None:
            level = self.compression_levels[CompressionType.ZLIB]
        return zlib.compress(data, level=level)
    
    def _decompress_zlib(self, data: bytes) -> bytes:
        """Decompress data using zlib."""
        return zlib.decompress(data)
    
    def compress_file(self, input_path: str, output_path: str,
                     algorithm: CompressionType = CompressionType.GZIP,
                     level: Optional[int] = None) -> CompressionResult:
        """Compress a file."""
        with open(input_path, 'rb') as f:
            data = f.read()
        
        result = self.compress(data, algorithm, level)
        
        with open(output_path, 'wb') as f:
            f.write(result.compressed_data)
        
        logger.info(f"Compressed {input_path} to {output_path}")
        return result
    
    def decompress_file(self, input_path: str, output_path: str,
                       algorithm: CompressionType = CompressionType.GZIP) -> bytes:
        """Decompress a file."""
        with open(input_path, 'rb') as f:
            compressed_data = f.read()
        
        decompressed_data = self.decompress(compressed_data, algorithm)
        
        with open(output_path, 'wb') as f:
            f.write(decompressed_data)
        
        logger.info(f"Decompressed {input_path} to {output_path}")
        return decompressed_data
    
    def compress_to_base64(self, data: Union[str, bytes, dict, list],
                          algorithm: CompressionType = CompressionType.GZIP,
                          level: Optional[int] = None) -> str:
        """Compress data and encode as base64 string."""
        result = self.compress(data, algorithm, level)
        return base64.b64encode(result.compressed_data).decode('utf-8')
    
    def decompress_from_base64(self, base64_data: str,
                              algorithm: CompressionType = CompressionType.GZIP) -> bytes:
        """Decompress data from base64 string."""
        compressed_data = base64.b64decode(base64_data)
        return self.decompress(compressed_data, algorithm)
    
    def get_compression_info(self, data: Union[str, bytes, dict, list]) -> Dict[str, Any]:
        """Get compression information for all algorithms."""
        info = {}
        
        for algorithm in CompressionType:
            if algorithm == CompressionType.NONE:
                continue
            
            try:
                result = self.compress(data, algorithm)
                info[algorithm.value] = {
                    'original_size': result.original_size,
                    'compressed_size': result.compressed_size,
                    'compression_ratio': result.compression_ratio,
                    'algorithm': algorithm.value
                }
            except Exception as e:
                logger.warning(f"Failed to compress with {algorithm.value}: {e}")
                info[algorithm.value] = {'error': str(e)}
        
        return info
    
    def set_compression_level(self, algorithm: CompressionType, level: int):
        """Set compression level for an algorithm."""
        if level < 1 or level > 9:
            raise ValueError("Compression level must be between 1 and 9")
        
        self.compression_levels[algorithm] = level
        logger.info(f"Set compression level for {algorithm.value} to {level}")

def create_compressor() -> DataCompressor:
    """Create a data compressor with default settings."""
    return DataCompressor()

def compress_data(data: Union[str, bytes, dict, list], 
                 algorithm: CompressionType = CompressionType.GZIP) -> CompressionResult:
    """Convenience function to compress data."""
    compressor = create_compressor()
    return compressor.compress(data, algorithm)

def decompress_data(compressed_data: bytes, 
                   algorithm: CompressionType = CompressionType.GZIP) -> bytes:
    """Convenience function to decompress data."""
    compressor = create_compressor()
    return compressor.decompress(compressed_data, algorithm)

if __name__ == "__main__":
    # Example usage
    compressor = create_compressor()
    
    # Sample data
    sample_data = {
        "users": [
            {"name": "John Doe", "email": "john@example.com", "age": 30},
            {"name": "Jane Smith", "email": "jane@example.com", "age": 25},
            {"name": "Bob Johnson", "email": "bob@example.com", "age": 35}
        ],
        "settings": {
            "theme": "dark",
            "notifications": True,
            "language": "en"
        }
    }
    
    # Test different compression algorithms
    print("=== Compression Test ===")
    info = compressor.get_compression_info(sample_data)
    
    for algorithm, details in info.items():
        if 'error' not in details:
            print(f"{algorithm.upper()}:")
            print(f"  Original size: {details['original_size']} bytes")
            print(f"  Compressed size: {details['compressed_size']} bytes")
            print(f"  Compression ratio: {details['compression_ratio']:.2f}%")
            print()
    
    # Test compression and decompression
    print("=== Compression/Decompression Test ===")
    result = compress_data(sample_data, CompressionType.GZIP)
    print(f"Compressed size: {result.compressed_size} bytes")
    print(f"Compression ratio: {result.compression_ratio:.2f}%")
    
    decompressed = decompress_data(result.compressed_data, CompressionType.GZIP)
    original_json = json.dumps(sample_data)
    decompressed_str = decompressed.decode('utf-8')
    
    print(f"Decompression successful: {original_json == decompressed_str}")
