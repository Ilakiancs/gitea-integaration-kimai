#!/usr/bin/env python3
"""
Backup Compression Module

Advanced compression functionality with multiple algorithms,
optimization features, and adaptive compression selection.
"""

import os
import gzip
import bz2
import lzma
import zlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import threading
import time

logger = logging.getLogger(__name__)

@dataclass
class CompressionResult:
    """Result of compression operation."""
    original_size: int
    compressed_size: int
    compression_ratio: float
    compression_time: float
    algorithm: str
    level: int

class BackupCompression:
    """Advanced compression handler for backup files."""
    
    def __init__(self):
        self.supported_algorithms = {
            'gzip': self._compress_gzip,
            'bz2': self._compress_bz2,
            'lzma': self._compress_lzma,
            'zlib': self._compress_zlib
        }
        self.lock = threading.RLock()
    
    def compress_file(self, input_path: Path, output_path: Path, 
                     algorithm: str = "gzip", level: int = 6) -> CompressionResult:
        """Compress a file using specified algorithm and level."""
        if algorithm not in self.supported_algorithms:
            raise ValueError(f"Unsupported compression algorithm: {algorithm}")
        
        start_time = time.time()
        original_size = input_path.stat().st_size
        
        try:
            # Perform compression
            compressed_size = self.supported_algorithms[algorithm](input_path, output_path, level)
            
            compression_time = time.time() - start_time
            compression_ratio = 1 - (compressed_size / original_size) if original_size > 0 else 0
            
            result = CompressionResult(
                original_size=original_size,
                compressed_size=compressed_size,
                compression_ratio=compression_ratio,
                compression_time=compression_time,
                algorithm=algorithm,
                level=level
            )
            
            logger.info(f"File compressed: {input_path} -> {output_path}")
            logger.info(f"Compression ratio: {compression_ratio*100:.1f}%")
            logger.info(f"Compression time: {compression_time:.2f}s")
            
            return result
            
        except Exception as e:
            logger.error(f"Compression failed: {e}")
            raise
    
    def _compress_gzip(self, input_path: Path, output_path: Path, level: int) -> int:
        """Compress file using gzip."""
        with open(input_path, 'rb') as infile:
            with gzip.open(output_path, 'wb', compresslevel=level) as outfile:
                outfile.write(infile.read())
        
        return output_path.stat().st_size
    
    def _compress_bz2(self, input_path: Path, output_path: Path, level: int) -> int:
        """Compress file using bzip2."""
        with open(input_path, 'rb') as infile:
            with bz2.open(output_path, 'wb', compresslevel=level) as outfile:
                outfile.write(infile.read())
        
        return output_path.stat().st_size
    
    def _compress_lzma(self, input_path: Path, output_path: Path, level: int) -> int:
        """Compress file using LZMA."""
        with open(input_path, 'rb') as infile:
            with lzma.open(output_path, 'wb', preset=level) as outfile:
                outfile.write(infile.read())
        
        return output_path.stat().st_size
    
    def _compress_zlib(self, input_path: Path, output_path: Path, level: int) -> int:
        """Compress file using zlib."""
        with open(input_path, 'rb') as infile:
            with open(output_path, 'wb') as outfile:
                compressor = zlib.compressobj(level)
                while True:
                    chunk = infile.read(8192)
                    if not chunk:
                        break
                    compressed_chunk = compressor.compress(chunk)
                    if compressed_chunk:
                        outfile.write(compressed_chunk)
                
                # Write remaining compressed data
                remaining = compressor.flush()
                if remaining:
                    outfile.write(remaining)
        
        return output_path.stat().st_size
    
    def decompress_file(self, input_path: Path, output_path: Path, 
                       algorithm: str = "gzip") -> bool:
        """Decompress a file."""
        try:
            if algorithm == "gzip":
                with gzip.open(input_path, 'rb') as infile:
                    with open(output_path, 'wb') as outfile:
                        outfile.write(infile.read())
            
            elif algorithm == "bz2":
                with bz2.open(input_path, 'rb') as infile:
                    with open(output_path, 'wb') as outfile:
                        outfile.write(infile.read())
            
            elif algorithm == "lzma":
                with lzma.open(input_path, 'rb') as infile:
                    with open(output_path, 'wb') as outfile:
                        outfile.write(infile.read())
            
            elif algorithm == "zlib":
                with open(input_path, 'rb') as infile:
                    with open(output_path, 'wb') as outfile:
                        decompressor = zlib.decompressobj()
                        while True:
                            chunk = infile.read(8192)
                            if not chunk:
                                break
                            decompressed_chunk = decompressor.decompress(chunk)
                            if decompressed_chunk:
                                outfile.write(decompressed_chunk)
                        
                        # Write remaining decompressed data
                        remaining = decompressor.flush()
                        if remaining:
                            outfile.write(remaining)
            
            else:
                raise ValueError(f"Unsupported decompression algorithm: {algorithm}")
            
            logger.info(f"File decompressed: {input_path} -> {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Decompression failed: {e}")
            return False
    
    def benchmark_compression(self, input_path: Path, algorithms: List[str] = None) -> Dict[str, CompressionResult]:
        """Benchmark different compression algorithms on a file."""
        if algorithms is None:
            algorithms = list(self.supported_algorithms.keys())
        
        results = {}
        temp_dir = Path("temp_compression")
        temp_dir.mkdir(exist_ok=True)
        
        try:
            for algorithm in algorithms:
                if algorithm not in self.supported_algorithms:
                    continue
                
                # Test different compression levels
                best_result = None
                
                for level in range(1, 10):
                    try:
                        output_path = temp_dir / f"test_{algorithm}_{level}.compressed"
                        result = self.compress_file(input_path, output_path, algorithm, level)
                        
                        if best_result is None or result.compression_ratio > best_result.compression_ratio:
                            best_result = result
                        
                        # Clean up test file
                        output_path.unlink()
                        
                    except Exception as e:
                        logger.warning(f"Compression test failed for {algorithm} level {level}: {e}")
                        continue
                
                if best_result:
                    results[algorithm] = best_result
            
            return results
            
        finally:
            # Clean up temp directory
            if temp_dir.exists():
                for file in temp_dir.glob("*"):
                    file.unlink()
                temp_dir.rmdir()
    
    def get_optimal_compression(self, input_path: Path, 
                              target_ratio: float = 0.5) -> Tuple[str, int]:
        """Find optimal compression algorithm and level for target ratio."""
        benchmark_results = self.benchmark_compression(input_path)
        
        if not benchmark_results:
            return "gzip", 6  # Default fallback
        
        # Find algorithm closest to target ratio
        best_algorithm = None
        best_level = 6
        best_diff = float('inf')
        
        for algorithm, result in benchmark_results.items():
            diff = abs(result.compression_ratio - target_ratio)
            if diff < best_diff:
                best_diff = diff
                best_algorithm = algorithm
                best_level = result.level
        
        return best_algorithm, best_level
    
    def get_compression_info(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Get information about a compressed file."""
        try:
            # Try to detect compression type
            with open(file_path, 'rb') as f:
                header = f.read(10)
            
            if header.startswith(b'\x1f\x8b'):
                return {'algorithm': 'gzip', 'compressed': True}
            elif header.startswith(b'BZ'):
                return {'algorithm': 'bz2', 'compressed': True}
            elif header.startswith(b'\xfd7zXZ'):
                return {'algorithm': 'lzma', 'compressed': True}
            else:
                return {'algorithm': 'unknown', 'compressed': False}
                
        except Exception as e:
            logger.error(f"Failed to get compression info: {e}")
            return None

def create_compression() -> BackupCompression:
    """Create and return a compression instance."""
    return BackupCompression()
