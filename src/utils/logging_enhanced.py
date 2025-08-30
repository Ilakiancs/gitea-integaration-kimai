#!/usr/bin/env python3
"""
Enhanced Logging Module

Provides advanced logging capabilities including structured logging,
log rotation, multiple output formats, and performance monitoring.
"""

import os
import sys
import json
import logging
import logging.handlers
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
from pathlib import Path
import threading
import time
from dataclasses import dataclass, asdict
import traceback

logger = logging.getLogger(__name__)

@dataclass
class LogEntry:
    """Structured log entry."""
    timestamp: str
    level: str
    logger_name: str
    message: str
    module: str
    function: str
    line_number: int
    thread_id: int
    process_id: int
    extra_fields: Dict[str, Any]

class StructuredFormatter(logging.Formatter):
    """Formatter for structured logging."""
    
    def __init__(self, include_timestamp: bool = True, include_location: bool = True):
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_location = include_location
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured data."""
        log_entry = LogEntry(
            timestamp=datetime.fromtimestamp(record.created).isoformat(),
            level=record.levelname,
            logger_name=record.name,
            message=record.getMessage(),
            module=record.module,
            function=record.funcName,
            line_number=record.lineno,
            thread_id=record.thread,
            process_id=record.process,
            extra_fields=getattr(record, 'extra_fields', {})
        )
        
        # Add exception info if present
        if record.exc_info:
            log_entry.extra_fields['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        return json.dumps(asdict(log_entry), default=str)

class PerformanceFormatter(logging.Formatter):
    """Formatter for performance logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format performance log record."""
        performance_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'duration_ms': getattr(record, 'duration_ms', None),
            'memory_mb': getattr(record, 'memory_mb', None),
            'cpu_percent': getattr(record, 'cpu_percent', None),
            'operation': getattr(record, 'operation', None),
            'repository': getattr(record, 'repository', None),
            'items_processed': getattr(record, 'items_processed', None),
            'success': getattr(record, 'success', None)
        }
        
        return json.dumps(performance_data, default=str)

class ColoredFormatter(logging.Formatter):
    """Colored console formatter."""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Format the message
        formatted = super().format(record)
        
        # Add colors
        if record.levelname in self.COLORS:
            formatted = f"{color}{formatted}{reset}"
        
        return formatted

class EnhancedLogger:
    """Enhanced logger with multiple handlers and formats."""
    
    def __init__(self, name: str, log_dir: str = "logs", level: str = "INFO"):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.level = getattr(logging, level.upper())
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.level)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Setup handlers
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup logging handlers."""
        # Console handler with colors
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.level)
        console_formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler for general logs
        general_log_file = self.log_dir / f"{self.name}.log"
        file_handler = logging.handlers.RotatingFileHandler(
            general_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(self.level)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Structured JSON handler
        json_log_file = self.log_dir / f"{self.name}_structured.json"
        json_handler = logging.handlers.RotatingFileHandler(
            json_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        json_handler.setLevel(self.level)
        json_formatter = StructuredFormatter()
        json_handler.setFormatter(json_formatter)
        self.logger.addHandler(json_handler)
        
        # Error handler for errors only
        error_log_file = self.log_dir / f"{self.name}_errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3
        )
        error_handler.setLevel(logging.ERROR)
        error_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(funcName)s - %(message)s\n'
            'Exception: %(exc_info)s\n'
        )
        error_handler.setFormatter(error_formatter)
        self.logger.addHandler(error_handler)
    
    def log_with_context(self, level: str, message: str, **context):
        """Log message with additional context."""
        extra_fields = context.copy()
        
        # Create a custom log record
        record = self.logger.makeRecord(
            self.name, getattr(logging, level.upper()), 
            "", 0, message, (), None
        )
        record.extra_fields = extra_fields
        
        self.logger.handle(record)
    
    def log_performance(self, operation: str, duration_ms: float, 
                       repository: str = None, items_processed: int = None,
                       success: bool = None, **extra):
        """Log performance metrics."""
        record = self.logger.makeRecord(
            self.name, logging.INFO, "", 0, 
            f"Performance: {operation}", (), None
        )
        
        record.operation = operation
        record.duration_ms = duration_ms
        record.repository = repository
        record.items_processed = items_processed
        record.success = success
        
        for key, value in extra.items():
            setattr(record, key, value)
        
        self.logger.handle(record)
    
    def log_sync_operation(self, operation: str, repository: str, 
                          items_processed: int, items_synced: int,
                          duration: float, success: bool, errors: int = 0):
        """Log sync operation details."""
        self.log_with_context(
            'INFO' if success else 'ERROR',
            f"Sync operation: {operation}",
            operation=operation,
            repository=repository,
            items_processed=items_processed,
            items_synced=items_synced,
            duration_seconds=duration,
            success=success,
            errors=errors
        )
    
    def log_api_call(self, endpoint: str, method: str, status_code: int,
                    duration: float, success: bool, retry_count: int = 0):
        """Log API call details."""
        self.log_with_context(
            'INFO' if success else 'WARNING',
            f"API call: {method} {endpoint}",
            endpoint=endpoint,
            method=method,
            status_code=status_code,
            duration_seconds=duration,
            success=success,
            retry_count=retry_count
        )

class LogManager:
    """Manages multiple loggers and provides centralized logging control."""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.loggers: Dict[str, EnhancedLogger] = {}
        self.log_stats: Dict[str, Dict[str, int]] = {}
        self.stats_lock = threading.Lock()
    
    def get_logger(self, name: str, level: str = "INFO") -> EnhancedLogger:
        """Get or create a logger."""
        if name not in self.loggers:
            self.loggers[name] = EnhancedLogger(name, str(self.log_dir), level)
            self.log_stats[name] = {
                'DEBUG': 0, 'INFO': 0, 'WARNING': 0, 'ERROR': 0, 'CRITICAL': 0
            }
        
        return self.loggers[name]
    
    def set_level(self, name: str, level: str):
        """Set log level for a specific logger."""
        if name in self.loggers:
            level_num = getattr(logging, level.upper())
            self.loggers[name].logger.setLevel(level_num)
            logger.info(f"Set log level for {name} to {level}")
    
    def set_global_level(self, level: str):
        """Set log level for all loggers."""
        level_num = getattr(logging, level.upper())
        for name, enhanced_logger in self.loggers.items():
            enhanced_logger.logger.setLevel(level_num)
        logger.info(f"Set global log level to {level}")
    
    def get_log_files(self) -> List[Dict[str, Any]]:
        """Get information about log files."""
        log_files = []
        
        for log_file in self.log_dir.glob("*.log"):
            try:
                stat = log_file.stat()
                log_files.append({
                    'name': log_file.name,
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'created': datetime.fromtimestamp(stat.st_ctime).isoformat()
                })
            except Exception as e:
                logger.error(f"Error getting info for {log_file}: {e}")
        
        return sorted(log_files, key=lambda x: x['modified'], reverse=True)
    
    def cleanup_old_logs(self, days: int = 30):
        """Remove log files older than specified days."""
        cutoff_date = datetime.now() - timedelta(days=days)
        deleted_count = 0
        
        for log_file in self.log_dir.glob("*.log"):
            try:
                if datetime.fromtimestamp(log_file.stat().st_mtime) < cutoff_date:
                    log_file.unlink()
                    deleted_count += 1
                    logger.info(f"Deleted old log file: {log_file}")
            except Exception as e:
                logger.error(f"Error deleting {log_file}: {e}")
        
        logger.info(f"Cleaned up {deleted_count} old log files")
        return deleted_count
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """Get logging statistics."""
        with self.stats_lock:
            total_logs = sum(sum(stats.values()) for stats in self.log_stats.values())
            
            return {
                'total_loggers': len(self.loggers),
                'total_log_entries': total_logs,
                'logger_stats': self.log_stats.copy(),
                'log_files': self.get_log_files()
            }
    
    def export_logs(self, output_file: str, days: int = 7, format: str = "json"):
        """Export logs for analysis."""
        cutoff_date = datetime.now() - timedelta(days=days)
        exported_logs = []
        
        for log_file in self.log_dir.glob("*.log"):
            try:
                if datetime.fromtimestamp(log_file.stat().st_mtime) >= cutoff_date:
                    with open(log_file, 'r') as f:
                        for line in f:
                            # Try to parse as JSON (structured logs)
                            try:
                                log_entry = json.loads(line.strip())
                                exported_logs.append(log_entry)
                            except json.JSONDecodeError:
                                # Regular log line
                                exported_logs.append({
                                    'file': log_file.name,
                                    'line': line.strip(),
                                    'timestamp': datetime.now().isoformat()
                                })
            except Exception as e:
                logger.error(f"Error reading {log_file}: {e}")
        
        # Sort by timestamp
        exported_logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Write to output file
        with open(output_file, 'w') as f:
            if format == "json":
                json.dump(exported_logs, f, indent=2)
            else:
                for log_entry in exported_logs:
                    f.write(f"{log_entry}\n")
        
        logger.info(f"Exported {len(exported_logs)} log entries to {output_file}")
        return len(exported_logs)

class PerformanceLogger:
    """Specialized logger for performance monitoring."""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create performance logger
        self.logger = logging.getLogger("performance")
        self.logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Performance log file
        perf_log_file = self.log_dir / "performance.log"
        handler = logging.handlers.RotatingFileHandler(
            perf_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        handler.setLevel(logging.INFO)
        formatter = PerformanceFormatter()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def log_operation(self, operation: str, duration_ms: float, 
                     repository: str = None, items_processed: int = None,
                     success: bool = None, memory_usage: float = None,
                     cpu_usage: float = None):
        """Log operation performance."""
        record = self.logger.makeRecord(
            "performance", logging.INFO, "", 0,
            f"Operation: {operation}", (), None
        )
        
        record.operation = operation
        record.duration_ms = duration_ms
        record.repository = repository
        record.items_processed = items_processed
        record.success = success
        record.memory_mb = memory_usage
        record.cpu_percent = cpu_usage
        
        self.logger.handle(record)
    
    def log_api_performance(self, endpoint: str, method: str, duration_ms: float,
                           status_code: int, response_size: int = None):
        """Log API performance."""
        record = self.logger.makeRecord(
            "performance", logging.INFO, "", 0,
            f"API: {method} {endpoint}", (), None
        )
        
        record.operation = f"api_{method.lower()}"
        record.duration_ms = duration_ms
        record.endpoint = endpoint
        record.method = method
        record.status_code = status_code
        record.response_size = response_size
        
        self.logger.handle(record)

class LogFilter(logging.Filter):
    """Custom log filter."""
    
    def __init__(self, include_patterns: List[str] = None, exclude_patterns: List[str] = None):
        super().__init__()
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Filter log records."""
        message = record.getMessage()
        
        # Check exclude patterns first
        for pattern in self.exclude_patterns:
            if pattern in message:
                return False
        
        # Check include patterns
        if self.include_patterns:
            for pattern in self.include_patterns:
                if pattern in message:
                    return True
            return False
        
        return True

def setup_logging(config: Dict[str, Any]) -> LogManager:
    """Setup logging based on configuration."""
    log_dir = config.get('log_dir', 'logs')
    log_level = config.get('log_level', 'INFO')
    
    log_manager = LogManager(log_dir)
    
    # Create main logger
    main_logger = log_manager.get_logger('sync_system', log_level)
    
    # Create specialized loggers
    log_manager.get_logger('api', config.get('api_log_level', 'INFO'))
    log_manager.get_logger('database', config.get('db_log_level', 'INFO'))
    log_manager.get_logger('performance', config.get('perf_log_level', 'INFO'))
    
    return log_manager

def create_log_rotation_schedule():
    """Create a log rotation schedule."""
    return {
        'daily': {
            'when': 'midnight',
            'interval': 1,
            'backup_count': 30
        },
        'weekly': {
            'when': 'W0',  # Monday
            'interval': 1,
            'backup_count': 12
        },
        'monthly': {
            'when': 'M1',  # First day of month
            'interval': 1,
            'backup_count': 12
        }
    }
