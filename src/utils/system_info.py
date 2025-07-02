#!/usr/bin/env python3
"""
System information utilities
"""

import psutil
import platform
from typing import Dict, Any

def get_system_info() -> Dict[str, Any]:
    """Get basic system information."""
    return {
        'platform': platform.system(),
        'platform_release': platform.release(),
        'architecture': platform.architecture()[0],
        'hostname': platform.node(),
        'cpu_count': psutil.cpu_count(),
        'memory_total': psutil.virtual_memory().total,
        'python_version': platform.python_version(),
        'boot_time': psutil.boot_time()
    }

def get_resource_usage() -> Dict[str, Any]:
    """Get current resource usage."""
    memory = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=1)
    
    return {
        'cpu_percent': cpu,
        'memory_percent': memory.percent,
        'memory_used': memory.used,
        'memory_available': memory.available,
        'disk_usage': psutil.disk_usage('/').percent,
        'load_average': psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0
    }
