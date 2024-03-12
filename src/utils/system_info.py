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
def get_process_count(): return len(psutil.pids())

def get_memory_stats() -> Dict[str, Any]:
    """Get detailed memory statistics."""
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()

    return {
        'total_memory': memory.total,
        'available_memory': memory.available,
        'used_memory': memory.used,
        'free_memory': memory.free,
        'memory_percent': memory.percent,
        'cached_memory': memory.cached,
        'buffers': memory.buffers,
        'swap_total': swap.total,
        'swap_used': swap.used,
        'swap_percent': swap.percent
    }
