#!/usr/bin/env python3
"""
Date utility functions
"""

from datetime import datetime, timedelta
from typing import Optional, List

def format_timestamp(timestamp: Optional[float] = None) -> str:
    """Format timestamp for display."""
    if timestamp is None:
        timestamp = datetime.now().timestamp()
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def get_date_range(days: int = 7) -> List[str]:
    """Get date range for the last N days."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates

def is_business_hour(hour: int = None) -> bool:
    """Check if current time is business hour."""
    if hour is None:
        hour = datetime.now().hour
    return 9 <= hour <= 17
