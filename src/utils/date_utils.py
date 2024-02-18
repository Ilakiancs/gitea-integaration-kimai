#!/usr/bin/env python3
"""
Date utility functions
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, List
import pytz

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

def time_ago(timestamp: float) -> str:
    """Get human readable time ago string."""
    diff = datetime.now().timestamp() - timestamp

    if diff < 60:
        return "just now"
    elif diff < 3600:
        minutes = int(diff / 60)
        return f"{minutes} minutes ago"
    elif diff < 86400:
        hours = int(diff / 3600)
        return f"{hours} hours ago"
    else:
        days = int(diff / 86400)
        return f"{days} days ago"
def format_duration(seconds): return f"{seconds:.2f}s"
def get_weekday(date=None): return (date or datetime.now()).strftime("%A")

def get_utc_timestamp() -> float:
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc).timestamp()

def convert_to_timezone(dt: datetime, target_tz: str = 'UTC') -> datetime:
    """Convert datetime to specified timezone."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    target_timezone = pytz.timezone(target_tz)
    return dt.astimezone(target_timezone)
