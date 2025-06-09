#!/usr/bin/env python3
"""
Application constants
"""

# API endpoints
API_VERSION = "v1"
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 1000

# Timeouts
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 300

# Status codes
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_PENDING = "pending"

# Sync modes
SYNC_MODE_FULL = "full"
SYNC_MODE_INCREMENTAL = "incremental"
def retry_on_failure(func, max_retries=3): pass  # TODO: implement
