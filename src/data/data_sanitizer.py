#!/usr/bin/env python3
"""
Data Sanitizer Utility

Provides utilities for cleaning and normalizing data to ensure safe
processing and storage in the sync system.
"""

import re
import html
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import unicodedata

logger = logging.getLogger(__name__)

class SanitizeLevel(Enum):
    """Sanitization levels."""
    MINIMAL = "minimal"
    MODERATE = "moderate"
    STRICT = "strict"

@dataclass
class SanitizeOptions:
    """Options for data sanitization."""
    level: SanitizeLevel = SanitizeLevel.MODERATE
    max_length: Optional[int] = None
    remove_html: bool = True
    normalize_unicode: bool = True
    strip_whitespace: bool = True
    allowed_tags: List[str] = None
    allowed_attributes: List[str] = None

class DataSanitizer:
    """Main data sanitization utility."""
    
    def __init__(self, options: SanitizeOptions = None):
        self.options = options or SanitizeOptions()
        self._setup_patterns()
    
    def _setup_patterns(self):
        """Setup regex patterns for sanitization."""
        # HTML tags pattern
        self.html_tag_pattern = re.compile(r'<[^>]+>')
        
        # Script tags pattern
        self.script_pattern = re.compile(r'<script[^>]*>.*?</script>', re.IGNORECASE | re.DOTALL)
        
        # Dangerous attributes pattern
        self.dangerous_attr_pattern = re.compile(
            r'\s+(on\w+|javascript:|vbscript:|data:)\s*=', 
            re.IGNORECASE
        )
        
        # Control characters pattern
        self.control_chars_pattern = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')
        
        # Multiple whitespace pattern
        self.multiple_whitespace_pattern = re.compile(r'\s+')
    
    def sanitize_string(self, text: str) -> str:
        """Sanitize a string value."""
        if not isinstance(text, str):
            return str(text) if text is not None else ""
        
        # Normalize unicode
        if self.options.normalize_unicode:
            text = unicodedata.normalize('NFKC', text)
        
        # Remove control characters
        text = self.control_chars_pattern.sub('', text)
        
        # Handle HTML based on level
        if self.options.remove_html:
            if self.options.level == SanitizeLevel.STRICT:
                text = self._remove_all_html(text)
            elif self.options.level == SanitizeLevel.MODERATE:
                text = self._remove_dangerous_html(text)
            else:  # MINIMAL
                text = self._remove_script_tags(text)
        
        # Strip whitespace
        if self.options.strip_whitespace:
            text = text.strip()
            text = self.multiple_whitespace_pattern.sub(' ', text)
        
        # Truncate if max length specified
        if self.options.max_length and len(text) > self.options.max_length:
            text = text[:self.options.max_length]
            logger.warning(f"Text truncated to {self.options.max_length} characters")
        
        return text
    
    def _remove_all_html(self, text: str) -> str:
        """Remove all HTML tags."""
        # First remove script tags
        text = self.script_pattern.sub('', text)
        
        # Then remove all remaining HTML tags
        text = self.html_tag_pattern.sub('', text)
        
        # Unescape HTML entities
        text = html.unescape(text)
        
        return text
    
    def _remove_dangerous_html(self, text: str) -> str:
        """Remove dangerous HTML while preserving safe tags."""
        # Remove script tags
        text = self.script_pattern.sub('', text)
        
        # Remove dangerous attributes
        text = self.dangerous_attr_pattern.sub('', text)
        
        # Remove tags with dangerous attributes
        text = re.sub(r'<[^>]*\s+(on\w+|javascript:|vbscript:|data:)[^>]*>', '', text, flags=re.IGNORECASE)
        
        return text
    
    def _remove_script_tags(self, text: str) -> str:
        """Remove only script tags."""
        return self.script_pattern.sub('', text)
    
    def sanitize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize a dictionary recursively."""
        if not isinstance(data, dict):
            return data
        
        sanitized = {}
        for key, value in data.items():
            # Sanitize key
            sanitized_key = self.sanitize_string(str(key))
            
            # Sanitize value
            if isinstance(value, str):
                sanitized_value = self.sanitize_string(value)
            elif isinstance(value, dict):
                sanitized_value = self.sanitize_dict(value)
            elif isinstance(value, list):
                sanitized_value = self.sanitize_list(value)
            else:
                sanitized_value = value
            
            sanitized[sanitized_key] = sanitized_value
        
        return sanitized
    
    def sanitize_list(self, data: List[Any]) -> List[Any]:
        """Sanitize a list recursively."""
        if not isinstance(data, list):
            return data
        
        sanitized = []
        for item in data:
            if isinstance(item, str):
                sanitized_item = self.sanitize_string(item)
            elif isinstance(item, dict):
                sanitized_item = self.sanitize_dict(item)
            elif isinstance(item, list):
                sanitized_item = self.sanitize_list(item)
            else:
                sanitized_item = item
            
            sanitized.append(sanitized_item)
        
        return sanitized
    
    def sanitize_data(self, data: Any) -> Any:
        """Sanitize any type of data."""
        if isinstance(data, str):
            return self.sanitize_string(data)
        elif isinstance(data, dict):
            return self.sanitize_dict(data)
        elif isinstance(data, list):
            return self.sanitize_list(data)
        else:
            return data
    
    def validate_email(self, email: str) -> bool:
        """Validate and sanitize email address."""
        if not email:
            return False
        
        # Basic email pattern
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        # Sanitize email
        sanitized_email = self.sanitize_string(email.lower())
        
        return bool(email_pattern.match(sanitized_email))
    
    def validate_url(self, url: str) -> bool:
        """Validate and sanitize URL."""
        if not url:
            return False
        
        # Basic URL pattern
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        # Sanitize URL
        sanitized_url = self.sanitize_string(url)
        
        return bool(url_pattern.match(sanitized_url))
    
    def normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace in text."""
        if not isinstance(text, str):
            return str(text) if text is not None else ""
        
        # Replace multiple whitespace with single space
        text = self.multiple_whitespace_pattern.sub(' ', text)
        
        # Strip leading/trailing whitespace
        return text.strip()
    
    def remove_special_chars(self, text: str, keep_chars: str = "") -> str:
        """Remove special characters from text."""
        if not isinstance(text, str):
            return str(text) if text is not None else ""
        
        # Keep alphanumeric, spaces, and specified characters
        pattern = f'[^a-zA-Z0-9\\s{re.escape(keep_chars)}]'
        return re.sub(pattern, '', text)
    
    def truncate_text(self, text: str, max_length: int, suffix: str = "...") -> str:
        """Truncate text to specified length."""
        if not isinstance(text, str):
            text = str(text) if text is not None else ""
        
        if len(text) <= max_length:
            return text
        
        # Calculate how much space we have for the main text
        available_length = max_length - len(suffix)
        
        # Truncate and add suffix
        return text[:available_length] + suffix

def create_sanitizer(level: SanitizeLevel = SanitizeLevel.MODERATE, 
                    max_length: int = None) -> DataSanitizer:
    """Create a data sanitizer with specified options."""
    options = SanitizeOptions(
        level=level,
        max_length=max_length
    )
    return DataSanitizer(options)

def sanitize_text(text: str, level: SanitizeLevel = SanitizeLevel.MODERATE) -> str:
    """Convenience function to sanitize text."""
    sanitizer = create_sanitizer(level)
    return sanitizer.sanitize_string(text)

if __name__ == "__main__":
    # Example usage
    sanitizer = create_sanitizer(SanitizeLevel.STRICT)
    
    # Test data
    test_data = {
        'title': '<script>alert("xss")</script>Hello World',
        'description': 'This is a <b>test</b> description with multiple    spaces',
        'email': 'test@example.com',
        'url': 'https://example.com',
        'content': 'Normal text content'
    }
    
    # Sanitize the data
    sanitized_data = sanitizer.sanitize_data(test_data)
    
    print("Original data:")
    print(test_data)
    print("\nSanitized data:")
    print(sanitized_data)
    
    # Test email validation
    print(f"\nEmail validation: {sanitizer.validate_email('test@example.com')}")
    print(f"URL validation: {sanitizer.validate_url('https://example.com')}")
