#!/usr/bin/env python3
"""
Security Utilities Module

Provides utility functions for common security operations including
token generation, password validation, security checks, and more.
"""

import os
import re
import hashlib
import secrets
import string
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import ipaddress
import socket

logger = logging.getLogger(__name__)


class PasswordValidator:
    """Advanced password validation utility."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {
            'min_length': 8,
            'max_length': 128,
            'require_uppercase': True,
            'require_lowercase': True,
            'require_digits': True,
            'require_special_chars': True,
            'min_special_chars': 1,
            'min_digits': 1,
            'min_uppercase': 1,
            'min_lowercase': 1,
            'forbidden_patterns': [
                r'password',
                r'123456',
                r'qwerty',
                r'admin',
                r'user',
                r'login',
                r'welcome'
            ],
            'max_repeating_chars': 3,
            'max_sequential_chars': 3
        }
    
    def validate_password(self, password: str) -> Dict[str, Any]:
        """Comprehensive password validation."""
        errors = []
        warnings = []
        score = 0
        
        # Length checks
        if len(password) < self.config['min_length']:
            errors.append(f"Password must be at least {self.config['min_length']} characters long")
        elif len(password) > self.config['max_length']:
            errors.append(f"Password must be no more than {self.config['max_length']} characters long")
        else:
            score += min(len(password) * 2, 40)
        
        # Character type checks
        uppercase_count = sum(1 for c in password if c.isupper())
        lowercase_count = sum(1 for c in password if c.islower())
        digit_count = sum(1 for c in password if c.isdigit())
        special_count = sum(1 for c in password if c in string.punctuation)
        
        if self.config['require_uppercase'] and uppercase_count < self.config['min_uppercase']:
            errors.append(f"Password must contain at least {self.config['min_uppercase']} uppercase letter(s)")
        else:
            score += min(uppercase_count * 5, 20)
        
        if self.config['require_lowercase'] and lowercase_count < self.config['min_lowercase']:
            errors.append(f"Password must contain at least {self.config['min_lowercase']} lowercase letter(s)")
        else:
            score += min(lowercase_count * 5, 20)
        
        if self.config['require_digits'] and digit_count < self.config['min_digits']:
            errors.append(f"Password must contain at least {self.config['min_digits']} digit(s)")
        else:
            score += min(digit_count * 5, 20)
        
        if self.config['require_special_chars'] and special_count < self.config['min_special_chars']:
            errors.append(f"Password must contain at least {self.config['min_special_chars']} special character(s)")
        else:
            score += min(special_count * 10, 30)
        
        # Pattern checks
        password_lower = password.lower()
        for pattern in self.config['forbidden_patterns']:
            if re.search(pattern, password_lower):
                errors.append(f"Password contains forbidden pattern: {pattern}")
                score -= 50
        
        # Repeating character checks
        for i in range(len(password) - self.config['max_repeating_chars'] + 1):
            if len(set(password[i:i + self.config['max_repeating_chars']])) == 1:
                errors.append(f"Password contains more than {self.config['max_repeating_chars']} repeating characters")
                score -= 20
                break
        
        # Sequential character checks
        for i in range(len(password) - self.config['max_sequential_chars'] + 1):
            substr = password[i:i + self.config['max_sequential_chars']]
            if self._is_sequential(substr):
                errors.append(f"Password contains sequential characters: {substr}")
                score -= 20
                break
        
        # Entropy calculation
        entropy = self._calculate_entropy(password)
        if entropy < 50:
            warnings.append("Password has low entropy - consider using more random characters")
        else:
            score += min(entropy // 10, 20)
        
        # Final score adjustment
        score = max(0, min(100, score))
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'score': score,
            'strength': self._get_strength_level(score),
            'entropy': entropy,
            'character_counts': {
                'uppercase': uppercase_count,
                'lowercase': lowercase_count,
                'digits': digit_count,
                'special': special_count
            }
        }
    
    def _is_sequential(self, text: str) -> bool:
        """Check if text contains sequential characters."""
        if len(text) < 2:
            return False
        
        # Check for sequential numbers
        if text.isdigit():
            for i in range(len(text) - 1):
                if int(text[i + 1]) - int(text[i]) != 1:
                    return False
            return True
        
        # Check for sequential letters
        if text.isalpha():
            for i in range(len(text) - 1):
                if ord(text[i + 1].lower()) - ord(text[i].lower()) != 1:
                    return False
            return True
        
        return False
    
    def _calculate_entropy(self, password: str) -> float:
        """Calculate password entropy."""
        if not password:
            return 0
        
        # Count unique characters
        unique_chars = len(set(password))
        
        # Calculate entropy based on character set
        if password.isdigit():
            charset_size = 10
        elif password.isalpha():
            charset_size = 26
        elif password.isalnum():
            charset_size = 36
        else:
            charset_size = 95  # ASCII printable characters
        
        # Use the minimum of unique characters and charset size
        effective_charset = min(unique_chars, charset_size)
        
        # Calculate entropy: log2(charset_size^length)
        entropy = len(password) * (effective_charset ** 0.5)
        
        return entropy
    
    def _get_strength_level(self, score: int) -> str:
        """Get strength level based on score."""
        if score >= 80:
            return "very_strong"
        elif score >= 60:
            return "strong"
        elif score >= 40:
            return "moderate"
        elif score >= 20:
            return "weak"
        else:
            return "very_weak"


class TokenGenerator:
    """Utility for generating various types of tokens."""
    
    @staticmethod
    def generate_secure_token(length: int = 32) -> str:
        """Generate a secure random token."""
        return secrets.token_urlsafe(length)
    
    @staticmethod
    def generate_api_key(prefix: str = "api", length: int = 32) -> str:
        """Generate an API key with prefix."""
        token = secrets.token_urlsafe(length)
        return f"{prefix}_{token}"
    
    @staticmethod
    def generate_session_token() -> str:
        """Generate a session token."""
        return secrets.token_urlsafe(24)
    
    @staticmethod
    def generate_verification_code(length: int = 6) -> str:
        """Generate a numeric verification code."""
        return ''.join(secrets.choice(string.digits) for _ in range(length))
    
    @staticmethod
    def generate_recovery_token() -> str:
        """Generate a recovery token."""
        return secrets.token_urlsafe(16)


class SecurityChecker:
    """Utility for various security checks."""
    
    @staticmethod
    def is_safe_ip_address(ip: str) -> bool:
        """Check if IP address is safe (not private/local)."""
        try:
            ip_obj = ipaddress.ip_address(ip)
            
            # Check if it's a private IP
            if ip_obj.is_private:
                return False
            
            # Check if it's a loopback IP
            if ip_obj.is_loopback:
                return False
            
            # Check if it's a link-local IP
            if ip_obj.is_link_local:
                return False
            
            return True
        except ValueError:
            return False
    
    @staticmethod
    def is_valid_email(email: str) -> bool:
        """Validate email format."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def is_strong_password(password: str, min_score: int = 60) -> bool:
        """Check if password meets strength requirements."""
        validator = PasswordValidator()
        result = validator.validate_password(password)
        return result['is_valid'] and result['score'] >= min_score
    
    @staticmethod
    def check_password_breach(password: str) -> bool:
        """Check if password has been breached (simplified version)."""
        # This is a simplified check - in production, you'd use a service like
        # HaveIBeenPwned API or similar
        common_passwords = [
            'password', '123456', '123456789', 'qwerty', 'abc123',
            'password123', 'admin', 'letmein', 'welcome', 'monkey'
        ]
        return password.lower() in common_passwords
    
    @staticmethod
    def validate_username(username: str) -> Dict[str, Any]:
        """Validate username format and security."""
        errors = []
        warnings = []
        
        if len(username) < 3:
            errors.append("Username must be at least 3 characters long")
        elif len(username) > 30:
            errors.append("Username must be no more than 30 characters long")
        
        if not re.match(r'^[a-zA-Z0-9_-]+$', username):
            errors.append("Username can only contain letters, numbers, underscores, and hyphens")
        
        if username.lower() in ['admin', 'root', 'system', 'guest', 'test']:
            warnings.append("Username is commonly used and may be targeted")
        
        if re.match(r'^[0-9]+$', username):
            warnings.append("Username contains only numbers")
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }


class RateLimiter:
    """Simple rate limiting utility."""
    
    def __init__(self):
        self.attempts = {}
    
    def is_allowed(self, identifier: str, max_attempts: int = 5, 
                   window_minutes: int = 15) -> bool:
        """Check if request is allowed based on rate limiting."""
        now = datetime.now()
        window_start = now - timedelta(minutes=window_minutes)
        
        if identifier not in self.attempts:
            self.attempts[identifier] = []
        
        # Remove old attempts outside the window
        self.attempts[identifier] = [
            attempt for attempt in self.attempts[identifier]
            if attempt > window_start
        ]
        
        # Check if under limit
        if len(self.attempts[identifier]) < max_attempts:
            self.attempts[identifier].append(now)
            return True
        
        return False
    
    def get_remaining_attempts(self, identifier: str, max_attempts: int = 5,
                             window_minutes: int = 15) -> int:
        """Get remaining attempts for an identifier."""
        now = datetime.now()
        window_start = now - timedelta(minutes=window_minutes)
        
        if identifier not in self.attempts:
            return max_attempts
        
        # Remove old attempts outside the window
        self.attempts[identifier] = [
            attempt for attempt in self.attempts[identifier]
            if attempt > window_start
        ]
        
        return max(0, max_attempts - len(self.attempts[identifier]))
    
    def reset_attempts(self, identifier: str):
        """Reset attempts for an identifier."""
        if identifier in self.attempts:
            del self.attempts[identifier]


class SecurityAuditor:
    """Utility for security auditing and logging."""
    
    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file or "security_audit.log"
        self.setup_logging()
    
    def setup_logging(self):
        """Setup security audit logging."""
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(formatter)
        
        self.logger = logging.getLogger('security_audit')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
    
    def log_login_attempt(self, username: str, success: bool, ip_address: str = None,
                         user_agent: str = None):
        """Log login attempt."""
        status = "SUCCESS" if success else "FAILED"
        message = f"LOGIN_ATTEMPT - {status} - User: {username}"
        if ip_address:
            message += f" - IP: {ip_address}"
        if user_agent:
            message += f" - UA: {user_agent}"
        
        self.logger.info(message)
    
    def log_permission_check(self, user_id: str, permission: str, granted: bool,
                           resource: str = None):
        """Log permission check."""
        status = "GRANTED" if granted else "DENIED"
        message = f"PERMISSION_CHECK - {status} - User: {user_id} - Permission: {permission}"
        if resource:
            message += f" - Resource: {resource}"
        
        self.logger.info(message)
    
    def log_security_event(self, event_type: str, details: Dict[str, Any]):
        """Log general security event."""
        message = f"SECURITY_EVENT - {event_type} - {details}"
        self.logger.warning(message)
    
    def log_configuration_change(self, user_id: str, change_type: str, details: str):
        """Log configuration changes."""
        message = f"CONFIG_CHANGE - User: {user_id} - Type: {change_type} - Details: {details}"
        self.logger.info(message)


# Utility functions
def generate_secure_password(length: int = 16, include_symbols: bool = True) -> str:
    """Generate a secure random password."""
    characters = string.ascii_letters + string.digits
    if include_symbols:
        characters += string.punctuation
    
    # Ensure at least one character from each required set
    password = [
        secrets.choice(string.ascii_lowercase),
        secrets.choice(string.ascii_uppercase),
        secrets.choice(string.digits)
    ]
    
    if include_symbols:
        password.append(secrets.choice(string.punctuation))
    
    # Fill the rest randomly
    while len(password) < length:
        password.append(secrets.choice(characters))
    
    # Shuffle the password
    password_list = list(password)
    secrets.SystemRandom().shuffle(password_list)
    
    return ''.join(password_list)


def hash_string(text: str, algorithm: str = 'sha256') -> str:
    """Hash a string using specified algorithm."""
    if algorithm == 'sha256':
        return hashlib.sha256(text.encode()).hexdigest()
    elif algorithm == 'sha512':
        return hashlib.sha512(text.encode()).hexdigest()
    elif algorithm == 'md5':
        return hashlib.md5(text.encode()).hexdigest()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")


def verify_hash(text: str, hash_value: str, algorithm: str = 'sha256') -> bool:
    """Verify a hash against text."""
    return hash_string(text, algorithm) == hash_value


def sanitize_input(text: str) -> str:
    """Basic input sanitization."""
    if not text:
        return ""
    
    # Remove null bytes
    text = text.replace('\x00', '')
    
    # Remove control characters except newlines and tabs
    text = ''.join(char for char in text if char.isprintable() or char in '\n\t')
    
    # Trim whitespace
    text = text.strip()
    
    return text


def is_valid_uuid(uuid_string: str) -> bool:
    """Check if string is a valid UUID."""
    pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(pattern, uuid_string.lower()))


def get_client_ip(request_headers: Dict[str, str]) -> Optional[str]:
    """Extract client IP from request headers."""
    # Check various headers that might contain the real IP
    headers_to_check = [
        'X-Forwarded-For',
        'X-Real-IP',
        'X-Client-IP',
        'X-Forwarded',
        'X-Cluster-Client-IP',
        'Forwarded-For',
        'Forwarded'
    ]
    
    for header in headers_to_check:
        if header in request_headers:
            ip = request_headers[header].split(',')[0].strip()
            if SecurityChecker.is_safe_ip_address(ip):
                return ip
    
    return None
