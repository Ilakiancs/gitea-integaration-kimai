#!/usr/bin/env python3
"""
Security and Authentication Module

Provides comprehensive security features including JWT token management,
role-based access control, password hashing, and security utilities.
"""

import os
import json
import logging
import hashlib
import hmac
import secrets
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import sqlite3
from pathlib import Path
import jwt
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

logger = logging.getLogger(__name__)

class Permission(Enum):
    """System permissions."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    SYNC = "sync"
    EXPORT = "export"
    CONFIGURE = "configure"

class Role(Enum):
    """User roles."""
    VIEWER = "viewer"
    OPERATOR = "operator"
    ADMIN = "admin"
    SUPER_ADMIN = "super_admin"

@dataclass
class User:
    """User information."""
    id: str
    username: str
    email: str
    role: Role
    permissions: List[Permission]
    created_at: datetime
    last_login: Optional[datetime] = None
    is_active: bool = True
    password_hash: str = ""

@dataclass
class Token:
    """JWT token information."""
    token: str
    user_id: str
    issued_at: datetime
    expires_at: datetime
    permissions: List[Permission]
    is_revoked: bool = False

@dataclass
class SecurityConfig:
    """Security configuration."""
    secret_key: str
    jwt_expiry_hours: int = 24
    password_min_length: int = 8
    require_special_chars: bool = True
    max_login_attempts: int = 5
    lockout_duration_minutes: int = 30
    session_timeout_minutes: int = 60
    session_invalidation_timeout: int = 1440  # 24 hours in minutes

class PasswordManager:
    """Manages password hashing and validation."""

    def __init__(self, salt_length: int = 32):
        self.salt_length = salt_length

    def hash_password(self, password: str) -> str:
        """Hash a password using PBKDF2."""
        salt = secrets.token_bytes(self.salt_length)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        salt_b64 = base64.urlsafe_b64encode(salt).decode()
        return f"{salt_b64}:{key.decode()}"

    def verify_password(self, password: str, password_hash: str) -> bool:
        """Verify a password against its hash."""
        try:
            salt_b64, key_b64 = password_hash.split(':')
            salt = base64.urlsafe_b64decode(salt_b64)
            key = base64.urlsafe_b64decode(key_b64)

            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            kdf.verify(password.encode(), key)
            return True
        except Exception:
            return False

    def validate_password_strength(self, password: str) -> Dict[str, Any]:
        """Validate password strength."""
        errors = []
        warnings = []

        if len(password) < 8:
            errors.append("Password must be at least 8 characters long")

        if not any(c.isupper() for c in password):
            warnings.append("Password should contain uppercase letters")

        if not any(c.islower() for c in password):
            warnings.append("Password should contain lowercase letters")

        if not any(c.isdigit() for c in password):
            warnings.append("Password should contain numbers")

        if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            warnings.append("Password should contain special characters")

        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'strength_score': self._calculate_strength_score(password)
        }

    def _calculate_strength_score(self, password: str) -> int:
        """Calculate password strength score (0-100)."""
        score = 0

        # Length bonus
        score += min(len(password) * 4, 40)

        # Character variety bonus
        if any(c.isupper() for c in password):
            score += 10
        if any(c.islower() for c in password):
            score += 10
        if any(c.isdigit() for c in password):
            score += 10
        if any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            score += 10

        # Deduct for common patterns
        if password.lower() in ['password', '123456', 'qwerty']:
            score -= 50

        return max(0, min(100, score))

class JWTManager:
    """Manages JWT token creation and validation."""

    def __init__(self, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm

    def create_token(self, user_id: str, permissions: List[Permission],
                    expiry_hours: int = 24) -> str:
        """Create a JWT token."""
        now = datetime.utcnow()
        payload = {
            'user_id': user_id,
            'permissions': [p.value for p in permissions],
            'iat': now,
            'exp': now + timedelta(hours=expiry_hours)
        }

        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def decode_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Decode and validate a JWT token."""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("JWT token has expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            return None

    def is_token_expired(self, token: str) -> bool:
        """Check if a token is expired."""
        payload = self.decode_token(token)
        if not payload:
            return True

        exp_timestamp = payload.get('exp')
        if not exp_timestamp:
            return True

        return datetime.utcnow().timestamp() > exp_timestamp

class SecurityDatabase:
    """Manages security-related data storage."""

    def __init__(self, db_path: str = "security.db"):
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize the security database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    role TEXT NOT NULL,
                    permissions TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    last_login TEXT,
                    is_active INTEGER DEFAULT 1,
                    password_hash TEXT NOT NULL
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    token TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    issued_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    permissions TEXT NOT NULL,
                    is_revoked INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS login_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    success INTEGER DEFAULT 0,
                    ip_address TEXT
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_username
                ON users(username)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tokens_user_id
                ON tokens(user_id)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_login_attempts_username
                ON login_attempts(username)
            """)

            conn.commit()

    def save_user(self, user: User):
        """Save user to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO users
                (id, username, email, role, permissions, created_at, last_login, is_active, password_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user.id,
                user.username,
                user.email,
                user.role.value,
                json.dumps([p.value for p in user.permissions]),
                user.created_at.isoformat(),
                user.last_login.isoformat() if user.last_login else None,
                1 if user.is_active else 0,
                user.password_hash
            ))
            conn.commit()

    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM users WHERE id = ?
            """, (user_id,))
            row = cursor.fetchone()

            if row:
                return User(
                    id=row[0],
                    username=row[1],
                    email=row[2],
                    role=Role(row[3]),
                    permissions=[Permission(p) for p in json.loads(row[4])],
                    created_at=datetime.fromisoformat(row[5]),
                    last_login=datetime.fromisoformat(row[6]) if row[6] else None,
                    is_active=bool(row[7]),
                    password_hash=row[8]
                )
            return None

    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM users WHERE username = ?
            """, (username,))
            row = cursor.fetchone()

            if row:
                return User(
                    id=row[0],
                    username=row[1],
                    email=row[2],
                    role=Role(row[3]),
                    permissions=[Permission(p) for p in json.loads(row[4])],
                    created_at=datetime.fromisoformat(row[5]),
                    last_login=datetime.fromisoformat(row[6]) if row[6] else None,
                    is_active=bool(row[7]),
                    password_hash=row[8]
                )
            return None

    def save_token(self, token: Token):
        """Save token to database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO tokens
                (token, user_id, issued_at, expires_at, permissions, is_revoked)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                token.token,
                token.user_id,
                token.issued_at.isoformat(),
                token.expires_at.isoformat(),
                json.dumps([p.value for p in token.permissions]),
                1 if token.is_revoked else 0
            ))
            conn.commit()

    def get_token(self, token_str: str) -> Optional[Token]:
        """Get token by token string."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT * FROM tokens WHERE token = ?
            """, (token_str,))
            row = cursor.fetchone()

            if row:
                return Token(
                    token=row[0],
                    user_id=row[1],
                    issued_at=datetime.fromisoformat(row[2]),
                    expires_at=datetime.fromisoformat(row[3]),
                    permissions=[Permission(p) for p in json.loads(row[4])],
                    is_revoked=bool(row[5])
                )
            return None

    def revoke_token(self, token_str: str):
        """Revoke a token."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE tokens SET is_revoked = 1 WHERE token = ?
            """, (token_str,))
            conn.commit()

    def record_login_attempt(self, username: str, success: bool, ip_address: str = None):
        """Record a login attempt."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO login_attempts (username, timestamp, success, ip_address)
                VALUES (?, ?, ?, ?)
            """, (
                username,
                datetime.now().isoformat(),
                1 if success else 0,
                ip_address
            ))
            conn.commit()

    def get_failed_login_attempts(self, username: str, hours: int = 1) -> int:
        """Get number of failed login attempts for a user."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM login_attempts
                WHERE username = ? AND success = 0 AND timestamp >= ?
            """, (username, cutoff_time.isoformat()))

            return cursor.fetchone()[0]

class RoleManager:
    """Manages user roles and permissions."""

    def __init__(self):
        self.role_permissions = {
            Role.VIEWER: [Permission.READ],
            Role.OPERATOR: [Permission.READ, Permission.WRITE, Permission.SYNC],
            Role.ADMIN: [Permission.READ, Permission.WRITE, Permission.DELETE,
                        Permission.SYNC, Permission.EXPORT, Permission.CONFIGURE],
            Role.SUPER_ADMIN: [p for p in Permission]  # All permissions
        }

    def get_permissions_for_role(self, role: Role) -> List[Permission]:
        """Get permissions for a role."""
        return self.role_permissions.get(role, [])

    def has_permission(self, user_permissions: List[Permission],
                      required_permission: Permission) -> bool:
        """Check if user has required permission."""
        return required_permission in user_permissions

    def has_any_permission(self, user_permissions: List[Permission],
                          required_permissions: List[Permission]) -> bool:
        """Check if user has any of the required permissions."""
        return any(p in user_permissions for p in required_permissions)

    def has_all_permissions(self, user_permissions: List[Permission],
                           required_permissions: List[Permission]) -> bool:
        """Check if user has all required permissions."""
        return all(p in user_permissions for p in required_permissions)

class SecurityManager:
    """Main security management system."""

    def __init__(self, config: SecurityConfig):
        self.config = config
        self.database = SecurityDatabase()
        self.password_manager = PasswordManager()
        self.jwt_manager = JWTManager(config.secret_key)
        self.role_manager = RoleManager()

        self._setup_default_users()

    def _setup_default_users(self):
        """Setup default users if none exist."""
        users = self.database.get_all_users()
        if not users:
            # Create default admin user
            admin_user = User(
                id="admin_001",
                username="admin",
                email="admin@example.com",
                role=Role.SUPER_ADMIN,
                permissions=self.role_manager.get_permissions_for_role(Role.SUPER_ADMIN),
                created_at=datetime.now()
            )
            admin_user.password_hash = self.password_manager.hash_password("admin123")
            self.database.save_user(admin_user)
            logger.info("Created default admin user")

    def authenticate_user(self, username: str, password: str, ip_address: str = None) -> Optional[Token]:
        """Authenticate a user and return a token."""
        # Check for too many failed attempts
        failed_attempts = self.database.get_failed_login_attempts(username)
        if failed_attempts >= self.config.max_login_attempts:
            logger.warning(f"Too many failed login attempts for user: {username}")
            return None

        # Get user
        user = self.database.get_user_by_username(username)
        if not user or not user.is_active:
            self.database.record_login_attempt(username, False, ip_address)
            return None

        # Verify password
        if not self.password_manager.verify_password(password, user.password_hash):
            self.database.record_login_attempt(username, False, ip_address)
            return None

        # Update last login
        user.last_login = datetime.now()
        self.database.save_user(user)

        # Record successful login
        self.database.record_login_attempt(username, True, ip_address)

        # Create token
        token_str = self.jwt_manager.create_token(
            user.id,
            user.permissions,
            self.config.jwt_expiry_hours
        )

        token = Token(
            token=token_str,
            user_id=user.id,
            issued_at=datetime.now(),
            expires_at=datetime.now() + timedelta(hours=self.config.jwt_expiry_hours),
            permissions=user.permissions
        )

        self.database.save_token(token)
        logger.info(f"User {username} authenticated successfully")

        return token

    def validate_token(self, token_str: str) -> Optional[User]:
        """Validate a token and return the associated user."""
        # Check if token is in database and not revoked
        token = self.database.get_token(token_str)
        if not token or token.is_revoked:
            return None

        # Check if token is expired
        if self.jwt_manager.is_token_expired(token_str):
            return None

        # Get user
        user = self.database.get_user(token.user_id)
        if not user or not user.is_active:
            return None

        return user

    def revoke_token(self, token_str: str):
        """Revoke a token."""
        self.database.revoke_token(token_str)
        logger.info("Token revoked")

    def create_user(self, username: str, email: str, password: str,
                   role: Role = Role.VIEWER) -> Optional[User]:
        """Create a new user."""
        # Validate password strength
        strength = self.password_manager.validate_password_strength(password)
        if not strength['is_valid']:
            raise ValueError(f"Password validation failed: {strength['errors']}")

        # Check if user already exists
        if self.database.get_user_by_username(username):
            raise ValueError("Username already exists")

        # Create user
        user = User(
            id=f"user_{int(time.time() * 1000)}",
            username=username,
            email=email,
            role=role,
            permissions=self.role_manager.get_permissions_for_role(role),
            created_at=datetime.now()
        )

        user.password_hash = self.password_manager.hash_password(password)
        self.database.save_user(user)

        logger.info(f"Created user: {username}")
        return user

    def update_user_role(self, user_id: str, new_role: Role):
        """Update user role."""
        user = self.database.get_user(user_id)
        if not user:
            raise ValueError("User not found")

        user.role = new_role
        user.permissions = self.role_manager.get_permissions_for_role(new_role)
        self.database.save_user(user)

        logger.info(f"Updated role for user {user.username} to {new_role.value}")

    def change_password(self, user_id: str, old_password: str, new_password: str) -> bool:
        """Change user password."""
        user = self.database.get_user(user_id)
        if not user:
            return False

        # Verify old password
        if not self.password_manager.verify_password(old_password, user.password_hash):
            return False

        # Validate new password strength
        strength = self.password_manager.validate_password_strength(new_password)
        if not strength['is_valid']:
            raise ValueError(f"Password validation failed: {strength['errors']}")

        # Update password
        user.password_hash = self.password_manager.hash_password(new_password)
        self.database.save_user(user)

        logger.info(f"Password changed for user: {user.username}")
        return True

    def check_permission(self, token_str: str, required_permission: Permission) -> bool:
        """Check if token has required permission."""
        user = self.validate_token(token_str)
        if not user:
            return False

        return self.role_manager.has_permission(user.permissions, required_permission)

    def get_user_info(self, token_str: str) -> Optional[Dict[str, Any]]:
        """Get user information from token."""
        user = self.validate_token(token_str)
        if not user:
            return None

        return {
            'id': user.id,
            'username': user.username,
            'email': user.email,
            'role': user.role.value,
            'permissions': [p.value for p in user.permissions],
            'created_at': user.created_at.isoformat(),
            'last_login': user.last_login.isoformat() if user.last_login else None
        }

def create_security_manager(config_file: str = "security_config.json") -> SecurityManager:
    """Create security manager from configuration."""
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)
    else:
        # Generate a secure secret key
        secret_key = secrets.token_urlsafe(32)

        config_data = {
            'secret_key': secret_key,
            'jwt_expiry_hours': 24,
            'password_min_length': 8,
            'require_special_chars': True,
            'max_login_attempts': 5,
            'lockout_duration_minutes': 30,
            'session_timeout_minutes': 60
        }

        # Save config
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)

    config = SecurityConfig(**config_data)
    return SecurityManager(config)

if __name__ == "__main__":
    # Example usage
    security_manager = create_security_manager()

    # Create a test user
    try:
        user = security_manager.create_user(
            username="testuser",
            email="test@example.com",
            password="SecurePass123!",
            role=Role.OPERATOR
        )
        print(f"Created user: {user.username}")
    except ValueError as e:
        print(f"User creation failed: {e}")

    # Authenticate user
    token = security_manager.authenticate_user("testuser", "SecurePass123!")
    if token:
        print(f"Authentication successful, token: {token.token[:20]}...")

        # Check permissions
        has_read = security_manager.check_permission(token.token, Permission.READ)
        has_admin = security_manager.check_permission(token.token, Permission.ADMIN)

        print(f"Has READ permission: {has_read}")
        print(f"Has ADMIN permission: {has_admin}")

        # Get user info
        user_info = security_manager.get_user_info(token.token)
        print(f"User info: {user_info}")
    else:
        print("Authentication failed")
