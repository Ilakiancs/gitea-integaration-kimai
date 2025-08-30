#!/usr/bin/env python3
"""
Unit tests for the security module.

Tests all security components including password management, JWT handling,
user authentication, role management, and database operations.
"""

import unittest
import tempfile
import os
import json
import time
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Add src to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from security.security import (
    SecurityManager, SecurityConfig, PasswordManager, JWTManager,
    SecurityDatabase, RoleManager, User, Token, Role, Permission
)


class TestPasswordManager(unittest.TestCase):
    """Test password management functionality."""
    
    def setUp(self):
        self.password_manager = PasswordManager()
    
    def test_hash_password(self):
        """Test password hashing."""
        password = "testpassword123"
        hash_result = self.password_manager.hash_password(password)
        
        # Check that hash is not the same as password
        self.assertNotEqual(password, hash_result)
        
        # Check that hash contains salt and key separated by colon
        self.assertIn(':', hash_result)
        parts = hash_result.split(':')
        self.assertEqual(len(parts), 2)
    
    def test_verify_password(self):
        """Test password verification."""
        password = "testpassword123"
        hash_result = self.password_manager.hash_password(password)
        
        # Should verify correctly
        self.assertTrue(self.password_manager.verify_password(password, hash_result))
        
        # Should fail with wrong password
        self.assertFalse(self.password_manager.verify_password("wrongpassword", hash_result))
    
    def test_validate_password_strength(self):
        """Test password strength validation."""
        # Test weak password
        weak_password = "123"
        result = self.password_manager.validate_password_strength(weak_password)
        self.assertFalse(result['is_valid'])
        self.assertGreater(len(result['errors']), 0)
        
        # Test strong password
        strong_password = "SecurePass123!"
        result = self.password_manager.validate_password_strength(strong_password)
        self.assertTrue(result['is_valid'])
        self.assertEqual(len(result['errors']), 0)
        self.assertGreater(result['strength_score'], 50)
    
    def test_calculate_strength_score(self):
        """Test password strength scoring."""
        # Test very weak password
        score = self.password_manager._calculate_strength_score("123")
        self.assertLess(score, 20)
        
        # Test strong password
        score = self.password_manager._calculate_strength_score("SecurePass123!")
        self.assertGreater(score, 70)


class TestJWTManager(unittest.TestCase):
    """Test JWT token management."""
    
    def setUp(self):
        self.secret_key = "test_secret_key_12345"
        self.jwt_manager = JWTManager(self.secret_key)
    
    def test_create_token(self):
        """Test JWT token creation."""
        user_id = "test_user_123"
        permissions = [Permission.READ, Permission.WRITE]
        
        token = self.jwt_manager.create_token(user_id, permissions)
        
        # Token should be a string
        self.assertIsInstance(token, str)
        self.assertGreater(len(token), 0)
    
    def test_decode_token(self):
        """Test JWT token decoding."""
        user_id = "test_user_123"
        permissions = [Permission.READ, Permission.WRITE]
        
        token = self.jwt_manager.create_token(user_id, permissions)
        payload = self.jwt_manager.decode_token(token)
        
        # Should decode successfully
        self.assertIsNotNone(payload)
        self.assertEqual(payload['user_id'], user_id)
        self.assertEqual(payload['permissions'], [p.value for p in permissions])
    
    def test_decode_invalid_token(self):
        """Test decoding invalid token."""
        invalid_token = "invalid.token.here"
        payload = self.jwt_manager.decode_token(invalid_token)
        
        # Should return None for invalid token
        self.assertIsNone(payload)
    
    def test_token_expiry(self):
        """Test token expiry checking."""
        user_id = "test_user_123"
        permissions = [Permission.READ]
        
        # Create token with 1 second expiry
        token = self.jwt_manager.create_token(user_id, permissions, expiry_hours=1/3600)
        
        # Should not be expired immediately
        self.assertFalse(self.jwt_manager.is_token_expired(token))
        
        # Wait and check again (in real test, would use time mocking)
        with patch('security.security.datetime') as mock_datetime:
            mock_datetime.utcnow.return_value = datetime.now() + timedelta(hours=2)
            self.assertTrue(self.jwt_manager.is_token_expired(token))


class TestSecurityDatabase(unittest.TestCase):
    """Test security database operations."""
    
    def setUp(self):
        # Use temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.database = SecurityDatabase(self.temp_db.name)
    
    def tearDown(self):
        # Clean up temporary database
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_save_and_get_user(self):
        """Test user save and retrieval."""
        user = User(
            id="test_user_123",
            username="testuser",
            email="test@example.com",
            role=Role.OPERATOR,
            permissions=[Permission.READ, Permission.WRITE],
            created_at=datetime.now()
        )
        user.password_hash = "hashed_password"
        
        # Save user
        self.database.save_user(user)
        
        # Retrieve user
        retrieved_user = self.database.get_user(user.id)
        
        # Should match
        self.assertIsNotNone(retrieved_user)
        self.assertEqual(retrieved_user.id, user.id)
        self.assertEqual(retrieved_user.username, user.username)
        self.assertEqual(retrieved_user.email, user.email)
        self.assertEqual(retrieved_user.role, user.role)
    
    def test_get_user_by_username(self):
        """Test user retrieval by username."""
        user = User(
            id="test_user_123",
            username="testuser",
            email="test@example.com",
            role=Role.OPERATOR,
            permissions=[Permission.READ, Permission.WRITE],
            created_at=datetime.now()
        )
        user.password_hash = "hashed_password"
        
        self.database.save_user(user)
        
        retrieved_user = self.database.get_user_by_username("testuser")
        
        self.assertIsNotNone(retrieved_user)
        self.assertEqual(retrieved_user.username, "testuser")
    
    def test_save_and_get_token(self):
        """Test token save and retrieval."""
        token = Token(
            token="test_token_123",
            user_id="test_user_123",
            issued_at=datetime.now(),
            expires_at=datetime.now() + timedelta(hours=1),
            permissions=[Permission.READ, Permission.WRITE]
        )
        
        self.database.save_token(token)
        
        retrieved_token = self.database.get_token("test_token_123")
        
        self.assertIsNotNone(retrieved_token)
        self.assertEqual(retrieved_token.token, token.token)
        self.assertEqual(retrieved_token.user_id, token.user_id)
    
    def test_revoke_token(self):
        """Test token revocation."""
        token = Token(
            token="test_token_123",
            user_id="test_user_123",
            issued_at=datetime.now(),
            expires_at=datetime.now() + timedelta(hours=1),
            permissions=[Permission.READ]
        )
        
        self.database.save_token(token)
        
        # Revoke token
        self.database.revoke_token("test_token_123")
        
        retrieved_token = self.database.get_token("test_token_123")
        self.assertTrue(retrieved_token.is_revoked)
    
    def test_login_attempts(self):
        """Test login attempt recording."""
        username = "testuser"
        
        # Record failed attempt
        self.database.record_login_attempt(username, False, "192.168.1.1")
        
        # Record successful attempt
        self.database.record_login_attempt(username, True, "192.168.1.1")
        
        # Check failed attempts
        failed_attempts = self.database.get_failed_login_attempts(username, hours=1)
        self.assertEqual(failed_attempts, 1)


class TestRoleManager(unittest.TestCase):
    """Test role and permission management."""
    
    def setUp(self):
        self.role_manager = RoleManager()
    
    def test_get_permissions_for_role(self):
        """Test getting permissions for roles."""
        viewer_permissions = self.role_manager.get_permissions_for_role(Role.VIEWER)
        self.assertEqual(viewer_permissions, [Permission.READ])
        
        admin_permissions = self.role_manager.get_permissions_for_role(Role.ADMIN)
        self.assertIn(Permission.READ, admin_permissions)
        self.assertIn(Permission.WRITE, admin_permissions)
        self.assertIn(Permission.DELETE, admin_permissions)
    
    def test_has_permission(self):
        """Test permission checking."""
        user_permissions = [Permission.READ, Permission.WRITE]
        
        # Should have READ permission
        self.assertTrue(self.role_manager.has_permission(user_permissions, Permission.READ))
        
        # Should not have ADMIN permission
        self.assertFalse(self.role_manager.has_permission(user_permissions, Permission.ADMIN))
    
    def test_has_any_permission(self):
        """Test checking for any of multiple permissions."""
        user_permissions = [Permission.READ, Permission.WRITE]
        required_permissions = [Permission.READ, Permission.ADMIN]
        
        # Should have at least one required permission
        self.assertTrue(self.role_manager.has_any_permission(user_permissions, required_permissions))
        
        required_permissions = [Permission.ADMIN, Permission.CONFIGURE]
        
        # Should not have any required permissions
        self.assertFalse(self.role_manager.has_any_permission(user_permissions, required_permissions))
    
    def test_has_all_permissions(self):
        """Test checking for all required permissions."""
        user_permissions = [Permission.READ, Permission.WRITE, Permission.SYNC]
        required_permissions = [Permission.READ, Permission.WRITE]
        
        # Should have all required permissions
        self.assertTrue(self.role_manager.has_all_permissions(user_permissions, required_permissions))
        
        required_permissions = [Permission.READ, Permission.ADMIN]
        
        # Should not have all required permissions
        self.assertFalse(self.role_manager.has_all_permissions(user_permissions, required_permissions))


class TestSecurityManager(unittest.TestCase):
    """Test main security manager."""
    
    def setUp(self):
        self.config = SecurityConfig(
            secret_key="test_secret_key_12345",
            jwt_expiry_hours=1,
            max_login_attempts=3
        )
        self.security_manager = SecurityManager(self.config)
    
    def test_create_user(self):
        """Test user creation."""
        user = self.security_manager.create_user(
            username="newuser",
            email="newuser@example.com",
            password="SecurePass123!",
            role=Role.OPERATOR
        )
        
        self.assertIsNotNone(user)
        self.assertEqual(user.username, "newuser")
        self.assertEqual(user.email, "newuser@example.com")
        self.assertEqual(user.role, Role.OPERATOR)
    
    def test_create_user_duplicate_username(self):
        """Test creating user with duplicate username."""
        # Create first user
        self.security_manager.create_user(
            username="duplicateuser",
            email="user1@example.com",
            password="SecurePass123!",
            role=Role.VIEWER
        )
        
        # Try to create second user with same username
        with self.assertRaises(ValueError):
            self.security_manager.create_user(
                username="duplicateuser",
                email="user2@example.com",
                password="SecurePass123!",
                role=Role.VIEWER
            )
    
    def test_create_user_weak_password(self):
        """Test creating user with weak password."""
        with self.assertRaises(ValueError):
            self.security_manager.create_user(
                username="weakuser",
                email="weak@example.com",
                password="123",
                role=Role.VIEWER
            )
    
    def test_authenticate_user(self):
        """Test user authentication."""
        # Create user
        self.security_manager.create_user(
            username="authuser",
            email="auth@example.com",
            password="SecurePass123!",
            role=Role.OPERATOR
        )
        
        # Authenticate with correct password
        token = self.security_manager.authenticate_user("authuser", "SecurePass123!")
        self.assertIsNotNone(token)
        
        # Authenticate with wrong password
        token = self.security_manager.authenticate_user("authuser", "wrongpassword")
        self.assertIsNone(token)
    
    def test_validate_token(self):
        """Test token validation."""
        # Create and authenticate user
        self.security_manager.create_user(
            username="tokenuser",
            email="token@example.com",
            password="SecurePass123!",
            role=Role.ADMIN
        )
        
        token = self.security_manager.authenticate_user("tokenuser", "SecurePass123!")
        
        # Validate token
        user = self.security_manager.validate_token(token.token)
        self.assertIsNotNone(user)
        self.assertEqual(user.username, "tokenuser")
    
    def test_check_permission(self):
        """Test permission checking."""
        # Create user with specific role
        self.security_manager.create_user(
            username="permuser",
            email="perm@example.com",
            password="SecurePass123!",
            role=Role.OPERATOR
        )
        
        token = self.security_manager.authenticate_user("permuser", "SecurePass123!")
        
        # Should have READ permission
        self.assertTrue(self.security_manager.check_permission(token.token, Permission.READ))
        
        # Should not have ADMIN permission
        self.assertFalse(self.security_manager.check_permission(token.token, Permission.ADMIN))
    
    def test_change_password(self):
        """Test password change."""
        # Create user
        self.security_manager.create_user(
            username="passuser",
            email="pass@example.com",
            password="OldPass123!",
            role=Role.VIEWER
        )
        
        # Get user ID
        user = self.security_manager.database.get_user_by_username("passuser")
        
        # Change password
        success = self.security_manager.change_password(
            user.id, "OldPass123!", "NewPass456!"
        )
        self.assertTrue(success)
        
        # Try to authenticate with new password
        token = self.security_manager.authenticate_user("passuser", "NewPass456!")
        self.assertIsNotNone(token)
    
    def test_get_user_info(self):
        """Test getting user information from token."""
        # Create and authenticate user
        self.security_manager.create_user(
            username="infouser",
            email="info@example.com",
            password="SecurePass123!",
            role=Role.OPERATOR
        )
        
        token = self.security_manager.authenticate_user("infouser", "SecurePass123!")
        
        # Get user info
        user_info = self.security_manager.get_user_info(token.token)
        self.assertIsNotNone(user_info)
        self.assertEqual(user_info['username'], "infouser")
        self.assertEqual(user_info['email'], "info@example.com")
        self.assertEqual(user_info['role'], Role.OPERATOR.value)


class TestSecurityConfig(unittest.TestCase):
    """Test security configuration."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = SecurityConfig(secret_key="test_key")
        
        self.assertEqual(config.secret_key, "test_key")
        self.assertEqual(config.jwt_expiry_hours, 24)
        self.assertEqual(config.password_min_length, 8)
        self.assertTrue(config.require_special_chars)
        self.assertEqual(config.max_login_attempts, 5)
        self.assertEqual(config.lockout_duration_minutes, 30)
        self.assertEqual(config.session_timeout_minutes, 60)
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = SecurityConfig(
            secret_key="custom_key",
            jwt_expiry_hours=48,
            password_min_length=12,
            require_special_chars=False,
            max_login_attempts=10,
            lockout_duration_minutes=60,
            session_timeout_minutes=120
        )
        
        self.assertEqual(config.secret_key, "custom_key")
        self.assertEqual(config.jwt_expiry_hours, 48)
        self.assertEqual(config.password_min_length, 12)
        self.assertFalse(config.require_special_chars)
        self.assertEqual(config.max_login_attempts, 10)
        self.assertEqual(config.lockout_duration_minutes, 60)
        self.assertEqual(config.session_timeout_minutes, 120)


if __name__ == '__main__':
    unittest.main()
