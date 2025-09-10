# Security Module Documentation

## Overview

The Enhanced Security Module provides comprehensive security features for the Gitea-Kimai integration system, including user authentication, role-based access control, password management, JWT token handling, and security auditing.

## Features

### 🔐 Authentication & Authorization
- **JWT Token Management**: Secure token creation, validation, and revocation
- **Role-Based Access Control**: Four user roles with granular permissions
- **Password Security**: Advanced password hashing with PBKDF2 and strength validation
- **Session Management**: Configurable session timeouts and token expiry

### Security Features
- **Rate Limiting**: Protection against brute force attacks
- **Audit Logging**: Comprehensive security event logging
- **Input Validation**: Sanitization and validation of user inputs
- **IP Address Tracking**: Client IP tracking for security monitoring
- **Configuration Validation**: Security configuration validation and recommendations

### Management Tools
- **CLI Interface**: Command-line tools for security management
- **Configuration Management**: Secure configuration generation and validation
- **Health Checks**: Security posture assessment and recommendations
- **User Management**: Create, update, and manage users and roles

## Architecture

### Core Components

#### SecurityManager
The main security management class that orchestrates all security operations.

```python
from security.security import SecurityManager, SecurityConfig

# Create security manager
config = SecurityConfig(secret_key="your-secret-key")
security_manager = SecurityManager(config)
```

#### PasswordManager
Handles password hashing, verification, and strength validation.

```python
from security.security import PasswordManager

password_manager = PasswordManager()
hash_result = password_manager.hash_password("my-password")
is_valid = password_manager.verify_password("my-password", hash_result)
```

#### JWTManager
Manages JWT token creation, validation, and expiry checking.

```python
from security.security import JWTManager

jwt_manager = JWTManager("secret-key")
token = jwt_manager.create_token(user_id, permissions)
payload = jwt_manager.decode_token(token)
```

#### SecurityDatabase
SQLite-based storage for users, tokens, and security events.

```python
from security.security import SecurityDatabase

db = SecurityDatabase("security.db")
user = db.get_user_by_username("john")
```

### User Roles & Permissions

#### Roles
- **VIEWER**: Read-only access to system data
- **OPERATOR**: Can read, write, and sync data
- **ADMIN**: Full access including configuration and user management
- **SUPER_ADMIN**: Complete system access

#### Permissions
- `READ`: View system data and reports
- `WRITE`: Create and modify data
- `DELETE`: Remove data and records
- `ADMIN`: Administrative operations
- `SYNC`: Trigger synchronization operations
- `EXPORT`: Export data and reports
- `CONFIGURE`: Modify system configuration

## Installation & Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

The security module requires:
- `PyJWT==2.8.0`
- `cryptography==41.0.7`

### 2. Generate Security Configuration

```bash
python main.py security generate-config
```

This creates a `security_config.json` file with secure defaults.

### 3. Create Initial Admin User

```bash
python main.py security create-user \
  --username admin \
  --email admin@example.com \
  --password "SecurePass123!" \
  --role super_admin
```

### 4. Validate Configuration

```bash
python main.py security validate-config
```

## Usage

### Command Line Interface

The security module provides a comprehensive CLI for all security operations:

#### User Management

```bash
# Create a new user
python main.py security create-user \
  --username john \
  --email john@example.com \
  --password "SecurePass123!" \
  --role operator

# List all users
python main.py security list-users

# Update user role
python main.py security update-role \
  --user-id user_123 \
  --role admin

# Change password
python main.py security change-password \
  --user-id user_123 \
  --old-password "OldPass123!" \
  --new-password "NewPass456!"
```

#### Authentication

```bash
# Authenticate user
python main.py security authenticate \
  --username john \
  --password "SecurePass123!" \
  --save-token token.json

# Validate token
python main.py security validate-token \
  --token "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
```

#### Password & Token Generation

```bash
# Generate secure password
python main.py security generate-password --length 16

# Validate password strength
python main.py security validate-password --password "MyPassword123!"

# Generate API token
python main.py security generate-token --type api --prefix myapp
```

#### Security Health

```bash
# Run security health check
python main.py security health-check

# Validate configuration
python main.py security validate-config
```

### API Integration

The security module integrates with the existing API to provide secure endpoints:

#### Authentication Endpoints

```bash
# Login
POST /api/v1/auth/login
{
  "username": "john",
  "password": "SecurePass123!"
}

# Register
POST /api/v1/auth/register
{
  "username": "newuser",
  "email": "newuser@example.com",
  "password": "SecurePass123!",
  "role": "viewer"
}

# Logout
POST /api/v1/auth/logout
Authorization: Bearer <token>

# Create user (admin only)
POST /api/v1/users
Authorization: Bearer <admin-token>
{
  "username": "newuser",
  "email": "newuser@example.com",
  "password": "SecurePass123!",
  "role": "operator"
}
```

#### Protected Endpoints

All API endpoints can be protected with authentication and authorization:

```python
# Example: Protected sync endpoint
@require_auth
def _handle_trigger_sync(self, body):
    # Check specific permission
    if not self.security_manager.check_permission(
        self.current_user.token, Permission.SYNC
    ):
        raise ApiError("SYNC permission required", 403)
    
    # Proceed with sync operation
    ...
```

### Programmatic Usage

#### Basic Authentication

```python
from security.security import SecurityManager, SecurityConfig

# Initialize security manager
config = SecurityConfig(secret_key="your-secret-key")
security_manager = SecurityManager(config)

# Authenticate user
token = security_manager.authenticate_user("john", "password123")
if token:
    print(f"Authentication successful: {token.token}")
```

#### User Management

```python
# Create user
user = security_manager.create_user(
    username="newuser",
    email="newuser@example.com",
    password="SecurePass123!",
    role=Role.OPERATOR
)

# Update user role
security_manager.update_user_role(user.id, Role.ADMIN)

# Change password
success = security_manager.change_password(
    user.id, "old-password", "new-password"
)
```

#### Permission Checking

```python
# Check if user has permission
has_permission = security_manager.check_permission(
    token.token, Permission.ADMIN
)

# Get user info from token
user_info = security_manager.get_user_info(token.token)
```

### Middleware Integration

The security module provides middleware for web frameworks:

#### Flask Integration

```python
from flask import Flask
from security.middleware import FlaskSecurityMiddleware
from security.security import SecurityManager

app = Flask(__name__)
security_manager = SecurityManager(config)
security_middleware = FlaskSecurityMiddleware(security_manager, app)

@app.route('/protected')
@security_middleware.require_auth(Permission.READ)
def protected_route():
    return "This is protected"
```

#### FastAPI Integration

```python
from fastapi import FastAPI, Depends
from security.middleware import FastAPISecurityMiddleware
from security.security import SecurityManager

app = FastAPI()
security_manager = SecurityManager(config)
security_middleware = FastAPISecurityMiddleware(security_manager)

@app.get("/protected")
def protected_route(
    current_user = Depends(security_middleware.require_permission(Permission.READ))
):
    return {"message": "This is protected", "user": current_user}
```

## Configuration

### Security Configuration File

The `security_config.json` file contains all security settings:

```json
{
  "secret_key": "your-64-character-secret-key",
  "jwt_expiry_hours": 24,
  "password_min_length": 12,
  "require_special_chars": true,
  "max_login_attempts": 5,
  "lockout_duration_minutes": 30,
  "session_timeout_minutes": 60,
  "require_https": true,
  "security_headers": {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "1; mode=block",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains"
  },
  "audit_logging": true,
  "backup_encryption": true,
  "password_history": 5,
  "password_max_length": 128,
  "rate_limiting": {
    "enabled": true,
    "requests_per_minute": 60,
    "burst_limit": 10
  }
}
```

### Environment Variables

```bash
# Security settings
SECURITY_ENABLED=true
SECURITY_CONFIG_FILE=security_config.json

# API security
API_SECRET_KEY=your-api-secret-key
API_REQUIRE_AUTH=true
API_TOKEN_EXPIRY=86400

# HTTPS and CORS
REQUIRE_HTTPS=true
API_ALLOWED_ORIGINS=https://yourdomain.com
```

## Security Best Practices

### 1. Password Security

- Use strong passwords (minimum 12 characters)
- Include uppercase, lowercase, numbers, and special characters
- Avoid common patterns and dictionary words
- Use the password generator for secure passwords

```bash
python main.py security generate-password --length 16
```

### 2. Token Management

- Use short-lived tokens (24 hours or less)
- Implement token revocation on logout
- Store tokens securely (not in localStorage)
- Use HTTPS for all token transmission

### 3. Access Control

- Follow the principle of least privilege
- Regularly review user roles and permissions
- Implement role-based access control
- Audit access logs regularly

### 4. Configuration Security

- Use strong secret keys (64+ characters)
- Enable HTTPS in production
- Configure secure headers
- Enable audit logging

### 5. Monitoring & Auditing

- Monitor failed login attempts
- Review security audit logs
- Run regular security health checks
- Keep dependencies updated

## Security Health Monitoring

### Health Check

Run comprehensive security health checks:

```bash
python main.py security health-check
```

This provides:
- Security grade (A-F)
- Configuration validation
- File permission checks
- Dependency verification
- Network security assessment
- Recommendations for improvement

### Audit Logging

Security events are logged to `security_audit.log`:

```
2024-01-15 10:30:15 - INFO - LOGIN_ATTEMPT - SUCCESS - User: john - IP: 192.168.1.100
2024-01-15 10:30:20 - INFO - PERMISSION_CHECK - GRANTED - User: john - Permission: read
2024-01-15 10:35:45 - WARNING - SECURITY_EVENT - LOGOUT - User: john
```

## Troubleshooting

### Common Issues

#### 1. "Security manager not available"
- Ensure security dependencies are installed
- Check that `security_config.json` exists and is valid
- Verify `SECURITY_ENABLED=true` in environment

#### 2. "Invalid credentials"
- Check username and password
- Verify user account is active
- Check for rate limiting (too many failed attempts)

#### 3. "Token has expired"
- Tokens expire after configured time (default 24 hours)
- Re-authenticate to get a new token
- Check system clock synchronization

#### 4. "Permission denied"
- Verify user has required role/permission
- Check token is valid and not revoked
- Ensure user account is active

### Debug Mode

Enable debug logging for troubleshooting:

```python
import logging
logging.getLogger('security').setLevel(logging.DEBUG)
```

## Testing

### Unit Tests

Run security module tests:

```bash
python -m pytest tests/unit/test_security.py -v
```

### Integration Tests

Test security with API:

```bash
# Create test user
python main.py security create-user \
  --username testuser \
  --email test@example.com \
  --password "TestPass123!" \
  --role operator

# Authenticate and test API
python main.py security authenticate \
  --username testuser \
  --password "TestPass123!" \
  --save-token test-token.json
```

## Migration Guide

### From Basic Authentication

If migrating from basic authentication:

1. **Backup existing data**
2. **Generate security configuration**
3. **Create admin user**
4. **Migrate existing users** (if any)
5. **Update API endpoints** to use security middleware
6. **Test thoroughly**

### Database Migration

The security module creates its own SQLite database (`security.db`). No migration is needed for existing data.

## Support

For security-related issues:

1. Check the security health check: `python main.py security health-check`
2. Review audit logs: `tail -f security_audit.log`
3. Validate configuration: `python main.py security validate-config`
4. Check documentation and examples

## Security Considerations

### Production Deployment

- Use strong, unique secret keys
- Enable HTTPS with valid certificates
- Configure proper CORS settings
- Set secure file permissions
- Enable audit logging
- Regular security updates

### Compliance

The security module supports:
- Password complexity requirements
- Session management
- Access control
- Audit logging
- Data encryption

### Updates

Keep the security module updated:
- Monitor for security updates
- Update dependencies regularly
- Review security configuration
- Test security features after updates
