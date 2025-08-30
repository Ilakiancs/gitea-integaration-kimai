#!/usr/bin/env python3
"""
API Module for Gitea-Kimai Integration

This module provides a simple REST API for the Gitea-Kimai integration,
allowing external systems to interact with the synchronization process.

Usage:
  python api.py
"""

import os
import sys
import json
import logging
import sqlite3
import argparse
import subprocess
import hashlib
from pathlib import Path
from datetime import datetime
from functools import wraps
from urllib.parse import parse_qs

import jwt
from dotenv import load_dotenv
from http.server import HTTPServer, BaseHTTPRequestHandler

# Import security modules
from security.security import SecurityManager, SecurityConfig, Permission, Role
from security.middleware import SecurityMiddleware
from security.utils import SecurityChecker, RateLimiter, SecurityAuditor

# Load environment variables
load_dotenv()

# API configuration
API_ENABLED = os.getenv('API_ENABLED', 'false').lower() == 'true'
API_HOST = os.getenv('API_HOST', 'localhost')
API_PORT = int(os.getenv('API_PORT', '8080'))
API_PREFIX = os.getenv('API_PREFIX', '/api/v1')
API_SECRET_KEY = os.getenv('API_SECRET_KEY', '')
API_TOKEN_EXPIRY = int(os.getenv('API_TOKEN_EXPIRY', '86400'))  # 24 hours in seconds
API_REQUIRE_AUTH = os.getenv('API_REQUIRE_AUTH', 'true').lower() == 'true'
API_ALLOWED_ORIGINS = os.getenv('API_ALLOWED_ORIGINS', '*')

# Security configuration
SECURITY_ENABLED = os.getenv('SECURITY_ENABLED', 'true').lower() == 'true'
SECURITY_CONFIG_FILE = os.getenv('SECURITY_CONFIG_FILE', 'security_config.json')

# Database configuration
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('api.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ApiError(Exception):
    """Custom API error class."""
    def __init__(self, message, status_code=400):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

def require_auth(func):
    """Decorator to require authentication for API endpoints."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not API_REQUIRE_AUTH:
            return func(self, *args, **kwargs)

        auth_header = self.headers.get('Authorization')
        if not auth_header:
            raise ApiError("Authorization header is required", 401)

        try:
            scheme, token = auth_header.split(' ', 1)
            if scheme.lower() != 'bearer':
                raise ApiError("Authorization scheme must be Bearer", 401)

            # Use security manager if available
            if hasattr(self, 'security_manager') and self.security_manager:
                user = self.security_manager.validate_token(token)
                if not user:
                    raise ApiError("Invalid or expired token", 401)
                self.current_user = user
            else:
                # Fallback to simple JWT verification
                try:
                    jwt.decode(token, API_SECRET_KEY, algorithms=['HS256'])
                except jwt.ExpiredSignatureError:
                    raise ApiError("Token has expired", 401)
                except jwt.InvalidTokenError:
                    raise ApiError("Invalid token", 401)

            return func(self, *args, **kwargs)
        except ValueError:
            raise ApiError("Invalid Authorization header format", 401)

    return wrapper

class GiteaKimaiApiHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the Gitea-Kimai API."""

    def __init__(self, *args, **kwargs):
        # Initialize security components
        self.security_manager = None
        self.security_middleware = None
        self.rate_limiter = RateLimiter()
        self.security_auditor = SecurityAuditor()
        
        # Initialize security if enabled
        if SECURITY_ENABLED:
            try:
                from security.security import create_security_manager
                self.security_manager = create_security_manager(SECURITY_CONFIG_FILE)
                self.security_middleware = SecurityMiddleware(self.security_manager)
            except Exception as e:
                logger.warning(f"Failed to initialize security manager: {e}")
        
        super().__init__(*args, **kwargs)

    def _parse_path(self):
        """Parse the request path to extract API endpoint."""
        path = self.path
        if '?' in path:
            path = path.split('?', 1)[0]
        if path.startswith(API_PREFIX):
            return path[len(API_PREFIX):]
        return path

    def _parse_query_params(self):
        """Parse query parameters from the request URL."""
        if '?' not in self.path:
            return {}
        query_string = self.path.split('?', 1)[1]
        return {k: v[0] for k, v in parse_qs(query_string).items()}

    def _parse_json_body(self):
        """Parse JSON body from the request."""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            return None

        body = self.rfile.read(content_length).decode('utf-8')
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            raise ApiError("Invalid JSON in request body")

    def _send_response(self, data, status_code=200):
        """Send a JSON response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', API_ALLOWED_ORIGINS)
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def _send_error_response(self, error, status_code=400):
        """Send an error response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', API_ALLOWED_ORIGINS)
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.end_headers()
        self.wfile.write(json.dumps({
            'error': error,
            'timestamp': datetime.now().isoformat()
        }).encode('utf-8'))

    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', API_ALLOWED_ORIGINS)
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.end_headers()

    def do_GET(self):
        """Handle GET requests."""
        try:
            path = self._parse_path()
            params = self._parse_query_params()

            if path == '/status':
                self._handle_status(params)
            elif path == '/activities':
                self._handle_get_activities(params)
            elif path == '/projects':
                self._handle_get_projects(params)
            elif path == '/sync/history':
                self._handle_get_sync_history(params)
            else:
                raise ApiError(f"Endpoint not found: {path}", 404)

        except ApiError as e:
            self._send_error_response(e.message, e.status_code)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            self._send_error_response("Internal server error", 500)

    def do_POST(self):
        """Handle POST requests."""
        try:
            path = self._parse_path()
            body = self._parse_json_body()

            if path == '/auth':
                self._handle_auth(body)
            elif path == '/auth/login':
                self._handle_login(body)
            elif path == '/auth/register':
                self._handle_register(body)
            elif path == '/auth/logout':
                self._handle_logout(body)
            elif path == '/users':
                self._handle_create_user(body)
            elif path == '/sync/trigger':
                self._handle_trigger_sync(body)
            else:
                raise ApiError(f"Endpoint not found: {path}", 404)

        except ApiError as e:
            self._send_error_response(e.message, e.status_code)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            self._send_error_response("Internal server error", 500)

    def _handle_status(self, params):
        """Handle status endpoint."""
        status = {
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0'
        }

        # Add scheduler status if available
        scheduler_status_file = 'scheduler_status.json'
        if os.path.exists(scheduler_status_file):
            try:
                with open(scheduler_status_file, 'r') as f:
                    scheduler_status = json.load(f)
                status['scheduler'] = scheduler_status
            except Exception as e:
                logger.error(f"Error reading scheduler status: {e}")

        # Add database stats
        try:
            if os.path.exists(DATABASE_PATH):
                conn = sqlite3.connect(DATABASE_PATH)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM activity_sync")
                record_count = cursor.fetchone()[0]
                status['database'] = {
                    'record_count': record_count,
                    'path': DATABASE_PATH
                }
                conn.close()
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")

        self._send_response(status)

    @require_auth
    def _handle_get_activities(self, params):
        """Handle activities endpoint."""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            limit = int(params.get('limit', '100'))
            offset = int(params.get('offset', '0'))
            repository = params.get('repository')
            state = params.get('state')

            query = "SELECT * FROM activity_sync"
            query_params = []

            if repository or state:
                query += " WHERE"
                conditions = []

                if repository:
                    conditions.append("repository_name = ?")
                    query_params.append(repository)

                if state:
                    conditions.append("issue_state = ?")
                    query_params.append(state)

                query += " " + " AND ".join(conditions)

            query += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
            query_params.extend([limit, offset])

            cursor.execute(query, query_params)
            activities = [dict(row) for row in cursor.fetchall()]

            self._send_response({
                'activities': activities,
                'count': len(activities),
                'limit': limit,
                'offset': offset
            })

            conn.close()
        except Exception as e:
            logger.error(f"Error getting activities: {e}")
            raise ApiError("Error retrieving activities", 500)

    @require_auth
    def _handle_get_projects(self, params):
        """Handle projects endpoint."""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT DISTINCT repository_name, project_name, kimai_project_id,
                    COUNT(*) as activity_count, MAX(updated_at) as last_update
                FROM activity_sync
                GROUP BY repository_name, project_name, kimai_project_id
                ORDER BY repository_name
            """)
            projects = [dict(row) for row in cursor.fetchall()]

            self._send_response({
                'projects': projects,
                'count': len(projects)
            })

            conn.close()
        except Exception as e:
            logger.error(f"Error getting projects: {e}")
            raise ApiError("Error retrieving projects", 500)

    @require_auth
    def _handle_get_sync_history(self, params):
        """Handle sync history endpoint."""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            limit = int(params.get('limit', '100'))
            cursor.execute("""
                SELECT repository_name, COUNT(*) as count,
                    MIN(created_at) as first_sync,
                    MAX(updated_at) as last_sync,
                    COUNT(CASE WHEN issue_state = 'open' THEN 1 END) as open_issues,
                    COUNT(CASE WHEN issue_state = 'closed' THEN 1 END) as closed_issues
                FROM activity_sync
                GROUP BY repository_name
                ORDER BY last_sync DESC
                LIMIT ?
            """, (limit,))
            history = [dict(row) for row in cursor.fetchall()]

            self._send_response({
                'history': history,
                'count': len(history)
            })

            conn.close()
        except Exception as e:
            logger.error(f"Error getting sync history: {e}")
            raise ApiError("Error retrieving sync history", 500)

    def _handle_auth(self, body):
        """Handle authentication endpoint."""
        if not body or 'username' not in body or 'password' not in body:
            raise ApiError("Username and password are required", 400)

        username = body['username']
        password = body['password']

        # In a real implementation, this would verify against a user database
        # For this example, we use a simple hardcoded check
        api_username = os.getenv('API_USERNAME')
        api_password = os.getenv('API_PASSWORD')

        if not api_username or not api_password:
            raise ApiError("API authentication not configured", 500)

        if username != api_username or password != api_password:
            raise ApiError("Invalid credentials", 401)

        # Generate JWT token
        now = datetime.utcnow()
        payload = {
            'sub': username,
            'iat': now,
            'exp': now + timedelta(seconds=API_TOKEN_EXPIRY)
        }
        token = jwt.encode(payload, API_SECRET_KEY, algorithm='HS256')

        self._send_response({
            'token': token,
            'expires_in': API_TOKEN_EXPIRY,
            'token_type': 'Bearer'
        })

    def _handle_login(self, body):
        """Handle login endpoint with security manager."""
        if not body or 'username' not in body or 'password' not in body:
            raise ApiError("Username and password are required", 400)

        username = body['username']
        password = body['password']
        
        # Get client IP for rate limiting and auditing
        client_ip = self._get_client_ip()
        
        # Check rate limiting
        if not self.rate_limiter.is_allowed(f"login_{client_ip}", max_attempts=5, window_minutes=15):
            self.security_auditor.log_login_attempt(username, False, client_ip)
            raise ApiError("Too many login attempts. Please try again later.", 429)
        
        if not self.security_manager:
            raise ApiError("Security manager not available", 500)
        
        try:
            # Authenticate user
            token = self.security_manager.authenticate_user(username, password, client_ip)
            
            if token:
                # Log successful login
                self.security_auditor.log_login_attempt(username, True, client_ip)
                
                # Get user info
                user_info = self.security_manager.get_user_info(token.token)
                
                self._send_response({
                    'status': 'success',
                    'token': token.token,
                    'expires_at': token.expires_at.isoformat(),
                    'user': user_info
                })
            else:
                # Log failed login
                self.security_auditor.log_login_attempt(username, False, client_ip)
                raise ApiError("Invalid credentials", 401)
                
        except Exception as e:
            logger.error(f"Login error: {e}")
            self.security_auditor.log_login_attempt(username, False, client_ip)
            raise ApiError("Authentication failed", 401)

    def _handle_register(self, body):
        """Handle user registration endpoint."""
        if not body or 'username' not in body or 'password' not in body or 'email' not in body:
            raise ApiError("Username, password, and email are required", 400)

        username = body['username']
        password = body['password']
        email = body['email']
        role = body.get('role', 'viewer')
        
        # Validate inputs
        if not SecurityChecker.is_valid_email(email):
            raise ApiError("Invalid email format", 400)
        
        username_validation = SecurityChecker.validate_username(username)
        if not username_validation['is_valid']:
            raise ApiError(f"Invalid username: {username_validation['errors'][0]}", 400)
        
        if not self.security_manager:
            raise ApiError("Security manager not available", 500)
        
        try:
            # Create user
            user = self.security_manager.create_user(
                username=username,
                email=email,
                password=password,
                role=Role(role)
            )
            
            self._send_response({
                'status': 'success',
                'message': 'User created successfully',
                'user_id': user.id,
                'username': user.username,
                'email': user.email,
                'role': user.role.value
            })
            
        except ValueError as e:
            raise ApiError(str(e), 400)
        except Exception as e:
            logger.error(f"Registration error: {e}")
            raise ApiError("Registration failed", 500)

    def _handle_logout(self, body):
        """Handle logout endpoint."""
        auth_header = self.headers.get('Authorization')
        if not auth_header:
            raise ApiError("Authorization header required", 401)
        
        try:
            scheme, token = auth_header.split(' ', 1)
            if scheme.lower() != 'bearer':
                raise ApiError("Authorization scheme must be Bearer", 401)
            
            if self.security_manager:
                # Revoke token
                self.security_manager.revoke_token(token)
                
                # Log logout
                if hasattr(self, 'current_user'):
                    self.security_auditor.log_security_event('LOGOUT', {
                        'user_id': self.current_user.id,
                        'username': self.current_user.username
                    })
            
            self._send_response({
                'status': 'success',
                'message': 'Logged out successfully'
            })
            
        except ValueError:
            raise ApiError("Invalid Authorization header format", 401)
        except Exception as e:
            logger.error(f"Logout error: {e}")
            raise ApiError("Logout failed", 500)

    def _handle_create_user(self, body):
        """Handle user creation endpoint (admin only)."""
        if not body or 'username' not in body or 'password' not in body or 'email' not in body:
            raise ApiError("Username, password, and email are required", 400)

        username = body['username']
        password = body['password']
        email = body['email']
        role = body.get('role', 'viewer')
        
        # Check if current user has admin permissions
        if not hasattr(self, 'current_user'):
            raise ApiError("Authentication required", 401)
        
        if not self.security_manager.check_permission(self.current_user.token, Permission.ADMIN):
            raise ApiError("Admin permission required", 403)
        
        # Validate inputs
        if not SecurityChecker.is_valid_email(email):
            raise ApiError("Invalid email format", 400)
        
        username_validation = SecurityChecker.validate_username(username)
        if not username_validation['is_valid']:
            raise ApiError(f"Invalid username: {username_validation['errors'][0]}", 400)
        
        try:
            # Create user
            user = self.security_manager.create_user(
                username=username,
                email=email,
                password=password,
                role=Role(role)
            )
            
            # Log user creation
            self.security_auditor.log_configuration_change(
                self.current_user.id,
                'USER_CREATION',
                f"Created user {username} with role {role}"
            )
            
            self._send_response({
                'status': 'success',
                'message': 'User created successfully',
                'user_id': user.id,
                'username': user.username,
                'email': user.email,
                'role': user.role.value
            })
            
        except ValueError as e:
            raise ApiError(str(e), 400)
        except Exception as e:
            logger.error(f"User creation error: {e}")
            raise ApiError("User creation failed", 500)

    def _get_client_ip(self):
        """Get client IP address from request headers."""
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
            if header in self.headers:
                ip = self.headers[header].split(',')[0].strip()
                if SecurityChecker.is_safe_ip_address(ip):
                    return ip
        
        return 'unknown'

    @require_auth
    def _handle_trigger_sync(self, body):
        """Handle sync trigger endpoint."""
        try:
            # Optional parameters
            repo = body.get('repository') if body else None
            dry_run = body.get('dry_run', False) if body else False

            command = ['python', 'sync.py']
            if dry_run:
                command.append('--dry-run')
            if repo:
                command.extend(['--repos', repo])

            # Run in background to avoid blocking the API
            subprocess.Popen(command)

            self._send_response({
                'status': 'success',
                'message': 'Sync triggered',
                'timestamp': datetime.now().isoformat(),
                'repository': repo,
                'dry_run': dry_run
            })
        except Exception as e:
            logger.error(f"Error triggering sync: {e}")
            raise ApiError("Error triggering synchronization", 500)

class GiteaKimaiApi:
    """Main API class for the Gitea-Kimai integration."""

    def __init__(self, host=API_HOST, port=API_PORT):
        """Initialize the API server."""
        self.host = host
        self.port = port
        self.server = None

    def start(self):
        """Start the API server."""
        if not API_ENABLED:
            logger.warning("API is disabled. Set API_ENABLED=true to enable.")
            return False

        if not API_SECRET_KEY and API_REQUIRE_AUTH:
            logger.error("API_SECRET_KEY must be set when API_REQUIRE_AUTH is enabled")
            return False

        try:
            self.server = HTTPServer((self.host, self.port), GiteaKimaiApiHandler)
            logger.info(f"API server starting on {self.host}:{self.port}")
            self.server.serve_forever()
        except KeyboardInterrupt:
            logger.info("API server stopped by user")
        except Exception as e:
            logger.error(f"Error starting API server: {e}")
            return False
        finally:
            if self.server:
                self.server.server_close()
        return True

    def stop(self):
        """Stop the API server."""
        if self.server:
            self.server.shutdown()
            logger.info("API server stopped")
            return True
        return False

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="API server for Gitea-Kimai integration")
    parser.add_argument('--host', help='Host to bind to', default=API_HOST)
    parser.add_argument('--port', type=int, help='Port to bind to', default=API_PORT)
    parser.add_argument('--generate-key', action='store_true', help='Generate a new secret key')
    return parser.parse_args()

def generate_secret_key():
    """Generate a random secret key."""
    return hashlib.sha256(os.urandom(32)).hexdigest()

def main():
    """Main entry point."""
    args = parse_arguments()

    if args.generate_key:
        key = generate_secret_key()
        print(f"Generated new secret key: {key}")
        print("Add this to your .env file as API_SECRET_KEY")
        return 0

    api = GiteaKimaiApi(host=args.host, port=args.port)
    return 0 if api.start() else 1

if __name__ == "__main__":
    sys.exit(main())
