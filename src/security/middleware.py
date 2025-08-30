#!/usr/bin/env python3
"""
Security Middleware Module

Provides middleware components for integrating security features
into web applications and API endpoints.
"""

import logging
import functools
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from http import HTTPStatus

from .security import SecurityManager, Permission, Role

logger = logging.getLogger(__name__)


class SecurityMiddleware:
    """Middleware for handling security in web applications."""
    
    def __init__(self, security_manager: SecurityManager):
        self.security_manager = security_manager
    
    def authenticate_request(self, request_headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Authenticate a request using headers."""
        auth_header = request_headers.get('Authorization')
        if not auth_header:
            return None
        
        try:
            scheme, token = auth_header.split(' ', 1)
            if scheme.lower() != 'bearer':
                return None
            
            # Validate token and get user info
            user_info = self.security_manager.get_user_info(token)
            if user_info:
                return {
                    'user_info': user_info,
                    'token': token,
                    'authenticated': True
                }
        except (ValueError, Exception) as e:
            logger.warning(f"Authentication failed: {e}")
        
        return None
    
    def check_permission(self, token: str, required_permission: Permission) -> bool:
        """Check if token has required permission."""
        return self.security_manager.check_permission(token, required_permission)
    
    def require_auth(self, required_permission: Optional[Permission] = None):
        """Decorator to require authentication for endpoints."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # This would be implemented based on the web framework being used
                # For now, we'll provide a template
                request = kwargs.get('request') or args[0] if args else None
                if not request:
                    raise ValueError("Request object not found")
                
                # Authenticate request
                auth_result = self.authenticate_request(request.headers)
                if not auth_result:
                    return self._unauthorized_response("Authentication required")
                
                # Check permission if required
                if required_permission:
                    if not self.check_permission(auth_result['token'], required_permission):
                        return self._forbidden_response(f"Permission {required_permission.value} required")
                
                # Add user info to request
                request.user = auth_result['user_info']
                request.token = auth_result['token']
                
                return func(*args, **kwargs)
            
            return wrapper
        return decorator
    
    def require_role(self, required_role: Role):
        """Decorator to require specific role for endpoints."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                request = kwargs.get('request') or args[0] if args else None
                if not request:
                    raise ValueError("Request object not found")
                
                # Authenticate request
                auth_result = self.authenticate_request(request.headers)
                if not auth_result:
                    return self._unauthorized_response("Authentication required")
                
                # Check role
                user_role = auth_result['user_info']['role']
                if user_role != required_role.value:
                    return self._forbidden_response(f"Role {required_role.value} required")
                
                # Add user info to request
                request.user = auth_result['user_info']
                request.token = auth_result['token']
                
                return func(*args, **kwargs)
            
            return wrapper
        return decorator
    
    def _unauthorized_response(self, message: str) -> Dict[str, Any]:
        """Return unauthorized response."""
        return {
            'status': 'error',
            'message': message,
            'code': HTTPStatus.UNAUTHORIZED
        }
    
    def _forbidden_response(self, message: str) -> Dict[str, Any]:
        """Return forbidden response."""
        return {
            'status': 'error',
            'message': message,
            'code': HTTPStatus.FORBIDDEN
        }


class FlaskSecurityMiddleware(SecurityMiddleware):
    """Flask-specific security middleware."""
    
    def __init__(self, security_manager: SecurityManager, app=None):
        super().__init__(security_manager)
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize the middleware with Flask app."""
        app.security_middleware = self
        
        # Add before_request handler
        @app.before_request
        def before_request():
            # Skip authentication for certain endpoints
            if self._should_skip_auth(app.request.path):
                return None
            
            # Authenticate request
            auth_result = self.authenticate_request(dict(app.request.headers))
            if not auth_result:
                return self._flask_unauthorized_response("Authentication required")
            
            # Store user info in request context
            app.request.user = auth_result['user_info']
            app.request.token = auth_result['token']
    
    def _should_skip_auth(self, path: str) -> bool:
        """Check if authentication should be skipped for this path."""
        skip_paths = ['/auth/login', '/auth/register', '/health', '/docs']
        return any(path.startswith(skip_path) for skip_path in skip_paths)
    
    def _flask_unauthorized_response(self, message: str):
        """Return Flask unauthorized response."""
        from flask import jsonify
        return jsonify({
            'status': 'error',
            'message': message
        }), HTTPStatus.UNAUTHORIZED
    
    def require_auth(self, required_permission: Optional[Permission] = None):
        """Flask decorator to require authentication."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                from flask import request, jsonify
                
                # Check if user is authenticated
                if not hasattr(request, 'user'):
                    return jsonify({
                        'status': 'error',
                        'message': 'Authentication required'
                    }), HTTPStatus.UNAUTHORIZED
                
                # Check permission if required
                if required_permission:
                    if not self.check_permission(request.token, required_permission):
                        return jsonify({
                            'status': 'error',
                            'message': f"Permission {required_permission.value} required"
                        }), HTTPStatus.FORBIDDEN
                
                return func(*args, **kwargs)
            
            return wrapper
        return decorator


class FastAPISecurityMiddleware(SecurityMiddleware):
    """FastAPI-specific security middleware."""
    
    def __init__(self, security_manager: SecurityManager):
        super().__init__(security_manager)
    
    def get_current_user(self, token: str):
        """Get current user from token (for FastAPI dependency injection)."""
        user_info = self.security_manager.get_user_info(token)
        if not user_info:
            from fastapi import HTTPException
            raise HTTPException(
                status_code=HTTPStatus.UNAUTHORIZED,
                detail="Invalid authentication credentials"
            )
        return user_info
    
    def require_permission(self, required_permission: Permission):
        """FastAPI dependency for requiring specific permission."""
        def permission_dependency(current_user: Dict[str, Any] = None):
            if not current_user:
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=HTTPStatus.UNAUTHORIZED,
                    detail="Authentication required"
                )
            
            # Check if user has required permission
            user_permissions = current_user.get('permissions', [])
            if required_permission.value not in user_permissions:
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail=f"Permission {required_permission.value} required"
                )
            
            return current_user
        
        return permission_dependency
    
    def require_role(self, required_role: Role):
        """FastAPI dependency for requiring specific role."""
        def role_dependency(current_user: Dict[str, Any] = None):
            if not current_user:
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=HTTPStatus.UNAUTHORIZED,
                    detail="Authentication required"
                )
            
            # Check if user has required role
            user_role = current_user.get('role')
            if user_role != required_role.value:
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=HTTPStatus.FORBIDDEN,
                    detail=f"Role {required_role.value} required"
                )
            
            return current_user
        
        return role_dependency


class SecurityDecorators:
    """Collection of security decorators for different frameworks."""
    
    def __init__(self, security_manager: SecurityManager):
        self.security_manager = security_manager
    
    def require_auth(self, required_permission: Optional[Permission] = None):
        """Generic decorator to require authentication."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # Extract request from args or kwargs
                request = None
                for arg in args:
                    if hasattr(arg, 'headers'):
                        request = arg
                        break
                
                if not request:
                    for value in kwargs.values():
                        if hasattr(value, 'headers'):
                            request = value
                            break
                
                if not request:
                    raise ValueError("Request object not found")
                
                # Authenticate
                auth_result = self._authenticate_request(request)
                if not auth_result:
                    return self._error_response("Authentication required", 401)
                
                # Check permission
                if required_permission:
                    if not self.security_manager.check_permission(
                        auth_result['token'], required_permission
                    ):
                        return self._error_response(
                            f"Permission {required_permission.value} required", 403
                        )
                
                # Add user info to request
                request.user = auth_result['user_info']
                request.token = auth_result['token']
                
                return func(*args, **kwargs)
            
            return wrapper
        return decorator
    
    def _authenticate_request(self, request) -> Optional[Dict[str, Any]]:
        """Authenticate request using headers."""
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return None
        
        try:
            scheme, token = auth_header.split(' ', 1)
            if scheme.lower() != 'bearer':
                return None
            
            user_info = self.security_manager.get_user_info(token)
            if user_info:
                return {
                    'user_info': user_info,
                    'token': token
                }
        except Exception as e:
            logger.warning(f"Authentication failed: {e}")
        
        return None
    
    def _error_response(self, message: str, status_code: int) -> Dict[str, Any]:
        """Return error response."""
        return {
            'status': 'error',
            'message': message,
            'code': status_code
        }


# Utility functions for common security operations
def create_security_middleware(security_manager: SecurityManager, framework: str = 'generic'):
    """Create security middleware for specified framework."""
    if framework.lower() == 'flask':
        return FlaskSecurityMiddleware(security_manager)
    elif framework.lower() == 'fastapi':
        return FastAPISecurityMiddleware(security_manager)
    else:
        return SecurityMiddleware(security_manager)


def require_permission(permission: Permission):
    """Simple decorator for requiring permission."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # This is a template - would need to be implemented based on the framework
            # being used and how the security manager is accessed
            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_role(role: Role):
    """Simple decorator for requiring role."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # This is a template - would need to be implemented based on the framework
            # being used and how the security manager is accessed
            return func(*args, **kwargs)
        return wrapper
    return decorator
