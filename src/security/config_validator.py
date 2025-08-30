#!/usr/bin/env python3
"""
Security Configuration Validator

Validates security configuration settings and provides recommendations
for improving security posture.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import secrets

logger = logging.getLogger(__name__)


class SecurityConfigValidator:
    """Validates security configuration settings."""
    
    def __init__(self, config_file: str = "security_config.json"):
        self.config_file = config_file
        self.config = {}
        self.issues = []
        self.warnings = []
        self.recommendations = []
    
    def load_config(self) -> bool:
        """Load configuration from file."""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
                return True
            else:
                logger.warning(f"Security config file not found: {self.config_file}")
                return False
        except Exception as e:
            logger.error(f"Error loading security config: {e}")
            return False
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate the security configuration."""
        self.issues = []
        self.warnings = []
        self.recommendations = []
        
        if not self.config:
            self.issues.append("No security configuration found")
            return self._get_validation_result()
        
        # Validate required fields
        self._validate_required_fields()
        
        # Validate secret key
        self._validate_secret_key()
        
        # Validate JWT settings
        self._validate_jwt_settings()
        
        # Validate password policy
        self._validate_password_policy()
        
        # Validate session settings
        self._validate_session_settings()
        
        # Validate rate limiting
        self._validate_rate_limiting()
        
        # Check for security best practices
        self._check_security_best_practices()
        
        return self._get_validation_result()
    
    def _validate_required_fields(self):
        """Validate required configuration fields."""
        required_fields = ['secret_key']
        
        for field in required_fields:
            if field not in self.config:
                self.issues.append(f"Missing required field: {field}")
            elif not self.config[field]:
                self.issues.append(f"Required field is empty: {field}")
    
    def _validate_secret_key(self):
        """Validate secret key configuration."""
        secret_key = self.config.get('secret_key', '')
        
        if not secret_key:
            self.issues.append("Secret key is not set")
            return
        
        # Check secret key length
        if len(secret_key) < 32:
            self.issues.append("Secret key is too short (minimum 32 characters)")
        elif len(secret_key) < 64:
            self.warnings.append("Secret key could be longer for better security")
        
        # Check if secret key is too simple
        if secret_key.lower() in ['secret', 'key', 'password', 'admin', 'default']:
            self.issues.append("Secret key is too simple or common")
        
        # Check for entropy
        unique_chars = len(set(secret_key))
        if unique_chars < 16:
            self.warnings.append("Secret key has low character diversity")
    
    def _validate_jwt_settings(self):
        """Validate JWT configuration."""
        jwt_expiry = self.config.get('jwt_expiry_hours', 24)
        
        if jwt_expiry <= 0:
            self.issues.append("JWT expiry must be positive")
        elif jwt_expiry > 168:  # 1 week
            self.warnings.append("JWT expiry is very long - consider shorter sessions")
        elif jwt_expiry < 1:
            self.warnings.append("JWT expiry is very short - may impact user experience")
        
        # Check for reasonable default
        if jwt_expiry == 24:
            self.recommendations.append("Consider adjusting JWT expiry based on your security requirements")
    
    def _validate_password_policy(self):
        """Validate password policy settings."""
        min_length = self.config.get('password_min_length', 8)
        require_special = self.config.get('require_special_chars', True)
        
        if min_length < 8:
            self.issues.append("Password minimum length should be at least 8 characters")
        elif min_length < 12:
            self.warnings.append("Consider increasing password minimum length to 12+ characters")
        
        if not require_special:
            self.warnings.append("Consider requiring special characters in passwords")
        
        # Check for additional password policy settings
        if 'password_max_length' not in self.config:
            self.recommendations.append("Consider setting password maximum length")
        
        if 'password_history' not in self.config:
            self.recommendations.append("Consider implementing password history to prevent reuse")
    
    def _validate_session_settings(self):
        """Validate session configuration."""
        session_timeout = self.config.get('session_timeout_minutes', 60)
        
        if session_timeout <= 0:
            self.issues.append("Session timeout must be positive")
        elif session_timeout > 1440:  # 24 hours
            self.warnings.append("Session timeout is very long - consider shorter sessions")
        elif session_timeout < 15:
            self.warnings.append("Session timeout is very short - may impact user experience")
    
    def _validate_rate_limiting(self):
        """Validate rate limiting configuration."""
        max_attempts = self.config.get('max_login_attempts', 5)
        lockout_duration = self.config.get('lockout_duration_minutes', 30)
        
        if max_attempts <= 0:
            self.issues.append("Maximum login attempts must be positive")
        elif max_attempts > 20:
            self.warnings.append("Maximum login attempts is very high")
        
        if lockout_duration <= 0:
            self.issues.append("Lockout duration must be positive")
        elif lockout_duration > 1440:  # 24 hours
            self.warnings.append("Lockout duration is very long")
    
    def _check_security_best_practices(self):
        """Check for security best practices."""
        # Check for HTTPS requirement
        if 'require_https' not in self.config:
            self.recommendations.append("Consider adding require_https setting")
        
        # Check for secure headers
        if 'security_headers' not in self.config:
            self.recommendations.append("Consider configuring security headers")
        
        # Check for audit logging
        if 'audit_logging' not in self.config:
            self.recommendations.append("Consider enabling audit logging")
        
        # Check for backup encryption
        if 'backup_encryption' not in self.config:
            self.recommendations.append("Consider enabling backup encryption")
    
    def _get_validation_result(self) -> Dict[str, Any]:
        """Get validation result summary."""
        return {
            'is_valid': len(self.issues) == 0,
            'issues': self.issues,
            'warnings': self.warnings,
            'recommendations': self.recommendations,
            'summary': {
                'total_issues': len(self.issues),
                'total_warnings': len(self.warnings),
                'total_recommendations': len(self.recommendations)
            }
        }
    
    def generate_secure_config(self) -> Dict[str, Any]:
        """Generate a secure default configuration."""
        return {
            'secret_key': secrets.token_urlsafe(64),
            'jwt_expiry_hours': 24,
            'password_min_length': 12,
            'require_special_chars': True,
            'max_login_attempts': 5,
            'lockout_duration_minutes': 30,
            'session_timeout_minutes': 60,
            'require_https': True,
            'security_headers': {
                'X-Content-Type-Options': 'nosniff',
                'X-Frame-Options': 'DENY',
                'X-XSS-Protection': '1; mode=block',
                'Strict-Transport-Security': 'max-age=31536000; includeSubDomains'
            },
            'audit_logging': True,
            'backup_encryption': True,
            'password_history': 5,
            'password_max_length': 128,
            'rate_limiting': {
                'enabled': True,
                'requests_per_minute': 60,
                'burst_limit': 10
            }
        }
    
    def save_secure_config(self, output_file: str = None) -> bool:
        """Save a secure configuration to file."""
        if not output_file:
            output_file = self.config_file
        
        try:
            secure_config = self.generate_secure_config()
            
            with open(output_file, 'w') as f:
                json.dump(secure_config, f, indent=2)
            
            logger.info(f"Secure configuration saved to {output_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving secure configuration: {e}")
            return False
    
    def fix_issues(self) -> Dict[str, Any]:
        """Attempt to fix common configuration issues."""
        fixes_applied = []
        
        if not self.config:
            self.config = {}
        
        # Fix missing secret key
        if not self.config.get('secret_key'):
            self.config['secret_key'] = secrets.token_urlsafe(64)
            fixes_applied.append("Generated secure secret key")
        
        # Fix weak secret key
        elif len(self.config.get('secret_key', '')) < 32:
            self.config['secret_key'] = secrets.token_urlsafe(64)
            fixes_applied.append("Replaced weak secret key with secure one")
        
        # Fix password policy
        if self.config.get('password_min_length', 0) < 8:
            self.config['password_min_length'] = 12
            fixes_applied.append("Increased password minimum length to 12")
        
        if not self.config.get('require_special_chars', False):
            self.config['require_special_chars'] = True
            fixes_applied.append("Enabled special character requirement")
        
        # Fix session settings
        if self.config.get('session_timeout_minutes', 0) <= 0:
            self.config['session_timeout_minutes'] = 60
            fixes_applied.append("Set session timeout to 60 minutes")
        
        # Fix rate limiting
        if self.config.get('max_login_attempts', 0) <= 0:
            self.config['max_login_attempts'] = 5
            fixes_applied.append("Set maximum login attempts to 5")
        
        if self.config.get('lockout_duration_minutes', 0) <= 0:
            self.config['lockout_duration_minutes'] = 30
            fixes_applied.append("Set lockout duration to 30 minutes")
        
        return {
            'fixed': len(fixes_applied) > 0,
            'fixes_applied': fixes_applied,
            'config': self.config
        }


class SecurityHealthChecker:
    """Checks overall security health of the system."""
    
    def __init__(self):
        self.checks = []
        self.score = 0
        self.max_score = 100
    
    def run_security_audit(self) -> Dict[str, Any]:
        """Run comprehensive security audit."""
        self.checks = []
        self.score = 0
        
        # Check configuration
        self._check_configuration()
        
        # Check file permissions
        self._check_file_permissions()
        
        # Check environment variables
        self._check_environment()
        
        # Check dependencies
        self._check_dependencies()
        
        # Check database security
        self._check_database_security()
        
        # Check network security
        self._check_network_security()
        
        return {
            'score': self.score,
            'max_score': self.max_score,
            'percentage': (self.score / self.max_score) * 100,
            'grade': self._get_grade(),
            'checks': self.checks,
            'recommendations': self._get_recommendations()
        }
    
    def _check_configuration(self):
        """Check security configuration."""
        validator = SecurityConfigValidator()
        if validator.load_config():
            result = validator.validate_config()
            
            if result['is_valid']:
                self.score += 20
                self.checks.append({
                    'name': 'Configuration',
                    'status': 'PASS',
                    'details': 'Security configuration is valid'
                })
            else:
                self.checks.append({
                    'name': 'Configuration',
                    'status': 'FAIL',
                    'details': f"Configuration issues: {len(result['issues'])}"
                })
        else:
            self.score += 10
            self.checks.append({
                'name': 'Configuration',
                'status': 'WARN',
                'details': 'No security configuration found'
            })
    
    def _check_file_permissions(self):
        """Check file permissions."""
        critical_files = [
            'security_config.json',
            'security.db',
            '.env'
        ]
        
        secure_files = 0
        for file_path in critical_files:
            if os.path.exists(file_path):
                stat_info = os.stat(file_path)
                mode = stat_info.st_mode & 0o777
                
                if mode <= 0o600:  # Only owner can read/write
                    secure_files += 1
                else:
                    self.checks.append({
                        'name': f'File Permissions: {file_path}',
                        'status': 'WARN',
                        'details': f'File permissions too permissive: {oct(mode)}'
                    })
        
        if secure_files == len(critical_files):
            self.score += 15
            self.checks.append({
                'name': 'File Permissions',
                'status': 'PASS',
                'details': 'Critical files have secure permissions'
            })
    
    def _check_environment(self):
        """Check environment variables."""
        env_vars = [
            'API_SECRET_KEY',
            'SECURITY_ENABLED',
            'API_REQUIRE_AUTH'
        ]
        
        secure_env = 0
        for var in env_vars:
            if os.getenv(var):
                secure_env += 1
            else:
                self.checks.append({
                    'name': f'Environment: {var}',
                    'status': 'WARN',
                    'details': f'Environment variable not set: {var}'
                })
        
        if secure_env == len(env_vars):
            self.score += 15
            self.checks.append({
                'name': 'Environment Variables',
                'status': 'PASS',
                'details': 'Security environment variables are set'
            })
    
    def _check_dependencies(self):
        """Check security dependencies."""
        required_deps = [
            'PyJWT',
            'cryptography'
        ]
        
        missing_deps = []
        for dep in required_deps:
            try:
                __import__(dep.lower())
            except ImportError:
                missing_deps.append(dep)
        
        if not missing_deps:
            self.score += 10
            self.checks.append({
                'name': 'Dependencies',
                'status': 'PASS',
                'details': 'Security dependencies are installed'
            })
        else:
            self.checks.append({
                'name': 'Dependencies',
                'status': 'FAIL',
                'details': f'Missing security dependencies: {missing_deps}'
            })
    
    def _check_database_security(self):
        """Check database security."""
        db_files = ['security.db', 'sync.db']
        
        for db_file in db_files:
            if os.path.exists(db_file):
                # Check if database is encrypted (simplified check)
                try:
                    with open(db_file, 'rb') as f:
                        header = f.read(16)
                        # This is a simplified check - in reality you'd check for encryption
                        if b'SQLite' in header:
                            self.checks.append({
                                'name': f'Database: {db_file}',
                                'status': 'WARN',
                                'details': 'Database is not encrypted'
                            })
                except Exception:
                    pass
        
        self.score += 10
        self.checks.append({
            'name': 'Database Security',
            'status': 'INFO',
            'details': 'Database security check completed'
        })
    
    def _check_network_security(self):
        """Check network security."""
        # Check if HTTPS is required
        if os.getenv('REQUIRE_HTTPS', 'false').lower() == 'true':
            self.score += 10
            self.checks.append({
                'name': 'HTTPS',
                'status': 'PASS',
                'details': 'HTTPS is required'
            })
        else:
            self.checks.append({
                'name': 'HTTPS',
                'status': 'WARN',
                'details': 'HTTPS is not required'
            })
        
        # Check CORS settings
        cors_origin = os.getenv('API_ALLOWED_ORIGINS', '*')
        if cors_origin == '*':
            self.checks.append({
                'name': 'CORS',
                'status': 'WARN',
                'details': 'CORS allows all origins'
            })
        else:
            self.score += 10
            self.checks.append({
                'name': 'CORS',
                'status': 'PASS',
                'details': 'CORS is properly configured'
            })
    
    def _get_grade(self) -> str:
        """Get security grade based on score."""
        percentage = (self.score / self.max_score) * 100
        
        if percentage >= 90:
            return 'A'
        elif percentage >= 80:
            return 'B'
        elif percentage >= 70:
            return 'C'
        elif percentage >= 60:
            return 'D'
        else:
            return 'F'
    
    def _get_recommendations(self) -> List[str]:
        """Get security recommendations based on checks."""
        recommendations = []
        
        for check in self.checks:
            if check['status'] == 'FAIL':
                recommendations.append(f"Fix {check['name']}: {check['details']}")
            elif check['status'] == 'WARN':
                recommendations.append(f"Improve {check['name']}: {check['details']}")
        
        if not recommendations:
            recommendations.append("Security posture is good - continue monitoring")
        
        return recommendations


def validate_security_config(config_file: str = "security_config.json") -> Dict[str, Any]:
    """Convenience function to validate security configuration."""
    validator = SecurityConfigValidator(config_file)
    return validator.validate_config()


def generate_secure_config(output_file: str = "security_config.json") -> bool:
    """Convenience function to generate secure configuration."""
    validator = SecurityConfigValidator()
    return validator.save_secure_config(output_file)


def run_security_health_check() -> Dict[str, Any]:
    """Convenience function to run security health check."""
    checker = SecurityHealthChecker()
    return checker.run_security_audit()


if __name__ == "__main__":
    # Example usage
    print("Security Configuration Validator")
    print("=" * 40)
    
    # Validate existing config
    result = validate_security_config()
    print(f"Configuration valid: {result['is_valid']}")
    print(f"Issues: {len(result['issues'])}")
    print(f"Warnings: {len(result['warnings'])}")
    
    # Run health check
    health = run_security_health_check()
    print(f"\nSecurity Grade: {health['grade']}")
    print(f"Score: {health['score']}/{health['max_score']} ({health['percentage']:.1f}%)")
    
    # Show recommendations
    print("\nRecommendations:")
    for rec in health['recommendations']:
        print(f"- {rec}")
