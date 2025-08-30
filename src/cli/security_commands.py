#!/usr/bin/env python3
"""
Security CLI Commands

Provides command-line interface for security operations including
user management, authentication, and security configuration.
"""

import argparse
import json
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from security.security import SecurityManager, SecurityConfig, Role, Permission
from security.utils import PasswordValidator, TokenGenerator, SecurityChecker
from security.config_validator import SecurityConfigValidator, SecurityHealthChecker


class SecurityCLI:
    """Command-line interface for security operations."""
    
    def __init__(self):
        self.security_manager = None
        self.config_file = "security_config.json"
    
    def setup_security_manager(self):
        """Initialize security manager."""
        try:
            from security.security import create_security_manager
            self.security_manager = create_security_manager(self.config_file)
        except Exception as e:
            print(f"Error initializing security manager: {e}")
            return False
        return True
    
    def create_user(self, args):
        """Create a new user."""
        if not self.setup_security_manager():
            return 1
        
        try:
            # Validate password strength
            validator = PasswordValidator()
            password_validation = validator.validate_password(args.password)
            
            if not password_validation['is_valid']:
                print("Password validation failed:")
                for error in password_validation['errors']:
                    print(f"  - {error}")
                for warning in password_validation['warnings']:
                    print(f"  - Warning: {warning}")
                return 1
            
            # Create user
            user = self.security_manager.create_user(
                username=args.username,
                email=args.email,
                password=args.password,
                role=Role(args.role)
            )
            
            print(f"User created successfully:")
            print(f"  ID: {user.id}")
            print(f"  Username: {user.username}")
            print(f"  Email: {user.email}")
            print(f"  Role: {user.role.value}")
            print(f"  Permissions: {[p.value for p in user.permissions]}")
            
            return 0
            
        except ValueError as e:
            print(f"Error creating user: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected error: {e}")
            return 1
    
    def authenticate_user(self, args):
        """Authenticate a user."""
        if not self.setup_security_manager():
            return 1
        
        try:
            token = self.security_manager.authenticate_user(args.username, args.password)
            
            if token:
                user_info = self.security_manager.get_user_info(token.token)
                
                print("Authentication successful:")
                print(f"  Token: {token.token}")
                print(f"  Expires: {token.expires_at}")
                print(f"  User: {user_info['username']}")
                print(f"  Role: {user_info['role']}")
                print(f"  Permissions: {user_info['permissions']}")
                
                # Save token to file if requested
                if args.save_token:
                    with open(args.save_token, 'w') as f:
                        json.dump({
                            'token': token.token,
                            'expires_at': token.expires_at.isoformat(),
                            'user': user_info
                        }, f, indent=2)
                    print(f"Token saved to {args.save_token}")
                
                return 0
            else:
                print("Authentication failed: Invalid credentials")
                return 1
                
        except Exception as e:
            print(f"Authentication error: {e}")
            return 1
    
    def validate_token(self, args):
        """Validate a token."""
        if not self.setup_security_manager():
            return 1
        
        try:
            user = self.security_manager.validate_token(args.token)
            
            if user:
                print("Token is valid:")
                print(f"  User ID: {user.id}")
                print(f"  Username: {user.username}")
                print(f"  Email: {user.email}")
                print(f"  Role: {user.role.value}")
                print(f"  Permissions: {[p.value for p in user.permissions]}")
                return 0
            else:
                print("Token is invalid or expired")
                return 1
                
        except Exception as e:
            print(f"Token validation error: {e}")
            return 1
    
    def list_users(self, args):
        """List all users."""
        if not self.setup_security_manager():
            return 1
        
        try:
            users = self.security_manager.database.get_all_users()
            
            if not users:
                print("No users found")
                return 0
            
            print(f"Found {len(users)} users:")
            print("-" * 80)
            print(f"{'ID':<20} {'Username':<15} {'Email':<25} {'Role':<12} {'Active':<6}")
            print("-" * 80)
            
            for user in users:
                print(f"{user.id:<20} {user.username:<15} {user.email:<25} {user.role.value:<12} {str(user.is_active):<6}")
            
            return 0
            
        except Exception as e:
            print(f"Error listing users: {e}")
            return 1
    
    def change_password(self, args):
        """Change user password."""
        if not self.setup_security_manager():
            return 1
        
        try:
            success = self.security_manager.change_password(
                args.user_id, args.old_password, args.new_password
            )
            
            if success:
                print("Password changed successfully")
                return 0
            else:
                print("Failed to change password: Invalid old password")
                return 1
                
        except ValueError as e:
            print(f"Password validation error: {e}")
            return 1
        except Exception as e:
            print(f"Error changing password: {e}")
            return 1
    
    def update_user_role(self, args):
        """Update user role."""
        if not self.setup_security_manager():
            return 1
        
        try:
            self.security_manager.update_user_role(args.user_id, Role(args.role))
            print(f"User role updated to {args.role}")
            return 0
            
        except ValueError as e:
            print(f"Error updating role: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected error: {e}")
            return 1
    
    def generate_password(self, args):
        """Generate a secure password."""
        from security.utils import generate_secure_password
        
        password = generate_secure_password(
            length=args.length,
            include_symbols=not args.no_symbols
        )
        
        print(f"Generated password: {password}")
        
        # Validate the generated password
        validator = PasswordValidator()
        validation = validator.validate_password(password)
        
        print(f"\nPassword strength: {validation['strength']}")
        print(f"Score: {validation['score']}/100")
        print(f"Entropy: {validation['entropy']:.1f}")
        
        return 0
    
    def validate_password(self, args):
        """Validate password strength."""
        validator = PasswordValidator()
        result = validator.validate_password(args.password)
        
        print(f"Password validation results:")
        print(f"  Valid: {result['is_valid']}")
        print(f"  Strength: {result['strength']}")
        print(f"  Score: {result['score']}/100")
        print(f"  Entropy: {result['entropy']:.1f}")
        
        if result['errors']:
            print(f"\nErrors:")
            for error in result['errors']:
                print(f"  - {error}")
        
        if result['warnings']:
            print(f"\nWarnings:")
            for warning in result['warnings']:
                print(f"  - {warning}")
        
        if result['character_counts']:
            print(f"\nCharacter counts:")
            counts = result['character_counts']
            print(f"  Uppercase: {counts['uppercase']}")
            print(f"  Lowercase: {counts['lowercase']}")
            print(f"  Digits: {counts['digits']}")
            print(f"  Special: {counts['special']}")
        
        return 0 if result['is_valid'] else 1
    
    def generate_token(self, args):
        """Generate various types of tokens."""
        if args.type == 'api':
            token = TokenGenerator.generate_api_key(args.prefix, args.length)
        elif args.type == 'session':
            token = TokenGenerator.generate_session_token()
        elif args.type == 'verification':
            token = TokenGenerator.generate_verification_code(args.length)
        elif args.type == 'recovery':
            token = TokenGenerator.generate_recovery_token()
        else:
            token = TokenGenerator.generate_secure_token(args.length)
        
        print(f"Generated {args.type} token: {token}")
        return 0
    
    def validate_config(self, args):
        """Validate security configuration."""
        validator = SecurityConfigValidator(args.config_file)
        
        if validator.load_config():
            result = validator.validate_config()
            
            print(f"Configuration validation results:")
            print(f"  Valid: {result['is_valid']}")
            print(f"  Issues: {result['summary']['total_issues']}")
            print(f"  Warnings: {result['summary']['total_warnings']}")
            print(f"  Recommendations: {result['summary']['total_recommendations']}")
            
            if result['issues']:
                print(f"\nIssues:")
                for issue in result['issues']:
                    print(f"  - {issue}")
            
            if result['warnings']:
                print(f"\nWarnings:")
                for warning in result['warnings']:
                    print(f"  - {warning}")
            
            if result['recommendations']:
                print(f"\nRecommendations:")
                for rec in result['recommendations']:
                    print(f"  - {rec}")
            
            return 0 if result['is_valid'] else 1
        else:
            print(f"Could not load configuration file: {args.config_file}")
            return 1
    
    def generate_config(self, args):
        """Generate secure configuration."""
        validator = SecurityConfigValidator()
        
        if validator.save_secure_config(args.output_file):
            print(f"Secure configuration generated: {args.output_file}")
            return 0
        else:
            print("Failed to generate configuration")
            return 1
    
    def health_check(self, args):
        """Run security health check."""
        checker = SecurityHealthChecker()
        result = checker.run_security_audit()
        
        print(f"Security Health Check Results:")
        print(f"  Grade: {result['grade']}")
        print(f"  Score: {result['score']}/{result['max_score']} ({result['percentage']:.1f}%)")
        
        print(f"\nChecks:")
        for check in result['checks']:
            status_icon = "✓" if check['status'] == 'PASS' else "⚠" if check['status'] == 'WARN' else "✗"
            print(f"  {status_icon} {check['name']}: {check['details']}")
        
        if result['recommendations']:
            print(f"\nRecommendations:")
            for rec in result['recommendations']:
                print(f"  - {rec}")
        
        return 0 if result['grade'] in ['A', 'B'] else 1


def create_parser():
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Security management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  security create-user --username john --email john@example.com --password SecurePass123! --role operator
  security authenticate --username john --password SecurePass123!
  security list-users
  security generate-password --length 16
  security validate-config
  security health-check
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Create user command
    create_user_parser = subparsers.add_parser('create-user', help='Create a new user')
    create_user_parser.add_argument('--username', required=True, help='Username')
    create_user_parser.add_argument('--email', required=True, help='Email address')
    create_user_parser.add_argument('--password', required=True, help='Password')
    create_user_parser.add_argument('--role', default='viewer', 
                                   choices=['viewer', 'operator', 'admin', 'super_admin'],
                                   help='User role')
    
    # Authenticate command
    auth_parser = subparsers.add_parser('authenticate', help='Authenticate user')
    auth_parser.add_argument('--username', required=True, help='Username')
    auth_parser.add_argument('--password', required=True, help='Password')
    auth_parser.add_argument('--save-token', help='Save token to file')
    
    # Validate token command
    token_parser = subparsers.add_parser('validate-token', help='Validate token')
    token_parser.add_argument('--token', required=True, help='JWT token')
    
    # List users command
    list_parser = subparsers.add_parser('list-users', help='List all users')
    
    # Change password command
    change_pass_parser = subparsers.add_parser('change-password', help='Change user password')
    change_pass_parser.add_argument('--user-id', required=True, help='User ID')
    change_pass_parser.add_argument('--old-password', required=True, help='Old password')
    change_pass_parser.add_argument('--new-password', required=True, help='New password')
    
    # Update role command
    role_parser = subparsers.add_parser('update-role', help='Update user role')
    role_parser.add_argument('--user-id', required=True, help='User ID')
    role_parser.add_argument('--role', required=True, 
                            choices=['viewer', 'operator', 'admin', 'super_admin'],
                            help='New role')
    
    # Generate password command
    gen_pass_parser = subparsers.add_parser('generate-password', help='Generate secure password')
    gen_pass_parser.add_argument('--length', type=int, default=16, help='Password length')
    gen_pass_parser.add_argument('--no-symbols', action='store_true', help='Exclude symbols')
    
    # Validate password command
    val_pass_parser = subparsers.add_parser('validate-password', help='Validate password strength')
    val_pass_parser.add_argument('--password', required=True, help='Password to validate')
    
    # Generate token command
    gen_token_parser = subparsers.add_parser('generate-token', help='Generate token')
    gen_token_parser.add_argument('--type', default='secure', 
                                 choices=['secure', 'api', 'session', 'verification', 'recovery'],
                                 help='Token type')
    gen_token_parser.add_argument('--length', type=int, default=32, help='Token length')
    gen_token_parser.add_argument('--prefix', default='api', help='API key prefix')
    
    # Validate config command
    val_config_parser = subparsers.add_parser('validate-config', help='Validate security configuration')
    val_config_parser.add_argument('--config-file', default='security_config.json', help='Config file path')
    
    # Generate config command
    gen_config_parser = subparsers.add_parser('generate-config', help='Generate secure configuration')
    gen_config_parser.add_argument('--output-file', default='security_config.json', help='Output file path')
    
    # Health check command
    health_parser = subparsers.add_parser('health-check', help='Run security health check')
    
    return parser


def main():
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    cli = SecurityCLI()
    
    # Map commands to methods
    command_map = {
        'create-user': cli.create_user,
        'authenticate': cli.authenticate_user,
        'validate-token': cli.validate_token,
        'list-users': cli.list_users,
        'change-password': cli.change_password,
        'update-role': cli.update_user_role,
        'generate-password': cli.generate_password,
        'validate-password': cli.validate_password,
        'generate-token': cli.generate_token,
        'validate-config': cli.validate_config,
        'generate-config': cli.generate_config,
        'health-check': cli.health_check
    }
    
    if args.command in command_map:
        return command_map[args.command](args)
    else:
        print(f"Unknown command: {args.command}")
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
