#!/usr/bin/env python3
"""
Configuration Validator

Provides validation for configuration files to ensure they are correct,
complete, and contain valid values for the sync system.
"""

import os
import json
import yaml
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)

class ValidationLevel(Enum):
    """Validation levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

@dataclass
class ValidationIssue:
    """A validation issue found in configuration."""
    level: ValidationLevel
    field: str
    message: str
    value: Any = None
    expected: Any = None

@dataclass
class ValidationResult:
    """Result of configuration validation."""
    is_valid: bool
    issues: List[ValidationIssue]
    config: Dict[str, Any]

class ConfigValidator:
    """Validates configuration files."""
    
    def __init__(self):
        self.required_fields = {
            'gitea_url': str,
            'gitea_token': str,
            'kimai_url': str,
            'kimai_token': str,
            'sync_interval': int,
            'database_path': str
        }
        
        self.optional_fields = {
            'log_level': str,
            'max_retries': int,
            'timeout': int,
            'enable_notifications': bool,
            'notification_email': str
        }
        
        self.field_validators = {
            'gitea_url': self._validate_url,
            'kimai_url': self._validate_url,
            'sync_interval': self._validate_positive_int,
            'max_retries': self._validate_positive_int,
            'timeout': self._validate_positive_int,
            'log_level': self._validate_log_level,
            'notification_email': self._validate_email
        }
    
    def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate a configuration dictionary."""
        issues = []
        
        # Check required fields
        for field, expected_type in self.required_fields.items():
            if field not in config:
                issues.append(ValidationIssue(
                    level=ValidationLevel.ERROR,
                    field=field,
                    message=f"Required field '{field}' is missing",
                    expected=expected_type.__name__
                ))
            else:
                # Check type
                if not isinstance(config[field], expected_type):
                    issues.append(ValidationIssue(
                        level=ValidationLevel.ERROR,
                        field=field,
                        message=f"Field '{field}' must be of type {expected_type.__name__}",
                        value=type(config[field]).__name__,
                        expected=expected_type.__name__
                    ))
                else:
                    # Run field-specific validation
                    if field in self.field_validators:
                        field_issues = self.field_validators[field](field, config[field])
                        issues.extend(field_issues)
        
        # Check optional fields if present
        for field, expected_type in self.optional_fields.items():
            if field in config:
                if not isinstance(config[field], expected_type):
                    issues.append(ValidationIssue(
                        level=ValidationLevel.WARNING,
                        field=field,
                        message=f"Optional field '{field}' should be of type {expected_type.__name__}",
                        value=type(config[field]).__name__,
                        expected=expected_type.__name__
                    ))
                else:
                    # Run field-specific validation
                    if field in self.field_validators:
                        field_issues = self.field_validators[field](field, config[field])
                        issues.extend(field_issues)
        
        # Check for unknown fields
        known_fields = set(self.required_fields.keys()) | set(self.optional_fields.keys())
        for field in config.keys():
            if field not in known_fields:
                issues.append(ValidationIssue(
                    level=ValidationLevel.WARNING,
                    field=field,
                    message=f"Unknown field '{field}' in configuration",
                    value=config[field]
                ))
        
        is_valid = all(issue.level == ValidationLevel.ERROR for issue in issues)
        
        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            config=config
        )
    
    def _validate_url(self, field: str, value: str) -> List[ValidationIssue]:
        """Validate URL format."""
        issues = []
        if not value.startswith(('http://', 'https://')):
            issues.append(ValidationIssue(
                level=ValidationLevel.ERROR,
                field=field,
                message=f"Field '{field}' must be a valid URL starting with http:// or https://",
                value=value
            ))
        return issues
    
    def _validate_positive_int(self, field: str, value: int) -> List[ValidationIssue]:
        """Validate positive integer."""
        issues = []
        if value <= 0:
            issues.append(ValidationIssue(
                level=ValidationLevel.ERROR,
                field=field,
                message=f"Field '{field}' must be a positive integer",
                value=value
            ))
        return issues
    
    def _validate_log_level(self, field: str, value: str) -> List[ValidationIssue]:
        """Validate log level."""
        issues = []
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if value.upper() not in valid_levels:
            issues.append(ValidationIssue(
                level=ValidationLevel.WARNING,
                field=field,
                message=f"Field '{field}' should be one of: {', '.join(valid_levels)}",
                value=value,
                expected=valid_levels
            ))
        return issues
    
    def _validate_email(self, field: str, value: str) -> List[ValidationIssue]:
        """Validate email format."""
        issues = []
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, value):
            issues.append(ValidationIssue(
                level=ValidationLevel.WARNING,
                field=field,
                message=f"Field '{field}' should be a valid email address",
                value=value
            ))
        return issues
    
    def validate_config_file(self, file_path: str) -> ValidationResult:
        """Validate a configuration file."""
        if not os.path.exists(file_path):
            return ValidationResult(
                is_valid=False,
                issues=[ValidationIssue(
                    level=ValidationLevel.ERROR,
                    field="file",
                    message=f"Configuration file '{file_path}' does not exist"
                )],
                config={}
            )
        
        try:
            with open(file_path, 'r') as f:
                if file_path.endswith('.json'):
                    config = json.load(f)
                elif file_path.endswith(('.yml', '.yaml')):
                    config = yaml.safe_load(f)
                else:
                    return ValidationResult(
                        is_valid=False,
                        issues=[ValidationIssue(
                            level=ValidationLevel.ERROR,
                            field="file",
                            message=f"Unsupported file format. Use .json or .yml/.yaml"
                        )],
                        config={}
                    )
            
            return self.validate_config(config)
            
        except (json.JSONDecodeError, yaml.YAMLError) as e:
            return ValidationResult(
                is_valid=False,
                issues=[ValidationIssue(
                    level=ValidationLevel.ERROR,
                    field="file",
                    message=f"Invalid file format: {str(e)}"
                )],
                config={}
            )
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                issues=[ValidationIssue(
                    level=ValidationLevel.ERROR,
                    field="file",
                    message=f"Error reading file: {str(e)}"
                )],
                config={}
            )
    
    def create_sample_config(self) -> Dict[str, Any]:
        """Create a sample configuration with all required fields."""
        return {
            'gitea_url': 'https://gitea.example.com',
            'gitea_token': 'your_gitea_token_here',
            'kimai_url': 'https://kimai.example.com',
            'kimai_token': 'your_kimai_token_here',
            'sync_interval': 300,
            'database_path': 'sync.db',
            'log_level': 'INFO',
            'max_retries': 3,
            'timeout': 30,
            'enable_notifications': False,
            'notification_email': 'admin@example.com'
        }

def validate_config_file(file_path: str) -> ValidationResult:
    """Convenience function to validate a configuration file."""
    validator = ConfigValidator()
    return validator.validate_config_file(file_path)

def create_valid_config_file(file_path: str, format_type: str = 'json'):
    """Create a valid configuration file."""
    validator = ConfigValidator()
    config = validator.create_sample_config()
    
    with open(file_path, 'w') as f:
        if format_type == 'json':
            json.dump(config, f, indent=2)
        elif format_type in ['yml', 'yaml']:
            yaml.dump(config, f, default_flow_style=False)
    
    logger.info(f"Created sample configuration file: {file_path}")

if __name__ == "__main__":
    # Example usage
    validator = ConfigValidator()
    
    # Create and validate a sample config
    sample_config = validator.create_sample_config()
    result = validator.validate_config(sample_config)
    
    print(f"Config valid: {result.is_valid}")
    for issue in result.issues:
        print(f"{issue.level.value.upper()}: {issue.field} - {issue.message}")
    
    # Test with invalid config
    invalid_config = {
        'gitea_url': 'not_a_url',
        'sync_interval': -1
    }
    
    result = validator.validate_config(invalid_config)
    print(f"\nInvalid config valid: {result.is_valid}")
    for issue in result.issues:
        print(f"{issue.level.value.upper()}: {issue.field} - {issue.message}")
