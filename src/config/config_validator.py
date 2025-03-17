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
import re
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

        self.env_var_pattern = re.compile(r'\$\{([A-Z_][A-Z0-9_]*)\}')
        self.env_defaults = {
            'GITEA_URL': 'https://gitea.example.com',
            'KIMAI_URL': 'https://kimai.example.com',
            'SYNC_INTERVAL': '300',
            'LOG_LEVEL': 'INFO',
            'MAX_RETRIES': '3',
            'TIMEOUT': '30'
        }

    def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate a configuration dictionary."""
        issues = []

        # Expand environment variables first
        expanded_config = self._expand_env_vars(config)

        # Validate environment variable references
        env_issues = self._validate_env_vars(config)
        issues.extend(env_issues)

        # Check required fields
        for field, expected_type in self.required_fields.items():
            if field not in expanded_config:
                issues.append(ValidationIssue(
                    level=ValidationLevel.ERROR,
                    field=field,
                    message=f"Required field '{field}' is missing",
                    expected=expected_type.__name__
                ))
            else:
                # Check type after env var expansion
                value = expanded_config[field]
                if not isinstance(value, expected_type):
                    # Try to convert strings to expected types
                    if expected_type == int and isinstance(value, str):
                        try:
                            value = int(value)
                            expanded_config[field] = value
                        except ValueError:
                            issues.append(ValidationIssue(
                                level=ValidationLevel.ERROR,
                                field=field,
                                message=f"Field '{field}' cannot be converted to {expected_type.__name__}",
                                value=value,
                                expected=expected_type.__name__
                            ))
                            continue
                    elif expected_type == bool and isinstance(value, str):
                        if value.lower() in ('true', 'false', '1', '0', 'yes', 'no'):
                            value = value.lower() in ('true', '1', 'yes')
                            expanded_config[field] = value
                        else:
                            issues.append(ValidationIssue(
                                level=ValidationLevel.ERROR,
                                field=field,
                                message=f"Field '{field}' cannot be converted to {expected_type.__name__}",
                                value=value,
                                expected=expected_type.__name__
                            ))
                            continue
                    else:
                        issues.append(ValidationIssue(
                            level=ValidationLevel.ERROR,
                            field=field,
                            message=f"Field '{field}' must be of type {expected_type.__name__}",
                            value=type(value).__name__,
                            expected=expected_type.__name__
                        ))
                        continue

                # Run field-specific validation
                if field in self.field_validators:
                    field_issues = self.field_validators[field](field, value)
                    issues.extend(field_issues)

        # Check optional fields if present
        for field, expected_type in self.optional_fields.items():
            if field in expanded_config:
                value = expanded_config[field]
                if not isinstance(value, expected_type):
                    # Try to convert strings to expected types for optional fields too
                    if expected_type == int and isinstance(value, str):
                        try:
                            value = int(value)
                            expanded_config[field] = value
                        except ValueError:
                            issues.append(ValidationIssue(
                                level=ValidationLevel.WARNING,
                                field=field,
                                message=f"Optional field '{field}' cannot be converted to {expected_type.__name__}",
                                value=value,
                                expected=expected_type.__name__
                            ))
                            continue
                    elif expected_type == bool and isinstance(value, str):
                        if value.lower() in ('true', 'false', '1', '0', 'yes', 'no'):
                            value = value.lower() in ('true', '1', 'yes')
                            expanded_config[field] = value
                        else:
                            issues.append(ValidationIssue(
                                level=ValidationLevel.WARNING,
                                field=field,
                                message=f"Optional field '{field}' cannot be converted to {expected_type.__name__}",
                                value=value,
                                expected=expected_type.__name__
                            ))
                            continue
                    else:
                        issues.append(ValidationIssue(
                            level=ValidationLevel.WARNING,
                            field=field,
                            message=f"Optional field '{field}' should be of type {expected_type.__name__}",
                            value=type(value).__name__,
                            expected=expected_type.__name__
                        ))
                        continue

                # Run field-specific validation
                if field in self.field_validators:
                    field_issues = self.field_validators[field](field, value)
                    issues.extend(field_issues)

        # Check for unknown fields
        known_fields = set(self.required_fields.keys()) | set(self.optional_fields.keys())
        for field in expanded_config.keys():
            if field not in known_fields:
                issues.append(ValidationIssue(
                    level=ValidationLevel.WARNING,
                    field=field,
                    message=f"Unknown field '{field}' in configuration",
                    value=expanded_config[field]
                ))

        is_valid = not any(issue.level == ValidationLevel.ERROR for issue in issues)

        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            config=expanded_config
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

    def _expand_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Expand environment variables in configuration values."""
        expanded = {}

        for key, value in config.items():
            if isinstance(value, str):
                # Find environment variable references
                matches = self.env_var_pattern.findall(value)
                expanded_value = value

                for env_var in matches:
                    env_value = os.environ.get(env_var)
                    if env_value is not None:
                        expanded_value = expanded_value.replace(f'${{{env_var}}}', env_value)
                    elif env_var in self.env_defaults:
                        expanded_value = expanded_value.replace(f'${{{env_var}}}', self.env_defaults[env_var])

                expanded[key] = expanded_value
            else:
                expanded[key] = value

        return expanded

    def _validate_env_vars(self, config: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate environment variable references."""
        issues = []

        for key, value in config.items():
            if isinstance(value, str):
                matches = self.env_var_pattern.findall(value)

                for env_var in matches:
                    if env_var not in os.environ and env_var not in self.env_defaults:
                        issues.append(ValidationIssue(
                            level=ValidationLevel.WARNING,
                            field=key,
                            message=f"Environment variable '{env_var}' is not set and has no default",
                            value=env_var
                        ))

        return issues

    def _validate_email(self, field: str, value: str) -> List[ValidationIssue]:
        """Validate email format."""
        issues = []
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
            'gitea_url': '${GITEA_URL}',
            'gitea_token': '${GITEA_TOKEN}',
            'kimai_url': '${KIMAI_URL}',
            'kimai_token': '${KIMAI_TOKEN}',
            'sync_interval': '${SYNC_INTERVAL}',
            'database_path': 'sync.db',
            'log_level': '${LOG_LEVEL}',
            'max_retries': '${MAX_RETRIES}',
            'timeout': '${TIMEOUT}',
            'enable_notifications': False,
            'notification_email': 'admin@example.com'
        }

    def validate_environment(self) -> ValidationResult:
        """Validate required environment variables are available."""
        issues = []
        required_env_vars = ['GITEA_TOKEN', 'KIMAI_TOKEN']

        for env_var in required_env_vars:
            if env_var not in os.environ:
                issues.append(ValidationIssue(
                    level=ValidationLevel.ERROR,
                    field=env_var,
                    message=f"Required environment variable '{env_var}' is not set"
                ))

        # Check optional env vars with warnings
        optional_env_vars = ['GITEA_URL', 'KIMAI_URL', 'SYNC_INTERVAL', 'LOG_LEVEL']
        for env_var in optional_env_vars:
            if env_var not in os.environ and env_var not in self.env_defaults:
                issues.append(ValidationIssue(
                    level=ValidationLevel.WARNING,
                    field=env_var,
                    message=f"Environment variable '{env_var}' is not set, will use default if available"
                ))

        is_valid = not any(issue.level == ValidationLevel.ERROR for issue in issues)

        return ValidationResult(
            is_valid=is_valid,
            issues=issues,
            config={}
        )

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
