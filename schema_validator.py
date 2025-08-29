#!/usr/bin/env python3
"""
Data Schema Validator

Provides utilities for validating data against schemas and custom
validation rules with detailed error reporting.
"""

import json
import re
import logging
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)

class ValidationError(Enum):
    """Types of validation errors."""
    REQUIRED_FIELD_MISSING = "required_field_missing"
    INVALID_TYPE = "invalid_type"
    INVALID_FORMAT = "invalid_format"
    INVALID_VALUE = "invalid_value"
    INVALID_LENGTH = "invalid_length"
    INVALID_RANGE = "invalid_range"
    CUSTOM_VALIDATION_FAILED = "custom_validation_failed"

@dataclass
class ValidationIssue:
    """A validation issue."""
    path: str
    error_type: ValidationError
    message: str
    value: Any = None
    expected: Any = None

@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool
    issues: List[ValidationIssue]
    data: Any

class SchemaValidator:
    """Main schema validator."""
    
    def __init__(self):
        self.custom_validators: Dict[str, Callable] = {}
        self._setup_default_validators()
    
    def _setup_default_validators(self):
        """Setup default validation functions."""
        self.custom_validators.update({
            'email': self._validate_email,
            'url': self._validate_url,
            'date': self._validate_date,
            'datetime': self._validate_datetime,
            'uuid': self._validate_uuid,
            'ipv4': self._validate_ipv4,
            'ipv6': self._validate_ipv6
        })
    
    def validate(self, data: Any, schema: Dict[str, Any]) -> ValidationResult:
        """Validate data against a schema."""
        issues = []
        
        try:
            self._validate_data(data, schema, "", issues)
        except Exception as e:
            issues.append(ValidationIssue(
                path="",
                error_type=ValidationError.CUSTOM_VALIDATION_FAILED,
                message=f"Validation error: {str(e)}",
                value=data
            ))
        
        return ValidationResult(
            is_valid=len(issues) == 0,
            issues=issues,
            data=data
        )
    
    def _validate_data(self, data: Any, schema: Dict[str, Any], path: str, issues: List[ValidationIssue]):
        """Recursively validate data against schema."""
        # Check required fields
        if 'required' in schema and isinstance(data, dict):
            required_fields = schema['required']
            for field in required_fields:
                if field not in data:
                    issues.append(ValidationIssue(
                        path=f"{path}.{field}" if path else field,
                        error_type=ValidationError.REQUIRED_FIELD_MISSING,
                        message=f"Required field '{field}' is missing",
                        expected=field
                    ))
        
        # Check type
        if 'type' in schema:
            if not self._validate_type(data, schema['type']):
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_TYPE,
                    message=f"Expected type '{schema['type']}', got '{type(data).__name__}'",
                    value=data,
                    expected=schema['type']
                ))
        
        # Check format
        if 'format' in schema and isinstance(data, str):
            if not self._validate_format(data, schema['format']):
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_FORMAT,
                    message=f"Invalid format '{schema['format']}' for value",
                    value=data,
                    expected=schema['format']
                ))
        
        # Check minimum/maximum values
        if 'minimum' in schema and isinstance(data, (int, float)):
            if data < schema['minimum']:
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_RANGE,
                    message=f"Value {data} is less than minimum {schema['minimum']}",
                    value=data,
                    expected=f">= {schema['minimum']}"
                ))
        
        if 'maximum' in schema and isinstance(data, (int, float)):
            if data > schema['maximum']:
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_RANGE,
                    message=f"Value {data} is greater than maximum {schema['maximum']}",
                    value=data,
                    expected=f"<= {schema['maximum']}"
                ))
        
        # Check minimum/maximum length
        if 'minLength' in schema and isinstance(data, str):
            if len(data) < schema['minLength']:
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_LENGTH,
                    message=f"String length {len(data)} is less than minimum {schema['minLength']}",
                    value=data,
                    expected=f"length >= {schema['minLength']}"
                ))
        
        if 'maxLength' in schema and isinstance(data, str):
            if len(data) > schema['maxLength']:
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_LENGTH,
                    message=f"String length {len(data)} is greater than maximum {schema['maxLength']}",
                    value=data,
                    expected=f"length <= {schema['maxLength']}"
                ))
        
        # Check pattern
        if 'pattern' in schema and isinstance(data, str):
            if not re.match(schema['pattern'], data):
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_FORMAT,
                    message=f"Value does not match pattern '{schema['pattern']}'",
                    value=data,
                    expected=f"matches pattern: {schema['pattern']}"
                ))
        
        # Check enum values
        if 'enum' in schema:
            if data not in schema['enum']:
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_VALUE,
                    message=f"Value {data} is not in allowed values {schema['enum']}",
                    value=data,
                    expected=schema['enum']
                ))
        
        # Validate nested objects
        if 'properties' in schema and isinstance(data, dict):
            for prop_name, prop_schema in schema['properties'].items():
                if prop_name in data:
                    prop_path = f"{path}.{prop_name}" if path else prop_name
                    self._validate_data(data[prop_name], prop_schema, prop_path, issues)
        
        # Validate arrays
        if 'items' in schema and isinstance(data, list):
            item_schema = schema['items']
            for i, item in enumerate(data):
                item_path = f"{path}[{i}]" if path else f"[{i}]"
                self._validate_data(item, item_schema, item_path, issues)
        
        # Check array length
        if 'minItems' in schema and isinstance(data, list):
            if len(data) < schema['minItems']:
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_LENGTH,
                    message=f"Array length {len(data)} is less than minimum {schema['minItems']}",
                    value=data,
                    expected=f"length >= {schema['minItems']}"
                ))
        
        if 'maxItems' in schema and isinstance(data, list):
            if len(data) > schema['maxItems']:
                issues.append(ValidationIssue(
                    path=path,
                    error_type=ValidationError.INVALID_LENGTH,
                    message=f"Array length {len(data)} is greater than maximum {schema['maxItems']}",
                    value=data,
                    expected=f"length <= {schema['maxItems']}"
                ))
    
    def _validate_type(self, data: Any, expected_type: str) -> bool:
        """Validate data type."""
        if expected_type == 'string':
            return isinstance(data, str)
        elif expected_type == 'number':
            return isinstance(data, (int, float))
        elif expected_type == 'integer':
            return isinstance(data, int)
        elif expected_type == 'boolean':
            return isinstance(data, bool)
        elif expected_type == 'object':
            return isinstance(data, dict)
        elif expected_type == 'array':
            return isinstance(data, list)
        elif expected_type == 'null':
            return data is None
        else:
            return True  # Unknown type, assume valid
    
    def _validate_format(self, data: str, format_type: str) -> bool:
        """Validate data format."""
        if format_type in self.custom_validators:
            return self.custom_validators[format_type](data)
        else:
            return True  # Unknown format, assume valid
    
    def _validate_email(self, email: str) -> bool:
        """Validate email format."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def _validate_url(self, url: str) -> bool:
        """Validate URL format."""
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, url))
    
    def _validate_date(self, date_str: str) -> bool:
        """Validate date format (YYYY-MM-DD)."""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def _validate_datetime(self, datetime_str: str) -> bool:
        """Validate datetime format (ISO 8601)."""
        try:
            datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            return True
        except ValueError:
            return False
    
    def _validate_uuid(self, uuid_str: str) -> bool:
        """Validate UUID format."""
        pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(pattern, uuid_str.lower()))
    
    def _validate_ipv4(self, ip_str: str) -> bool:
        """Validate IPv4 address."""
        pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
        if not re.match(pattern, ip_str):
            return False
        
        parts = ip_str.split('.')
        return all(0 <= int(part) <= 255 for part in parts)
    
    def _validate_ipv6(self, ip_str: str) -> bool:
        """Validate IPv6 address."""
        # Simplified IPv6 validation
        pattern = r'^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$'
        return bool(re.match(pattern, ip_str))
    
    def add_custom_validator(self, name: str, validator_func: Callable):
        """Add a custom validator function."""
        self.custom_validators[name] = validator_func
        logger.info(f"Added custom validator: {name}")
    
    def create_schema(self, **kwargs) -> Dict[str, Any]:
        """Create a schema with the specified properties."""
        return kwargs

def create_validator() -> SchemaValidator:
    """Create a schema validator with default validators."""
    return SchemaValidator()

def validate_data(data: Any, schema: Dict[str, Any]) -> ValidationResult:
    """Convenience function to validate data."""
    validator = create_validator()
    return validator.validate(data, schema)

if __name__ == "__main__":
    # Example usage
    validator = create_validator()
    
    # Sample schema
    user_schema = {
        "type": "object",
        "required": ["name", "email", "age"],
        "properties": {
            "name": {
                "type": "string",
                "minLength": 2,
                "maxLength": 50
            },
            "email": {
                "type": "string",
                "format": "email"
            },
            "age": {
                "type": "integer",
                "minimum": 0,
                "maximum": 150
            },
            "website": {
                "type": "string",
                "format": "url"
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 0,
                "maxItems": 10
            }
        }
    }
    
    # Test data
    valid_data = {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "website": "https://example.com",
        "tags": ["developer", "python"]
    }
    
    invalid_data = {
        "name": "J",  # Too short
        "email": "invalid-email",  # Invalid email
        "age": 200,  # Too old
        "website": "not-a-url",  # Invalid URL
        "tags": ["tag"] * 15  # Too many tags
    }
    
    # Validate data
    print("=== Validating valid data ===")
    result1 = validate_data(valid_data, user_schema)
    print(f"Valid: {result1.is_valid}")
    for issue in result1.issues:
        print(f"  {issue.path}: {issue.message}")
    
    print("\n=== Validating invalid data ===")
    result2 = validate_data(invalid_data, user_schema)
    print(f"Valid: {result2.is_valid}")
    for issue in result2.issues:
        print(f"  {issue.path}: {issue.message}")
