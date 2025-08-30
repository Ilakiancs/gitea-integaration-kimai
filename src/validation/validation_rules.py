#!/usr/bin/env python3
"""
Validation Rules Engine

Provides a flexible validation rules engine for dynamic data validation
with support for custom rules, rule chaining, and validation contexts.
"""

import re
import json
import logging
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)

class RuleType(Enum):
    """Types of validation rules."""
    REQUIRED = "required"
    LENGTH = "length"
    PATTERN = "pattern"
    RANGE = "range"
    EMAIL = "email"
    URL = "url"
    CUSTOM = "custom"

@dataclass
class ValidationRule:
    """A validation rule definition."""
    name: str
    rule_type: RuleType
    parameters: Dict[str, Any]
    message: str
    enabled: bool = True

@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    field_name: str
    value: Any

class ValidationRulesEngine:
    """Engine for managing and executing validation rules."""
    
    def __init__(self):
        self.rules: Dict[str, ValidationRule] = {}
        self.custom_validators: Dict[str, Callable] = {}
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """Setup default validation rules."""
        default_rules = [
            ValidationRule(
                name="required",
                rule_type=RuleType.REQUIRED,
                parameters={},
                message="Field is required"
            ),
            ValidationRule(
                name="email",
                rule_type=RuleType.EMAIL,
                parameters={},
                message="Invalid email format"
            ),
            ValidationRule(
                name="url",
                rule_type=RuleType.URL,
                parameters={},
                message="Invalid URL format"
            ),
            ValidationRule(
                name="min_length_3",
                rule_type=RuleType.LENGTH,
                parameters={"min": 3},
                message="Minimum length is 3 characters"
            ),
            ValidationRule(
                name="max_length_100",
                rule_type=RuleType.LENGTH,
                parameters={"max": 100},
                message="Maximum length is 100 characters"
            )
        ]
        
        for rule in default_rules:
            self.add_rule(rule)
    
    def add_rule(self, rule: ValidationRule):
        """Add a validation rule."""
        self.rules[rule.name] = rule
        logger.info(f"Added validation rule: {rule.name}")
    
    def remove_rule(self, rule_name: str):
        """Remove a validation rule."""
        if rule_name in self.rules:
            del self.rules[rule_name]
            logger.info(f"Removed validation rule: {rule_name}")
    
    def add_custom_validator(self, name: str, validator_func: Callable):
        """Add a custom validator function."""
        self.custom_validators[name] = validator_func
        logger.info(f"Added custom validator: {name}")
    
    def validate_field(self, field_name: str, value: Any, rule_names: List[str]) -> ValidationResult:
        """Validate a field using specified rules."""
        errors = []
        warnings = []
        
        for rule_name in rule_names:
            if rule_name not in self.rules:
                errors.append(f"Unknown validation rule: {rule_name}")
                continue
            
            rule = self.rules[rule_name]
            if not rule.enabled:
                continue
            
            if not self._apply_rule(rule, value):
                errors.append(rule.message)
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            field_name=field_name,
            value=value
        )
    
    def _apply_rule(self, rule: ValidationRule, value: Any) -> bool:
        """Apply a single validation rule."""
        if rule.rule_type == RuleType.REQUIRED:
            return self._validate_required(value)
        elif rule.rule_type == RuleType.LENGTH:
            return self._validate_length(value, rule.parameters)
        elif rule.rule_type == RuleType.PATTERN:
            return self._validate_pattern(value, rule.parameters)
        elif rule.rule_type == RuleType.RANGE:
            return self._validate_range(value, rule.parameters)
        elif rule.rule_type == RuleType.EMAIL:
            return self._validate_email(value)
        elif rule.rule_type == RuleType.URL:
            return self._validate_url(value)
        elif rule.rule_type == RuleType.CUSTOM:
            return self._validate_custom(value, rule.parameters)
        
        return True
    
    def _validate_required(self, value: Any) -> bool:
        """Validate that a field is required."""
        if value is None:
            return False
        if isinstance(value, str) and not value.strip():
            return False
        return True
    
    def _validate_length(self, value: Any, params: Dict[str, Any]) -> bool:
        """Validate string length."""
        if not isinstance(value, str):
            return True
        
        length = len(value)
        min_length = params.get('min')
        max_length = params.get('max')
        
        if min_length is not None and length < min_length:
            return False
        if max_length is not None and length > max_length:
            return False
        
        return True
    
    def _validate_pattern(self, value: Any, params: Dict[str, Any]) -> bool:
        """Validate against regex pattern."""
        if not isinstance(value, str):
            return True
        
        pattern = params.get('pattern')
        if not pattern:
            return True
        
        return bool(re.match(pattern, value))
    
    def _validate_range(self, value: Any, params: Dict[str, Any]) -> bool:
        """Validate numeric range."""
        if not isinstance(value, (int, float)):
            return True
        
        min_val = params.get('min')
        max_val = params.get('max')
        
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        
        return True
    
    def _validate_email(self, value: Any) -> bool:
        """Validate email format."""
        if not isinstance(value, str):
            return True
        
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, value))
    
    def _validate_url(self, value: Any) -> bool:
        """Validate URL format."""
        if not isinstance(value, str):
            return True
        
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, value))
    
    def _validate_custom(self, value: Any, params: Dict[str, Any]) -> bool:
        """Validate using custom validator."""
        validator_name = params.get('validator')
        if not validator_name or validator_name not in self.custom_validators:
            return True
        
        validator_func = self.custom_validators[validator_name]
        return validator_func(value, params)

def create_validation_engine() -> ValidationRulesEngine:
    """Create a validation rules engine with common rules."""
    engine = ValidationRulesEngine()
    
    # Add some custom validators
    def validate_positive_number(value, params):
        return isinstance(value, (int, float)) and value > 0
    
    def validate_alpha_numeric(value, params):
        return isinstance(value, str) and value.replace(' ', '').isalnum()
    
    engine.add_custom_validator('positive_number', validate_positive_number)
    engine.add_custom_validator('alpha_numeric', validate_alpha_numeric)
    
    return engine

if __name__ == "__main__":
    # Example usage
    engine = create_validation_engine()
    
    # Test validation
    result = engine.validate_field("email", "test@example.com", ["required", "email"])
    print(f"Email validation: {result.is_valid}")
    
    result = engine.validate_field("name", "", ["required"])
    print(f"Name validation: {result.is_valid}, Errors: {result.errors}")
