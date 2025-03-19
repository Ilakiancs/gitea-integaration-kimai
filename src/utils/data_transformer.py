#!/usr/bin/env python3
"""
Data Transformation Utilities for Gitea-Kimai Integration

This module provides utilities for transforming data between Gitea and Kimai formats,
handling field mappings, data validation, and format conversions.
"""

import re
import json
import logging
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

class DataType(Enum):
    """Data types for transformation."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    LIST = "list"
    DICT = "dict"

@dataclass
class FieldMapping:
    """Field mapping configuration."""
    source_field: str
    target_field: str
    data_type: DataType
    required: bool = False
    default_value: Any = None
    transformer: Optional[Callable] = None
    validator: Optional[Callable] = None

@dataclass
class TransformationRule:
    """Rule for data transformation."""
    name: str
    source_format: str
    target_format: str
    field_mappings: List[FieldMapping] = field(default_factory=list)
    custom_transformers: Dict[str, Callable] = field(default_factory=dict)

class DataTransformer:
    """Main data transformation engine."""

    def __init__(self):
        self.rules: Dict[str, TransformationRule] = {}
        self.custom_validators: Dict[str, Callable] = {}
        self.custom_converters: Dict[str, Callable] = {}
        self._register_default_converters()
        self._register_default_validators()

    def _register_default_converters(self):
        """Register default data type converters."""
        self.custom_converters.update({
            'to_string': lambda x: str(x) if x is not None else '',
            'to_int': lambda x: int(x) if x is not None and str(x).isdigit() else 0,
            'to_float': lambda x: float(x) if x is not None else 0.0,
            'to_bool': lambda x: bool(x) if x is not None else False,
            'to_datetime': self._convert_to_datetime,
            'to_list': lambda x: x if isinstance(x, list) else [x] if x is not None else [],
            'to_dict': lambda x: x if isinstance(x, dict) else {},
            'sanitize_text': self._sanitize_text,
            'extract_numbers': self._extract_numbers,
            'normalize_email': self._normalize_email,
            'format_duration': self._format_duration
        })

    def _register_default_validators(self):
        """Register default field validators."""
        self.custom_validators.update({
            'required': lambda x: x is not None and str(x).strip() != '',
            'email': lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', x) is not None,
            'url': lambda x: re.match(r'^https?://', x) is not None,
            'positive_number': lambda x: isinstance(x, (int, float)) and x > 0,
            'non_empty_string': lambda x: isinstance(x, str) and len(x.strip()) > 0,
            'valid_status': lambda x: x in ['open', 'closed', 'in_progress', 'completed'],
            'valid_priority': lambda x: x in ['low', 'medium', 'high', 'critical']
        })

    def register_rule(self, rule: TransformationRule):
        """Register a transformation rule."""
        self.rules[rule.name] = rule
        logger.debug(f"Registered transformation rule: {rule.name}")

    def register_converter(self, name: str, converter: Callable):
        """Register a custom converter function."""
        self.custom_converters[name] = converter

    def register_validator(self, name: str, validator: Callable):
        """Register a custom validator function."""
        self.custom_validators[name] = validator

    def transform(self, data: Dict[str, Any], rule_name: str) -> Dict[str, Any]:
        """Transform data using the specified rule."""
        if rule_name not in self.rules:
            raise ValueError(f"Unknown transformation rule: {rule_name}")

        rule = self.rules[rule_name]
        result = {}
        errors = []

        logger.debug(f"Transforming data using rule: {rule_name}")

        for mapping in rule.field_mappings:
            try:
                value = self._extract_value(data, mapping.source_field)

                # Apply default value if needed
                if value is None and mapping.default_value is not None:
                    value = mapping.default_value

                # Validate required fields
                if mapping.required and (value is None or str(value).strip() == ''):
                    errors.append(f"Required field '{mapping.source_field}' is missing or empty")
                    continue

                # Apply custom transformer if specified
                if mapping.transformer:
                    value = mapping.transformer(value)

                # Apply data type conversion
                value = self._convert_value(value, mapping.data_type)

                # Apply field validator if specified
                if mapping.validator and not mapping.validator(value):
                    errors.append(f"Validation failed for field '{mapping.source_field}' with value: {value}")
                    continue

                # Set the transformed value
                self._set_value(result, mapping.target_field, value)

            except Exception as e:
                error_msg = f"Error transforming field '{mapping.source_field}': {e}"
                errors.append(error_msg)
                logger.error(error_msg)

        # Apply custom transformers
        for transformer_name, transformer_func in rule.custom_transformers.items():
            try:
                result = transformer_func(result, data)
            except Exception as e:
                error_msg = f"Error in custom transformer '{transformer_name}': {e}"
                errors.append(error_msg)
                logger.error(error_msg)

        if errors:
            logger.warning(f"Transformation completed with {len(errors)} errors")
            result['_transformation_errors'] = errors

        return result

    def _extract_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """Extract value from nested dictionary using dot notation."""
        if '.' not in field_path:
            return data.get(field_path)

        keys = field_path.split('.')
        value = data

        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None

        return value

    def _set_value(self, data: Dict[str, Any], field_path: str, value: Any):
        """Set value in nested dictionary using dot notation."""
        if '.' not in field_path:
            data[field_path] = value
            return

        keys = field_path.split('.')
        current = data

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value

    def _convert_value(self, value: Any, data_type: DataType) -> Any:
        """Convert value to specified data type."""
        if value is None:
            return None

        try:
            if data_type == DataType.STRING:
                return str(value)
            elif data_type == DataType.INTEGER:
                if isinstance(value, str) and value.strip() == '':
                    return 0
                return int(float(value))  # Handle string floats
            elif data_type == DataType.FLOAT:
                if isinstance(value, str) and value.strip() == '':
                    return 0.0
                return float(value)
            elif data_type == DataType.BOOLEAN:
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                return bool(value)
            elif data_type == DataType.DATETIME:
                return self._convert_to_datetime(value)
            elif data_type == DataType.LIST:
                if isinstance(value, list):
                    return value
                elif isinstance(value, str):
                    # Try to parse as JSON array or comma-separated
                    try:
                        return json.loads(value)
                    except:
                        return [v.strip() for v in value.split(',') if v.strip()]
                else:
                    return [value]
            elif data_type == DataType.DICT:
                if isinstance(value, dict):
                    return value
                elif isinstance(value, str):
                    try:
                        return json.loads(value)
                    except:
                        return {}
                else:
                    return {}
        except Exception as e:
            logger.warning(f"Failed to convert value '{value}' to {data_type}: {e}")
            return value

        return value

    def _convert_to_datetime(self, value: Any) -> Optional[datetime]:
        """Convert various datetime formats to datetime object."""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, (int, float)):
            # Assume timestamp
            try:
                return datetime.fromtimestamp(value, tz=timezone.utc)
            except:
                return None

        if isinstance(value, str):
            # Try common datetime formats
            formats = [
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m/%d/%Y'
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue

        return None

    def _sanitize_text(self, text: str) -> str:
        """Sanitize text by removing unwanted characters."""
        if not isinstance(text, str):
            return str(text)

        # Remove control characters and normalize whitespace
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_numbers(self, text: str) -> List[float]:
        """Extract numbers from text."""
        if not isinstance(text, str):
            return []

        numbers = re.findall(r'-?\d+\.?\d*', text)
        return [float(n) for n in numbers if n]

    def _normalize_email(self, email: str) -> str:
        """Normalize email address."""
        if not isinstance(email, str):
            return ''

        email = email.lower().strip()
        # Remove dots from Gmail addresses before @
        if '@gmail.com' in email:
            local, domain = email.split('@', 1)
            local = local.replace('.', '')
            email = f"{local}@{domain}"

        return email

    def _format_duration(self, seconds: Union[int, float]) -> str:
        """Format duration in seconds to human readable format."""
        if not isinstance(seconds, (int, float)):
            return '0s'

        seconds = int(seconds)

        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            return f"{minutes}m {remaining_seconds}s" if remaining_seconds else f"{minutes}m"
        else:
            hours = seconds // 3600
            remaining_minutes = (seconds % 3600) // 60
            return f"{hours}h {remaining_minutes}m" if remaining_minutes else f"{hours}h"

    def create_gitea_to_kimai_rule(self) -> TransformationRule:
        """Create default transformation rule from Gitea to Kimai."""
        mappings = [
            FieldMapping("id", "external_id", DataType.STRING, required=True),
            FieldMapping("title", "description", DataType.STRING, required=True, transformer=self._sanitize_text),
            FieldMapping("state", "status", DataType.STRING, default_value="open"),
            FieldMapping("created_at", "begin", DataType.DATETIME, required=True),
            FieldMapping("updated_at", "end", DataType.DATETIME),
            FieldMapping("assignee.login", "user", DataType.STRING),
            FieldMapping("repository.name", "project", DataType.STRING, required=True),
            FieldMapping("labels", "tags", DataType.LIST),
            FieldMapping("milestone.title", "activity", DataType.STRING),
            FieldMapping("body", "description_long", DataType.STRING, transformer=self._sanitize_text)
        ]

        return TransformationRule(
            name="gitea_to_kimai",
            source_format="gitea_issue",
            target_format="kimai_timesheet",
            field_mappings=mappings,
            custom_transformers={
                'calculate_duration': self._calculate_duration_from_issue,
                'set_default_activity': self._set_default_activity
            }
        )

    def create_kimai_to_gitea_rule(self) -> TransformationRule:
        """Create default transformation rule from Kimai to Gitea."""
        mappings = [
            FieldMapping("id", "external_id", DataType.STRING, required=True),
            FieldMapping("description", "title", DataType.STRING, required=True),
            FieldMapping("begin", "created_at", DataType.DATETIME, required=True),
            FieldMapping("end", "updated_at", DataType.DATETIME),
            FieldMapping("user", "assignee", DataType.STRING),
            FieldMapping("project", "repository", DataType.STRING, required=True),
            FieldMapping("tags", "labels", DataType.LIST),
            FieldMapping("activity", "milestone", DataType.STRING)
        ]

        return TransformationRule(
            name="kimai_to_gitea",
            source_format="kimai_timesheet",
            target_format="gitea_issue",
            field_mappings=mappings,
            custom_transformers={
                'format_for_gitea': self._format_for_gitea_api
            }
        )

    def _calculate_duration_from_issue(self, result: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate duration from issue timestamps."""
        begin = result.get('begin')
        end = result.get('end')

        if begin and end and isinstance(begin, datetime) and isinstance(end, datetime):
            duration_seconds = (end - begin).total_seconds()
            result['duration'] = max(0, int(duration_seconds))
        else:
            result['duration'] = 0

        return result

    def _set_default_activity(self, result: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
        """Set default activity if not specified."""
        if not result.get('activity'):
            issue_type = source.get('labels', [])
            if 'bug' in [label.get('name', '').lower() for label in issue_type]:
                result['activity'] = 'Bug Fixing'
            elif 'enhancement' in [label.get('name', '').lower() for label in issue_type]:
                result['activity'] = 'Development'
            else:
                result['activity'] = 'General'

        return result

    def _format_for_gitea_api(self, result: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
        """Format data for Gitea API compatibility."""
        # Ensure required fields are present
        if 'labels' in result and isinstance(result['labels'], list):
            # Convert simple strings to label objects
            formatted_labels = []
            for label in result['labels']:
                if isinstance(label, str):
                    formatted_labels.append({'name': label})
                else:
                    formatted_labels.append(label)
            result['labels'] = formatted_labels

        return result

# Global transformer instance
_global_transformer = None

def get_transformer() -> DataTransformer:
    """Get global data transformer instance."""
    global _global_transformer

    if _global_transformer is None:
        _global_transformer = DataTransformer()

        # Register default rules
        _global_transformer.register_rule(_global_transformer.create_gitea_to_kimai_rule())
        _global_transformer.register_rule(_global_transformer.create_kimai_to_gitea_rule())

    return _global_transformer

def transform_data(data: Dict[str, Any], rule_name: str) -> Dict[str, Any]:
    """Convenience function to transform data."""
    transformer = get_transformer()
    return transformer.transform(data, rule_name)

def create_custom_mapping(source_field: str, target_field: str,
                         data_type: DataType = DataType.STRING,
                         required: bool = False, default_value: Any = None,
                         transformer: Optional[Callable] = None,
                         validator: Optional[Callable] = None) -> FieldMapping:
    """Create a custom field mapping."""
    return FieldMapping(
        source_field=source_field,
        target_field=target_field,
        data_type=data_type,
        required=required,
        default_value=default_value,
        transformer=transformer,
        validator=validator
    )
