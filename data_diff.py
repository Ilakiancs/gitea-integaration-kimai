#!/usr/bin/env python3
"""
Data Diff Utility

Provides utilities for comparing data structures and showing differences
between them, useful for sync operations and data validation.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)

class DiffType(Enum):
    """Types of differences."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"

@dataclass
class DiffItem:
    """A single difference item."""
    path: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None
    description: str = ""

@dataclass
class DiffResult:
    """Result of a diff operation."""
    has_changes: bool
    items: List[DiffItem]
    summary: Dict[str, int]

class DataDiff:
    """Main data diff utility."""
    
    def __init__(self, ignore_case: bool = False, ignore_whitespace: bool = False):
        self.ignore_case = ignore_case
        self.ignore_whitespace = ignore_whitespace
    
    def compare(self, old_data: Any, new_data: Any, path: str = "") -> DiffResult:
        """Compare two data structures and return differences."""
        items = []
        
        if isinstance(old_data, dict) and isinstance(new_data, dict):
            items.extend(self._compare_dicts(old_data, new_data, path))
        elif isinstance(old_data, list) and isinstance(new_data, list):
            items.extend(self._compare_lists(old_data, new_data, path))
        else:
            items.extend(self._compare_values(old_data, new_data, path))
        
        # Generate summary
        summary = {
            'added': len([item for item in items if item.diff_type == DiffType.ADDED]),
            'removed': len([item for item in items if item.diff_type == DiffType.REMOVED]),
            'modified': len([item for item in items if item.diff_type == DiffType.MODIFIED]),
            'unchanged': len([item for item in items if item.diff_type == DiffType.UNCHANGED])
        }
        
        has_changes = summary['added'] > 0 or summary['removed'] > 0 or summary['modified'] > 0
        
        return DiffResult(
            has_changes=has_changes,
            items=items,
            summary=summary
        )
    
    def _compare_dicts(self, old_dict: Dict, new_dict: Dict, path: str) -> List[DiffItem]:
        """Compare two dictionaries."""
        items = []
        
        # Find all keys
        all_keys = set(old_dict.keys()) | set(new_dict.keys())
        
        for key in all_keys:
            current_path = f"{path}.{key}" if path else key
            
            if key not in old_dict:
                # Key added in new data
                items.append(DiffItem(
                    path=current_path,
                    diff_type=DiffType.ADDED,
                    new_value=new_dict[key],
                    description=f"Key '{key}' was added"
                ))
            elif key not in new_dict:
                # Key removed from old data
                items.append(DiffItem(
                    path=current_path,
                    diff_type=DiffType.REMOVED,
                    old_value=old_dict[key],
                    description=f"Key '{key}' was removed"
                ))
            else:
                # Key exists in both, compare values
                old_value = old_dict[key]
                new_value = new_dict[key]
                
                if isinstance(old_value, dict) and isinstance(new_value, dict):
                    # Recursively compare nested dictionaries
                    items.extend(self._compare_dicts(old_value, new_value, current_path))
                elif isinstance(old_value, list) and isinstance(new_value, list):
                    # Compare lists
                    items.extend(self._compare_lists(old_value, new_value, current_path))
                else:
                    # Compare simple values
                    items.extend(self._compare_values(old_value, new_value, current_path))
        
        return items
    
    def _compare_lists(self, old_list: List, new_list: List, path: str) -> List[DiffItem]:
        """Compare two lists."""
        items = []
        
        # Simple list comparison - compare by index
        max_len = max(len(old_list), len(new_list))
        
        for i in range(max_len):
            current_path = f"{path}[{i}]"
            
            if i >= len(old_list):
                # Item added
                items.append(DiffItem(
                    path=current_path,
                    diff_type=DiffType.ADDED,
                    new_value=new_list[i],
                    description=f"Item at index {i} was added"
                ))
            elif i >= len(new_list):
                # Item removed
                items.append(DiffItem(
                    path=current_path,
                    diff_type=DiffType.REMOVED,
                    old_value=old_list[i],
                    description=f"Item at index {i} was removed"
                ))
            else:
                # Compare items at same index
                old_value = old_list[i]
                new_value = new_list[i]
                
                if isinstance(old_value, dict) and isinstance(new_value, dict):
                    items.extend(self._compare_dicts(old_value, new_value, current_path))
                elif isinstance(old_value, list) and isinstance(new_value, list):
                    items.extend(self._compare_lists(old_value, new_value, current_path))
                else:
                    items.extend(self._compare_values(old_value, new_value, current_path))
        
        return items
    
    def _compare_values(self, old_value: Any, new_value: Any, path: str) -> List[DiffItem]:
        """Compare two simple values."""
        items = []
        
        # Normalize values for comparison
        old_normalized = self._normalize_value(old_value)
        new_normalized = self._normalize_value(new_value)
        
        if old_normalized == new_normalized:
            items.append(DiffItem(
                path=path,
                diff_type=DiffType.UNCHANGED,
                old_value=old_value,
                new_value=new_value,
                description="Values are the same"
            ))
        else:
            items.append(DiffItem(
                path=path,
                diff_type=DiffType.MODIFIED,
                old_value=old_value,
                new_value=new_value,
                description=f"Value changed from '{old_value}' to '{new_value}'"
            ))
        
        return items
    
    def _normalize_value(self, value: Any) -> Any:
        """Normalize a value for comparison."""
        if isinstance(value, str):
            normalized = value
            if self.ignore_case:
                normalized = normalized.lower()
            if self.ignore_whitespace:
                normalized = normalized.strip()
            return normalized
        return value
    
    def format_diff(self, diff_result: DiffResult, format_type: str = "text") -> str:
        """Format diff result for display."""
        if format_type == "json":
            return self._format_json(diff_result)
        elif format_type == "text":
            return self._format_text(diff_result)
        elif format_type == "html":
            return self._format_html(diff_result)
        else:
            return self._format_text(diff_result)
    
    def _format_text(self, diff_result: DiffResult) -> str:
        """Format diff as text."""
        lines = []
        
        # Summary
        summary = diff_result.summary
        lines.append("=== Diff Summary ===")
        lines.append(f"Added: {summary['added']}")
        lines.append(f"Removed: {summary['removed']}")
        lines.append(f"Modified: {summary['modified']}")
        lines.append(f"Unchanged: {summary['unchanged']}")
        lines.append("")
        
        # Details
        if diff_result.has_changes:
            lines.append("=== Changes ===")
            for item in diff_result.items:
                if item.diff_type != DiffType.UNCHANGED:
                    lines.append(f"{item.diff_type.value.upper()}: {item.path}")
                    lines.append(f"  {item.description}")
                    if item.old_value is not None:
                        lines.append(f"  Old: {item.old_value}")
                    if item.new_value is not None:
                        lines.append(f"  New: {item.new_value}")
                    lines.append("")
        else:
            lines.append("No changes detected")
        
        return "\n".join(lines)
    
    def _format_json(self, diff_result: DiffResult) -> str:
        """Format diff as JSON."""
        return json.dumps({
            'has_changes': diff_result.has_changes,
            'summary': diff_result.summary,
            'items': [
                {
                    'path': item.path,
                    'type': item.diff_type.value,
                    'old_value': item.old_value,
                    'new_value': item.new_value,
                    'description': item.description
                }
                for item in diff_result.items
            ]
        }, indent=2)
    
    def _format_html(self, diff_result: DiffResult) -> str:
        """Format diff as HTML."""
        html_lines = ["<html><body>"]
        
        # Summary
        summary = diff_result.summary
        html_lines.append("<h2>Diff Summary</h2>")
        html_lines.append("<ul>")
        html_lines.append(f"<li>Added: {summary['added']}</li>")
        html_lines.append(f"<li>Removed: {summary['removed']}</li>")
        html_lines.append(f"<li>Modified: {summary['modified']}</li>")
        html_lines.append(f"<li>Unchanged: {summary['unchanged']}</li>")
        html_lines.append("</ul>")
        
        # Details
        if diff_result.has_changes:
            html_lines.append("<h2>Changes</h2>")
            for item in diff_result.items:
                if item.diff_type != DiffType.UNCHANGED:
                    color = {
                        DiffType.ADDED: "green",
                        DiffType.REMOVED: "red",
                        DiffType.MODIFIED: "orange"
                    }.get(item.diff_type, "black")
                    
                    html_lines.append(f"<div style='color: {color};'>")
                    html_lines.append(f"<strong>{item.diff_type.value.upper()}:</strong> {item.path}")
                    html_lines.append(f"<br>{item.description}")
                    if item.old_value is not None:
                        html_lines.append(f"<br>Old: {item.old_value}")
                    if item.new_value is not None:
                        html_lines.append(f"<br>New: {item.new_value}")
                    html_lines.append("</div><br>")
        else:
            html_lines.append("<p>No changes detected</p>")
        
        html_lines.append("</body></html>")
        return "\n".join(html_lines)

def compare_data(old_data: Any, new_data: Any, ignore_case: bool = False, 
                ignore_whitespace: bool = False) -> DiffResult:
    """Convenience function to compare data."""
    diff = DataDiff(ignore_case=ignore_case, ignore_whitespace=ignore_whitespace)
    return diff.compare(old_data, new_data)

if __name__ == "__main__":
    # Example usage
    old_data = {
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com",
        "settings": {
            "theme": "dark",
            "notifications": True
        },
        "tags": ["developer", "python"]
    }
    
    new_data = {
        "name": "John Doe",
        "age": 31,
        "email": "john.doe@example.com",
        "settings": {
            "theme": "light",
            "notifications": True
        },
        "tags": ["developer", "python", "senior"]
    }
    
    # Compare data
    diff_result = compare_data(old_data, new_data)
    
    # Format and display results
    print("Text Format:")
    print(diff_result.format_diff(diff_result, "text"))
    
    print("\nJSON Format:")
    print(diff_result.format_diff(diff_result, "json"))
