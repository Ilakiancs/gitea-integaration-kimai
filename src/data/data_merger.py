#!/usr/bin/env python3
"""
Data Merger Utility

Provides utilities for merging data from multiple sources with conflict
resolution strategies and data integrity checks.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)

class MergeStrategy(Enum):
    """Merge strategies for conflict resolution."""
    KEEP_FIRST = "keep_first"
    KEEP_LAST = "keep_last"
    KEEP_NEWEST = "keep_newest"
    KEEP_OLDEST = "keep_oldest"
    MERGE_COMBINE = "merge_combine"
    MERGE_APPEND = "merge_append"
    CUSTOM = "custom"

@dataclass
class MergeConflict:
    """A merge conflict."""
    path: str
    values: List[Any]
    sources: List[str]
    resolution: Optional[Any] = None
    strategy: Optional[MergeStrategy] = None

@dataclass
class MergeResult:
    """Result of a merge operation."""
    merged_data: Any
    conflicts: List[MergeConflict]
    success: bool
    summary: Dict[str, int]

class DataMerger:
    """Main data merger utility."""
    
    def __init__(self, default_strategy: MergeStrategy = MergeStrategy.KEEP_LAST):
        self.default_strategy = default_strategy
        self.custom_resolvers: Dict[str, Callable] = {}
    
    def merge(self, data_sources: List[Dict[str, Any]], 
              source_names: List[str] = None) -> MergeResult:
        """Merge multiple data sources."""
        if not data_sources:
            return MergeResult(
                merged_data={},
                conflicts=[],
                success=True,
                summary={'merged': 0, 'conflicts': 0}
            )
        
        if source_names is None:
            source_names = [f"source_{i}" for i in range(len(data_sources))]
        
        if len(data_sources) == 1:
            return MergeResult(
                merged_data=data_sources[0],
                conflicts=[],
                success=True,
                summary={'merged': 1, 'conflicts': 0}
            )
        
        # Start with the first source
        merged_data = data_sources[0].copy()
        conflicts = []
        
        # Merge each subsequent source
        for i, source_data in enumerate(data_sources[1:], 1):
            merge_result = self._merge_two(merged_data, source_data, 
                                         source_names[0], source_names[i])
            merged_data = merge_result.merged_data
            conflicts.extend(merge_result.conflicts)
        
        summary = {
            'merged': len(data_sources),
            'conflicts': len(conflicts)
        }
        
        return MergeResult(
            merged_data=merged_data,
            conflicts=conflicts,
            success=len(conflicts) == 0,
            summary=summary
        )
    
    def _merge_two(self, target: Dict[str, Any], source: Dict[str, Any],
                   target_name: str, source_name: str) -> MergeResult:
        """Merge two data structures."""
        conflicts = []
        merged = target.copy()
        
        for key, source_value in source.items():
            if key not in merged:
                # Key doesn't exist in target, add it
                merged[key] = source_value
            else:
                # Key exists in both, need to resolve conflict
                target_value = merged[key]
                conflict = self._resolve_conflict(
                    key, target_value, source_value, target_name, source_name
                )
                
                if conflict:
                    conflicts.append(conflict)
                    merged[key] = conflict.resolution
                else:
                    # No conflict, values are the same
                    pass
        
        return MergeResult(
            merged_data=merged,
            conflicts=conflicts,
            success=len(conflicts) == 0,
            summary={'merged': 2, 'conflicts': len(conflicts)}
        )
    
    def _resolve_conflict(self, key: str, value1: Any, value2: Any,
                         source1_name: str, source2_name: str) -> Optional[MergeConflict]:
        """Resolve a conflict between two values."""
        # Check if values are the same
        if value1 == value2:
            return None
        
        # Create conflict object
        conflict = MergeConflict(
            path=key,
            values=[value1, value2],
            sources=[source1_name, source2_name]
        )
        
        # Apply resolution strategy
        resolution = self._apply_strategy(conflict, self.default_strategy)
        conflict.resolution = resolution
        conflict.strategy = self.default_strategy
        
        return conflict
    
    def _apply_strategy(self, conflict: MergeConflict, strategy: MergeStrategy) -> Any:
        """Apply a merge strategy to resolve a conflict."""
        if strategy == MergeStrategy.KEEP_FIRST:
            return conflict.values[0]
        elif strategy == MergeStrategy.KEEP_LAST:
            return conflict.values[1]
        elif strategy == MergeStrategy.KEEP_NEWEST:
            return self._get_newest_value(conflict.values)
        elif strategy == MergeStrategy.KEEP_OLDEST:
            return self._get_oldest_value(conflict.values)
        elif strategy == MergeStrategy.MERGE_COMBINE:
            return self._combine_values(conflict.values)
        elif strategy == MergeStrategy.MERGE_APPEND:
            return self._append_values(conflict.values)
        elif strategy == MergeStrategy.CUSTOM:
            return self._apply_custom_resolver(conflict)
        else:
            return conflict.values[1]  # Default to last value
    
    def _get_newest_value(self, values: List[Any]) -> Any:
        """Get the newest value based on timestamp or modification time."""
        # This is a simplified implementation
        # In practice, you might look for timestamp fields or use file modification times
        return values[-1]  # Assume last value is newest
    
    def _get_oldest_value(self, values: List[Any]) -> Any:
        """Get the oldest value based on timestamp or modification time."""
        return values[0]  # Assume first value is oldest
    
    def _combine_values(self, values: List[Any]) -> Any:
        """Combine values intelligently."""
        if all(isinstance(v, dict) for v in values):
            # Merge dictionaries
            result = {}
            for value in values:
                result.update(value)
            return result
        elif all(isinstance(v, list) for v in values):
            # Combine lists
            result = []
            for value in values:
                result.extend(value)
            return result
        elif all(isinstance(v, (int, float)) for v in values):
            # Average numeric values
            return sum(values) / len(values)
        else:
            # Default to last value
            return values[-1]
    
    def _append_values(self, values: List[Any]) -> Any:
        """Append values to a list."""
        if all(isinstance(v, list) for v in values):
            result = []
            for value in values:
                result.extend(value)
            return result
        else:
            # Convert to list and append
            return list(values)
    
    def _apply_custom_resolver(self, conflict: MergeConflict) -> Any:
        """Apply a custom resolver function."""
        resolver_key = f"{conflict.path}"
        if resolver_key in self.custom_resolvers:
            return self.custom_resolvers[resolver_key](conflict)
        else:
            # Fall back to default strategy
            return self._apply_strategy(conflict, self.default_strategy)
    
    def add_custom_resolver(self, path: str, resolver_func: Callable):
        """Add a custom resolver function for a specific path."""
        self.custom_resolvers[path] = resolver_func
        logger.info(f"Added custom resolver for path: {path}")
    
    def merge_with_strategy(self, data_sources: List[Dict[str, Any]], 
                           strategy: MergeStrategy,
                           source_names: List[str] = None) -> MergeResult:
        """Merge data sources with a specific strategy."""
        original_strategy = self.default_strategy
        self.default_strategy = strategy
        
        try:
            result = self.merge(data_sources, source_names)
            return result
        finally:
            self.default_strategy = original_strategy
    
    def resolve_conflicts_manually(self, conflicts: List[MergeConflict], 
                                 resolutions: Dict[str, Any]) -> List[MergeConflict]:
        """Manually resolve conflicts with provided resolutions."""
        for conflict in conflicts:
            if conflict.path in resolutions:
                conflict.resolution = resolutions[conflict.path]
                conflict.strategy = MergeStrategy.CUSTOM
        
        return conflicts

def create_merger(strategy: MergeStrategy = MergeStrategy.KEEP_LAST) -> DataMerger:
    """Create a data merger with specified strategy."""
    return DataMerger(strategy)

def merge_data(data_sources: List[Dict[str, Any]], 
               strategy: MergeStrategy = MergeStrategy.KEEP_LAST) -> MergeResult:
    """Convenience function to merge data."""
    merger = create_merger(strategy)
    return merger.merge(data_sources)

if __name__ == "__main__":
    # Example usage
    source1 = {
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com",
        "settings": {
            "theme": "dark",
            "notifications": True
        },
        "tags": ["developer", "python"]
    }
    
    source2 = {
        "name": "John Doe",
        "age": 31,
        "email": "john.doe@example.com",
        "settings": {
            "theme": "light",
            "notifications": True
        },
        "tags": ["developer", "python", "senior"]
    }
    
    source3 = {
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com",
        "settings": {
            "theme": "dark",
            "notifications": False
        },
        "tags": ["developer"]
    }
    
    # Merge with different strategies
    sources = [source1, source2, source3]
    source_names = ["local", "remote", "backup"]
    
    print("=== Merge with KEEP_LAST strategy ===")
    result1 = merge_data(sources, MergeStrategy.KEEP_LAST)
    print(f"Success: {result1.success}")
    print(f"Conflicts: {len(result1.conflicts)}")
    print("Merged data:", json.dumps(result1.merged_data, indent=2))
    
    print("\n=== Merge with MERGE_COMBINE strategy ===")
    result2 = merge_data(sources, MergeStrategy.MERGE_COMBINE)
    print(f"Success: {result2.success}")
    print(f"Conflicts: {len(result2.conflicts)}")
    print("Merged data:", json.dumps(result2.merged_data, indent=2))
    
    # Show conflicts
    if result1.conflicts:
        print("\n=== Conflicts ===")
        for conflict in result1.conflicts:
            print(f"Path: {conflict.path}")
            print(f"Values: {conflict.values}")
            print(f"Resolution: {conflict.resolution}")
            print(f"Strategy: {conflict.strategy.value}")
            print("---")
