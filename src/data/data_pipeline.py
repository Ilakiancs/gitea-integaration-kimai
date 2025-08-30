#!/usr/bin/env python3
"""
Data Transformation Pipeline

Provides a flexible data transformation pipeline for processing data
through multiple stages with support for custom transformers.
"""

import logging
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class TransformType(Enum):
    """Types of data transformations."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    SORT = "sort"
    GROUP = "group"
    CUSTOM = "custom"

@dataclass
class TransformStep:
    """A transformation step in the pipeline."""
    name: str
    transform_type: TransformType
    function: Callable
    parameters: Dict[str, Any]
    enabled: bool = True

@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    data: Any
    metadata: Dict[str, Any]
    execution_time: float
    steps_executed: List[str]

class DataPipeline:
    """Pipeline for data transformation."""
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.steps: List[TransformStep] = []
        self.custom_transformers: Dict[str, Callable] = {}
    
    def add_step(self, step: TransformStep):
        """Add a transformation step to the pipeline."""
        self.steps.append(step)
        logger.info(f"Added step '{step.name}' to pipeline '{self.name}'")
    
    def add_custom_transformer(self, name: str, transformer_func: Callable):
        """Add a custom transformer function."""
        self.custom_transformers[name] = transformer_func
        logger.info(f"Added custom transformer: {name}")
    
    def execute(self, data: Any) -> PipelineResult:
        """Execute the pipeline on input data."""
        import time
        start_time = time.time()
        
        current_data = data
        steps_executed = []
        metadata = {}
        
        for step in self.steps:
            if not step.enabled:
                continue
            
            try:
                if step.transform_type == TransformType.CUSTOM:
                    current_data = step.function(current_data, **step.parameters)
                else:
                    current_data = self._apply_transform(step, current_data)
                
                steps_executed.append(step.name)
                metadata[f"step_{step.name}"] = "completed"
                
            except Exception as e:
                logger.error(f"Error in step '{step.name}': {e}")
                metadata[f"step_{step.name}"] = f"error: {str(e)}"
                break
        
        execution_time = time.time() - start_time
        
        return PipelineResult(
            data=current_data,
            metadata=metadata,
            execution_time=execution_time,
            steps_executed=steps_executed
        )
    
    def _apply_transform(self, step: TransformStep, data: Any) -> Any:
        """Apply a transformation step."""
        if step.transform_type == TransformType.MAP:
            return self._apply_map(step, data)
        elif step.transform_type == TransformType.FILTER:
            return self._apply_filter(step, data)
        elif step.transform_type == TransformType.REDUCE:
            return self._apply_reduce(step, data)
        elif step.transform_type == TransformType.SORT:
            return self._apply_sort(step, data)
        elif step.transform_type == TransformType.GROUP:
            return self._apply_group(step, data)
        
        return data
    
    def _apply_map(self, step: TransformStep, data: Any) -> Any:
        """Apply map transformation."""
        if isinstance(data, list):
            return [step.function(item, **step.parameters) for item in data]
        elif isinstance(data, dict):
            return {k: step.function(v, **step.parameters) for k, v in data.items()}
        else:
            return step.function(data, **step.parameters)
    
    def _apply_filter(self, step: TransformStep, data: Any) -> Any:
        """Apply filter transformation."""
        if isinstance(data, list):
            return [item for item in data if step.function(item, **step.parameters)]
        elif isinstance(data, dict):
            return {k: v for k, v in data.items() if step.function(v, **step.parameters)}
        else:
            return data if step.function(data, **step.parameters) else None
    
    def _apply_reduce(self, step: TransformStep, data: Any) -> Any:
        """Apply reduce transformation."""
        if isinstance(data, list) and len(data) > 0:
            result = data[0]
            for item in data[1:]:
                result = step.function(result, item, **step.parameters)
            return result
        return data
    
    def _apply_sort(self, step: TransformStep, data: Any) -> Any:
        """Apply sort transformation."""
        if isinstance(data, list):
            key_func = step.parameters.get('key')
            reverse = step.parameters.get('reverse', False)
            return sorted(data, key=key_func, reverse=reverse)
        return data
    
    def _apply_group(self, step: TransformStep, data: Any) -> Any:
        """Apply group transformation."""
        if isinstance(data, list):
            key_func = step.parameters.get('key')
            if key_func:
                groups = {}
                for item in data:
                    key = key_func(item)
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(item)
                return groups
        return data
    
    def clear_steps(self):
        """Clear all steps from the pipeline."""
        self.steps.clear()
        logger.info(f"Cleared all steps from pipeline '{self.name}'")
    
    def get_step(self, step_name: str) -> Optional[TransformStep]:
        """Get a step by name."""
        for step in self.steps:
            if step.name == step_name:
                return step
        return None
    
    def enable_step(self, step_name: str):
        """Enable a step by name."""
        step = self.get_step(step_name)
        if step:
            step.enabled = True
            logger.info(f"Enabled step '{step_name}'")
    
    def disable_step(self, step_name: str):
        """Disable a step by name."""
        step = self.get_step(step_name)
        if step:
            step.enabled = False
            logger.info(f"Disabled step '{step_name}'")

def create_sample_pipeline() -> DataPipeline:
    """Create a sample data transformation pipeline."""
    pipeline = DataPipeline("sample_pipeline")
    
    # Add some common transformations
    def to_uppercase(item, **kwargs):
        return str(item).upper() if isinstance(item, str) else item
    
    def filter_non_empty(item, **kwargs):
        return bool(item) and str(item).strip()
    
    def extract_numbers(item, **kwargs):
        import re
        if isinstance(item, str):
            numbers = re.findall(r'\d+', item)
            return [int(n) for n in numbers]
        return item
    
    # Add steps
    pipeline.add_step(TransformStep(
        name="to_uppercase",
        transform_type=TransformType.MAP,
        function=to_uppercase,
        parameters={}
    ))
    
    pipeline.add_step(TransformStep(
        name="filter_empty",
        transform_type=TransformType.FILTER,
        function=filter_non_empty,
        parameters={}
    ))
    
    pipeline.add_step(TransformStep(
        name="extract_numbers",
        transform_type=TransformType.MAP,
        function=extract_numbers,
        parameters={}
    ))
    
    return pipeline

if __name__ == "__main__":
    # Example usage
    pipeline = create_sample_pipeline()
    
    # Test data
    test_data = ["hello", "", "world123", "test456", None, "final"]
    
    # Execute pipeline
    result = pipeline.execute(test_data)
    
    print(f"Original data: {test_data}")
    print(f"Transformed data: {result.data}")
    print(f"Steps executed: {result.steps_executed}")
    print(f"Execution time: {result.execution_time:.4f}s")
