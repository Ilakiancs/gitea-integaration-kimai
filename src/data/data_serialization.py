#!/usr/bin/env python3
"""
Data Serialization Utility

Provides utilities for serializing and deserializing data in various
formats including JSON, YAML, XML, Pickle, and MessagePack.
"""

import json
import yaml
import pickle
import xml.etree.ElementTree as ET
import logging
from typing import Dict, List, Optional, Any, Union, Bytes
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
import io

logger = logging.getLogger(__name__)

class SerializationType(Enum):
    """Types of serialization formats."""
    JSON = "json"
    YAML = "yaml"
    XML = "xml"
    PICKLE = "pickle"
    MESSAGEPACK = "messagepack"

@dataclass
class SerializationResult:
    """Result of a serialization operation."""
    data: Union[str, bytes]
    format_type: SerializationType
    original_size: int
    serialized_size: int
    compression_ratio: float

class DataSerializer:
    """Main data serialization utility."""
    
    def __init__(self):
        self.default_format = SerializationType.JSON
        self.json_options = {
            'indent': 2,
            'sort_keys': True,
            'ensure_ascii': False
        }
        self.yaml_options = {
            'default_flow_style': False,
            'indent': 2
        }
    
    def serialize(self, data: Any, format_type: SerializationType = SerializationType.JSON,
                 **options) -> SerializationResult:
        """Serialize data to the specified format."""
        original_size = len(str(data))
        
        if format_type == SerializationType.JSON:
            serialized_data = self._serialize_json(data, **options)
        elif format_type == SerializationType.YAML:
            serialized_data = self._serialize_yaml(data, **options)
        elif format_type == SerializationType.XML:
            serialized_data = self._serialize_xml(data, **options)
        elif format_type == SerializationType.PICKLE:
            serialized_data = self._serialize_pickle(data, **options)
        elif format_type == SerializationType.MESSAGEPACK:
            serialized_data = self._serialize_messagepack(data, **options)
        else:
            raise ValueError(f"Unsupported serialization format: {format_type}")
        
        serialized_size = len(serialized_data)
        compression_ratio = (1 - serialized_size / original_size) * 100 if original_size > 0 else 0
        
        return SerializationResult(
            data=serialized_data,
            format_type=format_type,
            original_size=original_size,
            serialized_size=serialized_size,
            compression_ratio=compression_ratio
        )
    
    def deserialize(self, data: Union[str, bytes], 
                   format_type: SerializationType = SerializationType.JSON) -> Any:
        """Deserialize data from the specified format."""
        if format_type == SerializationType.JSON:
            return self._deserialize_json(data)
        elif format_type == SerializationType.YAML:
            return self._deserialize_yaml(data)
        elif format_type == SerializationType.XML:
            return self._deserialize_xml(data)
        elif format_type == SerializationType.PICKLE:
            return self._deserialize_pickle(data)
        elif format_type == SerializationType.MESSAGEPACK:
            return self._deserialize_messagepack(data)
        else:
            raise ValueError(f"Unsupported serialization format: {format_type}")
    
    def _serialize_json(self, data: Any, **options) -> str:
        """Serialize data to JSON."""
        opts = {**self.json_options, **options}
        return json.dumps(data, **opts)
    
    def _deserialize_json(self, data: Union[str, bytes]) -> Any:
        """Deserialize data from JSON."""
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return json.loads(data)
    
    def _serialize_yaml(self, data: Any, **options) -> str:
        """Serialize data to YAML."""
        opts = {**self.yaml_options, **options}
        return yaml.dump(data, **opts)
    
    def _deserialize_yaml(self, data: Union[str, bytes]) -> Any:
        """Deserialize data from YAML."""
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return yaml.safe_load(data)
    
    def _serialize_xml(self, data: Any, root_name: str = "data", **options) -> str:
        """Serialize data to XML."""
        root = ET.Element(root_name)
        self._dict_to_xml(data, root)
        return ET.tostring(root, encoding='unicode')
    
    def _deserialize_xml(self, data: Union[str, bytes]) -> Any:
        """Deserialize data from XML."""
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        root = ET.fromstring(data)
        return self._xml_to_dict(root)
    
    def _dict_to_xml(self, data: Any, parent: ET.Element):
        """Convert dictionary to XML elements."""
        if isinstance(data, dict):
            for key, value in data.items():
                child = ET.SubElement(parent, key)
                self._dict_to_xml(value, child)
        elif isinstance(data, list):
            for item in data:
                item_elem = ET.SubElement(parent, "item")
                self._dict_to_xml(item, item_elem)
        else:
            parent.text = str(data)
    
    def _xml_to_dict(self, element: ET.Element) -> Any:
        """Convert XML element to dictionary."""
        result = {}
        
        for child in element:
            if len(child) == 0:
                # Leaf element
                result[child.tag] = child.text
            else:
                # Element with children
                result[child.tag] = self._xml_to_dict(child)
        
        return result
    
    def _serialize_pickle(self, data: Any, protocol: int = pickle.HIGHEST_PROTOCOL, **options) -> bytes:
        """Serialize data using Pickle."""
        return pickle.dumps(data, protocol=protocol)
    
    def _deserialize_pickle(self, data: Union[str, bytes]) -> Any:
        """Deserialize data using Pickle."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        return pickle.loads(data)
    
    def _serialize_messagepack(self, data: Any, **options) -> bytes:
        """Serialize data using MessagePack."""
        try:
            import msgpack
            return msgpack.packb(data, **options)
        except ImportError:
            raise ImportError("MessagePack is not installed. Install with: pip install msgpack")
    
    def _deserialize_messagepack(self, data: Union[str, bytes]) -> Any:
        """Deserialize data using MessagePack."""
        try:
            import msgpack
            if isinstance(data, str):
                data = data.encode('utf-8')
            return msgpack.unpackb(data, raw=False)
        except ImportError:
            raise ImportError("MessagePack is not installed. Install with: pip install msgpack")
    
    def serialize_to_file(self, data: Any, file_path: str,
                         format_type: SerializationType = SerializationType.JSON,
                         **options) -> SerializationResult:
        """Serialize data and save to file."""
        result = self.serialize(data, format_type, **options)
        
        mode = 'wb' if isinstance(result.data, bytes) else 'w'
        encoding = None if isinstance(result.data, bytes) else 'utf-8'
        
        with open(file_path, mode, encoding=encoding) as f:
            f.write(result.data)
        
        logger.info(f"Serialized data to {file_path}")
        return result
    
    def deserialize_from_file(self, file_path: str,
                            format_type: SerializationType = SerializationType.JSON) -> Any:
        """Deserialize data from file."""
        # Determine file mode based on format
        if format_type in [SerializationType.PICKLE, SerializationType.MESSAGEPACK]:
            mode = 'rb'
            encoding = None
        else:
            mode = 'r'
            encoding = 'utf-8'
        
        with open(file_path, mode, encoding=encoding) as f:
            data = f.read()
        
        result = self.deserialize(data, format_type)
        logger.info(f"Deserialized data from {file_path}")
        return result
    
    def convert_format(self, data: Union[str, bytes], 
                      from_format: SerializationType,
                      to_format: SerializationType) -> SerializationResult:
        """Convert data from one format to another."""
        # Deserialize from source format
        deserialized_data = self.deserialize(data, from_format)
        
        # Serialize to target format
        return self.serialize(deserialized_data, to_format)
    
    def get_format_info(self, data: Any) -> Dict[str, SerializationResult]:
        """Get serialization information for all formats."""
        info = {}
        
        for format_type in SerializationType:
            try:
                result = self.serialize(data, format_type)
                info[format_type.value] = result
            except Exception as e:
                logger.warning(f"Failed to serialize with {format_type.value}: {e}")
        
        return info
    
    def validate_serialization(self, data: Any, format_type: SerializationType = SerializationType.JSON) -> bool:
        """Validate that data can be serialized and deserialized correctly."""
        try:
            serialized = self.serialize(data, format_type)
            deserialized = self.deserialize(serialized.data, format_type)
            
            # Compare the original and deserialized data
            return data == deserialized
        except Exception as e:
            logger.error(f"Serialization validation failed: {e}")
            return False

def create_serializer() -> DataSerializer:
    """Create a data serializer with default settings."""
    return DataSerializer()

def serialize_data(data: Any, format_type: SerializationType = SerializationType.JSON) -> SerializationResult:
    """Convenience function to serialize data."""
    serializer = create_serializer()
    return serializer.serialize(data, format_type)

def deserialize_data(data: Union[str, bytes], format_type: SerializationType = SerializationType.JSON) -> Any:
    """Convenience function to deserialize data."""
    serializer = create_serializer()
    return serializer.deserialize(data, format_type)

if __name__ == "__main__":
    # Example usage
    serializer = create_serializer()
    
    # Sample data
    sample_data = {
        "users": [
            {"name": "John Doe", "email": "john@example.com", "age": 30},
            {"name": "Jane Smith", "email": "jane@example.com", "age": 25}
        ],
        "settings": {
            "theme": "dark",
            "notifications": True,
            "language": "en"
        }
    }
    
    # Test different serialization formats
    print("=== Serialization Test ===")
    info = serializer.get_format_info(sample_data)
    
    for format_name, result in info.items():
        print(f"{format_name.upper()}:")
        print(f"  Size: {result.serialized_size} bytes")
        print(f"  Compression ratio: {result.compression_ratio:.2f}%")
        print()
    
    # Test serialization and deserialization
    print("=== Serialization/Deserialization Test ===")
    result = serialize_data(sample_data, SerializationType.JSON)
    print(f"JSON serialized size: {result.serialized_size} bytes")
    
    deserialized = deserialize_data(result.data, SerializationType.JSON)
    print(f"Deserialization successful: {sample_data == deserialized}")
    
    # Test format conversion
    print("\n=== Format Conversion Test ===")
    json_result = serialize_data(sample_data, SerializationType.JSON)
    yaml_result = serializer.convert_format(json_result.data, SerializationType.JSON, SerializationType.YAML)
    print(f"JSON to YAML conversion: {yaml_result.serialized_size} bytes")
