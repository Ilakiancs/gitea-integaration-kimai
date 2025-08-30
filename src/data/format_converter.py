#!/usr/bin/env python3
"""
Data Format Converter

Provides utilities for converting data between different formats
including JSON, YAML, XML, CSV, and custom formats.
"""

import json
import yaml
import csv
import xml.etree.ElementTree as ET
import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import io

logger = logging.getLogger(__name__)

class FormatType(Enum):
    """Supported data formats."""
    JSON = "json"
    YAML = "yaml"
    XML = "xml"
    CSV = "csv"
    TEXT = "text"

@dataclass
class ConversionOptions:
    """Options for data conversion."""
    indent: int = 2
    sort_keys: bool = False
    ensure_ascii: bool = False
    default_flow_style: bool = False
    csv_delimiter: str = ','
    csv_quotechar: str = '"'
    xml_root_tag: str = "data"
    xml_item_tag: str = "item"

class FormatConverter:
    """Main format conversion utility."""
    
    def __init__(self, options: ConversionOptions = None):
        self.options = options or ConversionOptions()
    
    def convert(self, data: Any, from_format: FormatType, to_format: FormatType) -> Any:
        """Convert data from one format to another."""
        # First parse the input format
        if from_format == FormatType.JSON:
            parsed_data = self._parse_json(data)
        elif from_format == FormatType.YAML:
            parsed_data = self._parse_yaml(data)
        elif from_format == FormatType.XML:
            parsed_data = self._parse_xml(data)
        elif from_format == FormatType.CSV:
            parsed_data = self._parse_csv(data)
        elif from_format == FormatType.TEXT:
            parsed_data = self._parse_text(data)
        else:
            raise ValueError(f"Unsupported input format: {from_format}")
        
        # Then convert to output format
        if to_format == FormatType.JSON:
            return self._to_json(parsed_data)
        elif to_format == FormatType.YAML:
            return self._to_yaml(parsed_data)
        elif to_format == FormatType.XML:
            return self._to_xml(parsed_data)
        elif to_format == FormatType.CSV:
            return self._to_csv(parsed_data)
        elif to_format == FormatType.TEXT:
            return self._to_text(parsed_data)
        else:
            raise ValueError(f"Unsupported output format: {to_format}")
    
    def _parse_json(self, data: Union[str, Dict, List]) -> Any:
        """Parse JSON data."""
        if isinstance(data, str):
            return json.loads(data)
        return data
    
    def _parse_yaml(self, data: Union[str, Dict, List]) -> Any:
        """Parse YAML data."""
        if isinstance(data, str):
            return yaml.safe_load(data)
        return data
    
    def _parse_xml(self, data: str) -> List[Dict[str, Any]]:
        """Parse XML data to list of dictionaries."""
        if not isinstance(data, str):
            raise ValueError("XML data must be a string")
        
        try:
            root = ET.fromstring(data)
            return self._xml_element_to_dict(root)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML: {e}")
    
    def _xml_element_to_dict(self, element: ET.Element) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Convert XML element to dictionary."""
        result = {}
        
        # Handle attributes
        if element.attrib:
            result['@attributes'] = element.attrib
        
        # Handle text content
        if element.text and element.text.strip():
            result['@text'] = element.text.strip()
        
        # Handle child elements
        children = {}
        for child in element:
            child_data = self._xml_element_to_dict(child)
            if child.tag in children:
                if not isinstance(children[child.tag], list):
                    children[child.tag] = [children[child.tag]]
                children[child.tag].append(child_data)
            else:
                children[child.tag] = child_data
        
        result.update(children)
        
        # If no attributes, text, or children, return just the tag name
        if not result:
            return element.tag
        
        return result
    
    def _parse_csv(self, data: Union[str, List[List[str]]]) -> List[Dict[str, str]]:
        """Parse CSV data to list of dictionaries."""
        if isinstance(data, str):
            # Parse CSV string
            csv_file = io.StringIO(data)
            reader = csv.DictReader(csv_file, delimiter=self.options.csv_delimiter)
            return list(reader)
        elif isinstance(data, list) and data:
            # Assume first row is headers
            headers = data[0]
            result = []
            for row in data[1:]:
                result.append(dict(zip(headers, row)))
            return result
        else:
            return []
    
    def _parse_text(self, data: str) -> str:
        """Parse text data."""
        return str(data)
    
    def _to_json(self, data: Any) -> str:
        """Convert data to JSON string."""
        return json.dumps(
            data,
            indent=self.options.indent,
            sort_keys=self.options.sort_keys,
            ensure_ascii=self.options.ensure_ascii
        )
    
    def _to_yaml(self, data: Any) -> str:
        """Convert data to YAML string."""
        return yaml.dump(
            data,
            default_flow_style=self.options.default_flow_style,
            indent=self.options.indent
        )
    
    def _to_xml(self, data: Any) -> str:
        """Convert data to XML string."""
        root = ET.Element(self.options.xml_root_tag)
        
        if isinstance(data, list):
            for item in data:
                item_elem = ET.SubElement(root, self.options.xml_item_tag)
                self._dict_to_xml(item, item_elem)
        elif isinstance(data, dict):
            self._dict_to_xml(data, root)
        else:
            root.text = str(data)
        
        return ET.tostring(root, encoding='unicode')
    
    def _dict_to_xml(self, data: Dict[str, Any], parent: ET.Element):
        """Convert dictionary to XML elements."""
        for key, value in data.items():
            if key.startswith('@'):
                # Handle special attributes
                if key == '@text':
                    parent.text = str(value)
                elif key == '@attributes':
                    parent.attrib.update(value)
            else:
                child = ET.SubElement(parent, key)
                if isinstance(value, dict):
                    self._dict_to_xml(value, child)
                elif isinstance(value, list):
                    for item in value:
                        item_elem = ET.SubElement(child, f"{key}_item")
                        if isinstance(item, dict):
                            self._dict_to_xml(item, item_elem)
                        else:
                            item_elem.text = str(item)
                else:
                    child.text = str(value)
    
    def _to_csv(self, data: List[Dict[str, Any]]) -> str:
        """Convert data to CSV string."""
        if not data:
            return ""
        
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=data[0].keys(),
            delimiter=self.options.csv_delimiter,
            quotechar=self.options.csv_quotechar
        )
        
        writer.writeheader()
        writer.writerows(data)
        
        return output.getvalue()
    
    def _to_text(self, data: Any) -> str:
        """Convert data to text string."""
        if isinstance(data, (dict, list)):
            return json.dumps(data, indent=self.options.indent)
        return str(data)
    
    def convert_file(self, input_path: str, output_path: str, 
                    from_format: FormatType, to_format: FormatType):
        """Convert a file from one format to another."""
        # Read input file
        with open(input_path, 'r', encoding='utf-8') as f:
            if from_format == FormatType.JSON:
                data = json.load(f)
            elif from_format == FormatType.YAML:
                data = yaml.safe_load(f)
            elif from_format == FormatType.XML:
                data = f.read()
            elif from_format == FormatType.CSV:
                data = f.read()
            else:
                data = f.read()
        
        # Convert data
        result = self.convert(data, from_format, to_format)
        
        # Write output file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(result)
        
        logger.info(f"Converted {input_path} to {output_path}")
    
    def detect_format(self, data: str) -> FormatType:
        """Detect the format of data string."""
        data = data.strip()
        
        # Try JSON
        try:
            json.loads(data)
            return FormatType.JSON
        except (json.JSONDecodeError, ValueError):
            pass
        
        # Try YAML
        try:
            yaml.safe_load(data)
            return FormatType.YAML
        except yaml.YAMLError:
            pass
        
        # Try XML
        try:
            ET.fromstring(data)
            return FormatType.XML
        except ET.ParseError:
            pass
        
        # Check if it looks like CSV
        if ',' in data and '\n' in data:
            return FormatType.CSV
        
        # Default to text
        return FormatType.TEXT

def convert_data(data: Any, from_format: FormatType, to_format: FormatType, 
                options: ConversionOptions = None) -> Any:
    """Convenience function to convert data."""
    converter = FormatConverter(options)
    return converter.convert(data, from_format, to_format)

def convert_file(input_path: str, output_path: str, from_format: FormatType, 
                to_format: FormatType, options: ConversionOptions = None):
    """Convenience function to convert files."""
    converter = FormatConverter(options)
    converter.convert_file(input_path, output_path, from_format, to_format)

if __name__ == "__main__":
    # Example usage
    converter = FormatConverter()
    
    # Sample data
    sample_data = {
        'users': [
            {'name': 'John', 'age': 30, 'email': 'john@example.com'},
            {'name': 'Jane', 'age': 25, 'email': 'jane@example.com'}
        ],
        'settings': {
            'theme': 'dark',
            'notifications': True
        }
    }
    
    # Convert JSON to YAML
    json_str = json.dumps(sample_data, indent=2)
    yaml_str = converter.convert(json_str, FormatType.JSON, FormatType.YAML)
    print("JSON to YAML:")
    print(yaml_str)
    
    # Convert JSON to XML
    xml_str = converter.convert(json_str, FormatType.JSON, FormatType.XML)
    print("\nJSON to XML:")
    print(xml_str)
    
    # Convert JSON to CSV
    csv_str = converter.convert(json_str, FormatType.JSON, FormatType.CSV)
    print("\nJSON to CSV:")
    print(csv_str)
