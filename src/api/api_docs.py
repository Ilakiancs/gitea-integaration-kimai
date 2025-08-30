#!/usr/bin/env python3
"""
API Documentation Generator

Automatically generates comprehensive API documentation for the sync system,
including OpenAPI/Swagger specifications, markdown docs, and interactive examples.
"""

import os
import json
import inspect
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from pathlib import Path
import re

logger = logging.getLogger(__name__)

class APIDocumentationGenerator:
    """Generates API documentation from code and configuration."""
    
    def __init__(self, output_dir: str = "docs"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api_endpoints = []
        self.openapi_spec = self._create_base_openapi_spec()
    
    def _create_base_openapi_spec(self) -> Dict[str, Any]:
        """Create base OpenAPI specification."""
        return {
            "openapi": "3.0.3",
            "info": {
                "title": "Gitea-Kimai Sync API",
                "description": "API for synchronizing issues and pull requests from Gitea to Kimai time tracking system",
                "version": "1.0.0",
                "contact": {
                    "name": "API Support",
                    "email": "support@example.com"
                },
                "license": {
                    "name": "MIT",
                    "url": "https://opensource.org/licenses/MIT"
                }
            },
            "servers": [
                {
                    "url": "http://localhost:5000",
                    "description": "Development server"
                },
                {
                    "url": "https://api.example.com",
                    "description": "Production server"
                }
            ],
            "paths": {},
            "components": {
                "schemas": {},
                "securitySchemes": {
                    "bearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT"
                    }
                }
            },
            "tags": []
        }
    
    def add_endpoint(self, path: str, method: str, handler: Callable, 
                    description: str = "", tags: List[str] = None,
                    request_schema: Dict[str, Any] = None,
                    response_schema: Dict[str, Any] = None,
                    examples: List[Dict[str, Any]] = None):
        """Add an API endpoint to the documentation."""
        endpoint = {
            'path': path,
            'method': method.upper(),
            'handler': handler,
            'description': description,
            'tags': tags or [],
            'request_schema': request_schema,
            'response_schema': response_schema,
            'examples': examples or []
        }
        
        self.api_endpoints.append(endpoint)
        self._add_to_openapi_spec(endpoint)
    
    def _add_to_openapi_spec(self, endpoint: Dict[str, Any]):
        """Add endpoint to OpenAPI specification."""
        path = endpoint['path']
        method = endpoint['method'].lower()
        
        if path not in self.openapi_spec['paths']:
            self.openapi_spec['paths'][path] = {}
        
        # Create operation object
        operation = {
            'summary': endpoint['description'],
            'description': endpoint['description'],
            'tags': endpoint['tags'],
            'responses': {
                '200': {
                    'description': 'Successful operation',
                    'content': {
                        'application/json': {
                            'schema': endpoint['response_schema'] or {
                                'type': 'object',
                                'properties': {
                                    'success': {'type': 'boolean'},
                                    'message': {'type': 'string'},
                                    'data': {'type': 'object'}
                                }
                            }
                        }
                    }
                },
                '400': {
                    'description': 'Bad request',
                    'content': {
                        'application/json': {
                            'schema': {
                                'type': 'object',
                                'properties': {
                                    'error': {'type': 'string'},
                                    'details': {'type': 'object'}
                                }
                            }
                        }
                    }
                },
                '401': {
                    'description': 'Unauthorized',
                    'content': {
                        'application/json': {
                            'schema': {
                                'type': 'object',
                                'properties': {
                                    'error': {'type': 'string'}
                                }
                            }
                        }
                    }
                },
                '500': {
                    'description': 'Internal server error',
                    'content': {
                        'application/json': {
                            'schema': {
                                'type': 'object',
                                'properties': {
                                    'error': {'type': 'string'}
                                }
                            }
                        }
                    }
                }
            }
        }
        
        # Add request body if method supports it
        if method in ['post', 'put', 'patch'] and endpoint['request_schema']:
            operation['requestBody'] = {
                'required': True,
                'content': {
                    'application/json': {
                        'schema': endpoint['request_schema']
                    }
                }
            }
        
        # Add examples
        if endpoint['examples']:
            operation['examples'] = {}
            for i, example in enumerate(endpoint['examples']):
                operation['examples'][f'example_{i+1}'] = {
                    'summary': example.get('summary', f'Example {i+1}'),
                    'description': example.get('description', ''),
                    'value': example.get('value', {})
                }
        
        self.openapi_spec['paths'][path][method] = operation
    
    def generate_openapi_spec(self) -> str:
        """Generate OpenAPI specification file."""
        spec_file = self.output_dir / "openapi.json"
        
        with open(spec_file, 'w') as f:
            json.dump(self.openapi_spec, f, indent=2)
        
        logger.info(f"Generated OpenAPI specification: {spec_file}")
        return str(spec_file)
    
    def generate_markdown_docs(self) -> str:
        """Generate markdown documentation."""
        md_file = self.output_dir / "api_documentation.md"
        
        with open(md_file, 'w') as f:
            f.write(self._generate_markdown_content())
        
        logger.info(f"Generated markdown documentation: {md_file}")
        return str(md_file)
    
    def _generate_markdown_content(self) -> str:
        """Generate markdown content."""
        content = []
        
        # Header
        content.append("# Gitea-Kimai Sync API Documentation")
        content.append("")
        content.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        content.append("")
        content.append("## Overview")
        content.append("")
        content.append("This API provides endpoints for managing the synchronization between Gitea repositories and Kimai time tracking system.")
        content.append("")
        
        # Authentication
        content.append("## Authentication")
        content.append("")
        content.append("The API uses Bearer token authentication. Include your token in the Authorization header:")
        content.append("")
        content.append("```")
        content.append("Authorization: Bearer <your-token>")
        content.append("```")
        content.append("")
        
        # Endpoints by tag
        tags = {}
        for endpoint in self.api_endpoints:
            for tag in endpoint['tags']:
                if tag not in tags:
                    tags[tag] = []
                tags[tag].append(endpoint)
        
        for tag, endpoints in tags.items():
            content.append(f"## {tag.title()}")
            content.append("")
            
            for endpoint in endpoints:
                content.append(f"### {endpoint['method']} {endpoint['path']}")
                content.append("")
                content.append(endpoint['description'])
                content.append("")
                
                # Request/Response examples
                if endpoint['examples']:
                    content.append("#### Examples")
                    content.append("")
                    
                    for i, example in enumerate(endpoint['examples']):
                        content.append(f"**Example {i+1}**: {example.get('summary', '')}")
                        content.append("")
                        content.append("```json")
                        content.append(json.dumps(example.get('value', {}), indent=2))
                        content.append("```")
                        content.append("")
                
                content.append("---")
                content.append("")
        
        return "\n".join(content)
    
    def generate_html_docs(self) -> str:
        """Generate HTML documentation using Swagger UI."""
        html_file = self.output_dir / "api_documentation.html"
        
        html_content = self._generate_html_content()
        
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Generated HTML documentation: {html_file}")
        return str(html_file)
    
    def _generate_html_content(self) -> str:
        """Generate HTML content with Swagger UI."""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gitea-Kimai Sync API Documentation</title>
    <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui.css" />
    <style>
        html {{
            box-sizing: border-box;
            overflow: -moz-scrollbars-vertical;
            overflow-y: scroll;
        }}
        *, *:before, *:after {{
            box-sizing: inherit;
        }}
        body {{
            margin:0;
            background: #fafafa;
        }}
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui-bundle.js"></script>
    <script src="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui-standalone-preset.js"></script>
    <script>
        window.onload = function() {{
            const ui = SwaggerUIBundle({{
                url: './openapi.json',
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [
                    SwaggerUIBundle.presets.apis,
                    SwaggerUIStandalonePreset
                ],
                plugins: [
                    SwaggerUIBundle.plugins.DownloadUrl
                ],
                layout: "StandaloneLayout"
            }});
        }};
    </script>
</body>
</html>"""
    
    def generate_postman_collection(self) -> str:
        """Generate Postman collection."""
        collection = {
            "info": {
                "name": "Gitea-Kimai Sync API",
                "description": "API collection for Gitea-Kimai sync operations",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "item": []
        }
        
        # Group endpoints by tag
        tags = {}
        for endpoint in self.api_endpoints:
            for tag in endpoint['tags']:
                if tag not in tags:
                    tags[tag] = []
                tags[tag].append(endpoint)
        
        for tag, endpoints in tags.items():
            folder = {
                "name": tag.title(),
                "item": []
            }
            
            for endpoint in endpoints:
                request = {
                    "name": f"{endpoint['method']} {endpoint['path']}",
                    "request": {
                        "method": endpoint['method'],
                        "header": [
                            {
                                "key": "Authorization",
                                "value": "Bearer {{token}}",
                                "type": "text"
                            },
                            {
                                "key": "Content-Type",
                                "value": "application/json",
                                "type": "text"
                            }
                        ],
                        "url": {
                            "raw": "{{base_url}}{endpoint['path']}",
                            "host": ["{{base_url}}"],
                            "path": endpoint['path'].strip('/').split('/')
                        }
                    }
                }
                
                # Add request body for POST/PUT/PATCH
                if endpoint['method'] in ['POST', 'PUT', 'PATCH'] and endpoint['examples']:
                    request['request']['body'] = {
                        "mode": "raw",
                        "raw": json.dumps(endpoint['examples'][0].get('value', {}), indent=2)
                    }
                
                folder['item'].append(request)
            
            collection['item'].append(folder)
        
        # Add variables
        collection['variable'] = [
            {
                "key": "base_url",
                "value": "http://localhost:5000",
                "type": "string"
            },
            {
                "key": "token",
                "value": "your-api-token-here",
                "type": "string"
            }
        ]
        
        collection_file = self.output_dir / "postman_collection.json"
        with open(collection_file, 'w') as f:
            json.dump(collection, f, indent=2)
        
        logger.info(f"Generated Postman collection: {collection_file}")
        return str(collection_file)
    
    def generate_curl_examples(self) -> str:
        """Generate cURL examples."""
        examples_file = self.output_dir / "curl_examples.md"
        
        content = ["# cURL Examples", ""]
        
        for endpoint in self.api_endpoints:
            content.append(f"## {endpoint['method']} {endpoint['path']}")
            content.append("")
            content.append(endpoint['description'])
            content.append("")
            
            # Generate cURL command
            curl_cmd = f"curl -X {endpoint['method']} \\"
            curl_cmd += f"\n  -H 'Authorization: Bearer YOUR_TOKEN' \\"
            curl_cmd += f"\n  -H 'Content-Type: application/json' \\"
            curl_cmd += f"\n  -d '{json.dumps(endpoint['examples'][0].get('value', {}))}' \\" if endpoint['examples'] else ""
            curl_cmd += f"\n  'http://localhost:5000{endpoint['path']}'"
            
            content.append("```bash")
            content.append(curl_cmd)
            content.append("```")
            content.append("")
            
            # Add example response
            if endpoint['response_schema']:
                content.append("**Example Response:**")
                content.append("")
                content.append("```json")
                content.append(json.dumps(endpoint['response_schema'], indent=2))
                content.append("```")
                content.append("")
            
            content.append("---")
            content.append("")
        
        with open(examples_file, 'w') as f:
            f.write("\n".join(content))
        
        logger.info(f"Generated cURL examples: {examples_file}")
        return str(examples_file)
    
    def generate_all_docs(self):
        """Generate all documentation formats."""
        logger.info("Generating API documentation...")
        
        files = []
        files.append(self.generate_openapi_spec())
        files.append(self.generate_markdown_docs())
        files.append(self.generate_html_docs())
        files.append(self.generate_postman_collection())
        files.append(self.generate_curl_examples())
        
        logger.info(f"Generated {len(files)} documentation files")
        return files

def create_api_documentation():
    """Create comprehensive API documentation."""
    generator = APIDocumentationGenerator()
    
    # Add sync endpoints
    generator.add_endpoint(
        path="/api/sync/start",
        method="POST",
        handler=None,
        description="Start a manual sync operation for a specific repository",
        tags=["sync"],
        request_schema={
            "type": "object",
            "properties": {
                "repository": {
                    "type": "string",
                    "description": "Repository name to sync"
                },
                "force": {
                    "type": "boolean",
                    "description": "Force sync even if no changes detected"
                }
            },
            "required": ["repository"]
        },
        response_schema={
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "message": {"type": "string"},
                "sync_id": {"type": "string"},
                "repository": {"type": "string"}
            }
        },
        examples=[
            {
                "summary": "Start sync for repository",
                "value": {
                    "repository": "my-project",
                    "force": False
                }
            }
        ]
    )
    
    generator.add_endpoint(
        path="/api/sync/status",
        method="GET",
        handler=None,
        description="Get sync operation status",
        tags=["sync"],
        response_schema={
            "type": "object",
            "properties": {
                "sync_id": {"type": "string"},
                "status": {"type": "string", "enum": ["running", "completed", "failed"]},
                "progress": {"type": "number"},
                "items_processed": {"type": "integer"},
                "items_synced": {"type": "integer"},
                "errors": {"type": "array", "items": {"type": "string"}}
            }
        }
    )
    
    generator.add_endpoint(
        path="/api/repositories",
        method="GET",
        handler=None,
        description="Get list of configured repositories",
        tags=["repositories"],
        response_schema={
            "type": "object",
            "properties": {
                "repositories": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "last_sync": {"type": "string", "format": "date-time"},
                            "status": {"type": "string"}
                        }
                    }
                }
            }
        }
    )
    
    generator.add_endpoint(
        path="/api/metrics",
        method="GET",
        handler=None,
        description="Get sync metrics and statistics",
        tags=["metrics"],
        response_schema={
            "type": "object",
            "properties": {
                "total_syncs": {"type": "integer"},
                "successful_syncs": {"type": "integer"},
                "failed_syncs": {"type": "integer"},
                "total_items_synced": {"type": "integer"},
                "average_duration": {"type": "number"}
            }
        }
    )
    
    generator.add_endpoint(
        path="/api/health",
        method="GET",
        handler=None,
        description="Get system health status",
        tags=["system"],
        response_schema={
            "type": "object",
            "properties": {
                "status": {"type": "string", "enum": ["healthy", "degraded", "unhealthy"]},
                "components": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "status": {"type": "string"},
                            "message": {"type": "string"}
                        }
                    }
                }
            }
        }
    )
    
    generator.add_endpoint(
        path="/api/config",
        method="GET",
        handler=None,
        description="Get current configuration",
        tags=["configuration"],
        response_schema={
            "type": "object",
            "properties": {
                "gitea_url": {"type": "string"},
                "kimai_url": {"type": "string"},
                "sync_interval": {"type": "integer"},
                "read_only_mode": {"type": "boolean"}
            }
        }
    )
    
    generator.add_endpoint(
        path="/api/config",
        method="PUT",
        handler=None,
        description="Update configuration",
        tags=["configuration"],
        request_schema={
            "type": "object",
            "properties": {
                "sync_interval": {"type": "integer", "minimum": 60},
                "read_only_mode": {"type": "boolean"}
            }
        },
        response_schema={
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "message": {"type": "string"}
            }
        },
        examples=[
            {
                "summary": "Update sync interval",
                "value": {
                    "sync_interval": 300,
                    "read_only_mode": False
                }
            }
        ]
    )
    
    generator.add_endpoint(
        path="/api/export",
        method="POST",
        handler=None,
        description="Export sync data",
        tags=["export"],
        request_schema={
            "type": "object",
            "properties": {
                "format": {"type": "string", "enum": ["csv", "json", "excel"]},
                "start_date": {"type": "string", "format": "date"},
                "end_date": {"type": "string", "format": "date"},
                "repositories": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["format"]
        },
        response_schema={
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "file_url": {"type": "string"},
                "file_size": {"type": "integer"}
            }
        },
        examples=[
            {
                "summary": "Export CSV data",
                "value": {
                    "format": "csv",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31"
                }
            }
        ]
    )
    
    # Generate all documentation
    return generator.generate_all_docs()

def generate_code_documentation():
    """Generate documentation from code analysis."""
    # This would analyze the actual code to extract API information
    pass

if __name__ == "__main__":
    # Generate API documentation
    files = create_api_documentation()
    print(f"Generated documentation files: {files}")
