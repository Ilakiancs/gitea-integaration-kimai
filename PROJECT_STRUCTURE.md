# Project Structure

This document describes the organized structure of the Gitea to Kimai Integration project.

## Overview

The project is organized into logical modules with clear separation of concerns:

```
gitea-kimai-integration/
├── main.py                 # Main entry point
├── requirements.txt        # Python dependencies
├── setup.py               # Package setup
├── README.md              # Project documentation
├── LICENSE                # License file
├── .env.example           # Environment variables template
├── .env.template          # Comprehensive configuration template
├── .gitignore             # Git ignore rules
├── src/                   # Source code directory
│   ├── __init__.py
│   ├── core/              # Core synchronization logic
│   ├── api/               # API and webhook handling
│   ├── data/              # Data processing and manipulation
│   ├── security/          # Security and authentication
│   ├── utils/             # Utility functions and helpers
│   ├── web/               # Web interface components
│   ├── cli/               # Command-line interface
│   ├── config/            # Configuration management
│   ├── backup/            # Backup and restore functionality
│   ├── diagnostics/       # Diagnostic tools
│   ├── validation/        # Data validation
│   ├── monitoring/        # System monitoring and metrics
│   └── storage/           # Data storage and caching
├── docs/                  # Documentation
├── tests/                 # Test files
├── scripts/               # Helper scripts
└── examples/              # Example configurations
```

## Detailed Module Structure

### Core (`src/core/`)
Core synchronization and task management functionality:

- **`sync.py`** - Main synchronization engine
- **`sync_engine.py`** - Advanced sync engine with conflict resolution
- **`task_queue.py`** - Task queue management
- **`scheduler.py`** - Scheduling and automation

### API (`src/api/`)
API handling and webhook management:

- **`api.py`** - REST API server
- **`api_client.py`** - API client for Gitea and Kimai
- **`api_docs.py`** - API documentation generator
- **`webhooks.py`** - Webhook handling
- **`webhook_handler.py`** - Webhook request processing

### Data (`src/data/`)
Data processing, manipulation, and transformation:

- **`data_encryption.py`** - Data encryption utilities
- **`data_compression.py`** - Data compression
- **`data_serialization.py`** - Data serialization
- **`data_validation.py`** - Data validation
- **`data_export.py`** - Data export functionality
- **`data_diff.py`** - Data comparison and diffing
- **`data_merger.py`** - Data merging utilities
- **`data_sanitizer.py`** - Data sanitization
- **`data_indexing.py`** - Data indexing
- **`data_checksum.py`** - Checksum calculation
- **`data_pipeline.py`** - Data processing pipeline
- **`format_converter.py`** - Format conversion utilities

### Security (`src/security/`)
Security, authentication, and authorization:

- **`security.py`** - Security management and access control

### Utils (`src/utils/`)
Utility functions and helper modules:

- **`error_handler.py`** - Error handling utilities
- **`logging_enhanced.py`** - Enhanced logging
- **`migration.py`** - Database migration utilities
- **`notification_system.py`** - Notification system
- **`notifications.py`** - Notification utilities
- **`rate_limiter.py`** - Rate limiting
- **`retry_handler.py`** - Retry logic
- **`user_profiles.py`** - User profile management
- **`diagnose.py`** - Diagnostic utilities
- **`test_connection.py`** - Connection testing
- **`report.py`** - Reporting utilities

### Web (`src/web/`)
Web interface and dashboard:

- **`web_dashboard.py`** - Web dashboard

### CLI (`src/cli/`)
Command-line interface components:

*(Currently empty - CLI functionality is in main.py)*

### Config (`src/config/`)
Configuration management:

- **`config_manager.py`** - Configuration management
- **`validate_config.py`** - Configuration validation
- **`config_validator.py`** - Additional configuration validation

### Backup (`src/backup/`)
Backup and restore functionality:

*(Currently empty - backup functionality is in storage/)*

### Diagnostics (`src/diagnostics/`)
Diagnostic and troubleshooting tools:

*(Currently empty - diagnostic functionality is in utils/)*

### Validation (`src/validation/`)
Data validation and schema management:

- **`schema_validator.py`** - Schema validation
- **`validation_rules.py`** - Validation rules engine

### Monitoring (`src/monitoring/`)
System monitoring, health checks, and metrics:

- **`performance_monitor.py`** - Performance monitoring
- **`health_check.py`** - Health checking
- **`metrics.py`** - Metrics collection
- **`statistics.py`** - Statistical analysis

### Storage (`src/storage/`)
Data storage, caching, and backup:

- **`backup_manager.py`** - Backup management
- **`cache_manager.py`** - Cache management
- **`encryption.py`** - Storage encryption
- **`backup.py`** - Backup utilities

## Key Files in Root Directory

### `main.py`
The main entry point for the application. Provides a comprehensive command-line interface for all operations:

- **sync** - Run synchronization
- **diagnose** - Run diagnostics
- **report** - Show sync reports
- **test** - Test connections
- **backup** - Backup operations
- **dashboard** - Start web dashboard
- **api** - Start API server
- **health** - Check system health

### `setup.py`
Package configuration for installation and distribution.

### `requirements.txt`
Python dependencies required for the project.

### Configuration Files
- **`.env.example`** - Template for environment variables
- **`.env.template`** - Comprehensive configuration template

## Usage

### Running the Application

```bash
# Show help
python main.py --help

# Run synchronization
python main.py sync

# Run diagnostics
python main.py diagnose

# Start web dashboard
python main.py dashboard

# Start API server
python main.py api
```

### Development

The modular structure makes it easy to:

1. **Add new features** - Place them in the appropriate module
2. **Modify existing functionality** - Locate the relevant module
3. **Test components** - Test individual modules in isolation
4. **Maintain code** - Clear separation of concerns

### Importing Modules

When importing from other modules, use the full path:

```python
from src.core.sync import EnhancedGiteeKimaiSync
from src.utils.diagnose import run_diagnostics
from src.api.api_client import GiteaKimaiClient
```

## Benefits of This Structure

1. **Modularity** - Each module has a specific responsibility
2. **Maintainability** - Easy to locate and modify code
3. **Testability** - Modules can be tested independently
4. **Scalability** - Easy to add new features
5. **Clarity** - Clear organization makes the codebase understandable
6. **Reusability** - Modules can be reused across different parts of the application

## Future Enhancements

The structure is designed to accommodate future enhancements:

- **Plugin system** - Easy to add new data processors
- **API extensions** - Simple to add new API endpoints
- **Additional integrations** - Framework for new service integrations
- **Custom validators** - Extensible validation system
- **New storage backends** - Pluggable storage system
