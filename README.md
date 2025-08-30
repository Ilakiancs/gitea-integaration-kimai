# Gitea-Kimai Integration System

A comprehensive integration system for synchronizing data between Gitea (Git hosting) and Kimai (time tracking) platforms.

## Features

- **Core Sync Engine**: Automated synchronization between Gitea issues and Kimai activities
- **Data Processing**: Advanced data transformation, validation, and sanitization
- **Security**: Encryption, checksums, and secure data handling
- **Monitoring**: Health checks, performance monitoring, and metrics
- **Web Interface**: Dashboard for monitoring and configuration
- **API Integration**: RESTful API clients for both Gitea and Kimai
- **Storage Management**: Backup, caching, and data persistence
- **Validation**: Schema validation and data integrity checks

## Project Structure

```
gitea-integaration-kimai/
├── src/                          # Source code
│   ├── core/                     # Core sync engine
│   │   ├── sync_engine.py       # Main sync engine
│   │   ├── sync.py              # Sync utilities
│   │   ├── scheduler.py         # Task scheduling
│   │   └── task_queue.py        # Task queue management
│   ├── api/                      # API integration
│   │   ├── api_client.py        # Generic API client
│   │   ├── api.py               # API utilities
│   │   └── webhooks.py          # Webhook handlers
│   ├── data/                     # Data processing
│   │   ├── data_compression.py  # Data compression
│   │   ├── data_encryption.py   # Data encryption
│   │   ├── data_checksum.py     # Checksum utilities
│   │   ├── data_serialization.py # Data serialization
│   │   ├── data_indexing.py     # Data indexing
│   │   ├── data_diff.py         # Data comparison
│   │   ├── data_merger.py       # Data merging
│   │   ├── data_sanitizer.py    # Data sanitization
│   │   ├── data_validation.py   # Data validation
│   │   ├── data_export.py       # Data export
│   │   ├── data_pipeline.py     # Data transformation pipeline
│   │   └── format_converter.py  # Format conversion
│   ├── validation/               # Validation utilities
│   │   ├── validation_rules.py  # Validation rules engine
│   │   └── schema_validator.py  # Schema validation
│   ├── storage/                  # Storage management
│   │   ├── backup_manager.py    # Backup management
│   │   ├── backup.py            # Backup utilities
│   │   ├── cache_manager.py     # Cache management
│   │   └── encryption.py        # Storage encryption
│   ├── utils/                    # Utility functions
│   │   ├── error_handler.py     # Error handling
│   │   ├── logging_enhanced.py  # Enhanced logging
│   │   ├── retry_handler.py     # Retry logic
│   │   ├── rate_limiter.py      # Rate limiting
│   │   ├── notification_system.py # Notification system
│   │   ├── notifications.py     # Notification utilities
│   │   ├── diagnose.py          # Diagnostics
│   │   ├── report.py            # Reporting
│   │   ├── test_connection.py   # Connection testing
│   │   ├── user_profiles.py     # User profile management
│   │   └── migration.py         # Data migration
│   ├── monitoring/               # Monitoring and health
│   │   ├── health_check.py      # Health checks
│   │   ├── performance_monitor.py # Performance monitoring
│   │   ├── metrics.py           # Metrics collection
│   │   └── statistics.py        # Statistics
│   ├── config/                   # Configuration
│   │   ├── config_manager.py    # Configuration management
│   │   └── validate_config.py   # Configuration validation
│   ├── web/                      # Web interface
│   │   └── web_dashboard.py     # Web dashboard
│   └── main.py                   # Main entry point
├── tests/                        # Test files
│   ├── unit/                     # Unit tests
│   └── integration/              # Integration tests
├── docs/                         # Documentation
│   ├── api/                      # API documentation
│   ├── user_guide/               # User guides
│   └── DIAGNOSTICS.md            # Diagnostics guide
├── scripts/                      # Scripts
│   └── run_sync.sh              # Sync runner script
├── examples/                     # Example configurations
├── requirements.txt              # Python dependencies
├── setup.py                      # Package setup
└── README.md                     # This file
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/gitea-kimai-integration.git
cd gitea-kimai-integration
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install the package:
```bash
pip install -e .
```

## Configuration

1. Create a configuration file:
```bash
cp examples/config.example.json config.json
```

2. Edit the configuration with your Gitea and Kimai credentials:
```json
{
  "gitea": {
    "url": "https://gitea.yourdomain.com",
    "token": "your_gitea_token",
    "organization": "your_org"
  },
  "kimai": {
    "url": "https://kimai.yourdomain.com",
    "token": "your_kimai_token"
  },
  "sync": {
    "interval": 300,
    "repositories": ["repo1", "repo2"]
  }
}
```

## Usage

### Basic Usage

Run the sync system:
```bash
python src/main.py
```

Or use the provided script:
```bash
./scripts/run_sync.sh
```

### Web Dashboard

Start the web dashboard:
```bash
python src/web/web_dashboard.py
```

### Testing Connections

Test your configuration:
```bash
python src/utils/test_connection.py
```

### Diagnostics

Run diagnostics:
```bash
python src/utils/diagnose.py
```

## Development

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# All tests with coverage
pytest --cov=src tests/
```

### Code Quality

```bash
# Format code
black src/

# Lint code
flake8 src/

# Type checking
mypy src/
```

## API Documentation

The system provides several APIs:

- **Sync API**: Core synchronization endpoints
- **Data API**: Data processing and manipulation
- **Monitoring API**: Health checks and metrics
- **Webhook API**: Webhook handling for Gitea events

See `docs/api/` for detailed API documentation.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support and questions:
- Create an issue on GitHub
- Check the documentation in `docs/`
- Review the diagnostics guide in `docs/DIAGNOSTICS.md`