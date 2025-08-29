# Diagnostic Tools for Gitea-Kimai Integration

This document provides information about the diagnostic tools included with the Gitea-Kimai integration.

## Overview

The diagnostic tools help troubleshoot configuration, connectivity, and operational issues with the Gitea-Kimai integration. They are designed to:

- Verify system configuration
- Check environment settings
- Test API connectivity
- Analyze database health
- Examine cache status
- Monitor network conditions

## Available Diagnostic Tools

### 1. `diagnose.py`

The main diagnostic tool that performs comprehensive checks of the entire system.

#### Usage

```bash
# Run basic diagnostics
python diagnose.py

# Run all available diagnostic checks
python diagnose.py --all

# Run specific checks
python diagnose.py --system --network --api

# Output in JSON format
python diagnose.py --json
```

#### Available Options

| Option | Description |
|--------|-------------|
| `--all` | Run all diagnostic tests |
| `--system` | Check system information (OS, Python version, disk space) |
| `--dependencies` | Verify required Python packages are installed |
| `--environment` | Check environment variables and configuration |
| `--directories` | Verify required directories exist and are writable |
| `--database` | Analyze database schema and contents |
| `--network` | Test network connectivity to Gitea and Kimai |
| `--api` | Check API access and permissions |
| `--cache` | Examine cache status |
| `--json` | Output results in JSON format |

### 2. `test_connection.py`

A focused tool to test connectivity to Gitea and Kimai APIs.

```bash
# Basic connection test
python test_connection.py

# With advanced options
python test_connection.py --json --verbose --check-cache
```

### 3. Command Line Tools via `run_sync.sh`

The run script includes diagnostic capabilities:

```bash
# Test connections
./run_sync.sh test

# Check cache status
./run_sync.sh cache status

# Clear the cache
./run_sync.sh cache clear
```

## Troubleshooting Common Issues

### Configuration Problems

If `diagnose.py` reports issues with configuration:

1. Verify your `.env` file contains all required variables
2. Compare against the `.env.template` file
3. Check for proper URL formats (including http/https prefix)
4. Ensure API tokens have the correct permissions

### API Connection Failures

If API connections are failing:

1. Check network connectivity
2. Verify API endpoints are accessible
3. Ensure authentication credentials are valid
4. Check firewall settings

### Database Issues

For database problems:

1. Check permissions on the database file
2. Verify SQLite is working properly
3. Consider backing up and recreating the database
4. Run `diagnose.py --database` for detailed analysis

### Cache Problems

If caching issues occur:

1. Clear the cache: `./run_sync.sh cache clear`
2. Verify cache directory permissions
3. Check available disk space

## Logging

All diagnostic tools log their output to the console. For persistent logs:

```bash
# Save diagnostic output to a file
python diagnose.py --all > diagnostic_results.log
```

## Advanced Diagnostics

For deeper analysis of API interactions, enable debug logging:

```bash
LOG_LEVEL=DEBUG python diagnose.py --api
```

## Getting Help

If the diagnostic tools don't resolve your issue, consider:

1. Checking the project documentation
2. Looking for similar issues in the issue tracker
3. Providing the diagnostic output when seeking help