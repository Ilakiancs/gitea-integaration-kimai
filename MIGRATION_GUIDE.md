# Migration Guide

This guide helps you transition from the old flat file structure to the new organized modular structure.

## What Changed

The project has been reorganized from a flat structure with all files in the root directory to a modular structure with clear separation of concerns.

### Old Structure (Before)
```
gitea-kimai-integration/
├── sync.py
├── api_client.py
├── data_encryption.py
├── backup_manager.py
├── ... (many more files in root)
└── README.md
```

### New Structure (After)
```
gitea-kimai-integration/
├── main.py                 # New main entry point
├── src/                    # New source directory
│   ├── core/              # Core sync functionality
│   ├── api/               # API handling
│   ├── data/              # Data processing
│   ├── utils/             # Utilities
│   └── ... (organized modules)
└── README.md
```

## Key Changes

### 1. New Main Entry Point
- **Old**: Run `python sync.py`
- **New**: Run `python main.py sync`

### 2. Organized Modules
Files are now organized into logical modules:

| Old Location | New Location | Purpose |
|--------------|--------------|---------|
| `sync.py` | `src/core/sync.py` | Main synchronization |
| `api_client.py` | `src/api/api_client.py` | API client |
| `data_encryption.py` | `src/data/data_encryption.py` | Data encryption |
| `backup_manager.py` | `src/storage/backup_manager.py` | Backup management |
| `diagnose.py` | `src/utils/diagnose.py` | Diagnostics |
| `report.py` | `src/utils/report.py` | Reporting |

### 3. Enhanced Command Line Interface
The new `main.py` provides a comprehensive CLI:

```bash
# Old way
python sync.py --dry-run

# New way
python main.py sync --dry-run

# Additional new commands
python main.py diagnose
python main.py report
python main.py dashboard
python main.py api
```

## Migration Steps

### Step 1: Update Your Scripts
If you have custom scripts that import the old modules, update them:

**Old imports:**
```python
from sync import EnhancedGiteeKimaiSync
from api_client import GiteaKimaiClient
```

**New imports:**
```python
from src.core.sync import EnhancedGiteeKimaiSync
from src.api.api_client import GiteaKimaiClient
```

### Step 2: Update Cron Jobs
If you have automated cron jobs, update them:

**Old cron job:**
```bash
0 * * * * cd /path/to/project && python sync.py
```

**New cron job:**
```bash
0 * * * * cd /path/to/project && python main.py sync
```

### Step 3: Update Documentation
Update any internal documentation that references the old file structure.

## New Features

### 1. Unified Command Line Interface
```bash
python main.py --help
```

Available commands:
- `sync` - Run synchronization
- `diagnose` - Run diagnostics
- `report` - Show reports
- `test` - Test connections
- `backup` - Backup operations
- `dashboard` - Start web dashboard
- `api` - Start API server
- `health` - Check system health

### 2. Better Organization
- **Core functionality** in `src/core/`
- **API handling** in `src/api/`
- **Data processing** in `src/data/`
- **Utilities** in `src/utils/`
- **Configuration** in `src/config/`

### 3. Enhanced Documentation
- `PROJECT_STRUCTURE.md` - Detailed structure documentation
- `MIGRATION_GUIDE.md` - This migration guide
- `scripts/show_structure.py` - Interactive structure viewer

## Backward Compatibility

### Import Compatibility
The old imports will **not work** with the new structure. You must update your imports to use the new module paths.

### Configuration Compatibility
- Environment variables remain the same
- Configuration files remain the same
- Database files remain the same

### API Compatibility
- The core functionality remains the same
- The same classes and methods are available
- Only the import paths have changed

## Testing the Migration

### 1. Test the New Structure
```bash
# View the new structure
python scripts/show_structure.py

# Test the main entry point
python main.py --help
```

### 2. Test Core Functionality
```bash
# Test synchronization
python main.py sync --dry-run

# Test diagnostics
python main.py diagnose

# Test connections
python main.py test
```

### 3. Test Web Interfaces
```bash
# Test web dashboard
python main.py dashboard --port 8080

# Test API server
python main.py api --port 5000
```

## Troubleshooting

### Import Errors
If you get import errors, make sure you're using the new import paths:

```python
# ❌ Old way (will fail)
from sync import EnhancedGiteeKimaiSync

# ✅ New way
from src.core.sync import EnhancedGiteeKimaiSync
```

### Module Not Found
If you get "module not found" errors, check that:
1. You're running from the project root directory
2. The `src/` directory exists
3. The module files are in the correct locations

### Configuration Issues
If configuration doesn't work:
1. Check that your `.env` file is in the project root
2. Verify environment variables are set correctly
3. Check the configuration validation: `python main.py diagnose`

## Getting Help

If you encounter issues during migration:

1. **Check the documentation:**
   - `README.md` - Main documentation
   - `PROJECT_STRUCTURE.md` - Structure details
   - `DIAGNOSTICS.md` - Troubleshooting guide

2. **Run diagnostics:**
   ```bash
   python main.py diagnose --all
   ```

3. **View project structure:**
   ```bash
   python scripts/show_structure.py
   ```

4. **Test connections:**
   ```bash
   python main.py test
   ```

## Benefits of the New Structure

1. **Better Organization** - Clear separation of concerns
2. **Easier Maintenance** - Related code is grouped together
3. **Improved Testing** - Modules can be tested independently
4. **Enhanced Documentation** - Better structure documentation
5. **Unified Interface** - Single entry point for all operations
6. **Future-Proof** - Easy to add new features and modules

## Summary

The migration to the new organized structure provides:
- Better code organization
- Enhanced maintainability
- Unified command-line interface
- Improved documentation
- Future scalability

The core functionality remains the same, but the code is now better organized and easier to work with.
