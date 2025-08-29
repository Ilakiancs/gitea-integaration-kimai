# Gitea to Kimai Issue Sync

A Python script that synchronizes issues/PRs from Gitea repositories to activities in Kimai time tracking system.

## Overview

This script implements a one-way sync from Gitea to Kimai with the following mapping:
- **Gitea Repository** → **Kimai Project**
- **Gitea Issue/PR** → **Kimai Activity**

The script maintains a SQLite database to track synced items and prevent duplicates.

## Features

- Syncs issues/PRs from multiple Gitea repositories
- Creates or updates Kimai activities based on changes
- Maintains sync history in SQLite database
- Handles authentication for both Gitea and Kimai
- Comprehensive logging and error handling
- Environment-based configuration
- Supports both issues and pull requests

## Requirements

- Python 3.7+
- Gitea instance with API access
- Kimai instance with API access
- Valid API tokens/credentials for both services

## Installation

1. **Clone or download this repository**
2. **Create and activate a virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with your actual configuration values.

## Configuration

Edit the `.env` file with your settings:

```env
# Gitea Configuration
GITEA_URL=https://your-gitea-instance.com
GITEA_TOKEN=your_gitea_api_token_here
GITEA_ORGANIZATION=your-organization-name

# Kimai Configuration
KIMAI_URL=https://your-kimai-instance.com
KIMAI_USERNAME=your_kimai_username
KIMAI_PASSWORD=your_kimai_password

# Repositories to sync (comma-separated)
REPOS_TO_SYNC=repo1,repo2,repo3

# Database Configuration
DATABASE_PATH=sync.db

# Logging Level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

### Getting API Tokens

**Gitea Token:**
1. Go to your Gitea instance
2. Navigate to Settings → Applications
3. Generate a new token with repository read permissions

**Kimai Credentials:**
- Use your regular Kimai username and password
- Ensure your user has permissions to create/edit projects and activities

## Usage

### Basic Sync
```bash
python sync.py
```

### Running with Different Log Levels
```bash
# Debug mode for troubleshooting
LOG_LEVEL=DEBUG python sync.py

# Quiet mode (errors only)
LOG_LEVEL=ERROR python sync.py
```

### Scheduled Syncing
You can set up automated syncing using cron (Linux/Mac) or Task Scheduler (Windows):

```bash
# Example cron job to sync every hour
0 * * * * cd /path/to/gitea-kimai-sync && python sync.py
```

## Database Schema

The script creates a SQLite database (`sync.db`) with the following table:

```sql
CREATE TABLE activity_sync (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gitea_issue_id INTEGER NOT NULL,
    kimai_activity_id INTEGER NOT NULL,
    kimai_project_id INTEGER NOT NULL,
    project_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(gitea_issue_id, project_name)
);
```

## How It Works

1. **Fetches Issues**: Gets all issues from specified Gitea repositories
2. **Project Mapping**: Creates or finds matching Kimai projects for each repository
3. **Activity Sync**: 
   - If issue exists in database → Updates existing Kimai activity
   - If issue is new → Creates new Kimai activity and logs in database
4. **Logging**: Records all actions taken during sync

## Logging

The script creates two log outputs:
- **Console output**: Real-time sync progress
- **File output**: Detailed log saved to `sync.log`

## Troubleshooting

### Common Issues

**Authentication Errors:**
- Verify your Gitea token has correct permissions
- Check Kimai username/password are correct
- Ensure API endpoints are accessible

**Missing Projects:**
- Projects are auto-created in Kimai if they don't exist
- Check Kimai user has project creation permissions

**Database Errors:**
- Delete `sync.db` to reset sync history (will recreate all activities)
- Check file permissions in the script directory

### Debug Mode

Run with debug logging to see detailed API interactions:
```bash
LOG_LEVEL=DEBUG python sync.py
```

### Manual Database Inspection

```bash
sqlite3 sync.db
.schema
SELECT * FROM activity_sync;
.quit
```

## File Structure

```
gitea-kimai-sync/
├── sync.py              # Main sync script
├── test_connection.py   # Script to test API connectivity
├── report.py            # Script to view sync status and results
├── requirements.txt     # Python dependencies
├── .env                 # Configuration (not in git)
├── .env.example         # Configuration template
├── .gitignore           # Git ignore rules
├── README.md            # This file
└── sync.db              # SQLite database (auto-created)
```

## Security Notes

- Never commit `.env` file to version control
- Use environment variables for sensitive data
- Regularly rotate API tokens
- Ensure proper file permissions on config files

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## Usage

### Testing Connectivity
```bash
python test_connection.py
```

### Running the Sync
```bash
python sync.py
```

### Viewing Sync Results
```bash
python report.py
```

### Setting Up Automatic Syncing
Add a cron job to run the script at regular intervals:

```bash
# Edit your crontab
crontab -e

# Add this line to run the sync every hour
0 * * * * cd /path/to/gitea-kimai-sync && source venv/bin/activate && python sync.py
```

## License

This project is provided as-is. Modify and use according to your needs.