#!/bin/bash

# run_sync.sh - Script to run Gitea to Kimai synchronization
# Usage: ./run_sync.sh [test|sync|report|auto]

# Set up environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Create essential directories if they don't exist
[ -d ".cache" ] || mkdir -p .cache
[ -d "exports" ] || mkdir -p exports

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Help message
show_help() {
    echo "Gitea to Kimai Sync - Helper Script"
    echo "Usage: ./run_sync.sh [command]"
    echo ""
    echo "Commands:"
    echo "  test    - Test connections to Gitea and Kimai"
    echo "  sync    - Run the sync once"
    echo "  report  - Show sync results and statistics"
    echo "  auto    - Set up automatic syncing (requires sudo)"
    echo "  export  - Export sync data to CSV files"
    echo "  cache   - Manage cache (clear/status)"
    echo "  backup  - Create/restore backups"
    echo "  diagnose - Run diagnostics"
    echo "  help    - Show this help message"
    echo ""
    echo "If no command is provided, runs the sync once."
}

# Set up automatic syncing
setup_auto_sync() {
    echo "Setting up automatic sync using cron..."

    # Create a temporary cron file
    TEMP_CRON=$(mktemp)
    crontab -l > "$TEMP_CRON" 2>/dev/null

    # Check if entry already exists
    if grep -q "gitea-kimai-sync/sync.py" "$TEMP_CRON"; then
        echo "Auto sync already set up in crontab."
    else
        # Add the cron entry - runs every hour
        echo "0 * * * * cd $SCRIPT_DIR && [ -f venv/bin/activate ] && source venv/bin/activate; python3 sync.py >> sync_cron.log 2>&1" >> "$TEMP_CRON"
        crontab "$TEMP_CRON"
        echo "Automatic sync scheduled to run every hour."
    fi

    # Clean up
    rm "$TEMP_CRON"
}

# Cache management functions
clear_cache() {
    echo "Clearing cache..."
    rm -rf .cache/*
    echo "Cache cleared."
}

show_cache_status() {
    echo "Cache status:"
    if [ -d ".cache" ]; then
        cache_count=$(find .cache -type f -name "*.cache" | wc -l)
        cache_size=$(du -sh .cache | cut -f1)
        echo "  Cache files: $cache_count"
        echo "  Cache size:  $cache_size"
    else
        echo "  Cache directory not found"
    fi
}

# Main logic based on arguments
case "$1" in
    "test")
        echo "Testing connections..."
        python3 test_connection.py
        ;;
    "sync")
        echo "Running sync..."
        shift
        python3 sync.py "$@"  # Pass additional arguments to sync.py
        ;;
    "report")
        echo "Showing sync report..."
        python3 report.py
        ;;
    "auto")
        setup_auto_sync
        ;;
    "export")
        echo "Exporting sync data..."
        python3 sync.py --export
        ;;
    "backup")
        case "$2" in
            "create")
                echo "Creating backup..."
                python3 backup.py backup
                ;;
            "restore")
                if [ -z "$3" ]; then
                    echo "Please specify backup file to restore:"
                    echo "  ./run_sync.sh backup restore <backup_file>"
                    python3 backup.py list
                else
                    echo "Restoring backup from $3..."
                    python3 backup.py restore "$3"
                fi
                ;;
            "list")
                echo "Available backups:"
                python3 backup.py list --detail
                ;;
            *)
                echo "Backup management commands:"
                echo "  ./run_sync.sh backup create - Create a new backup"
                echo "  ./run_sync.sh backup list   - List available backups"
                echo "  ./run_sync.sh backup restore <file> - Restore from backup"
                ;;
        esac
        ;;
    "cache")
        case "$2" in
            "clear")
                clear_cache
                ;;
            "status")
                show_cache_status
                ;;
            *)
                echo "Cache management commands:"
                echo "  ./run_sync.sh cache clear  - Clear cache"
                echo "  ./run_sync.sh cache status - Show cache status"
                ;;
        esac
        ;;
    "diagnose")
        echo "Running diagnostics..."
        python3 diagnose.py "$2"
        ;;
    "help")
        show_help
        ;;
    *)
        # Default action
        if [ -z "$1" ]; then
            echo "Running sync (default action)..."
            python3 sync.py
        else
            echo "Unknown command: $1"
            show_help
            exit 1
        fi
        ;;
esac

# Deactivate virtual environment if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

exit 0
