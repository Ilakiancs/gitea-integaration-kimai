#!/bin/bash

# run_sync.sh - Script to run Gitea to Kimai synchronization
# Usage: ./run_sync.sh [test|sync|report|auto]

# Set up environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

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

# Main logic based on arguments
case "$1" in
    "test")
        echo "Testing connections..."
        python3 test_connection.py
        ;;
    "sync")
        echo "Running sync..."
        python3 sync.py
        ;;
    "report")
        echo "Showing sync report..."
        python3 report.py
        ;;
    "auto")
        setup_auto_sync
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
