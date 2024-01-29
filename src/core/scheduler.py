#!/usr/bin/env python3
"""
Scheduler Module for Gitea-Kimai Integration

This module provides scheduling capabilities for the Gitea-Kimai integration,
allowing it to run synchronization jobs at specified intervals.

Usage:
  python scheduler.py start
  python scheduler.py stop
  python scheduler.py status
"""

import os
import sys
import time
import signal
import argparse
import logging
import subprocess
import json
import atexit
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Scheduler configuration from environment
SCHEDULER_ENABLED = os.getenv('SCHEDULER_ENABLED', 'false').lower() == 'true'
SCHEDULER_INTERVAL = int(os.getenv('SCHEDULER_INTERVAL', '3600'))  # Default: 1 hour
SCHEDULER_RETRY_INTERVAL = int(os.getenv('SCHEDULER_RETRY_INTERVAL', '300'))  # Default: 5 minutes
SCHEDULER_MAX_RETRIES = int(os.getenv('SCHEDULER_MAX_RETRIES', '3'))
SCHEDULER_DAEMON_MODE = os.getenv('SCHEDULER_DAEMON_MODE', 'false').lower() == 'true'
SCHEDULER_TIME_WINDOW = os.getenv('SCHEDULER_TIME_WINDOW', '').strip()  # Format: "HH:MM-HH:MM"
SCHEDULER_DAYS = os.getenv('SCHEDULER_DAYS', 'all')  # Format: "mon,tue,wed,thu,fri" or "all" or "weekdays" or "weekends"

# Process ID file path
PID_FILE = 'scheduler.pid'
STATUS_FILE = 'scheduler_status.json'

# Command to run for synchronization
SYNC_COMMAND = ['python', 'sync.py']

# Days of the week
DAYS_OF_WEEK = {
    'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6,
    'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6
}

class Scheduler:
    """
    Scheduler for running the Gitea-Kimai synchronization at regular intervals.
    """

    def __init__(self):
        """Initialize the scheduler."""
        self.running = False
        self.enabled = SCHEDULER_ENABLED
        self.interval = SCHEDULER_INTERVAL
        self.retry_interval = SCHEDULER_RETRY_INTERVAL
        self.max_retries = SCHEDULER_MAX_RETRIES
        self.daemon_mode = SCHEDULER_DAEMON_MODE
        self.time_window = self._parse_time_window(SCHEDULER_TIME_WINDOW)
        self.days = self._parse_days(SCHEDULER_DAYS)
        self.running = False
        self.last_run = None
        self.last_status = None
        self.runs_completed = 0
        self.runs_failed = 0
        self.next_run = None

    def _parse_time_window(self, time_window_str):
        """Parse the time window string."""
        if not time_window_str:
            return None

        try:
            start_time_str, end_time_str = time_window_str.split('-')
            start_hours, start_minutes = map(int, start_time_str.strip().split(':'))
            end_hours, end_minutes = map(int, end_time_str.strip().split(':'))

            return {
                'start': {'hours': start_hours, 'minutes': start_minutes},
                'end': {'hours': end_hours, 'minutes': end_minutes}
            }
        except Exception as e:
            logger.error(f"Error parsing time window '{time_window_str}': {e}")
            logger.info("Time window should be in the format 'HH:MM-HH:MM'")
            return None

    def _parse_days(self, days_str):
        """Parse the days string."""
        if not days_str or days_str.lower() == 'all':
            return list(range(7))  # All days

        if days_str.lower() == 'weekdays':
            return [0, 1, 2, 3, 4]  # Monday to Friday

        if days_str.lower() == 'weekends':
            return [5, 6]  # Saturday and Sunday

        try:
            days = []
            for day in days_str.lower().split(','):
                day = day.strip()
                if day in DAYS_OF_WEEK:
                    days.append(DAYS_OF_WEEK[day])
            return days
        except Exception as e:
            logger.error(f"Error parsing days '{days_str}': {e}")
            return list(range(7))  # Default to all days

    def _is_within_time_window(self):
        """Check if current time is within the configured time window."""
        if not self.time_window:
            return True

        now = datetime.now()
        start_time = now.replace(
            hour=self.time_window['start']['hours'],
            minute=self.time_window['start']['minutes'],
            second=0
        )
        end_time = now.replace(
            hour=self.time_window['end']['hours'],
            minute=self.time_window['end']['minutes'],
            second=0
        )

        # Handle overnight windows (e.g., 22:00-06:00)
        if end_time < start_time:
            return now >= start_time or now <= end_time
        else:
            return start_time <= now <= end_time

    def _is_allowed_day(self):
        """Check if current day is an allowed day."""
        return datetime.now().weekday() in self.days

    def _can_run_now(self):
        """Check if the scheduler can run a job now."""
        return self._is_within_time_window() and self._is_allowed_day()

    def _run_sync(self):
        """Run the synchronization process."""
        if not self._can_run_now():
            logger.info("Outside of allowed time window or day, skipping sync")
            self.next_run = datetime.now() + timedelta(seconds=self.interval)
            return

        logger.info("Starting synchronization...")
        self.last_run = datetime.now()
        success = False
        retries = 0

        while not success and retries <= self.max_retries:
            try:
                if retries > 0:
                    logger.info(f"Retry attempt {retries}/{self.max_retries}")

                process = subprocess.run(
                    SYNC_COMMAND,
                    capture_output=True,
                    text=True,
                    check=True
                )

                logger.info("Synchronization completed successfully")
                logger.debug(f"Output: {process.stdout}")
                self.runs_completed += 1
                success = True
                self.last_status = "success"

            except subprocess.CalledProcessError as e:
                retries += 1
                logger.error(f"Synchronization failed: {e}")
                logger.error(f"Error output: {e.stderr}")

                if retries <= self.max_retries:
                    logger.info(f"Waiting {self.retry_interval} seconds before retrying...")
                    time.sleep(self.retry_interval)
                else:
                    logger.error(f"Max retries ({self.max_retries}) reached. Giving up.")
                    self.runs_failed += 1
                    self.last_status = "failed"

            except Exception as e:
                retries += 1
                logger.error(f"Unexpected error during synchronization: {e}")

                if retries <= self.max_retries:
                    logger.info(f"Waiting {self.retry_interval} seconds before retrying...")
                    time.sleep(self.retry_interval)
                else:
                    logger.error(f"Max retries ({self.max_retries}) reached. Giving up.")
                    self.runs_failed += 1
                    self.last_status = "failed"

        # Calculate next run time
        self.next_run = datetime.now() + timedelta(seconds=self.interval)
        self._save_status()

    def _save_status(self):
        """Save the scheduler status to a file."""
        status = {
            'enabled': self.enabled,
            'running': self.running,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'next_run': self.next_run.isoformat() if self.next_run else None,
            'runs_completed': self.runs_completed,
            'runs_failed': self.runs_failed,
            'last_status': self.last_status,
            'interval': self.interval,
            'time_window': SCHEDULER_TIME_WINDOW,
            'days': SCHEDULER_DAYS,
            'daemon_mode': self.daemon_mode,
            'pid': os.getpid()
        }

        try:
            with open(STATUS_FILE, 'w') as f:
                json.dump(status, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving status file: {e}")

    def _load_status(self):
        """Load the scheduler status from a file."""
        try:
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE, 'r') as f:
                    status = json.load(f)

                self.runs_completed = status.get('runs_completed', 0)
                self.runs_failed = status.get('runs_failed', 0)
                self.last_status = status.get('last_status')

                if status.get('last_run'):
                    self.last_run = datetime.fromisoformat(status['last_run'])

                if status.get('next_run'):
                    self.next_run = datetime.fromisoformat(status['next_run'])
                else:
                    self.next_run = datetime.now() + timedelta(seconds=self.interval)

                logger.info(f"Loaded scheduler status: {self.runs_completed} completed runs, {self.runs_failed} failed runs")
        except Exception as e:
            logger.error(f"Error loading status file: {e}")
            self.next_run = datetime.now() + timedelta(seconds=self.interval)

    def start(self):
        """Start the scheduler."""
        if self.daemon_mode:
            logger.info("Starting scheduler in daemon mode...")
            self._write_pid_file()
            self._daemonize()
        else:
            logger.info("Starting scheduler in foreground mode...")
            self._write_pid_file()
            self._run_scheduler()

    def stop(self):
        """Stop the scheduler."""
        pid = self._read_pid_file()
        if pid:
            logger.info(f"Stopping scheduler (PID: {pid})...")
            try:
                os.kill(pid, signal.SIGTERM)
                logger.info("Scheduler stopped")
                return True
            except ProcessLookupError:
                logger.warning(f"No process found with PID {pid}")
                os.remove(PID_FILE)
                return True
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")
                return False
        else:
            logger.warning("Scheduler is not running")
            return True

    def status(self):
        """Get the scheduler status."""
        pid = self._read_pid_file()

        if pid:
            # Check if process is actually running
            try:
                os.kill(pid, 0)  # Signal 0 just checks if process exists
                status = "running"
            except ProcessLookupError:
                status = "stopped (stale PID file)"
                os.remove(PID_FILE)
            except Exception:
                status = "unknown"
        else:
            status = "stopped"

        # Load latest status from file
        try:
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE, 'r') as f:
                    status_data = json.load(f)
            else:
                status_data = {}
        except Exception as e:
            logger.error(f"Error reading status file: {e}")
            status_data = {}

        # Combine information
        result = {
            'status': status,
            'pid': pid,
            'scheduler_enabled': SCHEDULER_ENABLED,
            'interval': SCHEDULER_INTERVAL,
            'time_window': SCHEDULER_TIME_WINDOW,
            'days': SCHEDULER_DAYS,
            'daemon_mode': SCHEDULER_DAEMON_MODE
        }

        # Add data from status file
        result.update({k: v for k, v in status_data.items() if k not in result})

        return result

    def _daemonize(self):
        """Daemonize the scheduler process."""
        try:
            # First fork
            pid = os.fork()
            if pid > 0:
                # Exit from first parent
                sys.exit(0)
        except OSError as e:
            logger.error(f"Fork #1 failed: {e}")
            sys.exit(1)

        # Decouple from parent environment
        os.chdir('/')
        os.setsid()
        os.umask(0)

        try:
            # Second fork
            pid = os.fork()
            if pid > 0:
                # Exit from second parent
                sys.exit(0)
        except OSError as e:
            logger.error(f"Fork #2 failed: {e}")
            sys.exit(1)

        # Redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()

        with open(os.devnull, 'r') as f:
            os.dup2(f.fileno(), sys.stdin.fileno())

        log_file = open('scheduler.out', 'a+')
        err_file = open('scheduler.err', 'a+')

        os.dup2(log_file.fileno(), sys.stdout.fileno())
        os.dup2(err_file.fileno(), sys.stderr.fileno())

        # Run the scheduler
        self._run_scheduler()

    def _run_scheduler(self):
        """Run the scheduler loop."""
        self.running = True
        self._load_status()

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Register exit handler
        atexit.register(self._cleanup)

        logger.info(f"Scheduler started with interval of {self.interval} seconds")
        if self.time_window:
            window_str = f"{self.time_window['start']['hours']}:{self.time_window['start']['minutes']} - {self.time_window['end']['hours']}:{self.time_window['end']['minutes']}"
            logger.info(f"Time window: {window_str}")

        days_names = []
        if self.days == list(range(7)):
            days_names = ["all days"]
        elif self.days == list(range(5)):
            days_names = ["weekdays"]
        elif self.days == [5, 6]:
            days_names = ["weekends"]
        else:
            day_map = {v: k for k, v in DAYS_OF_WEEK.items() if len(k) == 3}
            days_names = [day_map.get(day, str(day)) for day in self.days]

        logger.info(f"Running on: {', '.join(days_names)}")

        try:
            # Run synchronization immediately if requested
            if self.next_run is None or self.next_run <= datetime.now():
                self._run_sync()

            # Main loop
            while self.running:
                now = datetime.now()

                if self.next_run and now >= self.next_run:
                    self._run_sync()

                # Sleep to avoid consuming too much CPU
                time.sleep(10)

        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            self.running = False

        logger.info("Scheduler stopped")

    def _handle_signal(self, signum, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up...")
        self._save_status()
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)

    def _write_pid_file(self):
        """Write PID to file."""
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))

    def _read_pid_file(self):
        """Read PID from file."""
        try:
            if os.path.exists(PID_FILE):
                with open(PID_FILE, 'r') as f:
                    return int(f.read().strip())
        except Exception as e:
            logger.error(f"Error reading PID file: {e}")
        return None

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Scheduler for Gitea-Kimai integration")

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Start command
    start_parser = subparsers.add_parser('start', help='Start the scheduler')
    start_parser.add_argument('--daemon', action='store_true', help='Run in daemon mode')
    start_parser.add_argument('--interval', type=int, help='Sync interval in seconds')

    # Stop command
    subparsers.add_parser('stop', help='Stop the scheduler')

    # Status command
    status_parser = subparsers.add_parser('status', help='Show scheduler status')
    status_parser.add_argument('--json', action='store_true', help='Output status as JSON')

    return parser.parse_args()

def main():
    """Main entry point."""
    args = parse_arguments()
    scheduler = Scheduler()

    if args.command == 'start':
        # Override settings from command line
        if args.daemon:
            scheduler.daemon_mode = True
        if args.interval:
            scheduler.interval = args.interval

        scheduler.start()
    elif args.command == 'stop':
        scheduler.stop()
    elif args.command == 'status':
        status = scheduler.status()
        if args.json:
            print(json.dumps(status, indent=2))
        else:
            print("\nScheduler Status")
            print("===============")
            print(f"Status: {status['status']}")
            print(f"PID: {status.get('pid', 'N/A')}")
            print(f"Enabled: {status.get('scheduler_enabled', 'N/A')}")
            print(f"Interval: {status.get('interval', 'N/A')} seconds")
            print(f"Time window: {status.get('time_window', 'All day')}")
            print(f"Days: {status.get('days', 'All days')}")

            if 'last_run' in status and status['last_run']:
                print(f"Last run: {status['last_run']}")
            if 'next_run' in status and status['next_run']:
                print(f"Next run: {status['next_run']}")

            print(f"Runs completed: {status.get('runs_completed', 0)}")
            print(f"Runs failed: {status.get('runs_failed', 0)}")
            print(f"Last status: {status.get('last_status', 'N/A')}")
    else:
        print("No command specified. Use start, stop, or status.")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
