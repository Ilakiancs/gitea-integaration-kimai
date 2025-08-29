#!/usr/bin/env python3
"""
Statistics Module for Gitea-Kimai Integration

This module provides statistical analysis of synchronization data,
generating reports and insights about synchronization patterns and performance.

Usage:
  python statistics.py
  python statistics.py --report daily
  python statistics.py --visualize
"""

import os
import sys
import json
import sqlite3
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from pathlib import Path
import csv

# Try to import optional visualization dependencies
try:
    import matplotlib.pyplot as plt
    import numpy as np
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('statistics.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_PATH = os.getenv('DATABASE_PATH', 'sync.db')
EXPORT_DIR = os.getenv('EXPORT_DIR', 'exports')

class SyncStatistics:
    """Analyze synchronization statistics from the database."""

    def __init__(self, database_path=DATABASE_PATH):
        """Initialize with database path."""
        self.database_path = database_path

        # Ensure the export directory exists
        if not os.path.exists(EXPORT_DIR):
            os.makedirs(EXPORT_DIR)

    def connect_db(self):
        """Connect to the SQLite database."""
        if not os.path.exists(self.database_path):
            logger.error(f"Database file not found: {self.database_path}")
            return None

        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return None

    def get_basic_stats(self) -> Dict[str, Any]:
        """Get basic synchronization statistics."""
        conn = self.connect_db()
        if not conn:
            return {}

        try:
            cursor = conn.cursor()

            # Total records
            cursor.execute("SELECT COUNT(*) as count FROM activity_sync")
            total_count = cursor.fetchone()['count']

            # Records by state
            cursor.execute("""
                SELECT issue_state, COUNT(*) as count
                FROM activity_sync
                GROUP BY issue_state
            """)
            states = {row['issue_state']: row['count'] for row in cursor.fetchall()}

            # Records by repository
            cursor.execute("""
                SELECT repository_name, COUNT(*) as count
                FROM activity_sync
                GROUP BY repository_name
                ORDER BY count DESC
            """)
            repositories = {row['repository_name']: row['count'] for row in cursor.fetchall()}

            # First and last sync
            cursor.execute("SELECT MIN(created_at) as first FROM activity_sync")
            first_sync = cursor.fetchone()['first']

            cursor.execute("SELECT MAX(updated_at) as last FROM activity_sync")
            last_sync = cursor.fetchone()['last']

            stats = {
                'total_records': total_count,
                'states': states,
                'repositories': repositories,
                'first_sync': first_sync,
                'last_sync': last_sync
            }

            return stats
        except sqlite3.Error as e:
            logger.error(f"Error getting basic stats: {e}")
            return {}
        finally:
            conn.close()

    def get_daily_stats(self, days=30) -> List[Dict[str, Any]]:
        """Get daily synchronization statistics for the past N days."""
        conn = self.connect_db()
        if not conn:
            return []

        try:
            cursor = conn.cursor()

            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Get daily stats
            cursor.execute("""
                SELECT
                    DATE(created_at) as date,
                    COUNT(*) as new_items,
                    SUM(CASE WHEN issue_state = 'open' THEN 1 ELSE 0 END) as open_items,
                    SUM(CASE WHEN issue_state = 'closed' THEN 1 ELSE 0 END) as closed_items,
                    COUNT(DISTINCT repository_name) as repos_synced
                FROM activity_sync
                WHERE created_at >= ?
                GROUP BY DATE(created_at)
                ORDER BY date ASC
            """, (start_date.isoformat(),))

            daily_stats = [dict(row) for row in cursor.fetchall()]

            # Fill in missing days
            date_dict = {row['date']: row for row in daily_stats}

            current_date = start_date
            complete_stats = []

            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                if date_str in date_dict:
                    complete_stats.append(date_dict[date_str])
                else:
                    complete_stats.append({
                        'date': date_str,
                        'new_items': 0,
                        'open_items': 0,
                        'closed_items': 0,
                        'repos_synced': 0
                    })
                current_date += timedelta(days=1)

            return complete_stats
        except sqlite3.Error as e:
            logger.error(f"Error getting daily stats: {e}")
            return []
        finally:
            conn.close()

    def get_repository_activity(self) -> List[Dict[str, Any]]:
        """Get activity statistics by repository."""
        conn = self.connect_db()
        if not conn:
            return []

        try:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    repository_name,
                    COUNT(*) as total_items,
                    SUM(CASE WHEN issue_state = 'open' THEN 1 ELSE 0 END) as open_items,
                    SUM(CASE WHEN issue_state = 'closed' THEN 1 ELSE 0 END) as closed_items,
                    MIN(created_at) as first_sync,
                    MAX(updated_at) as last_sync
                FROM activity_sync
                GROUP BY repository_name
                ORDER BY total_items DESC
            """)

            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting repository activity: {e}")
            return []
        finally:
            conn.close()

    def get_sync_efficiency(self) -> Dict[str, Any]:
        """Calculate synchronization efficiency metrics."""
        conn = self.connect_db()
        if not conn:
            return {}

        try:
            cursor = conn.cursor()

            # Get unique repositories count
            cursor.execute("SELECT COUNT(DISTINCT repository_name) as repo_count FROM activity_sync")
            repo_count = cursor.fetchone()['repo_count']

            # Get unique projects count
            cursor.execute("SELECT COUNT(DISTINCT kimai_project_id) as project_count FROM activity_sync")
            project_count = cursor.fetchone()['project_count']

            # Get average activities per repository
            cursor.execute("""
                SELECT repository_name, COUNT(*) as activity_count
                FROM activity_sync
                GROUP BY repository_name
            """)
            repo_activities = [row['activity_count'] for row in cursor.fetchall()]
            avg_activities_per_repo = sum(repo_activities) / len(repo_activities) if repo_activities else 0

            # Calculate sync frequency
            cursor.execute("""
                SELECT
                    COUNT(DISTINCT DATE(updated_at)) as unique_days,
                    MIN(updated_at) as first_update,
                    MAX(updated_at) as last_update
                FROM activity_sync
            """)
            row = cursor.fetchone()
            unique_days = row['unique_days']

            if row['first_update'] and row['last_update']:
                first_date = datetime.fromisoformat(row['first_update'].replace('Z', '+00:00'))
                last_date = datetime.fromisoformat(row['last_update'].replace('Z', '+00:00'))
                date_diff = (last_date - first_date).days + 1
                sync_frequency = unique_days / date_diff if date_diff > 0 else 0
            else:
                sync_frequency = 0

            return {
                'repository_count': repo_count,
                'project_count': project_count,
                'avg_activities_per_repo': avg_activities_per_repo,
                'unique_sync_days': unique_days,
                'sync_frequency': sync_frequency  # as a ratio of days with syncs to total days
            }
        except sqlite3.Error as e:
            logger.error(f"Error calculating sync efficiency: {e}")
            return {}
        finally:
            conn.close()

    def export_csv_report(self, report_type='basic') -> str:
        """
        Export statistics to a CSV file.

        Args:
            report_type: Type of report to generate ('basic', 'daily', 'repository')

        Returns:
            Path to the generated CSV file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{report_type}_stats_{timestamp}.csv"
        filepath = os.path.join(EXPORT_DIR, filename)

        try:
            if report_type == 'basic':
                stats = self.get_basic_stats()

                with open(filepath, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['Metric', 'Value'])
                    writer.writerow(['Total Records', stats.get('total_records', 0)])
                    writer.writerow(['First Sync', stats.get('first_sync', 'N/A')])
                    writer.writerow(['Last Sync', stats.get('last_sync', 'N/A')])

                    writer.writerow([])
                    writer.writerow(['Issue State', 'Count'])
                    for state, count in stats.get('states', {}).items():
                        writer.writerow([state, count])

                    writer.writerow([])
                    writer.writerow(['Repository', 'Count'])
                    for repo, count in stats.get('repositories', {}).items():
                        writer.writerow([repo, count])

            elif report_type == 'daily':
                daily_stats = self.get_daily_stats()

                with open(filepath, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['Date', 'New Items', 'Open Items', 'Closed Items', 'Repositories Synced'])

                    for day in daily_stats:
                        writer.writerow([
                            day.get('date', ''),
                            day.get('new_items', 0),
                            day.get('open_items', 0),
                            day.get('closed_items', 0),
                            day.get('repos_synced', 0)
                        ])

            elif report_type == 'repository':
                repo_stats = self.get_repository_activity()

                with open(filepath, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['Repository', 'Total Items', 'Open Items', 'Closed Items', 'First Sync', 'Last Sync'])

                    for repo in repo_stats:
                        writer.writerow([
                            repo.get('repository_name', ''),
                            repo.get('total_items', 0),
                            repo.get('open_items', 0),
                            repo.get('closed_items', 0),
                            repo.get('first_sync', ''),
                            repo.get('last_sync', '')
                        ])

            logger.info(f"Exported {report_type} statistics to {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error exporting {report_type} statistics: {e}")
            return ""

    def visualize_statistics(self, output_dir=None) -> List[str]:
        """
        Create visualizations of the statistics.

        Args:
            output_dir: Directory to save visualizations (defaults to export dir)

        Returns:
            List of paths to the generated visualization files
        """
        if not VISUALIZATION_AVAILABLE:
            logger.warning("Visualization requires matplotlib and numpy. Install with: pip install matplotlib numpy")
            return []

        if output_dir is None:
            output_dir = EXPORT_DIR

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        generated_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        try:
            # 1. Repository Activity Pie Chart
            repo_stats = self.get_repository_activity()
            if repo_stats:
                plt.figure(figsize=(10, 8))
                repos = [r['repository_name'] for r in repo_stats]
                counts = [r['total_items'] for r in repo_stats]

                # If there are too many repositories, show only top 10
                if len(repos) > 10:
                    other_count = sum(counts[9:])
                    repos = repos[:9] + ['Others']
                    counts = counts[:9] + [other_count]

                plt.pie(counts, labels=repos, autopct='%1.1f%%', startangle=90)
                plt.title('Activity Distribution by Repository')

                filename = f"repo_distribution_{timestamp}.png"
                filepath = os.path.join(output_dir, filename)
                plt.savefig(filepath)
                plt.close()
                generated_files.append(filepath)

            # 2. Open vs Closed Issues
            basic_stats = self.get_basic_stats()
            if basic_stats and 'states' in basic_stats:
                plt.figure(figsize=(8, 6))
                states = basic_stats['states']
                labels = list(states.keys())
                values = list(states.values())

                plt.bar(labels, values, color=['#2ecc71', '#e74c3c'])
                plt.title('Issues by State')
                plt.ylabel('Count')

                filename = f"issue_states_{timestamp}.png"
                filepath = os.path.join(output_dir, filename)
                plt.savefig(filepath)
                plt.close()
                generated_files.append(filepath)

            # 3. Daily Activity Line Chart
            daily_stats = self.get_daily_stats(days=30)
            if daily_stats:
                plt.figure(figsize=(12, 6))
                dates = [d['date'] for d in daily_stats]
                new_items = [d['new_items'] for d in daily_stats]

                plt.plot(dates, new_items, marker='o', linestyle='-', color='#3498db')
                plt.title('Daily Synchronization Activity (Last 30 Days)')
                plt.xlabel('Date')
                plt.ylabel('New Items Synced')
                plt.xticks(rotation=45)
                plt.tight_layout()

                filename = f"daily_activity_{timestamp}.png"
                filepath = os.path.join(output_dir, filename)
                plt.savefig(filepath)
                plt.close()
                generated_files.append(filepath)

            # 4. Repository Open/Closed Distribution
            if repo_stats:
                plt.figure(figsize=(12, 8))
                repos = [r['repository_name'] for r in repo_stats[:10]]  # Top 10 repos
                open_items = [r['open_items'] for r in repo_stats[:10]]
                closed_items = [r['closed_items'] for r in repo_stats[:10]]

                x = np.arange(len(repos))
                width = 0.35

                plt.bar(x - width/2, open_items, width, label='Open', color='#3498db')
                plt.bar(x + width/2, closed_items, width, label='Closed', color='#e74c3c')

                plt.xlabel('Repository')
                plt.ylabel('Count')
                plt.title('Open vs Closed Issues by Repository')
                plt.xticks(x, repos, rotation=45, ha='right')
                plt.legend()
                plt.tight_layout()

                filename = f"repo_open_closed_{timestamp}.png"
                filepath = os.path.join(output_dir, filename)
                plt.savefig(filepath)
                plt.close()
                generated_files.append(filepath)

            logger.info(f"Generated {len(generated_files)} visualization files")
            return generated_files

        except Exception as e:
            logger.error(f"Error creating visualizations: {e}")
            return generated_files

    def generate_summary_report(self) -> Dict[str, Any]:
        """Generate a comprehensive summary report with all statistics."""
        try:
            summary = {
                'timestamp': datetime.now().isoformat(),
                'basic_stats': self.get_basic_stats(),
                'daily_stats': self.get_daily_stats(days=7),  # Last week
                'repository_stats': self.get_repository_activity(),
                'efficiency_metrics': self.get_sync_efficiency()
            }

            # Save to JSON
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"summary_report_{timestamp}.json"
            filepath = os.path.join(EXPORT_DIR, filename)

            with open(filepath, 'w') as f:
                json.dump(summary, f, indent=2)

            logger.info(f"Generated summary report: {filepath}")
            summary['report_path'] = filepath
            return summary

        except Exception as e:
            logger.error(f"Error generating summary report: {e}")
            return {'error': str(e)}

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Statistics for Gitea-Kimai Integration")
    parser.add_argument('--database', help='Path to the SQLite database')
    parser.add_argument('--report', choices=['basic', 'daily', 'repository', 'all'],
                        help='Generate CSV report')
    parser.add_argument('--visualize', action='store_true', help='Generate visualizations')
    parser.add_argument('--summary', action='store_true', help='Generate comprehensive summary report')
    parser.add_argument('--output', help='Output directory for reports and visualizations')

    args = parser.parse_args()

    # Set up output directory
    output_dir = args.output if args.output else EXPORT_DIR
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Initialize statistics
    db_path = args.database if args.database else DATABASE_PATH
    stats = SyncStatistics(db_path)

    # Generate requested outputs
    if args.report:
        if args.report == 'all':
            stats.export_csv_report('basic')
            stats.export_csv_report('daily')
            stats.export_csv_report('repository')
        else:
            stats.export_csv_report(args.report)

    if args.visualize:
        stats.visualize_statistics(output_dir)

    if args.summary:
        summary = stats.generate_summary_report()
        print(json.dumps(summary, indent=2))

    # If no specific action requested, print basic stats
    if not (args.report or args.visualize or args.summary):
        basic_stats = stats.get_basic_stats()
        print("\nGitea-Kimai Sync Statistics")
        print("===========================")
        print(f"Total Records: {basic_stats.get('total_records', 0)}")
        print(f"First Sync: {basic_stats.get('first_sync', 'N/A')}")
        print(f"Last Sync: {basic_stats.get('last_sync', 'N/A')}")

        print("\nIssue States:")
        for state, count in basic_stats.get('states', {}).items():
            print(f"  {state}: {count}")

        print("\nTop Repositories:")
        for i, (repo, count) in enumerate(basic_stats.get('repositories', {}).items()):
            print(f"  {repo}: {count}")
            if i >= 4:  # Show only top 5
                remaining = len(basic_stats.get('repositories', {})) - 5
                if remaining > 0:
                    print(f"  ... and {remaining} more")
                break

        print("\nFor more detailed statistics, use --report, --visualize, or --summary options.")

if __name__ == "__main__":
    main()
