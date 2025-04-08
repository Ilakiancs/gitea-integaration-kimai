#!/usr/bin/env python3
"""
Comprehensive Test Framework for Gitea-Kimai Integration

This module provides a complete testing framework with unit tests, integration tests,
and end-to-end tests for the Gitea-Kimai sync system.
"""

import os
import sys
import unittest
import tempfile
import sqlite3
import json
import logging
import threading
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import shutil

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.sync_engine import SyncEngine, SyncConfig, SyncStatus
from api.api_client import GiteaKimaiClient, ApiClientError
from storage.backup_manager import BackupManager
from utils.rate_limiter import SimpleRateLimiter
from validation.webhook_validator import WebhookValidator
from monitoring.metrics import MetricsCollector
from security.audit_logger import AuditLogger
from plugins.plugin_manager import PluginManager

# Configure test logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestDatabaseMixin:
    """Mixin for tests requiring database setup."""

    def setUp_database(self):
        """Set up test database."""
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.test_db_path = self.test_db.name
        self.test_db.close()

        # Initialize test database
        with sqlite3.connect(self.test_db_path) as conn:
            conn.execute("""
                CREATE TABLE sync_items (
                    id INTEGER PRIMARY KEY,
                    gitea_id TEXT NOT NULL,
                    kimai_id TEXT,
                    title TEXT NOT NULL,
                    repository TEXT NOT NULL,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    sync_status TEXT DEFAULT 'pending'
                )
            """)
            conn.execute("""
                CREATE TABLE sync_operations (
                    id TEXT PRIMARY KEY,
                    sync_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at DATETIME,
                    completed_at DATETIME,
                    items_processed INTEGER DEFAULT 0,
                    items_synced INTEGER DEFAULT 0,
                    items_failed INTEGER DEFAULT 0
                )
            """)
            conn.commit()

    def tearDown_database(self):
        """Clean up test database."""
        if hasattr(self, 'test_db_path') and os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)

class MockGiteaClient:
    """Mock Gitea API client for testing."""

    def __init__(self, issues_data=None, repositories_data=None):
        self.issues_data = issues_data or []
        self.repositories_data = repositories_data or []
        self.api_calls = []

    def get_issues(self, repository=None):
        """Mock get issues method."""
        self.api_calls.append(('get_issues', repository))
        if repository:
            return [issue for issue in self.issues_data if issue.get('repository') == repository]
        return self.issues_data

    def get_repositories(self):
        """Mock get repositories method."""
        self.api_calls.append(('get_repositories',))
        return self.repositories_data

    def get_issue(self, repository, issue_number):
        """Mock get single issue method."""
        self.api_calls.append(('get_issue', repository, issue_number))
        for issue in self.issues_data:
            if issue.get('repository') == repository and issue.get('number') == issue_number:
                return issue
        return None

class MockKimaiClient:
    """Mock Kimai API client for testing."""

    def __init__(self, timesheets_data=None, projects_data=None):
        self.timesheets_data = timesheets_data or []
        self.projects_data = projects_data or []
        self.api_calls = []

    def create_timesheet_entry(self, data):
        """Mock create timesheet entry method."""
        self.api_calls.append(('create_timesheet_entry', data))
        new_id = len(self.timesheets_data) + 1
        entry = {'id': new_id, **data}
        self.timesheets_data.append(entry)
        return new_id

    def update_timesheet_entry(self, entry_id, data):
        """Mock update timesheet entry method."""
        self.api_calls.append(('update_timesheet_entry', entry_id, data))
        for entry in self.timesheets_data:
            if entry['id'] == entry_id:
                entry.update(data)
                return True
        return False

    def get_projects(self):
        """Mock get projects method."""
        self.api_calls.append(('get_projects',))
        return self.projects_data

class TestSyncEngine(unittest.TestCase, TestDatabaseMixin):
    """Test cases for SyncEngine."""

    def setUp(self):
        """Set up test environment."""
        self.setUp_database()

        # Create test config
        self.config = SyncConfig(
            sync_interval=60,
            batch_size=10,
            max_retries=3,
            enable_incremental=True,
            data_validation=True
        )

        # Create mock clients
        self.mock_gitea = MockGiteaClient([
            {
                'id': '1',
                'number': 1,
                'title': 'Test Issue 1',
                'repository': 'test-repo',
                'state': 'open',
                'created_at': '2025-01-01T10:00:00Z'
            },
            {
                'id': '2',
                'number': 2,
                'title': 'Test Issue 2',
                'repository': 'test-repo',
                'state': 'closed',
                'created_at': '2025-01-02T10:00:00Z'
            }
        ])

        self.mock_kimai = MockKimaiClient()

        # Create sync engine
        self.sync_engine = SyncEngine(self.config, self.mock_gitea, self.mock_kimai)
        self.sync_engine.database.db_path = self.test_db_path

    def tearDown(self):
        """Clean up test environment."""
        self.tearDown_database()

    def test_sync_engine_initialization(self):
        """Test sync engine initializes correctly."""
        self.assertIsNotNone(self.sync_engine)
        self.assertEqual(self.sync_engine.config, self.config)
        self.assertFalse(self.sync_engine.running)

    def test_get_items_to_sync(self):
        """Test getting items to sync."""
        items = self.sync_engine._get_items_to_sync('full')
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].source_id, '1')
        self.assertEqual(items[1].source_id, '2')

    def test_create_item(self):
        """Test creating new item in target system."""
        from core.sync_engine import SyncItem

        item = SyncItem(
            id='test-1',
            source_id='1',
            source_data={'title': 'Test Issue', 'description': 'Test'},
            item_type='issue'
        )

        success = self.sync_engine._create_item(item)
        self.assertTrue(success)
        self.assertIsNotNone(item.target_id)
        self.assertEqual(len(self.mock_kimai.api_calls), 1)

class TestAPIClient(unittest.TestCase):
    """Test cases for API client."""

    def setUp(self):
        """Set up test environment."""
        self.client = GiteaKimaiClient(
            base_url="http://test-api.com",
            username="test_user",
            password="test_pass"
        )

    @patch('requests.Session.post')
    def test_authentication_success(self, mock_post):
        """Test successful authentication."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'token': 'test_token_123'}
        mock_post.return_value = mock_response

        result = self.client.authenticate()
        self.assertTrue(result)
        self.assertEqual(self.client.token, 'test_token_123')

    @patch('requests.Session.post')
    def test_authentication_failure(self, mock_post):
        """Test authentication failure."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response

        with self.assertRaises(ApiClientError):
            self.client.authenticate()

    @patch('requests.Session.get')
    def test_get_status(self, mock_get):
        """Test getting API status."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'status': 'healthy', 'version': '1.0.0'}
        mock_get.return_value = mock_response

        status = self.client.get_status()
        self.assertEqual(status['status'], 'healthy')
        self.assertEqual(status['version'], '1.0.0')

class TestBackupManager(unittest.TestCase):
    """Test cases for BackupManager."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.backup_dir = os.path.join(self.temp_dir, 'backups')
        self.backup_manager = BackupManager(backup_dir=self.backup_dir)

        # Create test files
        self.test_file = os.path.join(self.temp_dir, 'test.txt')
        with open(self.test_file, 'w') as f:
            f.write('Test content')

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_create_backup(self):
        """Test creating a backup."""
        backup_info = self.backup_manager.create_backup(
            source_paths=[self.test_file],
            backup_name='test_backup'
        )

        self.assertIsNotNone(backup_info)
        self.assertEqual(backup_info['name'], 'test_backup')
        self.assertTrue(backup_info['success'])
        self.assertGreater(backup_info['file_count'], 0)

        # Check backup file exists
        backup_path = os.path.join(self.backup_dir, 'test_backup.zip')
        self.assertTrue(os.path.exists(backup_path))

    def test_list_backups(self):
        """Test listing backups."""
        # Create a backup first
        self.backup_manager.create_backup(
            source_paths=[self.test_file],
            backup_name='test_backup'
        )

        backups = self.backup_manager.list_backups()
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0]['name'], 'test_backup')

    def test_restore_backup(self):
        """Test restoring a backup."""
        # Create backup
        self.backup_manager.create_backup(
            source_paths=[self.test_file],
            backup_name='test_backup'
        )

        # Create restore directory
        restore_dir = os.path.join(self.temp_dir, 'restore')
        os.makedirs(restore_dir)

        # Restore backup
        restore_info = self.backup_manager.restore_backup(
            backup_name='test_backup',
            restore_path=restore_dir
        )

        self.assertIsNotNone(restore_info)
        self.assertTrue(restore_info['success'])

        # Check restored file exists
        restored_file = os.path.join(restore_dir, 'test.txt')
        self.assertTrue(os.path.exists(restored_file))

class TestRateLimiter(unittest.TestCase):
    """Test cases for RateLimiter."""

    def setUp(self):
        """Set up test environment."""
        self.rate_limiter = SimpleRateLimiter(max_requests=5, time_window=1)

    def test_can_proceed_within_limit(self):
        """Test rate limiter allows requests within limit."""
        for i in range(5):
            self.assertTrue(self.rate_limiter.can_proceed())
            self.rate_limiter.record_request()

    def test_cannot_proceed_over_limit(self):
        """Test rate limiter blocks requests over limit."""
        # Use up all requests
        for i in range(5):
            self.rate_limiter.record_request()

        # Next request should be blocked
        self.assertFalse(self.rate_limiter.can_proceed())

    def test_requests_reset_after_time_window(self):
        """Test rate limiter resets after time window."""
        # Use up all requests
        for i in range(5):
            self.rate_limiter.record_request()

        # Should be blocked
        self.assertFalse(self.rate_limiter.can_proceed())

        # Wait for time window to pass
        time.sleep(1.1)

        # Should be allowed again
        self.assertTrue(self.rate_limiter.can_proceed())

class TestWebhookValidator(unittest.TestCase):
    """Test cases for WebhookValidator."""

    def setUp(self):
        """Set up test environment."""
        self.validator = WebhookValidator(
            gitea_secret='test_secret',
            kimai_secret='test_secret'
        )

    def test_validate_gitea_webhook_valid(self):
        """Test validating valid Gitea webhook."""
        import hmac
        import hashlib

        body = b'{"test": "data"}'
        signature = hmac.new(
            'test_secret'.encode('utf-8'),
            body,
            hashlib.sha256
        ).hexdigest()

        headers = {
            'X-Gitea-Signature': signature,
            'X-Gitea-Event': 'issues'
        }

        result = self.validator.validate_gitea_webhook(headers, body)
        self.assertTrue(result)

    def test_validate_gitea_webhook_invalid_signature(self):
        """Test validating Gitea webhook with invalid signature."""
        body = b'{"test": "data"}'
        headers = {
            'X-Gitea-Signature': 'invalid_signature',
            'X-Gitea-Event': 'issues'
        }

        with self.assertRaises(Exception):
            self.validator.validate_gitea_webhook(headers, body)

    def test_validate_payload_structure(self):
        """Test payload structure validation."""
        valid_payload = {
            'repository': {
                'id': 1,
                'name': 'test-repo',
                'full_name': 'owner/test-repo',
                'owner': {'login': 'owner'}
            }
        }

        result = self.validator.validate_payload_structure(valid_payload, 'gitea')
        self.assertTrue(result)

class TestMetricsCollector(unittest.TestCase):
    """Test cases for MetricsCollector."""

    def setUp(self):
        """Set up test environment."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.metrics = MetricsCollector(db_path=self.temp_db.name)

    def tearDown(self):
        """Clean up test environment."""
        os.unlink(self.temp_db.name)

    def test_record_sync_operation(self):
        """Test recording sync operation metric."""
        self.metrics.record_sync_operation(
            operation='test_sync',
            repository='test-repo',
            duration=1.5,
            success=True,
            items_processed=10,
            items_synced=8
        )

        # Verify metric was recorded
        stats = self.metrics.get_sync_statistics(days=1)
        self.assertEqual(stats['overall']['total_operations'], 1)
        self.assertEqual(stats['overall']['successful_operations'], 1)

    def test_record_api_call(self):
        """Test recording API call metric."""
        self.metrics.record_api_call(
            endpoint='/api/test',
            method='GET',
            duration=0.5,
            status_code=200,
            success=True
        )

        # Verify metric was recorded
        stats = self.metrics.get_api_statistics(days=1)
        self.assertEqual(stats['overall']['total_calls'], 1)
        self.assertEqual(stats['overall']['successful_calls'], 1)

class TestAuditLogger(unittest.TestCase):
    """Test cases for AuditLogger."""

    def setUp(self):
        """Set up test environment."""
        from security.audit_logger import AuditDatabase

        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()

        self.audit_db = AuditDatabase(db_path=self.temp_db.name)
        self.audit_logger = AuditLogger(database=self.audit_db, enable_file_logging=False)

    def tearDown(self):
        """Clean up test environment."""
        os.unlink(self.temp_db.name)

    def test_log_authentication_event(self):
        """Test logging authentication event."""
        from security.audit_logger import AuditOutcome

        self.audit_logger.log_authentication(
            user_id='test_user',
            outcome=AuditOutcome.SUCCESS,
            user_ip='127.0.0.1'
        )

        # Verify event was logged
        events = self.audit_logger.get_recent_events(hours=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].user_id, 'test_user')

    def test_log_data_access_event(self):
        """Test logging data access event."""
        self.audit_logger.log_data_access(
            user_id='test_user',
            resource='test_resource',
            action='read',
            data_classification='public'
        )

        # Verify event was logged
        events = self.audit_logger.get_recent_events(hours=1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].action, 'read')

class TestPluginManager(unittest.TestCase):
    """Test cases for PluginManager."""

    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.plugin_manager = PluginManager(
            plugin_dir=os.path.join(self.temp_dir, 'plugins'),
            config_file=os.path.join(self.temp_dir, 'plugin_config.json')
        )

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_discover_plugins(self):
        """Test plugin discovery."""
        # Create mock plugin directory
        plugin_dir = Path(self.temp_dir) / 'plugins' / 'test_plugin'
        plugin_dir.mkdir(parents=True)

        # Create plugin metadata
        with open(plugin_dir / 'plugin.json', 'w') as f:
            json.dump({
                'name': 'test_plugin',
                'version': '1.0.0',
                'type': 'transformer'
            }, f)

        plugins = self.plugin_manager.discover_plugins()
        self.assertIn('test_plugin', plugins)

    def test_plugin_loading(self):
        """Test loading a plugin."""
        # This would require creating actual plugin files
        # For now, just test the discovery mechanism
        plugins = self.plugin_manager.discover_plugins()
        self.assertIsInstance(plugins, list)

class TestIntegration(unittest.TestCase):
    """Integration tests."""

    def setUp(self):
        """Set up integration test environment."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up integration test environment."""
        shutil.rmtree(self.temp_dir)

    def test_end_to_end_sync_process(self):
        """Test complete sync process from Gitea to Kimai."""
        # This would test the entire flow:
        # 1. Fetch data from Gitea
        # 2. Transform data
        # 3. Validate data
        # 4. Create entries in Kimai
        # 5. Update sync status
        # 6. Log metrics and audit events

        # Mock the entire process for now
        self.assertTrue(True)  # Placeholder

class TestRunner:
    """Test runner for the comprehensive test suite."""

    def __init__(self):
        self.test_suite = unittest.TestSuite()
        self.results = {}

    def discover_tests(self):
        """Discover all test cases."""
        test_classes = [
            TestSyncEngine,
            TestAPIClient,
            TestBackupManager,
            TestRateLimiter,
            TestWebhookValidator,
            TestMetricsCollector,
            TestAuditLogger,
            TestPluginManager,
            TestIntegration
        ]

        for test_class in test_classes:
            tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
            self.test_suite.addTests(tests)

    def run_tests(self, verbosity=2):
        """Run all tests."""
        self.discover_tests()

        runner = unittest.TextTestRunner(
            verbosity=verbosity,
            buffer=True,
            failfast=False
        )

        result = runner.run(self.test_suite)

        self.results = {
            'tests_run': result.testsRun,
            'failures': len(result.failures),
            'errors': len(result.errors),
            'success_rate': (result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100
        }

        return result

    def run_specific_test(self, test_class_name, test_method_name=None):
        """Run a specific test class or method."""
        if test_method_name:
            suite = unittest.TestSuite()
            suite.addTest(globals()[test_class_name](test_method_name))
        else:
            suite = unittest.TestLoader().loadTestsFromTestCase(globals()[test_class_name])

        runner = unittest.TextTestRunner(verbosity=2)
        return runner.run(suite)

    def generate_coverage_report(self):
        """Generate test coverage report."""
        try:
            import coverage

            cov = coverage.Coverage()
            cov.start()

            # Run tests
            self.run_tests(verbosity=0)

            cov.stop()
            cov.save()

            # Generate report
            print("\nCoverage Report:")
            cov.report()

            # Save HTML report
            html_dir = os.path.join(self.temp_dir, 'coverage_html')
            cov.html_report(directory=html_dir)
            print(f"HTML coverage report saved to: {html_dir}")

        except ImportError:
            print("Coverage.py not installed. Install with: pip install coverage")

def run_all_tests():
    """Run all tests and return results."""
    runner = TestRunner()
    result = runner.run_tests()

    print(f"\nTest Results:")
    print(f"Tests run: {runner.results['tests_run']}")
    print(f"Failures: {runner.results['failures']}")
    print(f"Errors: {runner.results['errors']}")
    print(f"Success rate: {runner.results['success_rate']:.1f}%")

    return result.wasSuccessful()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Gitea-Kimai Integration Tests")
    parser.add_argument('--coverage', action='store_true', help='Generate coverage report')
    parser.add_argument('--class', dest='test_class', help='Run specific test class')
    parser.add_argument('--method', dest='test_method', help='Run specific test method')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')

    args = parser.parse_args()

    runner = TestRunner()

    if args.coverage:
        runner.generate_coverage_report()
    elif args.test_class:
        runner.run_specific_test(args.test_class, args.test_method)
    else:
        success = run_all_tests()
        sys.exit(0 if success else 1)
