#!/usr/bin/env python3
"""
Integration Test for Full Sync Workflow

Tests the complete synchronization workflow from Gitea to Kimai.
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from core.sync import EnhancedGiteeKimaiSync

class TestFullSyncWorkflow(unittest.TestCase):
    """Integration tests for the complete sync workflow."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_sync.db")
        
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'GITEA_URL': 'https://gitea.test.com',
            'GITEA_TOKEN': 'test_gitea_token',
            'GITEA_ORGANIZATION': 'test-org',
            'KIMAI_URL': 'https://kimai.test.com',
            'KIMAI_USERNAME': 'test_user',
            'KIMAI_PASSWORD': 'test_password',
            'DATABASE_PATH': self.db_path,
            'LOG_LEVEL': 'ERROR'
        })
        self.env_patcher.start()
        
        self.sync = EnhancedGiteeKimaiSync()
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.env_patcher.stop()
        
        # Clean up database file
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        
        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)
    
    @patch('core.sync.requests.Session')
    def test_complete_sync_workflow(self, mock_session_class):
        """Test the complete sync workflow."""
        # Mock session
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        
        # Mock Gitea API responses
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.side_effect = [
            # Gitea version check
            {'version': '1.0.0'},
            # Repository list
            [{'name': 'test-repo', 'full_name': 'test-org/test-repo'}],
            # Issues list
            [
                {
                    'id': 1,
                    'title': 'Test Issue 1',
                    'state': 'open',
                    'body': 'Test issue description',
                    'created_at': '2023-01-01T00:00:00Z',
                    'updated_at': '2023-01-01T00:00:00Z'
                },
                {
                    'id': 2,
                    'title': 'Test Issue 2',
                    'state': 'closed',
                    'body': 'Another test issue',
                    'created_at': '2023-01-02T00:00:00Z',
                    'updated_at': '2023-01-02T00:00:00Z'
                }
            ]
        ]
        
        # Mock Kimai API responses
        mock_session.post.return_value.status_code = 200
        mock_session.post.return_value.json.side_effect = [
            # Kimai authentication
            {'token': 'test_kimai_token'},
            # Project creation
            {'id': 1, 'name': 'test-repo'},
            # Activity creation
            {'id': 1, 'name': 'Test Issue 1'},
            {'id': 2, 'name': 'Test Issue 2'}
        ]
        
        # Run sync
        self.sync.repos_to_sync = ['test-repo']
        self.sync.read_only = False
        
        # Mock database operations
        with patch('core.sync.sqlite3.connect') as mock_db_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_db_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchone.return_value = None  # No existing sync records
            
            # Run the sync
            self.sync.run()
            
            # Verify database operations
            mock_cursor.execute.assert_called()
            mock_conn.commit.assert_called()
    
    @patch('core.sync.requests.Session')
    def test_sync_with_existing_activities(self, mock_session_class):
        """Test sync when activities already exist."""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        
        # Mock API responses
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.side_effect = [
            {'version': '1.0.0'},
            [{'name': 'test-repo', 'full_name': 'test-org/test-repo'}],
            [{'id': 1, 'title': 'Updated Issue', 'state': 'open'}]
        ]
        
        mock_session.post.return_value.status_code = 200
        mock_session.post.return_value.json.side_effect = [
            {'token': 'test_kimai_token'},
            {'id': 1, 'name': 'test-repo'}
        ]
        
        mock_session.patch.return_value.status_code = 200
        mock_session.patch.return_value.json.return_value = {'id': 1, 'name': 'Updated Issue'}
        
        # Mock existing sync record
        with patch('core.sync.sqlite3.connect') as mock_db_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_db_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchone.return_value = (1, 1, 1, 'test-repo')  # Existing record
            
            self.sync.repos_to_sync = ['test-repo']
            self.sync.run()
            
            # Verify update was called
            mock_session.patch.assert_called()
    
    @patch('core.sync.requests.Session')
    def test_sync_error_handling(self, mock_session_class):
        """Test error handling during sync."""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        
        # Mock API error
        mock_session.get.return_value.status_code = 401
        mock_session.get.return_value.raise_for_status.side_effect = Exception("Unauthorized")
        
        self.sync.repos_to_sync = ['test-repo']
        
        # Should handle error gracefully
        with self.assertRaises(Exception):
            self.sync.run()
    
    @patch('core.sync.requests.Session')
    def test_dry_run_mode(self, mock_session_class):
        """Test sync in dry-run mode."""
        mock_session = Mock()
        mock_session_class.return_value = mock_session
        
        # Mock API responses
        mock_session.get.return_value.status_code = 200
        mock_session.get.return_value.json.side_effect = [
            {'version': '1.0.0'},
            [{'name': 'test-repo', 'full_name': 'test-org/test-repo'}],
            [{'id': 1, 'title': 'Test Issue', 'state': 'open'}]
        ]
        
        self.sync.repos_to_sync = ['test-repo']
        self.sync.read_only = True  # Dry run mode
        
        # Mock database operations
        with patch('core.sync.sqlite3.connect') as mock_db_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_db_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.fetchone.return_value = None
            
            # Run dry run
            self.sync.run()
            
            # Verify no Kimai API calls were made
            mock_session.post.assert_not_called()
    
    def test_configuration_validation(self):
        """Test configuration validation."""
        # Test with valid config
        self.sync.validate_config()
        
        # Test with missing config
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(ValueError):
                self.sync.validate_config()

if __name__ == '__main__':
    unittest.main()
