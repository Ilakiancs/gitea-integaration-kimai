#!/usr/bin/env python3
"""
Unit Tests for Sync Module

Tests for the core synchronization functionality.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from core.sync import EnhancedGiteeKimaiSync

class TestEnhancedGiteeKimaiSync(unittest.TestCase):
    """Test cases for EnhancedGiteeKimaiSync class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sync = EnhancedGiteeKimaiSync()
    
    def test_initialization(self):
        """Test sync object initialization."""
        self.assertIsNotNone(self.sync)
        self.assertTrue(hasattr(self.sync, 'read_only'))
        self.assertTrue(hasattr(self.sync, 'project_mapping'))
    
    def test_validate_config(self):
        """Test configuration validation."""
        # Test with valid config
        with patch.dict('os.environ', {
            'GITEA_URL': 'https://gitea.example.com',
            'GITEA_TOKEN': 'test_token',
            'KIMAI_URL': 'https://kimai.example.com',
            'KIMAI_USERNAME': 'test_user',
            'KIMAI_PASSWORD': 'test_pass'
        }):
            # Should not raise exception
            self.sync.validate_config()
    
    def test_validate_config_missing_vars(self):
        """Test configuration validation with missing variables."""
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(ValueError):
                self.sync.validate_config()
    
    @patch('core.sync.sqlite3.connect')
    def test_setup_database(self, mock_connect):
        """Test database setup."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        self.sync.setup_database()
        
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('core.sync.requests.Session')
    def test_test_gitea_connection(self, mock_session):
        """Test Gitea connection testing."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.get.return_value.status_code = 200
        mock_session_instance.get.return_value.json.return_value = {'version': '1.0.0'}
        
        result = self.sync.test_gitea_connection()
        self.assertTrue(result)
    
    @patch('core.sync.requests.Session')
    def test_test_kimai_connection(self, mock_session):
        """Test Kimai connection testing."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.post.return_value.status_code = 200
        mock_session_instance.post.return_value.json.return_value = {'token': 'test_token'}
        
        result = self.sync.test_kimai_connection()
        self.assertTrue(result)
    
    def test_get_gitea_issues(self):
        """Test getting Gitea issues."""
        # Mock the session and response
        mock_response = Mock()
        mock_response.json.return_value = [
            {'id': 1, 'title': 'Test Issue', 'state': 'open'},
            {'id': 2, 'title': 'Another Issue', 'state': 'closed'}
        ]
        
        self.sync.session = Mock()
        self.sync.session.get.return_value = mock_response
        
        issues = self.sync.get_gitea_issues('test-repo')
        
        self.assertEqual(len(issues), 2)
        self.assertEqual(issues[0]['title'], 'Test Issue')
    
    def test_create_kimai_project(self):
        """Test creating Kimai project."""
        mock_response = Mock()
        mock_response.json.return_value = {'id': 1, 'name': 'Test Project'}
        
        self.sync.kimai_session = Mock()
        self.sync.kimai_session.post.return_value = mock_response
        
        project_id = self.sync.create_kimai_project('Test Project')
        
        self.assertEqual(project_id, 1)
    
    def test_create_kimai_activity(self):
        """Test creating Kimai activity."""
        mock_response = Mock()
        mock_response.json.return_value = {'id': 1, 'name': 'Test Activity'}
        
        self.sync.kimai_session = Mock()
        self.sync.kimai_session.post.return_value = mock_response
        
        activity_id = self.sync.create_kimai_activity('Test Activity', 1)
        
        self.assertEqual(activity_id, 1)
    
    def test_update_kimai_activity(self):
        """Test updating Kimai activity."""
        mock_response = Mock()
        mock_response.json.return_value = {'id': 1, 'name': 'Updated Activity'}
        
        self.sync.kimai_session = Mock()
        self.sync.kimai_session.patch.return_value = mock_response
        
        result = self.sync.update_kimai_activity(1, 'Updated Activity')
        
        self.assertTrue(result)
    
    def test_log_sync_activity(self):
        """Test logging sync activity."""
        with patch('core.sync.sqlite3.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            self.sync.log_sync_activity(1, 1, 1, 'test-repo')
            
            mock_cursor.execute.assert_called()
            mock_conn.commit.assert_called_once()
    
    def test_get_synced_activity(self):
        """Test getting synced activity."""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1, 1, 1, 'test-repo')
        
        with patch('core.sync.sqlite3.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            result = self.sync.get_synced_activity(1, 'test-repo')
            
            self.assertIsNotNone(result)
            self.assertEqual(result[0], 1)
    
    def test_rate_limiting(self):
        """Test rate limiting functionality."""
        with patch('time.sleep') as mock_sleep:
            self.sync.rate_limit()
            mock_sleep.assert_called_once()
    
    def test_export_sync_data(self):
        """Test exporting sync data."""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            (1, 1, 1, 'test-repo', '2023-01-01', '2023-01-01'),
            (2, 2, 1, 'test-repo', '2023-01-02', '2023-01-02')
        ]
        
        with patch('core.sync.sqlite3.connect') as mock_connect:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            with patch('builtins.open', create=True) as mock_open:
                mock_file = Mock()
                mock_open.return_value.__enter__.return_value = mock_file
                
                self.sync.export_sync_data('test.csv')
                
                mock_file.write.assert_called()

if __name__ == '__main__':
    unittest.main()
