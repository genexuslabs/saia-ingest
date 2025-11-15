import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path

from gdrive.gdrive_reader import GoogleDriveReader


@pytest.fixture
def mock_service():
    """Create a mock Google Drive API service for testing"""
    service_mock = MagicMock()
    files_mock = MagicMock()
    service_mock.files.return_value = files_mock
    return service_mock


@pytest.fixture
def gdrive_reader():
    """Create a GoogleDriveReader instance for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        reader = GoogleDriveReader(
            folder_id="test_folder_id",
            download_dir=temp_dir,
            use_metadata_file=True,
            client_config={"installed": {"client_id": "test"}},
            authorized_user_info={"token": "test"},
        )
        # Mock the credentials to avoid actual auth
        reader._creds = MagicMock()
        yield reader


class TestGoogleDriveReader:
    """Test cases for Google Drive Reader functionality"""

    @patch('googleapiclient.discovery.build')
    def test_shortcut_detection_in_folder(self, mock_build, gdrive_reader, mock_service):
        """Test detection and resolution of shortcuts within a folder"""
        # Setup mock API responses
        mock_build.return_value = mock_service
        
        # Mock a folder containing a shortcut
        folder_items = {
            "files": [
                {
                    "id": "shortcut_123",
                    "name": "Document Shortcut",
                    "mimeType": "application/vnd.google-apps.shortcut",
                    "shortcutDetails": {"targetId": "target_456"},
                    "owners": [{"displayName": "Test User"}],
                    "modifiedTime": "2025-01-01T00:00:00.000Z",
                    "createdTime": "2025-01-01T00:00:00.000Z"
                }
            ],
            "nextPageToken": None
        }
        
        # Mock the target file that the shortcut points to
        target_file = {
            "id": "target_456",
            "name": "Target Document",
            "mimeType": "application/vnd.google-apps.document",
            "owners": [{"displayName": "Target Owner"}],
            "modifiedTime": "2025-01-01T00:00:00.000Z",
            "createdTime": "2025-01-01T00:00:00.000Z"
        }
        
        # Setup complex mocking for the API calls
        def get_mock_side_effect(*args, **kwargs):
            mock_response = MagicMock()
            # Return folder details
            if kwargs.get('fileId') == 'test_folder':
                mock_response.execute.return_value = {"name": "Test Folder"}
            # Return shortcut details when requesting them specifically 
            elif kwargs.get('fileId') == 'shortcut_123':
                if kwargs.get('fields') == 'shortcutDetails':
                    mock_response.execute.return_value = {"shortcutDetails": {"targetId": "target_456"}}
            # Return target file details
            elif kwargs.get('fileId') == 'target_456':
                mock_response.execute.return_value = target_file
            return mock_response
        
        # Setup list API call
        list_mock = mock_service.files.return_value.list
        list_mock.return_value.execute.return_value = folder_items
        
        # Setup get API call
        get_mock = mock_service.files.return_value.get
        get_mock.side_effect = get_mock_side_effect
        
        # Mock _get_relative_path to avoid additional API calls
        gdrive_reader._get_relative_path = MagicMock(return_value="test_folder/Document Shortcut")
        
        # Mock other dependencies to focus on the shortcut resolution logic
        gdrive_reader.save_metadata = MagicMock()
        gdrive_reader.is_greater_than_timestamp = MagicMock(return_value=True)
        gdrive_reader.exclude_ids = []
        
        # When _resolve_shortcut is called, return expected values directly
        # This isolates the test from implementation details of _resolve_shortcut
        # Also add the shortcut metadata to the target file
        target_file_with_shortcut = target_file.copy()
        target_file_with_shortcut["sourceShortcutId"] = "shortcut_123"
        target_file_with_shortcut["sourceShortcutName"] = "Document Shortcut"
        target_file_with_shortcut["sourceShortcutPath"] = "test_folder/Document Shortcut"
        
        gdrive_reader._resolve_shortcut = MagicMock(return_value=(
            "target_456",             # target ID
            target_file_with_shortcut, # target file metadata with shortcut info
            "Target Owner"            # target author
        ))
        
        # Call the method being tested
        result = gdrive_reader._get_fileids_meta(folder_id="test_folder")
        
        # Verify the results
        assert len(result) == 1, "Should have one file after resolving shortcut"
        assert result[0][0] == "target_456", "Should use target file ID"
        assert result[0][1] == "Target Owner", "Should use target file owner"
        assert result[0][3] == "application/vnd.google-apps.document", "Should use target MIME type"
        
        # Verify shortcut information was captured in metadata
        save_metadata_calls = gdrive_reader.save_metadata.call_args_list
        assert len(save_metadata_calls) == 1, "save_metadata should be called once"
        metadata = save_metadata_calls[0][0][0]
        assert "sourceShortcutId" in metadata, "Should include shortcut ID in metadata"
        assert "sourceShortcutName" in metadata, "Should include shortcut name in metadata"

    @patch('googleapiclient.discovery.build')
    def test_direct_shortcut_access(self, mock_build, gdrive_reader, mock_service):
        """Test direct access to a shortcut by ID"""
        # Setup mock API responses
        mock_build.return_value = mock_service
        
        # Mock a shortcut file
        shortcut_file = {
            "id": "direct_shortcut_123",
            "name": "Direct Shortcut",
            "mimeType": "application/vnd.google-apps.shortcut",
            "shortcutDetails": {"targetId": "direct_target_456"},
            "modifiedTime": "2025-01-01T00:00:00.000Z",
            "createdTime": "2025-01-01T00:00:00.000Z"
        }
        
        # Mock the target file
        target_file = {
            "id": "direct_target_456",
            "name": "Direct Target",
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "owners": [{"displayName": "Direct Owner"}],
            "modifiedTime": "2025-01-01T00:00:00.000Z",
            "createdTime": "2025-01-01T00:00:00.000Z"
        }
        
        # First, mock the initial file details fetch
        def get_mock_side_effect(*args, **kwargs):
            mock_response = MagicMock()
            if kwargs.get('fileId') == 'direct_shortcut_123':
                if kwargs.get('fields') == '*':
                    mock_response.execute.return_value = shortcut_file
                elif kwargs.get('fields') == 'shortcutDetails':
                    mock_response.execute.return_value = {"shortcutDetails": {"targetId": "direct_target_456"}}
            elif kwargs.get('fileId') == 'direct_target_456':
                mock_response.execute.return_value = target_file
            return mock_response
        
        get_mock = mock_service.files.return_value.get
        get_mock.side_effect = get_mock_side_effect
        
        # Mock save_metadata and _get_relative_path
        gdrive_reader.save_metadata = MagicMock()
        gdrive_reader._get_relative_path = MagicMock(return_value="path/to/shortcut")
        gdrive_reader.is_greater_than_timestamp = MagicMock(return_value=True)
        gdrive_reader.exclude_ids = []
        
        # Call the method with a direct shortcut ID
        result = gdrive_reader._get_fileids_meta(file_id="direct_shortcut_123")
        
        # Verify results
        assert len(result) == 1, "Should resolve to one target file"
        assert result[0][0] == "direct_target_456", "Should use target file ID"
        assert result[0][3] == "application/vnd.google-apps.spreadsheet", "Should use target MIME type"
        
        # Verify shortcut info in metadata
        save_metadata_calls = gdrive_reader.save_metadata.call_args_list
        assert len(save_metadata_calls) == 1, "save_metadata should be called once"
        metadata = save_metadata_calls[0][0][0]
        assert metadata["sourceShortcutId"] == "direct_shortcut_123"
        assert metadata["sourceShortcutName"] == "Direct Shortcut"

    @patch('googleapiclient.discovery.build')
    def test_broken_shortcut_handling(self, mock_build, gdrive_reader, mock_service):
        """Test graceful handling of broken shortcuts (target not accessible)"""
        # Setup mock API responses
        mock_build.return_value = mock_service
        
        # Mock a folder containing a shortcut
        folder_items = {
            "files": [
                {
                    "id": "broken_shortcut_123",
                    "name": "Broken Shortcut",
                    "mimeType": "application/vnd.google-apps.shortcut",
                    "shortcutDetails": {"targetId": "nonexistent_target"},
                    "owners": [{"displayName": "Test User"}],
                    "modifiedTime": "2025-01-01T00:00:00.000Z",
                    "createdTime": "2025-01-01T00:00:00.000Z"
                },
                {
                    "id": "regular_file_789",
                    "name": "Regular File",
                    "mimeType": "application/vnd.google-apps.document",
                    "owners": [{"displayName": "Test User"}],
                    "modifiedTime": "2025-01-01T00:00:00.000Z",
                    "createdTime": "2025-01-01T00:00:00.000Z"
                }
            ],
            "nextPageToken": None
        }
        
        # Setup mock API call responses
        list_mock = mock_service.files.return_value.list
        list_mock.return_value.execute.return_value = folder_items
        
        # Mock get to raise an exception for the broken target
        get_mock = mock_service.files.return_value.get
        get_mock.return_value.execute.side_effect = Exception("File not found or not accessible")
        
        # Mock save_metadata
        gdrive_reader.save_metadata = MagicMock()
        
        # Call the method
        result = gdrive_reader._get_fileids_meta(folder_id="test_folder")
        
        # Verify it skipped the broken shortcut and processed regular files
        assert len(result) == 1, "Should only include the regular file"
        assert result[0][0] == "regular_file_789", "Should include the regular file ID"
        
        # Verify save_metadata was only called for the regular file
        save_metadata_calls = gdrive_reader.save_metadata.call_args_list
        assert len(save_metadata_calls) == 1, "save_metadata should be called once"
        assert save_metadata_calls[0][0][0]["id"] == "regular_file_789"

    @patch('googleapiclient.discovery.build')
    def test_shortcut_metadata_in_saved_file(self, mock_build, gdrive_reader, mock_service):
        """Test that shortcut metadata is correctly stored in the output metadata"""
        # Setup mock API responses
        mock_build.return_value = mock_service
        
        # Mock a shortcut file
        shortcut_file = {
            "id": "metadata_shortcut_123",
            "name": "Metadata Shortcut",
            "mimeType": "application/vnd.google-apps.shortcut",
            "shortcutDetails": {"targetId": "metadata_target_456"},
            "modifiedTime": "2025-01-01T00:00:00.000Z",
            "createdTime": "2025-01-01T00:00:00.000Z"
        }
        
        # Mock target file
        target_file = {
            "id": "metadata_target_456",
            "name": "Metadata Target",
            "mimeType": "application/vnd.google-apps.document",
            "owners": [{"displayName": "Metadata Owner"}],
            "modifiedTime": "2025-01-01T00:00:00.000Z",
            "createdTime": "2025-01-01T00:00:00.000Z"
        }
        
        # Need to mock a sequence of responses for the get() method
        def get_side_effect(*args, **kwargs):
            mock = MagicMock()
            # First call will be for the shortcut
            if 'fileId' in kwargs and kwargs['fileId'] == 'metadata_shortcut_123':
                mock.execute.return_value = shortcut_file
            # Second call will be for the target file
            elif 'fileId' in kwargs and kwargs['fileId'] == 'metadata_target_456':
                mock.execute.return_value = target_file
            return mock
            
        # Apply the side_effect
        mock_service.files.return_value.get.side_effect = get_side_effect
        
        # Set a temporary directory for download and metadata
        with tempfile.TemporaryDirectory() as temp_dir:
            gdrive_reader.download_dir = temp_dir
            gdrive_reader.use_metadata_file = True
            
            # Mock is_greater_than_timestamp to return True
            gdrive_reader.is_greater_than_timestamp = MagicMock(return_value=True)
            gdrive_reader.exclude_ids = []
            
            # Replace _get_relative_path with a mock
            gdrive_reader._get_relative_path = MagicMock(return_value="metadata/path/to/shortcut")
            
            # Instead of testing the actual save_metadata, we'll mock it and verify what's passed to it
            gdrive_reader.save_metadata = MagicMock()
            
            # Call the method to test
            gdrive_reader._get_fileids_meta(file_id="metadata_shortcut_123")
            
            # Verify save_metadata was called with the correct target file with shortcut info
            assert gdrive_reader.save_metadata.called, "save_metadata should be called"
            metadata = gdrive_reader.save_metadata.call_args[0][0]
            
            # Check that shortcut fields were added to the target file metadata
            assert metadata["id"] == "metadata_target_456", "Should use target ID"
            assert metadata["sourceShortcutId"] == "metadata_shortcut_123", "Should include source shortcut ID"
            assert metadata["sourceShortcutName"] == "Metadata Shortcut", "Should include source shortcut name"
            assert metadata["sourceShortcutPath"] == "metadata/path/to/shortcut", "Should include source shortcut path"
            
    # Regular file functionality tests
    
    @patch('googleapiclient.discovery.build')
    def test_regular_file_in_folder(self, mock_build, gdrive_reader, mock_service):
        """Test basic listing and handling of regular files in a folder"""
        # Setup mock API responses
        mock_build.return_value = mock_service
        
        # Mock a folder with multiple regular files of different types
        folder_items = {
            "files": [
                {
                    "id": "doc_123",
                    "name": "Test Document",
                    "mimeType": "application/vnd.google-apps.document",
                    "owners": [{"displayName": "Test User"}],
                    "modifiedTime": "2025-01-01T00:00:00.000Z",
                    "createdTime": "2025-01-01T00:00:00.000Z"
                },
                {
                    "id": "sheet_456",
                    "name": "Test Spreadsheet",
                    "mimeType": "application/vnd.google-apps.spreadsheet",
                    "owners": [{"displayName": "Test User"}],
                    "modifiedTime": "2025-01-01T00:00:00.000Z",
                    "createdTime": "2025-01-01T00:00:00.000Z"
                }
            ],
            "nextPageToken": None
        }
        
        # Mock folder API responses
        folder_response = {"name": "Test Folder"}
        
        def get_mock_side_effect(*args, **kwargs):
            mock_response = MagicMock()
            if kwargs.get('fileId') == 'test_folder':
                mock_response.execute.return_value = folder_response
            return mock_response
            
        # Setup API call responses
        list_mock = mock_service.files.return_value.list
        list_mock.return_value.execute.return_value = folder_items
        
        get_mock = mock_service.files.return_value.get
        get_mock.side_effect = get_mock_side_effect
        
        # Mock helper methods
        gdrive_reader._get_relative_path = MagicMock()
        gdrive_reader._get_relative_path.side_effect = lambda service, file_id, folder_id: f"Test Folder/{folder_items['files'][0]['name'] if file_id == 'doc_123' else folder_items['files'][1]['name']}"
        
        gdrive_reader.save_metadata = MagicMock()
        gdrive_reader.is_greater_than_timestamp = MagicMock(return_value=True)
        gdrive_reader.exclude_ids = []
        
        # Call the method to test folder listing
        result = gdrive_reader._get_fileids_meta(folder_id="test_folder")
        
        # Verify results: we should get all regular files in the folder
        assert len(result) == 2, "Should find both files in the folder"
        
        # Check first file
        assert result[0][0] == "doc_123", "Should get correct file ID"
        assert result[0][1] == "Test User", "Should get correct owner"
        assert result[0][3] == "application/vnd.google-apps.document", "Should get correct MIME type"
        
        # Check second file
        assert result[1][0] == "sheet_456", "Should get correct file ID"
        assert result[1][3] == "application/vnd.google-apps.spreadsheet", "Should get correct MIME type"
        
        # Verify save_metadata was called for both files
        save_metadata_calls = gdrive_reader.save_metadata.call_args_list
        assert len(save_metadata_calls) == 2, "save_metadata should be called for both files"
        assert save_metadata_calls[0][0][0]["id"] == "doc_123"
        assert save_metadata_calls[1][0][0]["id"] == "sheet_456"

    @patch('googleapiclient.discovery.build')
    def test_direct_file_access(self, mock_build, gdrive_reader, mock_service):
        """Test direct access to a regular file by ID"""
        # Setup mock API responses
        mock_build.return_value = mock_service
        
        # Mock a regular file
        file_data = {
            "id": "file_123",
            "name": "Direct Access File",
            "mimeType": "application/vnd.google-apps.document",
            "owners": [{"displayName": "File Owner"}],
            "modifiedTime": "2025-01-01T00:00:00.000Z",
            "createdTime": "2025-01-01T00:00:00.000Z"
        }
        
        # Setup file get response
        get_mock = mock_service.files.return_value.get
        get_mock.return_value.execute.return_value = file_data
        
        # Mock other methods
        gdrive_reader._get_relative_path = MagicMock(return_value="Direct Access File")
        gdrive_reader.save_metadata = MagicMock()
        gdrive_reader.is_greater_than_timestamp = MagicMock(return_value=True)
        gdrive_reader.exclude_ids = []
        
        # Call the method with direct file ID
        result = gdrive_reader._get_fileids_meta(file_id="file_123")
        
        # Verify results
        assert len(result) == 1, "Should return one file"
        assert result[0][0] == "file_123", "Should get correct file ID"
        assert result[0][1] == "File Owner", "Should get correct owner"
        assert result[0][3] == "application/vnd.google-apps.document", "Should get correct MIME type"
        
        # Verify save_metadata was called correctly
        save_metadata_calls = gdrive_reader.save_metadata.call_args_list
        assert len(save_metadata_calls) == 1, "save_metadata should be called once"
        assert save_metadata_calls[0][0][0]["id"] == "file_123"
        assert save_metadata_calls[0][0][0]["name"] == "Direct Access File"

    @patch('googleapiclient.discovery.build')
    @patch('io.BytesIO')
    @patch('builtins.open', new_callable=mock_open)
    @patch('googleapiclient.http.MediaIoBaseDownload')
    def test_download_file(self, mock_download, mock_file_open, mock_bytesio, mock_build, gdrive_reader, mock_service):
        """Test file download functionality"""
        # Setup API mock
        mock_build.return_value = mock_service
        
        # Mock file data
        file_data = {
            "id": "download_123",
            "name": "Download Test.docx",
            "mimeType": "application/vnd.google-apps.document"
        }
        
        # Mock get response
        get_mock = mock_service.files.return_value.get
        get_mock.return_value.execute.return_value = file_data
        
        # Mock export media for Google Docs
        export_mock = mock_service.files.return_value.export_media
        export_mock.return_value = "export_request"
        
        # Mock download process
        downloader_instance = mock_download.return_value
        downloader_instance.next_chunk.return_value = (None, True)  # (status, done)
        
        # Call download method
        result = gdrive_reader._download_file("download_123", "/tmp/test_file")
        
        # Verify export_media was called for Google Doc
        export_mock.assert_called_once()
        
        # Verify file was written
        mock_file_open.assert_called_once()
        
        # Verify file path returned
        assert result is not None, "Should return downloaded file path"