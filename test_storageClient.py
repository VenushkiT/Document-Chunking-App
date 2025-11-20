import unittest
from unittest.mock import patch, MagicMock
from storageClient import StorageClient
from pathlib import Path

class TestStorageClient(unittest.TestCase):

    def setUp(self):
        # Patch the factory method that creates BlobServiceClient instance
        patcher = patch('storageClient.BlobServiceClient.from_connection_string')
        self.addCleanup(patcher.stop)
        self.mock_from_connection_string = patcher.start()

        # Mock blob service client to be returned by the factory
        self.mock_blob_service_client = MagicMock()
        self.mock_from_connection_string.return_value = self.mock_blob_service_client

        # Instantiate your StorageClient which uses the mocked BlobServiceClient
        self.storage_client = StorageClient()

    def test_list_files_success(self):
        # Setup mock container client with blobs
        mock_container_client = MagicMock()
        self.mock_blob_service_client.get_container_client.return_value = mock_container_client

        mock_blob1 = MagicMock()
        mock_blob1.name = "folder/file1.html"
        mock_blob2 = MagicMock()
        mock_blob2.name = "folder/file2.htm"
        mock_blob3 = MagicMock()
        mock_blob3.name = "folder/file3.txt"  # Should be ignored

        mock_container_client.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        files = self.storage_client.list_files({"container": "test-container", "path": "folder/"})

        self.assertIn("folder/file1.html", files)
        self.assertIn("folder/file2.htm", files)
        self.assertNotIn("folder/file3.txt", files)
        self.assertEqual(len(files), 2)

    def test_list_files_empty(self):
        mock_container_client = MagicMock()
        self.mock_blob_service_client.get_container_client.return_value = mock_container_client
        mock_container_client.list_blobs.return_value = []

        files = self.storage_client.list_files({"container": "test-container"})
        self.assertEqual(files, [])

    def test_list_files_exception(self):
        self.mock_blob_service_client.get_container_client.side_effect = Exception("Container not found")
        files = self.storage_client.list_files({"container": "bad-container"})
        self.assertEqual(files, [])

    def test_read_file_success(self):
        mock_blob_client = MagicMock()
        mock_blob_data = MagicMock()
        mock_blob_data.readall.return_value = b"file content bytes"
        mock_blob_client.download_blob.return_value = mock_blob_data
        self.mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        content = self.storage_client.read_file("test-container", "file1.html")
        self.assertEqual(content, b"file content bytes")

    def test_read_file_exception(self):
        mock_blob_client = MagicMock()
        mock_blob_client.download_blob.side_effect = Exception("Blob not found")
        self.mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        content = self.storage_client.read_file("test-container", "missing.html")
        self.assertIsNone(content)  # On error, returns None

    def test_write_files_success(self):
        mock_blob_client = MagicMock()
        self.mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        artifacts = [
            {"chunk_id": "abc123", "content": "data1"},
            {"chunk_id": "def456", "content": "data2"},
        ]

        self.storage_client.write_files(artifacts, {"container": "dest-container", "path": "dest/"})

        # Assert get_blob_client called twice (once per artifact)
        self.assertEqual(self.mock_blob_service_client.get_blob_client.call_count, 2)
        # Assert upload_blob called twice
        self.assertEqual(mock_blob_client.upload_blob.call_count, 2)

    def test_write_files_with_manifest(self):
        mock_blob_client = MagicMock()
        self.mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        artifacts = [
            {"chunk_id": "abc123", "content": "data1"},
            {"chunk_id": "def456", "content": "data2"},
        ]

        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            manifest_file = self.storage_client.write_files(
                artifacts, 
                {"container": "dest-container", "path": "dest/", "batch_id": "test_batch"}
            )

        self.assertTrue(isinstance(manifest_file, Path))
        self.assertEqual(mock_blob_client.upload_blob.call_count, 2)
        mock_file.assert_called()

    def test_write_progress_log_success(self):
        mock_blob_client = MagicMock()
        self.mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        log_entry = {"status": "done"}
        self.storage_client.write_progress_log(log_entry, {"container": "dest-container", "path": "dest/"})

        self.mock_blob_service_client.get_blob_client.assert_called_once()
        mock_blob_client.upload_blob.assert_called_once()

    def test_get_file_size_success(self):
        mock_blob_client = MagicMock()
        mock_properties = MagicMock()
        mock_properties.size = 1024
        mock_blob_client.get_blob_properties.return_value = mock_properties
        self.mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        size = self.storage_client.get_file_size("test-container", "test.html")
        self.assertEqual(size, 1024)

    def test_get_file_size_error(self):
        mock_blob_client = MagicMock()
        mock_blob_client.get_blob_properties.side_effect = Exception("File not found")
        self.mock_blob_service_client.get_blob_client.return_value = mock_blob_client

        size = self.storage_client.get_file_size("test-container", "missing.html")
        self.assertIsNone(size)

    def test_folder_exists_true(self):
        mock_container_client = MagicMock()
        self.mock_blob_service_client.get_container_client.return_value = mock_container_client
        mock_container_client.list_blobs.return_value = [MagicMock()]

        exists = self.storage_client.folder_exists("test-container", "folder/")
        self.assertTrue(exists)

    def test_folder_exists_false(self):
        mock_container_client = MagicMock()
        self.mock_blob_service_client.get_container_client.return_value = mock_container_client
        mock_container_client.list_blobs.return_value = []

        exists = self.storage_client.folder_exists("test-container", "empty-folder/")
        self.assertFalse(exists)

    def test_folder_exists_error(self):
        mock_container_client = MagicMock()
        self.mock_blob_service_client.get_container_client.return_value = mock_container_client
        mock_container_client.list_blobs.side_effect = Exception("Container not found")

        exists = self.storage_client.folder_exists("bad-container", "folder/")
        self.assertFalse(exists)

    @patch('analyze_artifacts.Path')  # Update import path
    def test_trigger_analysis_success(self, mock_path):
        # Update to use correct import path
        with patch('analyze_artifacts.ArtifactAnalyzer') as mock_analyzer_class:
            mock_analyzer = MagicMock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_analyzer.load_artifacts.return_value = 1
            mock_analyzer.generate_report.return_value = {"txt_report": "report.txt"}

            reports = self.storage_client.trigger_analysis("manifest.jsonl", "output_dir")
            
            self.assertIsNotNone(reports)
            self.assertEqual(reports["txt_report"], "report.txt")
            mock_analyzer.analyze.assert_called_once()

    def test_trigger_analysis_no_artifacts(self):
        # Update to use correct import path
        with patch('analyze_artifacts.ArtifactAnalyzer') as mock_analyzer_class:
            mock_analyzer = MagicMock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_analyzer.load_artifacts.return_value = 0

            reports = self.storage_client.trigger_analysis("manifest.jsonl")
            
            self.assertIsNone(reports)
            mock_analyzer.analyze.assert_not_called()

if __name__ == "__main__":
    unittest.main()