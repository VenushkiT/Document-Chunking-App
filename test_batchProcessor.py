import unittest
from unittest.mock import patch, MagicMock
from batchProcessor import BatchProcessor
from datetime import datetime

class TestBatchProcessor(unittest.TestCase):

    def setUp(self):
        # Patch StorageClient before creating BatchProcessor
        patcher = patch('batchProcessor.StorageClient')
        self.MockStorageClient = patcher.start()
        self.addCleanup(patcher.stop)

        self.source_config = {"container": "source-container"}
        self.destination_config = {"container": "dest-container", "path": "dest-path"}
        self.options = {
            "chunk_size": 100, 
            "chunk_overlap": 10,
            "batch_id": "test_batch_123"
        }

        # Now BatchProcessor uses the mocked StorageClient instance
        self.processor = BatchProcessor(self.source_config, self.destination_config, self.options)
        self.mock_storage_client_instance = self.MockStorageClient.return_value

    def test_generate_new_folder_name(self):
        # Test when first attempt is available
        self.mock_storage_client_instance.folder_exists.side_effect = [False]
        new_name = self.processor.generate_new_folder_name("container", "base_path")
        self.assertEqual(new_name, "base_path_1")

        # Test when need to try multiple times
        self.mock_storage_client_instance.folder_exists.side_effect = [True, True, False]
        new_name = self.processor.generate_new_folder_name("container", "base_path")
        self.assertEqual(new_name, "base_path_3")

    @patch("batchProcessor.get_html_chunks")
    @patch("batchProcessor.get_metadata_paths")
    @patch("batchProcessor.get_html_title")
    @patch("batchProcessor.generate_artifact")
    def test_process_single_file_success(self, mock_generate_artifact, mock_get_title, 
                                       mock_get_metadata_paths, mock_get_html_chunks):
        # Setup mocks for StorageClient methods
        self.mock_storage_client_instance.read_file.return_value = b"<html></html>"
        self.mock_storage_client_instance.write_files.return_value = None

        # Setup mocks for chunking and artifact generation
        mock_get_html_chunks.return_value = [{"chunk": "text chunk"}]
        mock_get_metadata_paths.return_value = ["meta1", "meta2"]
        mock_get_title.return_value = "Test Title"
        mock_generate_artifact.return_value = [{"artifact": "data"}]

        result = self.processor.process_single_file("file1.html")

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["file_path"], "file1.html")
        self.assertEqual(result["chunks"], 1)

        self.mock_storage_client_instance.read_file.assert_called_once()
        self.mock_storage_client_instance.write_files.assert_called_once()
        mock_get_html_chunks.assert_called_once()
        mock_get_metadata_paths.assert_called_once()
        mock_get_title.assert_called_once()
        mock_generate_artifact.assert_called_once()

    def test_process_batch(self):
        # Setup mock for list_files
        self.mock_storage_client_instance.list_files.return_value = ["file1.html", "file2.html"]
        self.mock_storage_client_instance.folder_exists.return_value = False
        self.mock_storage_client_instance.trigger_analysis.return_value = {"txt_report": "report.txt"}

        # Test normal batch processing
        with patch.object(self.processor, 'process_files_parallel') as mock_process:
            mock_process.return_value = [{"status": "success", "chunks": 1}]
            results = self.processor.process_batch()
            self.assertEqual(len(results), 1)
            mock_process.assert_called_once()

        # Test preview mode
        self.processor.options["preview_mode"] = True
        with patch.object(self.processor, 'process_files_parallel') as mock_process:
            mock_process.return_value = [{"status": "success", "chunks": 1}]
            results = self.processor.process_batch()
            self.assertEqual(len(results), 1)
            # Should only process first 5 files in preview mode
            file_list = mock_process.call_args[0][0]
            self.assertLessEqual(len(file_list), 5)

    def test_process_single_file_failure(self):
        # Setup StorageClient mocks to simulate failure in reading file
        self.mock_storage_client_instance.read_file.side_effect = Exception("Test error")
        self.mock_storage_client_instance.get_file_size.return_value = 4321

        result = self.processor.process_single_file("file2.html")

        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["file_path"], "file2.html")
        self.assertEqual(result["error_type"], "Exception")
        self.assertEqual(result["error_message"], "Test error")
        self.assertEqual(result["file_size"], 4321)
        self.assertEqual(result["processing_stage"], "reading_file")

    def test_clean_chunk_text_basic(self):
        raw_text = "This   is   a   test\n\n### Heading\n\n```code block```\n-----\n!!!!!!!!!aaaaaa"
        cleaned = self.processor.clean_chunk_text(raw_text)

        self.assertIn("### Heading", cleaned)
        self.assertIn("```code block```", cleaned)
        self.assertIn("-----", cleaned)
        self.assertNotIn("!!!!!!!!!", cleaned) 
        self.assertNotIn("   ", cleaned) 

    def test_apply_quality_filters_filters_gibberish(self):
        gibberish_chunk = {
            "chunk": "aaaaaaa111111111111111111111111111111111111111111111111111111111111!!!@@@$$$%%%^^^&&&***"
        }
        valid_chunk = {"chunk": "Normal text chunk with valid content."}
        chunks = [gibberish_chunk, valid_chunk]

        filtered = self.processor.apply_quality_filters(chunks)
        self.assertEqual(len(filtered), 1)
        self.assertIn("Normal text chunk", filtered[0]["chunk"])

    def test_apply_quality_filters_allows_code_blocks(self):
        code_chunk = {"chunk": "```aaaaaaa111111111111111111111111111111111```"}
        filtered = self.processor.apply_quality_filters([code_chunk])
        self.assertEqual(len(filtered), 1)

    @patch("batchProcessor.ThreadPoolExecutor")
    def test_process_files_parallel_uses_max_workers(self, mock_executor):
        # Setup mock executor
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        mock_executor_instance.map.return_value = [{"status": "success", "chunks": 1}]

        # Test default max_workers (4)
        self.processor.process_files_parallel(["file1.html", "file2.html"])
        mock_executor.assert_called_with(max_workers=4)

        # Test custom max_workers
        self.processor.options["max_workers"] = 8
        self.processor.process_files_parallel(["file1.html", "file2.html"])
        mock_executor.assert_called_with(max_workers=8)

if __name__ == "__main__":

    unittest.main()