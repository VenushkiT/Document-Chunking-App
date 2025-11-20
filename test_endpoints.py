import unittest
from unittest.mock import patch
import json
from app import app, latest_failure_summary  

class FlaskEndpointsTest(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        latest_failure_summary.clear()  

    @patch("app.BatchProcessor")
    def test_process_batch_success(self, MockBatchProcessor):
        # Configure the mock instance
        mock_instance = MockBatchProcessor.return_value
        mock_instance.process_batch.return_value = [
            {
                "status": "success", 
                "chunks": 3, 
                "file_path": "ifs-25r1/index.html",
                "chunk_details": [
                    {"chunk": "# Fundamentals¶\n## IFS Cloud Platform¶\nThe IFS Cloud Platform..."},
                    {"chunk": "# High Level Architecture¶\nAt the highest level..."},
                    {"chunk": "# Physical Architecture¶\nThe physical architecture..."}
                ]
            }
        ]

        # Test payload remains the same
        payload = {
            "source": {
                "container": "public-data",
                "path": "ifs-25r1/",
                "connection_string": "fake-connection-string"
            },
            "destination": {
                "container": "testing-datasets",
                "path": "public-document-indexing-pipeline/test-destination/fixed_size_clean_html",
                "connection_string": "fake-connection-string"
            },
            "options": {
                "batch_id": "test_batch_fixed",
                "max_workers": 6,
                "chunking_strategy": "fixed_size",
                "extract_main_content": True,
                "chunk_size": 750,
                "chunk_overlap": 100,
                "preview_mode": True
            }
        }

        response = self.client.post(
            "/process_batch",
            data=json.dumps(payload),
            content_type="application/json"
        )

        # Test response structure
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["batch_id"], "test_batch_fixed")
        self.assertEqual(data["status"], "completed")
        
        # Verify the actual call arguments instead of using assert_called_once_with
        args, kwargs = MockBatchProcessor.call_args
        expected_args = (
            payload["source"],
            payload["destination"],
            payload["options"]
        )
        self.assertEqual(args, expected_args)

    @patch("app.BatchProcessor")
    def test_process_batch_partial_failure(self, MockBatchProcessor):
        # Mock process_batch to simulate partial failure
        mock_instance = MockBatchProcessor.return_value
        mock_instance.process_batch.return_value = [
            {"status": "success", "chunks": 2, "file_path": "file1.html"},
            {
                "status": "failed",
                "file_path": "file2.html",
                "error_type": "ReadError",
                "error_message": "File corrupted",
                "timestamp": "2025-08-11T10:00:00",
                "processing_stage": "reading_file"
            }
        ]

        payload = {
            "source": {"container": "test", "path": "some/path", "connection_string": "fake"},
            "destination": {"container": "test", "path": "dest/path", "connection_string": "fake"},
            "options": {"batch_id": "batch_456"}
        }

        response = self.client.post(
            "/process_batch",
            data=json.dumps(payload),
            content_type="application/json"
        )

        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["files_processed"], 1)
        self.assertEqual(data["files_failed"], 1)
        self.assertEqual(data["status"], "partial_success")
        self.assertIn("failure_summary", data)
        self.assertEqual(data["failure_summary"]["total_failures"], 1)
        self.assertIn("ReadError", data["failure_summary"]["common_errors"])

    def test_process_batch_missing_source_or_destination(self):
        # Missing 'source'
        payload = {
            "destination": {"container": "test", "path": "dest/path", "connection_string": "fake"},
            "options": {}
        }
        response = self.client.post(
            "/process_batch",
            data=json.dumps(payload),
            content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn("error", data)

        # Missing 'destination'
        payload = {
            "source": {"container": "test", "path": "some/path", "connection_string": "fake"},
            "options": {}
        }
        response = self.client.post(
            "/process_batch",
            data=json.dumps(payload),
            content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

    def test_failure_logs_no_failures(self):
        latest_failure_summary.clear()
        response = self.client.get("/failure_logs")
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["status"], "ok")
        self.assertIn("message", data)

    def test_failure_logs_with_failures(self):
        latest_failure_summary.clear()
        latest_failure_summary["batch_789"] = {
            "timestamp": "2025-08-11T10:00:00",
            "total_failures": 1,
            "failure_details": [
                {
                    "file_path": "file2.html",
                    "error_type": "ReadError",
                    "error_message": "File corrupted",
                    "timestamp": "2025-08-11T10:00:00",
                    "file_size": 12345,
                    "processing_stage": "reading_file"
                }
            ],
            "common_errors": {"ReadError": 1}
        }

        response = self.client.get("/failure_logs")
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["status"], "degraded")
        self.assertEqual(data["batch_id"], "batch_789")
        self.assertEqual(data["failures"]["total_failures"], 1)

if __name__ == "__main__":
    unittest.main()