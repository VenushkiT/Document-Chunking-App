import os
import logging
import sys
import time
from datetime import datetime
from flask import Flask, request, jsonify
from batchProcessor import BatchProcessor

app = Flask(__name__)

def setup_logging():
    gunicorn_logger = logging.getLogger("gunicorn.error")

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    root_logger.setLevel(logging.DEBUG)
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

setup_logging()

# In-memory failure summary
latest_failure_summary = {}

@app.route("/process_batch", methods=["POST"])
def process_batch():
    """
    Accepts source and destination config and runs document processing
    """
    app.logger.info("Received batch processing request")
    app.logger.debug("Starting batch processing...")
    start_time = time.time()

    try:
        data = request.get_json(force=True)
        app.logger.debug(f"Request data: {data}")

        source = data.get("source")
        destination = data.get("destination")
        options = data.get("options", {})

        if not source or not destination:
            app.logger.warning("Missing 'source' or 'destination' in request")
            return jsonify({"error": "Missing 'source' or 'destination' in request"}), 400

        batch_id = options.get("batch_id")
        if not batch_id:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            batch_id = f"batch_{timestamp}"

        app.logger.debug(f"Batch ID: {batch_id}")
        processor = BatchProcessor(source, destination, options)
        results = processor.process_batch()

        successes = [r for r in results if r["status"] == "success"]
        failures = [r for r in results if r["status"] == "failed"]
        processing_time = round(time.time() - start_time, 2)

        summary = {
            "batch_id": batch_id,
            "status": (
                "completed" if not failures else
                "partial_success" if successes else
                "failed"
            ),
            "files_processed": len(successes),
            "files_failed": len(failures),
            "chunks_generated": sum(r.get("chunks", 0) for r in successes),
            "processing_time_seconds": processing_time,
            "destination_path": destination.get("path", "")
        }

        if failures:
            timestamp_now = datetime.now().isoformat()
            failure_details = []
            error_counts = {}

            for failure in failures:
                detail = {
                    "file_path": failure.get("file_path", "unknown"),
                    "error_type": failure.get("error_type", "unknown"),
                    "error_message": failure.get("error_message", "No details available"),
                    "timestamp": failure.get("timestamp", timestamp_now),
                    "file_size": failure.get("file_size"),
                    "processing_stage": failure.get("processing_stage", "unknown")
                }
                failure_details.append(detail)
                error_type = detail["error_type"]
                error_counts[error_type] = error_counts.get(error_type, 0) + 1

            # Store detailed failures separately for GET access
            latest_failure_summary[batch_id] = {
                "timestamp": timestamp_now,
                "total_failures": len(failure_details),
                "failure_details": failure_details,
                "common_errors": error_counts
            }

            # Include summary in POST response
            summary["failure_summary"] = {
                "total_failures": len(failure_details),
                "common_errors": error_counts
            }

            # Logging
            app.logger.error(f"[{batch_id}] {len(failures)} files failed")
            top_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:3]
            app.logger.error(f"[{batch_id}] Top error types: {top_errors}")

        else:
            # Clear previous failures
            latest_failure_summary.clear()

        app.logger.info(f"Batch completed: {summary}")
        return jsonify(summary), 200

    except Exception as e:
        processing_time = round(time.time() - start_time, 2)
        app.logger.exception("Batch processing failed")
        return jsonify({
            "error": str(e),
            "processing_time_seconds": processing_time
        }), 500

# GET endpoint for detailed failure logs
@app.route("/failure_logs", methods=["GET"])
def get_failure_logs():
    if not latest_failure_summary:
        return jsonify({
            "status": "ok",
            "message": "No recent failures"
        }), 200

    most_recent_batch = max(latest_failure_summary.keys())
    return jsonify({
        "status": "degraded",
        "batch_id": most_recent_batch,
        "timestamp": latest_failure_summary[most_recent_batch]["timestamp"],
        "failures": latest_failure_summary[most_recent_batch]
    }), 200

# Health check endpoint to verify connection string
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }), 200

if __name__ == "__main__":
    app.run(port=8000, debug=True)