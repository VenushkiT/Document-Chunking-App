# htmlProcessingAPP — README

Purpose
-------
This app extracts main content from HTML files, converts to Markdown, and emits chunks as JSON artifacts that align with the Azure AI Search required format for downstream indexing. It supports three chunking strategies, optional main content extraction, parallel processing, failure reporting via a GET endpoint, and artifact analysis.

Quick summary
- POST /process_batch — start a batch (source, destination, options)
- GET /failure_logs — retrieve recent failure summary and details
- Helper script: request.py
- Chunking logic: chunking.py
- Batch orchestration: batchProcessor.py
- Artifact analysis: analyze_artifacts.py
- Manifests stored under ./manifests/{batch_id}/

Requirements
------------
- Python 3.8+
- Key libraries: requests, beautifulsoup4, trafilatura, markdownify, langchain_text_splitters, tiktoken, pandas, numpy, matplotlib, seaborn
- Install via requirements.txt or pip:
  - pip install -r requirements.txt

Configuration (payload)
-----------------------
POST /process_batch accepts JSON with three top-level keys:

- source: { container, path }
- destination: { container, path } — `batch_id` is injected into destination_config if omitted
- options: see table below

Options reference (defaults)
----------------------------
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| chunk_size | int | 750 | Max tokens per chunk |
| chunk_overlap | int | 100 | Token overlap between consecutive chunks |
| chunking_strategy | str | "fixed_size" | One of: "fixed_size", "h1_heading_based", "h2_heading_based" |
| extract_main_content | bool | false | Attempt to extract the document's main content (remove TOC/navigation/sidebar elements) |
| preview_mode | bool | false | When true, process only the first 5 files |
| max_workers | int | 4 | Number of parallel worker threads used by BatchProcessor |
| batch_id | str | auto-generated | Custom identifier for the batch; if omitted BatchProcessor generates batch_{YYYYMMDD_HHMMSS} |
| connection_string | str | required | Azure Storage connection string for accessing containers |

Batch ID (behavior & recommendations)
------------------------------------
- If you supply `options.batch_id`, that value is used for manifest and artifact naming. Example: "test_batch_20250117".
- If omitted, BatchProcessor generates a batch id using the current timestamp: `batch_YYYYMMDD_HHMMSS`.
- The generated or provided batch_id is injected into destination config and used for manifest path: `./manifests/{batch_id}/manifest.jsonl`.
- Recommendation: provide a meaningful batch_id for repeatable runs and easier artifact / manifest lookup.

max_workers (behavior & recommendations)
----------------------------------------
- Controlled via `options.max_workers`. Defaults to 4.
- Controls ThreadPoolExecutor worker count used in BatchProcessor.process_files_parallel.
- Tune based on CPU, memory, and storage I/O/throughput limits:
  - Low concurrency (1-2): safer for low-memory or rate-limited storage.
  - Medium (4-8): balanced for typical multi-core dev machines.
  - High (>8): only if storage and CPU can handle parallel reads/writes.
- In test_mode you can keep max_workers low (1-2) for deterministic, easier debugging.

Chunking strategies
-------------------
- fixed_size (default): token-based splitting using RecursiveCharacterTextSplitter — fastest.
- h1_heading_based: prefer H1 splits; fallback to H2 or fixed-size; merges tiny sections and uses continuation markers when needed.
- h2_heading_based: hierarchical H1+H2 splitting; preserves heading context for nested technical docs.

Main content extraction
-----------------------
- Option: `extract_main_content` (bool) — When true, extraction uses the open-source library trafilatura to extract the page's main content (removing navigation, TOC, and other boilerplate).

Preview mode
---------
- Option: `preview_mode` (bool)
- When true, BatchProcessor restricts processing to the first 5 files returned by the source listing. Use this to quickly validate configuration and chunking before large runs.

API examples
------------
Fixed-size (default)
```json
{
  "source": {"container":"raw-docs","path":"documentation/user-guides/"},
  "destination": {"container":"processed-docs","path":"chunks/user-guides-v1/"},
  "options": {
    "chunk_size": 750,
    "chunk_overlap": 100,
    "chunking_strategy": "fixed_size",
    "extract_main_content": false,
    "connection_string": "<AZURE_STORAGE_CONNECTION_STRING>"
  }
}
```

H1-based with main content extraction, preview mode, and explicit max_workers & batch_id
```json
{
  "source": {"container":"raw-docs","path":"documentation/technical/"},
  "destination": {"container":"processed-docs","path":"chunks/technical-h1/"},
  "options": {
    "chunk_size": 1000,
    "chunk_overlap": 150,
    "chunking_strategy": "h1_heading_based",
    "extract_main_content": true,
    "preview_mode": true,
    "max_workers": 6,
    "batch_id": "technical_h1_preview_20251017",
    "connection_string": "<AZURE_STORAGE_CONNECTION_STRING>"
  }
}
```

Response format (success)
-------------------------
Example returned JSON after processing:
```json
{
  "batch_id": "batch_1",
  "status": "completed",
  "files_processed": 42,
  "files_failed": 0,
  "chunks_generated": 1250,
  "processing_time_seconds": 87.5,
  "destination_path": "chunks/technical-h1-based/"
}
```

Failure reporting — GET /failure_logs
-------------------------------------
- Purpose: quick access to recent failure summary and per-file failure details for the most recent batch with failures.
- Behavior:
  - If no recent failures: returns 200 with {"status":"ok","message":"No recent failures"}.
  - If failures exist: returns 200 with a summary for the most recent batch_id, including timestamp, total_failures, failure_details (file_path, error_type, error_message, timestamp, file_size, processing_stage), and common_errors counts.
- Example usage (local):
  - curl http://127.0.0.1:8000/failure_logs
- Notes:
  - The GET endpoint returns the most recent batch with failures and is intended for quick diagnostics without inspecting manifests or raw logs.

Artifacts, manifests & analysis reports
-------------------------------------
- Manifest lines: JSONL entries for each generated chunk (chunk text, chunk_id, metadata).
- When a batch completes, the following are automatically generated in Azure Storage:
  - Chunks are stored in the specified destination container and path
  - A sibling folder named {batch_id}_manifests is created containing:
    - manifest.jsonl: The batch manifest file
    - analysis_report.txt: Statistical analysis of chunks
    - token_distribution.png: Visualization of token distribution
    - char_distribution.png: Visualization of character distribution
- The analysis includes:
  - Character and token statistics across chunks
  - Descriptive statistics
  - Distribution visualizations
  - Token counts using tiktoken (cl100k_base)

Logging, errors & diagnostics
----------------------------
- The app logs per-file stages: reading_file, preparing_html_doc, chunking, filtering_chunks, extracting_metadata, generating_artifacts, writing_artifacts.
- Invalid chunking_strategy → logs a warning and defaults to fixed_size.
- If destination folder exists the code appends _1, _2, ... to create a new folder.
- Check ./manifests/{batch_id}/manifest.jsonl, the /failure_logs endpoint, and application logs for diagnostics.

Where to look in code
---------------------
- chunking.py — extraction, HTML→Markdown, chunking strategies
- batchProcessor.py — batch orchestration, defaults, preview_mode, max_workers, worker pool
- app.py — HTTP endpoints (POST /process_batch and GET /failure_logs)
- request.py — example payload and test runner
- storageClient.py / utils.py — storage access and artifact generation utilities

Usage Instructions
----------------
1. Start the Azure Container App:
   - Navigate to Azure Portal -> Container Apps -> public-html-chunking container app
   - Start the container if it's not running
2. Update and run the request script:
   - Update the endpoint URL in request.py with the correct Container App URL
   - python request.py
3. If any failures occur, check the failure logs endpoint
4. After batch success, check the {batch_id}_manifests folder in Azure Storage for analysis reports and visualizations

Deployment Instructions
----------------------
To deploy changes to Azure Container Registry and update the container:

1. Login to Azure:
   ```bash
   az login
   ```

2. Login to Azure Container Registry:
   ```bash
   az acr login --name ifsmlacr
   ```

3. Build and push the new image (replace <new-version> with semantic version like 1.6.1):
   ```bash
   docker build -t ifsmlacr.azurecr.io/public-doc-chunking-app:<new-version> .
   docker push ifsmlacr.azurecr.io/public-doc-chunking-app:<new-version>
   ```

4. Update in Azure Portal:
   - Navigate to Azure Portal → Containers → Container Details
   - Select the new image tag from the dropdown menu
   - Save as a new revision