import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import re
from pathlib import Path
from storageClient import StorageClient
from chunking import (
    get_html_chunks, 
    get_metadata_paths, 
    get_html_title,
    CHUNKING_STRATEGY_FIXED,
    CHUNKING_STRATEGY_H1_BASED,
    CHUNKING_STRATEGY_H2_BASED,
)
from utils import generate_artifact  
import traceback
import logging

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, source_config, destination_config, options):
        self.source_config = source_config
        self.destination_config = destination_config
        self.options = options
        self.storage_client = StorageClient(self.options.get("connection_string"))
        
        # Pass batch_id to destination_config for manifest naming
        batch_id = options.get("batch_id", f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        self.destination_config["batch_id"] = batch_id

    def process_batch(self):
        # Check if destination exists
        destination_path = self.destination_config.get("path")
        container = self.destination_config["container"]

        if self.storage_client.folder_exists(container, destination_path):
            logger.warning(f"The destination folder '{destination_path}' already exists. Creating a new folder.")
            destination_path = self.generate_new_folder_name(container, destination_path)
            self.destination_config["path"] = destination_path  
            logger.info(f"New destination folder created: {destination_path}")

        # Get all source files
        file_paths = self.storage_client.list_files(self.source_config)
        if not file_paths:
            logger.warning("The source folder is empty. No files to process.")
            return [{"status": "failed", "error_message": "No files to process"}]

        # Preview mode (optional)
        if self.options.get("preview_mode", False):
            file_paths = file_paths[:5]
            logger.info(f"Preview mode enabled: Processing first {len(file_paths)} files for validation")

        # Process files in parallel
        results = self.process_files_parallel(file_paths)

        # Generate one consolidated report after all workers finish
        batch_id = self.destination_config["batch_id"]
        manifest_file = Path("./manifests") / batch_id / "manifest.jsonl"
        reports = self.storage_client.trigger_analysis(str(manifest_file))
        if reports:
            logger.info(f"Consolidated report generated at: {reports.get('txt_report')}")

        # Upload manifests folder to Azure
        manifest_dir = Path("./manifests") / batch_id
        if manifest_dir.exists():
            logger.info(f"Uploading manifests folder to Azure: {manifest_dir}")
            upload_result = self.storage_client.upload_manifest_folder(
                str(manifest_dir), 
                self.destination_config
            )
            logger.info(f"Upload complete: {upload_result}")
        else:
            logger.warning(f"Manifest directory not found: {manifest_dir}")

        return results


    def generate_new_folder_name(self, container, base_path):
        """
        Generate a new folder name by appending '_1', '_2', etc., to the base path.
        """
        counter = 1
        new_path = f"{base_path}_{counter}"
        while self.storage_client.folder_exists(container, new_path):
            logger.debug(f"Folder '{new_path}' already exists. Trying next.")
            counter += 1
            new_path = f"{base_path}_{counter}"
        logger.info(f"Generated new folder name: {new_path}")
        return new_path

    def process_files_parallel(self, file_paths):
        max_workers = self.options.get("max_workers", 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.process_single_file, file_paths))
        return results

    def process_single_file(self, file_path):
        processing_stage = "init"
        try:
            # Stage 1: Read raw bytes of file
            processing_stage = "reading_file"
            logger.info(f"[{processing_stage}] Processing html document: [{file_path.split('/')[-1]}]")
            html_bytes = self.storage_client.read_file(self.source_config['container'], file_path)

            # Stage 2: Prepare HTML doc (base64 encoded and decoded)
            processing_stage = "preparing_html_doc"
            logger.info(f"[{processing_stage}] Processing html document: [{file_path.split('/')[-1]}]")

            # Base64 encode the raw bytes
            base64_encoded_data = base64.b64encode(html_bytes).decode("utf-8")

            # Decode raw bytes into a string, with fallback for decoding errors
            try:
                decoded_html = html_bytes.decode("utf-8")
            except UnicodeDecodeError:
                logger.warning(f"UTF-8 decode failed for {file_path}, falling back to Windows-1252")
                decoded_html = html_bytes.decode("windows-1252", errors="replace")

            # Construct the HTML document dictionary
            html_doc = {
                "file_data": {"data": base64_encoded_data},
                "metadata_storage_path": file_path
            }

            # Stage 3: Chunking with strategy selection
            processing_stage = "chunking"
            logger.info(f"[{processing_stage}] Processing html document: [{file_path.split('/')[-1]}]")
            
            # Get chunking parameters from options
            chunk_size = self.options.get("chunk_size", 750)
            chunk_overlap = self.options.get("chunk_overlap", 100)
            chunking_strategy = self.options.get("chunking_strategy", CHUNKING_STRATEGY_FIXED)
            extract_main_content = self.options.get("extract_main_content", False)
            
            # Validate chunking strategy
            valid_strategies = [CHUNKING_STRATEGY_FIXED, CHUNKING_STRATEGY_H1_BASED, CHUNKING_STRATEGY_H2_BASED]
            if chunking_strategy not in valid_strategies:
                logger.warning(f"Invalid chunking strategy '{chunking_strategy}', defaulting to '{CHUNKING_STRATEGY_FIXED}'")
                chunking_strategy = CHUNKING_STRATEGY_FIXED
            
            chunks = get_html_chunks(
                html_doc, 
                chunk_size=chunk_size, 
                chunk_overlap=chunk_overlap,
                chunking_strategy=chunking_strategy,
                extract_main_content=extract_main_content
            )

            # Stage 4: Filtering
            processing_stage = "filtering_chunks"
            logger.info(f"[{processing_stage}] Processing html document: [{file_path.split('/')[-1]}]")
            filtered_chunks = self.apply_quality_filters(chunks)

            # Stage 5: Metadata extraction 
            processing_stage = "extracting_metadata"
            logger.info(f"[{processing_stage}] Processing html document: [{file_path.split('/')[-1]}]")
            metadata_paths = get_metadata_paths(decoded_html, file_path)
            html_title = get_html_title(decoded_html, file_path)

            # Stage 6: Artifact generation
            processing_stage = "generating_artifacts"
            logger.info(f"[{processing_stage}] Processing html document: [{file_path.split('/')[-1]}]")
            artifacts = generate_artifact(filtered_chunks, metadata_paths, html_title, file_path)

            # Stage 7: Writing artifacts
            processing_stage = "writing_artifacts"
            logger.info(f"[{processing_stage}] Processing html document: [{file_path.split('/')[-1]}]")
            self.storage_client.write_files(artifacts, self.destination_config)

            return {
                "file_path": file_path,
                "status": "success",
                "chunks": len(artifacts)
            }

        except Exception as e:
            error_type = type(e).__name__
            error_message = str(e)
            stack = traceback.format_exc()
            file_size = None

            try:
                file_size = self.storage_client.get_file_size(self.source_config['container'], file_path)
            except Exception:
                pass

            logger.error(f"[{processing_stage}] Error processing html document: [{file_path.split('/')[-1]}] - {error_type}: {error_message}")

            return {
                "file_path": file_path,
                "status": "failed",
                "error_type": error_type,
                "error_message": error_message,
                "stack_trace": stack,
                "file_size": file_size,
                "processing_stage": processing_stage, 
                "timestamp": datetime.now().isoformat()
            }

    def clean_chunk_text(self, text):
        """
        Cleans a Markdown chunk while preserving Markdown formatting,
        code fences, headings, and tables.
        """
        lines = text.splitlines()

        cleaned_lines = []
        for line in lines:
            # Skip aggressive cleaning for code fences or table separators
            if re.match(r"^\s*(```|~~~)", line) or re.match(r"^\s*\|.*\|$", line) or re.match(r"^\s*#{1,6}\s", line):
                cleaned_lines.append(line)
                continue

            # Normalize tabs and carriage returns to spaces
            line = re.sub(r"[\t\r]+", " ", line)
            # Collapse multiple spaces, but keep leading indentation (for code)
            leading_spaces = len(line) - len(line.lstrip(" "))
            line = " " * leading_spaces + re.sub(r" {2,}", " ", line.lstrip())

            # Remove only non-printable control characters (keep unicode & punctuation)
            line = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", line)

            # Remove extreme repetition but skip markdown symbols (#, -, =, etc.)
            if not re.match(r"^\s*[-=]{3,}\s*$", line):  
                line = re.sub(r"([^#\-=])\1{5,}", r"\1", line)

            cleaned_lines.append(line)

        return "\n".join(cleaned_lines).strip()


    def apply_quality_filters(self, chunks):
        """
        Filters out bad Markdown chunks while keeping valid formatting,
        tables, and code blocks.
        """
        def is_gibberish(text):
            # Allow code blocks and tables to pass
            if re.search(r"```|~~~|\|.*\|", text):
                return False

            stripped_text = re.sub(r"[#|\-`]", "", text)

            # Filter out chunks with 5 or more repeated alphanumeric characters
            if re.search(r"([a-zA-Z0-9])\1{5,}", stripped_text):
                return True

            # Filter out chunks with more than 10% non-standard symbols
            non_basic = re.findall(r"[^a-zA-Z0-9\s.,;:!?()\"'{}\[\]<>=+\-/%$#@&*]", stripped_text)
            if len(stripped_text) > 0 and len(non_basic) / len(stripped_text) > 0.1:
                return True

            return False

        filtered = []
        for c in chunks:
            text = self.clean_chunk_text(c.get("chunk", ""))
            if text and not is_gibberish(text):
                c["chunk"] = text
                filtered.append(c)
        return filtered