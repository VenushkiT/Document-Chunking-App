import json
import logging
from pathlib import Path
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class StorageClient:
    def __init__(self, connection_string=None):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
        logger.info("Initialized StorageClient with Azure Blob Storage")

    def list_files(self, source_config):
        container_name = source_config.get("container")
        path_prefix = source_config.get("path", "")
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs(name_starts_with=path_prefix)
            html_files = [b.name for b in blobs if b.name.lower().endswith(('.html', '.htm'))]
            logger.info(f"Found {len(html_files)} HTML files in {container_name}/{path_prefix}")
            return html_files
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []

    def read_file(self, container, file_path):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container, blob=file_path)
            return blob_client.download_blob().readall()
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return None

    def write_files(self, artifacts, destination_config):
        """
        Write artifacts to Azure and append to a manifest.
        Does NOT trigger analysis here anymore.
        """
        container_name = destination_config.get("container")
        base_path = destination_config.get("path", "").rstrip("/")
        batch_id = destination_config.get("batch_id") or "batch_all"

        manifest_dir = Path("./manifests") / batch_id
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_file = manifest_dir / "manifest.jsonl"

        # Write artifacts to Azure and local manifest
        for artifact in artifacts:
            chunk_id = artifact.get("chunk_id") or "no_id"
            blob_name = f"{base_path}/{chunk_id}.json" if base_path else f"{chunk_id}.json"
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(
                json.dumps(artifact, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json")
            )

            # Append to local manifest
            with open(manifest_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(artifact, ensure_ascii=False) + "\n")

        logger.info(f"Uploaded {len(artifacts)} artifacts and updated manifest: {manifest_file}")
        return manifest_file

    def trigger_analysis(self, manifest_path, output_dir=None):
        """Run analysis and generate a report"""
        try:
            from analyze_artifacts import ArtifactAnalyzer
        except Exception as e:
            logger.error(f"Failed to import ArtifactAnalyzer: {e}")
            return None

        output_dir = Path(output_dir or Path(manifest_path).parent / "reports")
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            analyzer = ArtifactAnalyzer(manifest_path)
            count = analyzer.load_artifacts()
            if count == 0:
                logger.warning("No artifacts to analyze.")
                return None

            analyzer.analyze()
            reports = analyzer.generate_report(output_dir=output_dir)
            if reports and reports.get("txt_report"):
                logger.info(f"Report generated at: {reports['txt_report']}")
            return reports
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return None

    def upload_manifest_folder(self, local_manifest_dir, destination_config):
        """
        Upload the entire manifest folder (including reports) to Azure Blob Storage.
        This uploads to a SEPARATE 'manifests' folder, NOT inside the artifacts folder.
        
        Structure:
        - Artifacts go to: {container}/{base_path}/artifact_1.json, artifact_2.json, etc.
        - Manifests go to: {container}/{base_path}_manifests/{batch_id}/manifest.jsonl, reports/, etc.
        
        Args:
            local_manifest_dir: Path to the local manifest directory (e.g., ./manifests/batch_xyz)
            destination_config: Destination configuration with container and path
        """
        container_name = destination_config.get("container")
        base_path = destination_config.get("path", "").rstrip("/")
        batch_id = destination_config.get("batch_id", "batch_all")
        
        manifest_dir = Path(local_manifest_dir)
        
        if not manifest_dir.exists():
            logger.warning(f"Manifest directory does not exist: {manifest_dir}")
            return
        
        uploaded_count = 0
        failed_count = 0
        
        # Walk through all files in the manifest directory
        for file_path in manifest_dir.rglob("*"):
            if file_path.is_file():
                try:
                    # Calculate relative path from manifest_dir
                    relative_path = file_path.relative_to(manifest_dir)
                    
                    # Upload to SEPARATE manifests folder (not inside artifacts folder)
                    # Pattern: {base_path}_manifests/{batch_id}/{file}
                    manifest_base = f"{base_path}_manifests" if base_path else "manifests"
                    blob_name = f"{manifest_base}/{batch_id}/{relative_path}"
                    
                    # Convert Windows paths to forward slashes for Azure
                    blob_name = blob_name.replace("\\", "/")
                    
                    # Read and upload file
                    with open(file_path, 'rb') as f:
                        file_data = f.read()
                    
                    blob_client = self.blob_service_client.get_blob_client(
                        container=container_name, 
                        blob=blob_name
                    )
                    
                    blob_client.upload_blob(file_data, overwrite=True)
                    
                    logger.debug(f"Uploaded: {blob_name}")
                    uploaded_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to upload {file_path}: {e}")
                    failed_count += 1
        
        manifest_destination = f"{manifest_base}/{batch_id}" if base_path else f"manifests/{batch_id}"
        logger.info(f"Manifest upload complete: {uploaded_count} files uploaded, {failed_count} failed")
        logger.info(f"Manifests uploaded to: {container_name}/{manifest_destination} (separate from artifacts)")
        
        return {
            "uploaded": uploaded_count,
            "failed": failed_count,
            "destination": f"{container_name}/{manifest_destination}"
        }

    def write_progress_log(self, log_entry, destination_config):
        container_name = destination_config.get("container")
        base_path = destination_config.get("path", "").rstrip("/")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{base_path}/logs/progress_{timestamp}.json" if base_path else f"logs/progress_{timestamp}.json"
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=log_filename)
            blob_client.upload_blob(
                json.dumps(log_entry, ensure_ascii=False, indent=2).encode('utf-8'),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json")
            )
            logger.debug(f"Progress log written to: {log_filename}")
        except Exception as e:
            logger.error(f"Error writing progress log: {e}")

    def get_file_size(self, container_name, file_path):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=file_path)
            return blob_client.get_blob_properties().size
        except Exception as e:
            logger.error(f"Error getting file size for {file_path}: {e}")
            return None

    def folder_exists(self, container, folder_path):
        try:
            blobs = self.blob_service_client.get_container_client(container).list_blobs(name_starts_with=folder_path)
            return any(True for _ in blobs)
        except Exception as e:
            logger.error(f"Error checking folder existence: {e}")
            return False