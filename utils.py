from langchain_text_splitters import RecursiveCharacterTextSplitter
import hashlib
import base64 

def generate_artifact(chunks, metadata_paths, html_title, metadata_storage_path):
    """
    Create artifact dicts from chunks and metadata.

    Args:
        chunks (list of dict): Each dict with keys "url", "chunk", "position"
        metadata_paths (list of str): Categories or tags extracted from metadata
        html_title (str): Title from HTML <title> tag
        metadata_storage_path (str): Original metadata storage path

    Returns:
        List of artifact dicts matching your structure
    """
    artifacts = []
    for chunk in chunks:
        url = chunk.get("url", "")
        text = chunk.get("chunk", "")
        pos = chunk.get("position", 0)

        # Derive a simple name from the URL (e.g., filename)
        name = url.split("/")[-1] if url else "unknown"

        # parent_id is base64 of metadata_storage_path
        parent_id = base64.b64encode(metadata_storage_path.encode("utf-8")).decode("utf-8")

        # Create a unique chunk_id (e.g., hash of parent_id + position)
        chunk_id = hashlib.sha256((parent_id + str(pos)).encode('utf-8')).hexdigest()

        artifact = {
            "title": html_title, 
            "parent_id": parent_id,
            "location": url,
            "chunk": text,
            "category": metadata_paths,
            "name": name,
            "chunk_id": chunk_id,
        }
        artifacts.append(artifact)
    return artifacts