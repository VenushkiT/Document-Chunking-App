import requests
import json
import logging
from config import AZURE_STORAGE_CONNECTION_STRING

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Flask app URLs
# POST_URL = "http://127.0.0.1:8000/process_batch"
# GET_FAILURES_URL = "http://127.0.0.1:8000/failure_logs"
# Azure Container App URLs
BASE_URL = "https://public-html-chunking.blackglacier-63c3b877.eastus2.azurecontainerapps.io"
POST_URL = f"{BASE_URL}/process_batch"
GET_FAILURES_URL = f"{BASE_URL}/failure_logs"

# Payload for POST
payload = {
    "source": {
        "container": "public-data",
        "path": "ifs-25r1/",
    },
"destination": {
    "container": "testing-datasets",
    "path": "public-document-indexing-pipeline/chunking_experiments/demo_run_01",
},
"options": {
    "batch_id": "fixed_01",
    "max_workers": 6,
    "connection_string": AZURE_STORAGE_CONNECTION_STRING,
    # Chunking strategy options:
    # - fixed_size (default): Uses RecursiveCharacterTextSplitter for token-based splitting.
    #   Fastest method but doesn't preserve document structure.
    # - h1_heading_based: Splits on H1 headings first, falls back to H2 or fixed-size if needed.
    #   Merges small sections and adds continuation markers for better context.
    # - h2_heading_based: Hierarchical splitting using both H1 and H2 headings.
    #   Best for technical docs as it maintains heading hierarchy and context.
    "chunking_strategy": "Fixed_Size",
    "extract_main_content": True,
    "chunk_size": 750,
    "chunk_overlap": 100,
    "preview_mode": True
}
}

headers = {
    "Content-Type": "application/json"
}

# --- POST /process_batch ---
try:
    # 4-hour timeout (14400 seconds)
    response = requests.post(POST_URL, 
                           headers=headers, 
                           data=json.dumps(payload),
                           timeout=14400)
    logger.info("=== POST /process_batch ===")
    logger.info(f"Status Code: {response.status_code}")
    
    if response.status_code == 504:
        logger.info("=== Processing Status ===")
        logger.info("Application is running successfully in the container.")
        logger.info("This is a long-running operation and the request will continue processing.")
        logger.info("The 504 timeout is normal - checking failure logs for any issues...")
    else:
        try:
            logger.info("Response JSON:")
            logger.info(json.dumps(response.json(), indent=4))
        except json.JSONDecodeError:
            if response.text:
                logger.info(f"Raw response: {response.text}")
            else:
                logger.info("No response body returned")
except requests.exceptions.ConnectionError as e:
    logger.error(f"Connection error: {str(e)}")
except requests.exceptions.Timeout as e:
    logger.info("Request timed out - this is normal for long-running operations.")
    logger.info("The process continues in the container.")
except requests.exceptions.RequestException as e:
    logger.error(f"POST request failed: {str(e)}")

# --- GET /failure_logs ---
try:
    response = requests.get(GET_FAILURES_URL, timeout=30)  # 30 seconds timeout for GET
    logger.info("=== GET /failure_logs ===")
    logger.info(f"Status Code: {response.status_code}")
    logger.info("Response JSON:")
    logger.info(json.dumps(response.json(), indent=4))
except requests.exceptions.RequestException as e:
    logger.error(f"GET request failed: {str(e)}")