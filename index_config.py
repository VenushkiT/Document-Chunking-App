from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import logging
import os
import requests
import json

from sqlalchemy import true

env_path = find_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
if env_path:
    logging.info(f"Loading .env file from: {Path(env_path).resolve()}")
else:
    logging.warning(".env file not found.")
load_dotenv(dotenv_path=env_path)

AZURE_SEARCH_SERVICE = os.getenv("AZURE_SEARCH_SERVICE")
AZURE_SEARCH_API_KEY = os.getenv("AZURE_SEARCH_API_KEY")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

HEADERS = {
    "Content-Type": "application/json",
    "api-key": AZURE_SEARCH_API_KEY
}

datasource_name = "docs28r1"
index_name = "docs28r1"
indexer_name = "docs28r1"
skillset_name = "docs28r1-skillset"
API_VERSION = "2023-10-01-Preview"

def delete_resource(resource_type, resource_name):
    url = f"{AZURE_SEARCH_SERVICE}/{resource_type}/{resource_name}?api-version={API_VERSION}"
    response = requests.delete(url, headers=HEADERS)
    if response.status_code == 204:
        logging.info(f"{resource_type[:-1].capitalize()} '{resource_name}' deleted successfully.")
    elif response.status_code == 404:
        logging.info(f"{resource_type[:-1].capitalize()} '{resource_name}' does not exist.")
    else:
        logging.warning(f"Failed to delete {resource_type[:-1]} '{resource_name}': {response.status_code}")
    return response

def create_resource(resource_type, payload):
    url = f"{AZURE_SEARCH_SERVICE}/{resource_type}/{payload['name']}?api-version={API_VERSION}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        logging.info(f"{resource_type[:-1].capitalize()} '{payload['name']}' already exists.")
        return response
    response = requests.put(url, headers=HEADERS, data=json.dumps(payload))
    if response.status_code in [200, 201]:
        logging.info(f"{resource_type[:-1].capitalize()} '{payload['name']}' created successfully.")
    else:
        logging.error(f"Failed to create {resource_type[:-1]} '{payload['name']}': {response.status_code}")
        logging.error(response.text)
    return response

logging.info("Cleaning up existing resources...")
delete_resource("indexers", indexer_name)
delete_resource("skillsets", skillset_name)
delete_resource("indexes", index_name)
delete_resource("datasources", datasource_name)

datasource_payload = {
    "name": datasource_name,
    "description": "docs28r1 Data Source",
    "type": "azureblob",
    "credentials": {"connectionString": AZURE_STORAGE_CONNECTION_STRING},
    "container": {
        "name": "testing-datasets",
        "query": "public-document-indexing-pipeline/ifs_public_docs_artifacts_28r1/docs28r1/"
    }
}

index_payload = {
    "name": index_name,
    "fields": [
        {"name": "parent_id", "type": "Edm.String", "sortable": "true", "filterable": "true", "facetable": "true"},
        {"name": "location", "type": "Edm.String"},
        {"name": "chunk_id", "type": "Edm.String", "key": "true", "sortable": "true", "filterable": "true", "facetable": "true", "analyzer": "keyword"},
        {"name": "chunk", "type": "Edm.String"},
        {"name": "vector","type": "Collection(Edm.Single)","searchable": "true","retrievable": "true","dimensions": 1536, "vectorSearchProfile": "vectorConfig"},
        {"name": "title", "type": "Edm.String", "searchable": "true", "retrievable": "true", "filterable": "true"},
        {"name": "name", "type": "Edm.String", "searchable": "true", "retrievable": "true"},
        {"name": "category", "type": "Collection(Edm.String)", "searchable": "true", "retrievable": "true"},
    ],
    "vectorSearch": {
        "algorithms": [
            {"name": "hnsw", "kind": "hnsw"}
        ],
        "profiles": [
            {"name": "vectorConfig", "algorithm": "hnsw"}
        ]
    },
    "semantic": {
        "defaultConfiguration": "ctx-reranking-semantic-config",
        "configurations": [
            {
                "name": "ctx-reranking-semantic-config",
                "prioritizedFields": {
                    "titleField": {"fieldName": "title"},
                    "prioritizedContentFields": [
                        {"fieldName": "chunk"},
                        {"fieldName": "category"}
                    ]
                }
            }
        ]
    }
}

skills = [
    {
        "@odata.type": "#Microsoft.Skills.Text.AzureOpenAIEmbeddingSkill",
        "description": "Generate vector embeddings using Azure OpenAI",
        "resourceUri": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "apiKey": os.getenv("AZURE_OPENAI_KEY"),
        "deploymentId": os.getenv("AZURE_OPENAI_DEPLOYMENT_ID"),
        "name": "azure-openai-embedding-skill",
        "context": "/document",
        "inputs": [
            {"name": "text", "source": "/document/chunk"}
        ],
        "outputs": [
            {"name": "embedding", "targetName": "vector"}
        ]
    }
]

skillset_payload = {
    "name": skillset_name,
    "description": "Generate vector embeddings from pre-processed JSON artifacts",
    "skills": skills,
    "indexProjections": {
        "selectors": [
            {
                "targetIndexName": index_name,
                "parentKeyFieldName": "parent_id",
                "sourceContext": "/document",
                "mappings": [
                    {"name": "location", "source": "/document/location"},
                    {"name": "chunk", "source": "/document/chunk"},
                    {"name": "vector", "source": "/document/vector"},
                    {"name": "title", "source": "/document/title"},
                    {"name": "name", "source": "/document/name"},
                    {"name": "category", "source": "/document/category"}
                ]
            }
        ]
    }
}

indexer_payload = {
    "name": indexer_name,
    "description": "docs28r1 indexer for pre-processed JSON artifacts",
    "dataSourceName": datasource_name,
    "skillsetName": skillset_name,
    "targetIndexName": index_name,
    "schedule": {"interval": "P1D"},
    "parameters": {
        "maxFailedItems": -1,
        "maxFailedItemsPerBatch": -1,
        "configuration": {
            "dataToExtract": "contentAndMetadata",
            "indexedFileNameExtensions": ".json, .JSON",
            "parsingMode": "json",
            "allowSkillsetToReadFileData": True
        }
    },
    "fieldMappings": [],
    "outputFieldMappings": [
        {
            "sourceFieldName": "/document/vector",
            "targetFieldName": "vector"
        }
    ]
    
}

logging.info("Creating resources...")
datasource_response = create_resource("datasources", datasource_payload)
index_response = create_resource("indexes", index_payload)
skillset_response = create_resource("skillsets", skillset_payload)
indexer_response = create_resource("indexers", indexer_payload)

if all(r.status_code in [200, 201] for r in [datasource_response, index_response, skillset_response, indexer_response]):
    logging.info("All resources created successfully!")
else:
    logging.error("Some resources failed to create. Check the errors above.")