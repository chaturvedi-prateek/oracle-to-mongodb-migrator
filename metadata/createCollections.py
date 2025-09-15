# metadata/createCollections.py

import os
import json
import logging
import signal
import sys
from pymongo import MongoClient, ASCENDING
from pymongo.errors import CollectionInvalid, PyMongoError
from typing import List, Dict
from tenacity import retry, stop_after_attempt, wait_fixed
from config import MongoUri, MetadataDb  # Importing from your config.py

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants for collection names
DATA_CHUNKS_COLLECTION = "DataChunks"
CHANGE_LOG_COLLECTION = "ChangeLog"

# Signal handler for graceful shutdown
def handle_exit(signal, frame):
    logging.info("Shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)

def validate_config():
    """Validate MongoDB configuration."""
    if not MongoUri or not MetadataDb:
        raise ValueError("MongoUri or MetadataDb is not configured properly.")

def load_index_definitions(file_path: str) -> Dict[str, List[Dict]]:
    """Load index definitions from a JSON file."""
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(f"Index definitions file '{file_path}' not found.")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing index definitions file: {e}")
        raise

def validate_index_definitions(index_definitions: Dict[str, List[Dict]]):
    """Validate the structure of index definitions."""
    required_keys = ["fields"]
    for collection, indexes in index_definitions.items():
        for index in indexes:
            if not all(key in index for key in required_keys):
                raise ValueError(f"Invalid index definition for collection '{collection}': {index}")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def create_collection(db, collection_name: str, dry_run=False):
    """Create a MongoDB collection if it doesn't already exist."""
    if dry_run:
        logging.info(f"[DRY-RUN] Would create collection '{collection_name}' in database '{db.name}'.")
        return
    try:
        db.create_collection(collection_name)
        logging.info(f"Collection '{collection_name}' created in database '{db.name}'.")
    except CollectionInvalid:
        logging.info(f"Collection '{collection_name}' already exists in database '{db.name}'.")
    except PyMongoError as e:
        logging.error(f"Error creating collection '{collection_name}': {e}")
        raise

def create_indexes(collection, indexes: List[Dict], dry_run=False):
    """Create indexes for a MongoDB collection."""
    for index in indexes:
        if dry_run:
            logging.info(f"[DRY-RUN] Would create index {index['fields']} for collection '{collection.name}'.")
            continue
        try:
            collection.create_index(index["fields"], unique=index.get("unique", False))
            logging.info(f"Index {index['fields']} created for collection '{collection.name}'.")
        except PyMongoError as e:
            logging.error(f"Failed to create index {index['fields']} for collection '{collection.name}': {e}")
            raise

def check_mongo_connection(client: MongoClient):
    """Check if the MongoDB server is reachable."""
    try:
        client.admin.command("ping")
        logging.info("MongoDB connection successful.")
    except PyMongoError as e:
        logging.error(f"MongoDB connection failed: {e}")
        raise

def setup_metadata_collections(db, index_definitions, dry_run=False):
    """Set up metadata collections and their indexes in MongoDB."""
    try:
        # Create and index DataChunks collection
        create_collection(db, DATA_CHUNKS_COLLECTION, dry_run=dry_run)
        create_indexes(db[DATA_CHUNKS_COLLECTION], index_definitions[DATA_CHUNKS_COLLECTION], dry_run=dry_run)

        # Create and index ChangeLog collection
        create_collection(db, CHANGE_LOG_COLLECTION, dry_run=dry_run)
        create_indexes(db[CHANGE_LOG_COLLECTION], index_definitions[CHANGE_LOG_COLLECTION], dry_run=dry_run)

        logging.info("MongoDB metadata setup complete.")
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
    except PyMongoError as e:
        logging.error(f"MongoDB operation failed: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def generate_summary_report(created_collections, created_indexes):
    """Generate a summary report of the collections and indexes created."""
    logging.info("Summary Report:")
    logging.info(f"Collections created: {created_collections}")
    logging.info(f"Indexes created: {created_indexes}")

if __name__ == "__main__":
    # Parse command-line arguments
    import argparse

    parser = argparse.ArgumentParser(description="Setup MongoDB metadata collections.")
    parser.add_argument("--uri", type=str, help="MongoDB URI", default=MongoUri)
    parser.add_argument("--db", type=str, help="Metadata database name", default=MetadataDb)
    parser.add_argument("--dry-run", action="store_true", help="Simulate the operations without making changes.")
    parser.add_argument("--index-file", type=str, help="Path to the index definitions file.", default="index_definitions.json")
    args = parser.parse_args()

    try:
        validate_config()
        index_definitions = load_index_definitions(args.index_file)
        validate_index_definitions(index_definitions)

        with MongoClient(args.uri) as client:
            check_mongo_connection(client)
            db = client[args.db]
            setup_metadata_collections(db, index_definitions, dry_run=args.dry_run)

        if not args.dry_run:
            generate_summary_report([DATA_CHUNKS_COLLECTION, CHANGE_LOG_COLLECTION], index_definitions.keys())
    except Exception as e:
        logging.error(f"Failed to set up metadata collections: {e}")
