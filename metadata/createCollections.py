# metadata/createCollections.py

import json
import logging
import sys
import signal
from typing import List, Dict
from pymongo.errors import CollectionInvalid, PyMongoError
from pymongo import ASCENDING
from pymongo import WriteConcern
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.mongo_client import AsyncMongoClient
from tenacity import retry, stop_after_attempt, wait_fixed
from config.config import MongoUri, MetadataDb  # Importing from your config.py
import certifi
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants for collection names
DATA_CHUNKS_COLLECTION = "DataChunks"
CHANGE_LOG_COLLECTION = "ChangeLog"

# Signal handler for graceful shutdown
def handle_exit(signal_num, frame):
    logging.info("Shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

def validate_config():
    """Validate MongoDB configuration."""
    if not MongoUri or not MetadataDb:
        raise ValueError("MongoUri or MetadataDb is not configured properly.")

async def load_index_definitions(file_path: str) -> Dict[str, List[Dict]]:
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
async def create_collection(db: Database, collection_name: str, dry_run=False):
    """Create a MongoDB collection if it doesn't already exist."""
    if dry_run:
        logging.info(f"[DRY-RUN] Would create collection '{collection_name}' in database '{db.name}'.")
        return
    try:
        await db.create_collection(collection_name)
        logging.info(f"Collection '{collection_name}' created in database '{db.name}'.")
    except CollectionInvalid:
        logging.info(f"Collection '{collection_name}' already exists in database '{db.name}'.")
    except PyMongoError as e:
        logging.error(f"Error creating collection '{collection_name}': {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def create_indexes(collection: Collection, indexes: List[Dict], dry_run=False):
    """Create indexes for a MongoDB collection."""
    for index in indexes:
        if dry_run:
            logging.info(f"[DRY-RUN] Would create index {index['fields']} for collection '{collection.name}'.")
            continue
        try:
            await collection.create_index(index["fields"], unique=index.get("unique", False))
            logging.info(f"Index {index['fields']} created for collection '{collection.name}'.")
        except PyMongoError as e:
            logging.error(f"Failed to create index {index['fields']} for collection '{collection.name}': {e}")
            raise

async def check_mongo_connection(client: AsyncMongoClient):
    """Check if the MongoDB server is reachable."""
    try:
        await client.admin.command("ping")
        logging.info("MongoDB connection successful.")
    except PyMongoError as e:
        logging.error(f"MongoDB connection failed: {e}")
        raise

async def setup_metadata_collections(db: Database, index_definitions: Dict[str, List[Dict]], dry_run=False):
    """Set up metadata collections and their indexes in MongoDB."""
    try:
        await create_collection(db, DATA_CHUNKS_COLLECTION, dry_run=dry_run)
        await create_indexes(db[DATA_CHUNKS_COLLECTION], index_definitions.get(DATA_CHUNKS_COLLECTION, []), dry_run=dry_run)

        await create_collection(db, CHANGE_LOG_COLLECTION, dry_run=dry_run)
        await create_indexes(db[CHANGE_LOG_COLLECTION], index_definitions.get(CHANGE_LOG_COLLECTION, []), dry_run=dry_run)

        logging.info("MongoDB metadata setup complete.")
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
    except PyMongoError as e:
        logging.error(f"MongoDB operation failed: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

async def create_metadata(dry_run=False, index_file="index_definitions.json"):
    """Main entry point to create metadata collections."""
    validate_config()
    index_definitions = await load_index_definitions(index_file)
    validate_index_definitions(index_definitions)

    client = AsyncMongoClient(
        MongoUri,
        tls=True,
        tlsCAFile=certifi.where(),
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=60000
    )

    await check_mongo_connection(client)
    db = client[MetadataDb]
    await setup_metadata_collections(db, index_definitions, dry_run=dry_run)
    await client.close()
    logging.info("Metadata creation process completed.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Setup MongoDB metadata collections.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate the operations without making changes.")
    parser.add_argument("--index-file", type=str, default="index_definitions.json", help="Path to index definitions file.")
    args = parser.parse_args()

    asyncio.run(create_metadata(dry_run=args.dry_run, index_file=args.index_file))