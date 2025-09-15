# worker/worker.py

import logging
import time
import signal
import sys
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import oracledb  # Replacing cx_Oracle with oracledb
from bson import ObjectId
from tenacity import retry, stop_after_attempt, wait_fixed
from concurrent.futures import ThreadPoolExecutor
from config.config import (
    MongoUri, MetaDb, DatabaseMappings, DefaultBatchSize,
    MaxRetries, WorkerPollInterval, MaxWorkerThreads
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Collection names
DataChunksCollection = "DataChunks"

# Signal handler for graceful shutdown
def handle_exit(signal, frame):
    logging.info("Worker shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


class Worker:
    def __init__(self, worker_id: str, client: MongoClient = None):
        """Initialize the Worker with a unique ID and MongoDB client."""
        self.worker_id = worker_id
        self.client = client or MongoClient(MongoUri)
        self.meta_db = self.client[MetaDb]
        self.data_chunks = self.meta_db[DataChunksCollection]

    def assign_chunk(self):
        """Atomically assign the next pending chunk to this worker."""
        try:
            chunk = self.data_chunks.find_one_and_update(
                {"Status": "Pending", "RetryCount": {"$lt": MaxRetries}},
                {"$set": {"Status": "InProgress", "AssignedWorker": self.worker_id}},
            )
            if not chunk:
                logging.info(f"No pending chunks available for Worker {self.worker_id}.")
            return chunk
        except PyMongoError as e:
            logging.error(f"Worker {self.worker_id} failed to assign a chunk: {e}")
            return None

    def mark_chunk_completed(self, chunk_id: ObjectId):
        """Mark chunk as completed."""
        try:
            result = self.data_chunks.update_one({"_id": chunk_id}, {"$set": {"Status": "Completed"}})
            if result.modified_count == 0:
                logging.warning(f"No chunk found with ID {chunk_id} to mark as completed.")
        except PyMongoError as e:
            logging.error(f"Failed to mark chunk {chunk_id} as completed: {e}")

    def mark_chunk_failed(self, chunk_id: ObjectId):
        """Mark chunk as failed and increment retry count."""
        try:
            self.data_chunks.update_one(
                {"_id": chunk_id},
                {"$set": {"Status": "Pending", "AssignedWorker": None}, "$inc": {"RetryCount": 1}}
            )
        except PyMongoError as e:
            logging.error(f"Failed to mark chunk {chunk_id} as failed: {e}")

    @retry(stop=stop_after_attempt(MaxRetries), wait=wait_fixed(2))
    def migrate_chunk(self, chunk: dict):
        """Migrate data from Oracle to MongoDB for the given chunk."""
        source_db_name = chunk["SourceDb"]
        target_db_name = chunk["TargetDb"]
        table_name = chunk["TableName"]
        start_id = chunk["StartId"]
        end_id = chunk["EndId"]

        # Get Oracle connection info
        source_db_info = next((db for db in DatabaseMappings if db["SourceDb"] == source_db_name), None)
        if not source_db_info:
            logging.error(f"No Oracle DB info for {source_db_name}")
            self.mark_chunk_failed(chunk["_id"])
            return

        try:
            # Connect to Oracle using oracledb
            with oracledb.connect(
                user=source_db_info["User"],
                password=source_db_info["Password"],
                dsn=source_db_info["Dsn"]
            ) as conn:
                cursor = conn.cursor()

                # Use primary key for efficient chunking
                pk_column = source_db_info.get("PrimaryKey", "ID")
                query = f"SELECT * FROM {table_name} WHERE {pk_column} >= :start_id AND {pk_column} <= :end_id"
                cursor.execute(query, {"start_id": start_id, "end_id": end_id})
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]

                # Insert into MongoDB
                target_db = self.client[target_db_name]
                target_collection = target_db[table_name]
                docs = [dict(zip(columns, row)) for row in rows]
                if docs:
                    start_time = time.time()
                    target_collection.insert_many(docs)
                    duration = time.time() - start_time
                    logging.info(f"Worker {self.worker_id} migrated {len(docs)} rows in {duration:.2f}s "
                                 f"for {source_db_name}.{table_name} (StartId: {start_id}, EndId: {end_id})")

            self.mark_chunk_completed(chunk["_id"])

        except Exception as e:
            logging.error(f"Worker {self.worker_id} failed chunk {chunk['_id']} for {source_db_name}.{table_name}: {e}")
            self.mark_chunk_failed(chunk["_id"])

    def capture_cdc(self, table_name: str, source_db_name: str, target_db_name: str, last_sync_time: str):
        """Poll Oracle for incremental changes since last_sync_time."""
        source_db_info = next((db for db in DatabaseMappings if db["SourceDb"] == source_db_name), None)
        if not source_db_info:
            logging.error(f"No Oracle DB info for {source_db_name} in CDC")
            return

        try:
            with oracledb.connect(
                user=source_db_info["User"],
                password=source_db_info["Password"],
                dsn=source_db_info["Dsn"]
            ) as conn:
                cursor = conn.cursor()
                query = f"SELECT * FROM {table_name} WHERE LAST_UPDATED > TO_TIMESTAMP(:last_sync_time, 'YYYY-MM-DD HH24:MI:SS')"
                cursor.execute(query, {"last_sync_time": last_sync_time})
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]
                docs = [dict(zip(columns, row)) for row in rows]
                if docs:
                    target_db = self.client[target_db_name]
                    target_collection = target_db[table_name]
                    target_collection.insert_many(docs)
        except Exception as e:
            logging.error(f"CDC failed for {source_db_name}.{table_name}: {e}")

    def run(self):
        """Main loop to continuously pick and process chunks concurrently."""
        logging.info(f"Worker {self.worker_id} started.")
        with ThreadPoolExecutor(max_workers=MaxWorkerThreads) as executor:
            try:
                while True:
                    chunk = self.assign_chunk()
                    if chunk:
                        executor.submit(self.migrate_chunk, chunk)
                    else:
                        time.sleep(WorkerPollInterval)
            except KeyboardInterrupt:
                logging.info(f"Worker {self.worker_id} shutting down...")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start a migration worker.")
    parser.add_argument("--worker-id", type=str, required=True, help="Unique worker ID")
    args = parser.parse_args()

    worker = Worker(args.worker_id)
    worker.run()
