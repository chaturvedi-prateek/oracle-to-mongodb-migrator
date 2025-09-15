# worker/worker.py

import logging
import time
import signal
import sys
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import cx_Oracle  # or 'import oracledb' for the new driver
from bson import ObjectId
from tenacity import retry, stop_after_attempt, wait_fixed
from config.config import MongoUri, MetaDb, DatabaseMappings, DefaultBatchSize, MaxRetries, WorkerPollInterval

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Collection names
DataChunksCollection = "DataChunks"

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

        # Get Oracle connection details from DatabaseMappings
        source_db_info = next((db for db in DatabaseMappings if db["SourceDb"] == source_db_name), None)
        if not source_db_info:
            logging.error(f"Oracle DB info not found for {source_db_name}")
            self.mark_chunk_failed(chunk["_id"])
            return

        try:
            # Connect to Oracle
            dsn = cx_Oracle.makedsn(
                host=source_db_info["Dsn"].split(":")[0],
                port=int(source_db_info["Dsn"].split(":")[1].split("/")[0]),
                service_name=source_db_info["Dsn"].split("/")[1]
            )
            with cx_Oracle.connect(user=source_db_info["User"], password=source_db_info["Password"], dsn=dsn) as conn:
                cursor = conn.cursor()
                query = f"SELECT * FROM {table_name} WHERE ROWNUM BETWEEN :start_id AND :end_id"
                cursor.execute(query, {"start_id": start_id, "end_id": end_id})
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]

                # Insert into MongoDB
                target_db = self.client[target_db_name]
                target_collection = target_db[table_name]
                docs = [dict(zip(columns, row)) for row in rows]
                if docs:
                    target_collection.insert_many(docs)

            self.mark_chunk_completed(chunk["_id"])
            logging.info(f"Worker {self.worker_id} completed chunk {chunk['_id']} for {source_db_name}.{table_name} (StartId: {start_id}, EndId: {end_id}).")

        except Exception as e:
            logging.error(f"Worker {self.worker_id} failed chunk {chunk['_id']} for {source_db_name}.{table_name}: {e}")
            self.mark_chunk_failed(chunk["_id"])

    def generate_summary_report(self, completed_chunks: int, failed_chunks: int):
        """Generate a summary report of the chunks processed."""
        logging.info(f"Summary: Worker {self.worker_id} processed {completed_chunks} chunks successfully and {failed_chunks} chunks failed.")

    def run(self):
        """Main loop to continuously pick and process chunks."""
        logging.info(f"Worker {self.worker_id} started.")
        completed_chunks = 0
        failed_chunks = 0

        try:
            while True:
                chunk = self.assign_chunk()
                if chunk:
                    try:
                        self.migrate_chunk(chunk)
                        completed_chunks += 1
                    except Exception:
                        failed_chunks += 1
                else:
                    time.sleep(WorkerPollInterval)
        except KeyboardInterrupt:
            logging.info(f"Worker {self.worker_id} shutting down...")
            self.generate_summary_report(completed_chunks, failed_chunks)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start a migration worker.")
    parser.add_argument("--worker-id", type=str, required=True, help="Unique worker ID")
    args = parser.parse_args()

    worker = Worker(args.worker_id)
    worker.run()
