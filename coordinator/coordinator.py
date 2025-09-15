# coordinator/coordinator.py

import logging
import time
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from config.config import MongoUri, MetaDb, OracleDatabases, DatabaseMappings, DefaultBatchSize, MaxRetries, WorkerPollInterval

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Collection names
DataChunksCollection = "DataChunks"

class Coordinator:
    def __init__(self):
        self.client = MongoClient(MongoUri)
        self.db = self.client[MetaDb]
        self.data_chunks = self.db[DataChunksCollection]

    def split_table_into_chunks(self, source_db: str, target_db: str, table_name: str, total_rows: int, batch_size: int = DefaultBatchSize):
        """Split a table into chunks and insert into DataChunks collection."""
        chunks = []
        for start_id in range(1, total_rows + 1, batch_size):
            end_id = min(start_id + batch_size - 1, total_rows)
            chunk = {
                "SourceDb": source_db,
                "TargetDb": target_db,
                "TableName": table_name,
                "StartId": start_id,
                "EndId": end_id,
                "LastProcessedId": None,
                "Status": "Pending",
                "AssignedWorker": None,
                "RetryCount": 0
            }
            chunks.append(chunk)
        if chunks:
            try:
                self.data_chunks.insert_many(chunks, ordered=False)
                logging.info(f"Inserted {len(chunks)} chunks for {source_db}.{table_name} → {target_db}")
            except PyMongoError as e:
                logging.error(f"Failed to insert chunks for {source_db}.{table_name}: {e}")

    def assign_chunk_to_worker(self, worker_id: str):
        """Atomically assign the next pending chunk to a worker."""
        chunk = self.data_chunks.find_one_and_update(
            {"Status": "Pending"},
            {"$set": {"Status": "InProgress", "AssignedWorker": worker_id}},
            sort=[("StartId", ASCENDING)]
        )
        return chunk

    def mark_chunk_completed(self, chunk_id):
        """Mark a chunk as completed."""
        self.data_chunks.update_one({"_id": chunk_id}, {"$set": {"Status": "Completed"}})

    def mark_chunk_failed(self, chunk_id):
        """Mark a chunk as failed and increment retry count."""
        self.data_chunks.update_one(
            {"_id": chunk_id},
            {"$set": {"Status": "Pending"}, "$inc": {"RetryCount": 1}}
        )

    def generate_chunks_for_all_databases(self):
        """Generate chunks for all source Oracle databases and their corresponding MongoDB targets."""
        for mapping in DatabaseMappings:
            source_db = mapping["SourceDb"]
            target_db = mapping["TargetDb"]
            # TODO: Fetch list of tables and row counts from Oracle
            tables = self.get_tables_from_oracle(source_db)
            for table_name, total_rows in tables.items():
                self.split_table_into_chunks(source_db, target_db, table_name, total_rows)

    def get_tables_from_oracle(self, source_db: str) -> dict:
        """Stub: fetch table names and row counts from Oracle database."""
        # Replace this stub with actual Oracle queries
        # Example return format: {"EMPLOYEES": 100000, "DEPARTMENTS": 500}
        return {"EMPLOYEES": 100000, "DEPARTMENTS": 500}

    def run(self):
        """Main loop for coordinator (can be expanded to assign workers dynamically)."""
        logging.info("Coordinator started. Generating chunks...")
        self.generate_chunks_for_all_databases()
        logging.info("All chunks generated.")

        # Example loop to assign chunks (can be replaced by REST API or messaging system)
        while True:
            # For demo purposes, just sleep
            time.sleep(WorkerPollInterval)


if __name__ == "__main__":
    coordinator = Coordinator()
    coordinator.run()
