# coordinator/coordinator.py

import logging
import time
import signal
import sys
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from tenacity import retry, stop_after_attempt, wait_fixed
from worker.worker import Worker  # Import Worker to use capture_cdc
from config.config import (
    MongoUri, MetaDb, DatabaseMappings, DefaultBatchSize,
    MaxRetries, WorkerPollInterval, CdcPollInterval
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Collection names
DataChunksCollection = "DataChunks"
MigrationStatusCollection = "MigrationStatus"

# Signal handler for graceful shutdown
def handle_exit(signal, frame):
    logging.info("Coordinator shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


class Coordinator:
    def __init__(self, client: MongoClient = None):
        """Initialize the Coordinator with MongoDB client."""
        self.client = client or MongoClient(MongoUri)
        self.db = self.client[MetaDb]
        self.data_chunks = self.db[DataChunksCollection]
        self.migration_status = self.db[MigrationStatusCollection]

    def validate_inputs(self, source_db: str, target_db: str, table_name: str, total_rows: int):
        if not source_db or not target_db or not table_name:
            raise ValueError("SourceDb, TargetDb, and TableName must be provided.")
        if total_rows <= 0:
            raise ValueError(f"Invalid total_rows value: {total_rows} for table {table_name}.")

    def split_table_into_chunks(self, source_db: str, target_db: str, table_name: str, total_rows: int,
                                pk_column: str, batch_size: int = DefaultBatchSize, dry_run=False):
        self.validate_inputs(source_db, target_db, table_name, total_rows)

        chunks = [
            {
                "SourceDb": source_db,
                "TargetDb": target_db,
                "TableName": table_name,
                "PrimaryKey": pk_column,
                "StartId": start_id,
                "EndId": min(start_id + batch_size - 1, total_rows),
                "LastProcessedId": None,
                "Status": "Pending",
                "AssignedWorker": None,
                "RetryCount": 0,
                "LastProcessedTimestamp": None
            }
            for start_id in range(1, total_rows + 1, batch_size)
        ]

        if dry_run:
            logging.info(f"[DRY-RUN] Would insert {len(chunks)} chunks for {source_db}.{table_name} → {target_db}.")
            return

        if chunks:
            try:
                self.data_chunks.insert_many(chunks, ordered=False)
                logging.info(f"Inserted {len(chunks)} chunks for {source_db}.{table_name} → {target_db}")
            except PyMongoError as e:
                logging.error(f"Failed to insert chunks for {source_db}.{table_name}: {e}")
                raise

    @retry(stop=stop_after_attempt(MaxRetries), wait=wait_fixed(2))
    def mark_migration_state(self, migration_id: str, state: str):
        try:
            self.migration_status.update_one(
                {"MigrationId": migration_id},
                {"$set": {"State": state, "LastUpdated": time.time()}},
                upsert=True
            )
            logging.info(f"Migration state updated to '{state}' for migration ID '{migration_id}'.")
        except PyMongoError as e:
            logging.error(f"Failed to update migration state for migration ID '{migration_id}': {e}")
            raise

    def get_tables_from_oracle(self, source_db: str) -> dict:
        """Stub: fetch table names, row counts, and primary keys from Oracle database."""
        return {
            "EMPLOYEES": {"TotalRows": 100000, "PrimaryKey": "EMP_ID"},
            "DEPARTMENTS": {"TotalRows": 500, "PrimaryKey": "DEPT_ID"}
        }

    def generate_chunks_for_all_databases(self, dry_run=False):
        for mapping in DatabaseMappings:
            source_db = mapping["SourceDb"]
            target_db = mapping["TargetDb"]
            tables = self.get_tables_from_oracle(source_db)
            for table_name, table_info in tables.items():
                self.split_table_into_chunks(
                    source_db,
                    target_db,
                    table_name,
                    table_info["TotalRows"],
                    pk_column=table_info["PrimaryKey"],
                    dry_run=dry_run
                )

    def run_cdc(self):
        """Continuously capture incremental changes for all tables until cutover."""
        worker = Worker("CDCWorker", client=self.client)
        logging.info("Starting CDC polling loop...")

        while True:
            for mapping in DatabaseMappings:
                source_db = mapping["SourceDb"]
                target_db = mapping["TargetDb"]
                tables = self.get_tables_from_oracle(source_db)
                for table_name in tables.keys():
                    # Get last sync timestamp for resumability
                    last_sync_doc = self.db["DataChunks"].find_one(
                        {"SourceDb": source_db, "TableName": table_name},
                        sort=[("LastProcessedTimestamp", -1)]
                    )
                    last_sync_time = last_sync_doc["LastProcessedTimestamp"] if last_sync_doc else "1970-01-01 00:00:00"
                    worker.capture_cdc(table_name, source_db, target_db, last_sync_time)

            time.sleep(CdcPollInterval)

    def run(self, dry_run=False):
        """Main coordinator loop."""
        migration_id = "migration_001"
        logging.info("Coordinator started.")

        # Mark migration in progress
        self.mark_migration_state(migration_id, "InProgress")

        # Generate chunks if not already present
        if self.data_chunks.count_documents({}) == 0:
            logging.info("Generating migration chunks...")
            self.generate_chunks_for_all_databases(dry_run=dry_run)
        else:
            logging.info("Chunks already exist; resuming migration...")

        if dry_run:
            logging.info("[DRY-RUN] Exiting without making changes.")
            return

        # Start CDC polling in parallel
        import threading
        cdc_thread = threading.Thread(target=self.run_cdc, daemon=True)
        cdc_thread.start()

        # Monitor chunk progress
        try:
            while True:
                pending_count = self.data_chunks.count_documents({"Status": "Pending"})
                inprogress_count = self.data_chunks.count_documents({"Status": "InProgress"})
                logging.info(f"Migration Status: Pending={pending_count}, InProgress={inprogress_count}")

                if pending_count == 0 and inprogress_count == 0:
                    logging.info("All chunks processed. Migration ready for cutover.")
                    self.mark_migration_state(migration_id, "CutoverPending")
                    break

                time.sleep(WorkerPollInterval)

            logging.info("Coordinator exiting. Migration ready for final cutover.")

        except KeyboardInterrupt:
            logging.info("Coordinator interrupted. Exiting...")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start the migration coordinator.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate operations without making changes.")
    args = parser.parse_args()

    coordinator = Coordinator()
    coordinator.run(dry_run=args.dry_run)
