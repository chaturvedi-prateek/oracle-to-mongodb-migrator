# coordinator/coordinator.py

import asyncio
import logging
import certifi
from functools import wraps
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from pymongo.errors import PyMongoError
from pymongo import AsyncMongoClient
from prometheus_client import Counter, Gauge, start_http_server
from tenacity import retry, stop_after_attempt, wait_fixed
from config.config import (
    MongoUri, MetaDb, DatabaseMappings, DefaultBatchSize,
    MaxRetries, WorkerPollInterval, MetricsPort, MaxPoolSize
)
import structlog

# Collection names
DataChunksCollection = "DataChunks"
MigrationStatusCollection = "MigrationStatus"

# Configure structured logging
logger = structlog.get_logger()

# Configure metrics
start_http_server(MetricsPort)

@dataclass
class MigrationProgress:
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    start_time: datetime
    last_updated: datetime

    def calculate_progress(self) -> float:
        return (self.completed_chunks / self.total_chunks) * 100 if self.total_chunks > 0 else 0

def circuit_breaker(failure_threshold: int = 3, reset_timeout: int = 60):
    def decorator(func):
        failures = 0
        last_failure_time = None

        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal failures, last_failure_time
            if failures >= failure_threshold:
                if datetime.now(timezone.utc) - last_failure_time < timedelta(seconds=reset_timeout):
                    raise Exception("Circuit breaker open")
                failures = 0
            try:
                return await func(*args, **kwargs)
            except Exception:
                failures += 1
                last_failure_time = datetime.now(timezone.utc)
                raise
        return wrapper
    return decorator

class Coordinator:
    _metrics_initialized = False
    _chunks_created = None
    _chunks_completed = None
    _migration_progress = None

    def __init__(self, client: AsyncMongoClient = None):
        """Initialize Coordinator with async MongoDB client."""
        self.client = client or AsyncMongoClient(
            MongoUri,
            maxPoolSize=MaxPoolSize,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=60000,
            connectTimeoutMS=60000
        )
        self.db = self.client[MetaDb]
        self.data_chunks = self.db[DataChunksCollection]
        self.migration_status = self.db[MigrationStatusCollection]
        self.logger = logger.bind(component="coordinator")

        if not Coordinator._metrics_initialized:
            Coordinator._chunks_created = Counter(
                'migration_chunks_created_total',
                'Total number of migration chunks created'
            )
            Coordinator._chunks_completed = Counter(
                'migration_chunks_completed_total',
                'Total number of migration chunks completed'
            )
            Coordinator._migration_progress = Gauge(
                'migration_progress_percentage',
                'Current migration progress as a percentage'
            )
            Coordinator._metrics_initialized = True

        self.chunks_created = self._chunks_created
        self.chunks_completed = self._chunks_completed
        self.migration_progress = self._migration_progress

    async def check_health(self) -> bool:
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            self.logger.error("health_check_failed", error=str(e))
            return False

    def validate_configuration(self):
        required_configs = ['MongoUri', 'MetaDb', 'DatabaseMappings']
        for config in required_configs:
            if not globals().get(config):
                raise ValueError(f"Missing required configuration: {config}")

    def validate_inputs(self, source_db: str, target_db: str, table_name: str, total_rows: int):
        if not source_db or not target_db or not table_name:
            raise ValueError("SourceDb, TargetDb, and TableName must be provided")
        if total_rows <= 0:
            raise ValueError(f"Invalid total_rows value: {total_rows} for table {table_name}")

    @circuit_breaker()
    async def split_table_into_chunks(
        self,
        source_db: str,
        target_db: str,
        table_name: str,
        total_rows: int,
        pk_column: str,
        batch_size: int = DefaultBatchSize,
        dry_run: bool = False
    ):
        self.validate_inputs(source_db, target_db, table_name, total_rows)
        chunks = [
            {
                "SourceDb": source_db,
                "TargetDb": target_db,
                "TableName": table_name,
                "PrimaryKey": pk_column,
                "StartId": start_id,
                "EndId": min(start_id + batch_size - 1, total_rows),
                "Status": "Pending",
                "AssignedWorker": None,
                "RetryCount": 0,
                "LastUpdated": datetime.now(timezone.utc)
            }
            for start_id in range(1, total_rows + 1, batch_size)
        ]

        if dry_run:
            self.logger.info(
                "dry_run_chunks_created",
                source_db=source_db,
                target_db=target_db,
                table_name=table_name,
                chunk_count=len(chunks)
            )
            return

        try:
            await self.data_chunks.insert_many(chunks)
            self.chunks_created.inc(len(chunks))
            self.logger.info(
                "chunks_created",
                source_db=source_db,
                target_db=target_db,
                table_name=table_name,
                chunk_count=len(chunks)
            )
        except PyMongoError as e:
            self.logger.error(
                "chunk_creation_failed",
                source_db=source_db,
                target_db=target_db,
                table_name=table_name,
                error=str(e)
            )
            raise

    async def cleanup_stale_chunks(self, max_age_hours: int = 24):
        stale_threshold = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        result = await self.data_chunks.update_many(
            {
                "Status": "InProgress",
                "LastUpdated": {"$lt": stale_threshold}
            },
            {"$set": {"Status": "Pending", "AssignedWorker": None}}
        )
        self.logger.info("stale_chunks_reset", count=result.modified_count)

    async def recover_failed_chunks(self):
        result = await self.data_chunks.update_many(
            {"Status": "Failed", "RetryCount": {"$lt": MaxRetries}},
            {"$set": {"Status": "Pending"}, "$inc": {"RetryCount": 1}}
        )
        self.logger.info("failed_chunks_recovered", count=result.modified_count)
        return result.modified_count

    async def mark_migration_state(self, migration_id: str, state: str) -> None:
        try:
            await self.migration_status.update_one(
                {"_id": migration_id},
                {"$set": {"Status": state, "LastUpdated": datetime.now(timezone.utc)}},
                upsert=True
            )
            self.logger.info("migration_state_updated", migration_id=migration_id, state=state)
        except PyMongoError as e:
            self.logger.error("migration_state_update_failed", migration_id=migration_id, state=state, error=str(e))
            raise

    async def get_migration_progress(self, migration_id: str) -> dict:
        try:
            total_chunks = await self.data_chunks.count_documents({"MigrationId": migration_id})
            completed_chunks = await self.data_chunks.count_documents({
                "MigrationId": migration_id,
                "Status": "Completed"
            })
            status = await self.migration_status.find_one({"_id": migration_id})
            return {
                "migration_id": migration_id,
                "total_chunks": total_chunks,
                "completed_chunks": completed_chunks,
                "status": status.get("Status") if status else "Unknown",
                "progress": (completed_chunks / total_chunks * 100) if total_chunks > 0 else 0
            }
        except PyMongoError as e:
            self.logger.error("get_migration_progress_failed", migration_id=migration_id, error=str(e))
            raise

    async def run(self, dry_run: bool = False):
        self.validate_configuration()

        if not await self.check_health():
            self.logger.error("coordinator_startup_failed")
            return

        try:
            while True:
                await self.cleanup_stale_chunks()
                await self.recover_failed_chunks()
                total_chunks = await self.data_chunks.count_documents({})
                completed_chunks = await self.data_chunks.count_documents({"Status": "Completed"})
                self.migration_progress.set((completed_chunks / total_chunks * 100) if total_chunks > 0 else 0)
                await asyncio.sleep(WorkerPollInterval)

        except KeyboardInterrupt:
            self.logger.info("coordinator_shutting_down")
            await self.client.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start the migration coordinator")
    parser.add_argument("--dry-run", action="store_true", help="Simulate operations without making changes")
    args = parser.parse_args()

    asyncio.run(Coordinator().run(dry_run=args.dry_run))
