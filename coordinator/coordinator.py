# coordinator/coordinator.py

import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
from prometheus_client import Counter, Gauge, start_http_server
from tenacity import retry, stop_after_attempt, wait_fixed
import structlog
from config.config import (
    MongoUri, MetaDb, DatabaseMappings, DefaultBatchSize,
    MaxRetries, WorkerPollInterval, CdcPollInterval, MaxPoolSize, MetricsPort
)

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
                if datetime.now() - last_failure_time < timedelta(seconds=reset_timeout):
                    raise Exception("Circuit breaker open")
                failures = 0
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                failures += 1
                last_failure_time = datetime.now()
                raise
        return wrapper
    return decorator

class Coordinator:
    def __init__(self, client: AsyncIOMotorClient = None):
        self.client = client or AsyncIOMotorClient(MongoUri, maxPoolSize=MaxPoolSize)
        self.db = self.client[MetaDb]
        self.data_chunks = self.db[DataChunksCollection]
        self.migration_status = self.db[MigrationStatusCollection]
        self.logger = logger.bind(component="coordinator")
        
        # Metrics
        self.chunks_created = Counter('chunks_created_total', 'Total chunks created')
        self.chunks_completed = Counter('chunks_completed_total', 'Total chunks completed')
        self.migration_progress = Gauge('migration_progress_percent', 'Migration progress')

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
                "LastUpdated": datetime.utcnow()
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
        stale_threshold = datetime.utcnow() - timedelta(hours=max_age_hours)
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

    async def run(self, dry_run: bool = False):
        self.validate_configuration()
        
        if not await self.check_health():
            self.logger.error("coordinator_startup_failed")
            return

        try:
            while True:
                await self.cleanup_stale_chunks()
                await self.recover_failed_chunks()
                
                # Update progress metrics
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
