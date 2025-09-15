import pytest
from pathlib import Path
import sys
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from coordinator.coordinator import Coordinator, DataChunksCollection, MigrationStatusCollection
from config.config import MetaDb

class MockCollection:
    """Mock MongoDB collection"""
    def __init__(self):
        self.insert_many = AsyncMock()
        self.count_documents = AsyncMock(return_value=0)
        self.update_many = AsyncMock()
        self.update_one = AsyncMock()
        self.find_one = AsyncMock()
        self.find = AsyncMock()
        self.delete_many = AsyncMock()

class MockDatabase:
    """Mock MongoDB database"""
    def __init__(self, name: str):
        self.name = name
        self._collections = {
            DataChunksCollection: MockCollection(),
            MigrationStatusCollection: MockCollection()
        }
    
    def __getitem__(self, name):
        return self._collections.get(name, MockCollection())

class MockMotorClient:
    """Mock Motor client for testing"""
    def __init__(self):
        self.admin = AsyncMock()
        self.admin.command = AsyncMock(return_value=True)
        self._dbs = {MetaDb: MockDatabase(MetaDb)}
        
    def __getitem__(self, db_name):
        return self._dbs.get(db_name, MockDatabase(db_name))

    async def close(self):
        pass

@pytest.fixture
def mock_client():
    return MockMotorClient()

@pytest.fixture
def coordinator(mock_client):
    return Coordinator(client=mock_client)

@pytest.mark.asyncio
async def test_split_table_into_chunks(coordinator):
    # Test splitting table into chunks
    await coordinator.split_table_into_chunks(
        source_db="test_source",
        target_db="test_target",
        table_name="test_table",
        total_rows=100,
        pk_column="id"
    )
    
    # Verify insert_many was called
    assert coordinator.client[MetaDb][DataChunksCollection].insert_many.called

@pytest.mark.asyncio
async def test_health_check(coordinator):
    # Test health check
    result = await coordinator.check_health()
    assert result is True
    coordinator.client.admin.command.assert_called_once_with('ping')

@pytest.mark.asyncio
async def test_split_table_invalid_inputs(coordinator):
    # Test splitting table with invalid inputs
    with pytest.raises(ValueError):
        await coordinator.split_table_into_chunks(
            source_db="",
            target_db="target",
            table_name="table",
            total_rows=100,
            pk_column="id"
        )

@pytest.mark.asyncio
async def test_split_table_zero_rows(coordinator):
    # Test splitting table with zero rows
    with pytest.raises(ValueError):
        await coordinator.split_table_into_chunks(
            source_db="source",
            target_db="target",
            table_name="table",
            total_rows=0,
            pk_column="id"
        )

@pytest.mark.asyncio
async def test_cleanup_stale_chunks(coordinator):
    # Test cleanup of stale chunks
    await coordinator.cleanup_stale_chunks(max_age_hours=24)
    collection = coordinator.client[MetaDb][DataChunksCollection]
    collection.update_many.assert_called_once()

@pytest.mark.asyncio
async def test_recover_failed_chunks(coordinator):
    # Test recovery of failed chunks
    collection = coordinator.client[MetaDb][DataChunksCollection]
    collection.update_many.return_value.modified_count = 5
    count = await coordinator.recover_failed_chunks()
    assert count == 5
    collection.update_many.assert_called_once()

@pytest.mark.asyncio
async def test_migration_progress(coordinator):
    """Test migration progress calculation"""
    collection = coordinator.client[MetaDb][DataChunksCollection]
    collection.count_documents.side_effect = [100, 75]  # total, completed
    progress = await coordinator.get_migration_progress()
    assert progress == 75.0

@pytest.mark.asyncio
async def test_mark_migration_state(coordinator):
    """Test marking migration state"""
    migration_id = "test_migration"
    await coordinator.mark_migration_state(migration_id, "InProgress")
    collection = coordinator.client[MetaDb][MigrationStatusCollection]
    collection.update_one.assert_called_once()

@pytest.mark.asyncio
async def test_dry_run_mode(coordinator):
    """Test dry run mode doesn't modify database"""
    await coordinator.split_table_into_chunks(
        source_db="test_source",
        target_db="test_target",
        table_name="test_table",
        total_rows=100,
        pk_column="id",
        dry_run=True
    )
    collection = coordinator.client[MetaDb][DataChunksCollection]
    collection.insert_many.assert_not_called()

@pytest.mark.asyncio
async def test_cleanup_stale_chunks_no_chunks(coordinator):
    """Test cleanup with no stale chunks"""
    collection = coordinator.client[MetaDb][DataChunksCollection]
    collection.update_many.return_value.modified_count = 0
    await coordinator.cleanup_stale_chunks()
    collection.update_many.assert_called_once()

@pytest.mark.asyncio
async def test_health_check_failure(coordinator):
    """Test health check when MongoDB is unavailable"""
    coordinator.client.admin.command.side_effect = Exception("Connection failed")
    result = await coordinator.check_health()
    assert result is False

@pytest.mark.asyncio
async def test_batch_size_validation(coordinator):
    """Test chunk creation with invalid batch size"""
    with pytest.raises(ValueError):
        await coordinator.split_table_into_chunks(
            source_db="test_source",
            target_db="test_target",
            table_name="test_table",
            total_rows=100,
            pk_column="id",
            batch_size=0
        )

@pytest.mark.asyncio
async def test_mark_migration_state(coordinator):
    """Test marking migration state updates correctly"""
    # Arrange
    migration_id = "test_migration"
    state = "InProgress"
    
    # Act
    await coordinator.mark_migration_state(migration_id, state)
    
    # Assert
    coordinator.migration_status.update_one.assert_called_once()
    args = coordinator.migration_status.update_one.call_args[0]
    assert args[0] == {"_id": migration_id}
    assert "Status" in args[1]["$set"]
    assert args[1]["$set"]["Status"] == state

@pytest.mark.asyncio
async def test_migration_progress(coordinator):
    migration_id = "test_migration"
    coordinator.data_chunks.count_documents.side_effect = [10, 5]  # total, completed
    coordinator.migration_status.find_one.return_value = {"Status": "InProgress"}
    
    progress = await coordinator.get_migration_progress(migration_id)
    
    assert progress["total_chunks"] == 10
    assert progress["completed_chunks"] == 5
    assert progress["progress"] == 50.0
    assert progress["status"] == "InProgress"