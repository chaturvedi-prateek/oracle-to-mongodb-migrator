import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from bson import ObjectId
from worker.worker import Worker, DataChunksCollection
from config.config import MetaDb

# Add the project root to sys.path for imports (optional if running from project root)
sys.path.append(str(Path(__file__).resolve().parents[1]))

class MockCollection:
    """Mock MongoDB collection"""
    def __init__(self):
        self.find_one_and_update = AsyncMock()
        self.update_one = AsyncMock()
        self.insert_many = AsyncMock()

class MockDatabase:
    """Mock MongoDB database"""
    def __init__(self, name: str):
        self.name = name
        self._collections = {
            DataChunksCollection: MockCollection()
        }

    def __getitem__(self, name):
        return self._collections.get(name, MockCollection())

class MockClient:
    """Mock MongoDB client"""
    def __init__(self):
        self._dbs = {MetaDb: MockDatabase(MetaDb)}

    def __getitem__(self, db_name):
        return self._dbs.get(db_name, MockDatabase(db_name))

@pytest.fixture
def mock_client():
    return MockClient()

@pytest.fixture
def worker(mock_client):
    return Worker(worker_id="test_worker", client=mock_client)

@pytest.mark.asyncio
async def test_assign_chunk(worker):
    """Test assigning a chunk to the worker"""
    mock_chunk = {"_id": ObjectId(), "Status": "Pending"}
    collection = worker.data_chunks
    collection.find_one_and_update.return_value = mock_chunk

    chunk = worker.assign_chunk()
    result = await chunk

    assert result == mock_chunk
    collection.find_one_and_update.assert_called_once()

@pytest.mark.asyncio
async def test_mark_chunk_completed(worker):
    """Test marking chunk completed"""
    chunk_id = ObjectId()
    await worker.mark_chunk_completed(chunk_id)
    collection = worker.data_chunks
    collection.update_one.assert_called_once_with({"_id": chunk_id}, {"$set": {"Status": "Completed"}})

@pytest.mark.asyncio
async def test_mark_chunk_failed(worker):
    """Test marking chunk failed"""
    chunk_id = ObjectId()
    await worker.mark_chunk_failed(chunk_id)
    collection = worker.data_chunks
    collection.update_one.assert_called_once_with(
        {"_id": chunk_id},
        {"$set": {"Status": "Pending", "AssignedWorker": None}, "$inc": {"RetryCount": 1}}
    )

@pytest.mark.asyncio
async def test_migrate_chunk_success(worker, monkeypatch):
    """Test migrating a chunk successfully"""

    # Mock chunk data
    chunk = {
        "_id": ObjectId(),
        "SourceDb": "source_db",
        "TargetDb": "target_db",
        "TableName": "test_table",
        "StartId": 1,
        "EndId": 10
    }

    # Mock DatabaseMappings
    monkeypatch.setattr(worker, "client", worker.client)
    from config import config
    config.DatabaseMappings = [
        {
            "SourceDb": "source_db",
            "Dsn": "localhost:1521/test",
            "User": "user",
            "Password": "pass",
            "PrimaryKey": "ID"
        }
    ]

    # Mock oracledb connection
    class MockCursor:
        def __init__(self):
            self.description = [("ID",), ("NAME",)]
        def execute(self, query, params):
            pass
        def fetchall(self):
            return [(1, "Alice"), (2, "Bob")]
