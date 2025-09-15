import pytest
from unittest.mock import Mock, AsyncMock
from motor.motor_asyncio import AsyncIOMotorClient
from coordinator.coordinator import Coordinator

@pytest.fixture
async def mock_coordinator():
    mock_client = AsyncMock(spec=AsyncIOMotorClient)
    return Coordinator(client=mock_client)

@pytest.mark.asyncio
async def test_split_table_into_chunks(mock_coordinator):
    await mock_coordinator.split_table_into_chunks(
        source_db="test_source",
        target_db="test_target",
        table_name="test_table",
        total_rows=100,
        pk_column="id"
    )
    assert mock_coordinator.chunks_created._value == 1

@pytest.mark.asyncio
async def test_health_check(mock_coordinator):
    mock_coordinator.client.admin.command.return_value = True
    assert await mock_coordinator.check_health() is True