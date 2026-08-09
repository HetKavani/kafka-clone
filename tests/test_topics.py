import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import Topic, Partition

@pytest.mark.asyncio
async def test_create_topic(client: AsyncClient, db: AsyncSession):
    # Test creation
    payload = {"name": "test-topic-1", "partition_count": 3}
    response = await client.post("/api/v1/topics/", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "test-topic-1"
    assert data["partition_count"] == 3
    assert "id" in data

    # Verify db state
    stmt = select(Topic).where(Topic.name == "test-topic-1")
    res = await db.execute(stmt)
    topic = res.scalar_one_or_none()
    assert topic is not None
    assert topic.partition_count == 3

    # Verify partitions were created automatically
    stmt_parts = select(Partition).where(Partition.topic_id == topic.id)
    res_parts = await db.execute(stmt_parts)
    partitions = res_parts.scalars().all()
    assert len(partitions) == 3
    for i, part in enumerate(sorted(partitions, key=lambda x: x.partition_number)):
        assert part.partition_number == i
        assert part.next_offset == 0

@pytest.mark.asyncio
async def test_create_topic_duplicate(client: AsyncClient):
    payload = {"name": "duplicate-topic", "partition_count": 2}
    resp1 = await client.post("/api/v1/topics/", json=payload)
    assert resp1.status_code == 201

    resp2 = await client.post("/api/v1/topics/", json=payload)
    assert resp2.status_code == 400
    assert "already exists" in resp2.json()["detail"]

@pytest.mark.asyncio
async def test_list_topics(client: AsyncClient):
    await client.post("/api/v1/topics/", json={"name": "topic-a", "partition_count": 1})
    await client.post("/api/v1/topics/", json={"name": "topic-b", "partition_count": 2})

    response = await client.get("/api/v1/topics/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    names = [t["name"] for t in data]
    assert "topic-a" in names
    assert "topic-b" in names

@pytest.mark.asyncio
async def test_get_topic(client: AsyncClient):
    await client.post("/api/v1/topics/", json={"name": "topic-c", "partition_count": 4})
    
    response = await client.get("/api/v1/topics/topic-c")
    assert response.status_code == 200
    assert response.json()["name"] == "topic-c"
    assert response.json()["partition_count"] == 4

    response_missing = await client.get("/api/v1/topics/missing-topic")
    assert response_missing.status_code == 404

@pytest.mark.asyncio
async def test_delete_topic(client: AsyncClient, db: AsyncSession):
    await client.post("/api/v1/topics/", json={"name": "delete-me", "partition_count": 2})
    
    response = await client.delete("/api/v1/topics/delete-me")
    assert response.status_code == 204

    # Verify deleted
    stmt = select(Topic).where(Topic.name == "delete-me")
    res = await db.execute(stmt)
    assert res.scalar_one_or_none() is None
