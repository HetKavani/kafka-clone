import pytest
import asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models import Event

@pytest.mark.asyncio
async def test_publish_explicit_partition(client: AsyncClient, db: AsyncSession):
    # 1. Create topic with 3 partitions
    await client.post("/api/v1/topics/", json={"name": "my-topic", "partition_count": 3})

    # 2. Publish to partition 2
    payload = {"key": "user-1", "value": {"event": "login"}, "partition": 2, "acks": "leader"}
    response = await client.post("/api/v1/topics/my-topic/publish", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["topic"] == "my-topic"
    assert data["partition"] == 2
    assert data["offset"] == 0

    # 3. Publish again to partition 2, offset should be 1
    response = await client.post("/api/v1/topics/my-topic/publish", json=payload)
    assert response.status_code == 201
    assert response.json()["offset"] == 1

@pytest.mark.asyncio
async def test_publish_key_hashing(client: AsyncClient):
    await client.post("/api/v1/topics/", json={"name": "hash-topic", "partition_count": 5})

    # Test key hashing partition routing consistency
    payload1 = {"key": "stable-key", "value": "msg1", "acks": "leader"}
    resp1 = await client.post("/api/v1/topics/hash-topic/publish", json=payload1)
    assert resp1.status_code == 201
    part1 = resp1.json()["partition"]

    resp2 = await client.post("/api/v1/topics/hash-topic/publish", json=payload1)
    assert resp2.status_code == 201
    part2 = resp2.json()["partition"]
    
    # Partition should be identical for the same key
    assert part1 == part2
    assert 0 <= part1 < 5

@pytest.mark.asyncio
async def test_publish_round_robin(client: AsyncClient):
    await client.post("/api/v1/topics/", json={"name": "rr-topic", "partition_count": 3})

    # Publish three messages without key or explicit partition
    partitions = []
    for i in range(3):
        payload = {"value": f"msg{i}", "acks": "leader"}
        resp = await client.post("/api/v1/topics/rr-topic/publish", json=payload)
        assert resp.status_code == 201
        partitions.append(resp.json()["partition"])

    # Verify they were routed round-robin
    assert len(set(partitions)) == 3
    assert sorted(partitions) == [0, 1, 2]

@pytest.mark.asyncio
async def test_publish_acks_none(client: AsyncClient, db: AsyncSession):
    await client.post("/api/v1/topics/", json={"name": "acks-topic", "partition_count": 1})

    payload = {"value": "hello", "acks": "none"}
    resp = await client.post("/api/v1/topics/acks-topic/publish", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["offset"] == -1  # Returned immediately without offset commitment
    
    # Wait slightly to let background task run
    await asyncio.sleep(0.1)

    # Verify event got written anyway in background
    stmt = select(Event).where(Event.topic == "acks-topic")
    res = await db.execute(stmt)
    event = res.scalar_one_or_none()
    assert event is not None
    assert event.value == "hello"

@pytest.mark.asyncio
async def test_concurrent_producers(client: AsyncClient, db: AsyncSession):
    await client.post("/api/v1/topics/", json={"name": "concurrent-topic", "partition_count": 1})

    # Trigger 20 concurrent publish requests with 50ms staggering to avoid SQLite connection pool snapshot isolation issues
    payloads = [{"value": f"msg-{i}", "partition": 0, "acks": "leader"} for i in range(20)]
    
    async def publish(payload, delay):
        await asyncio.sleep(delay)
        return await client.post("/api/v1/topics/concurrent-topic/publish", json=payload)

    responses = await asyncio.gather(*(publish(p, i * 0.05) for i, p in enumerate(payloads)))
    
    for resp in responses:
        assert resp.status_code == 201

    # Fetch all events and verify offsets are strictly 0 to 19 (no duplicates or gaps)
    stmt = select(Event).where(Event.topic == "concurrent-topic").order_by(Event.offset)
    result = await db.execute(stmt)
    events = result.scalars().all()
    
    assert len(events) == 20
    offsets = [e.offset for e in events]
    assert offsets == list(range(20))
