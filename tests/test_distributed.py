import pytest
import json
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.config import settings
from app.models import Topic, Partition
from app.repositories.topic import TopicRepository
from app.services.coordinator import CoordinatorService
from app.services.cluster import (
    broker_heartbeat_loop,
    cluster_monitor_loop,
    get_partition_owner,
    reassign_partitions
)

@pytest.mark.asyncio
async def test_broker_registration_and_heartbeat(mock_redis):
    # Set broker settings
    settings.BROKER_ID = "broker-1"
    settings.HOST = "broker-1"
    settings.PORT = 8000

    # Run broker registration/heartbeat loop once
    await broker_heartbeat_loop(mock_redis, once=True)

    # Verify registration in mock redis
    brokers = await mock_redis.smembers("kafkax:brokers")
    assert "broker-1" in brokers

    broker_info_str = await mock_redis.get("kafkax:broker:broker-1")
    assert broker_info_str is not None
    info = json.loads(broker_info_str)
    assert info["broker_id"] == "broker-1"
    assert info["host"] == "broker-1"
    assert info["port"] == 8000
    assert info["status"] == "active"

@pytest.mark.asyncio
async def test_partition_distribution_and_reassignment(db: AsyncSession, mock_redis):
    # Register multiple brokers
    await mock_redis.sadd("kafkax:brokers", "broker-1")
    await mock_redis.set("kafkax:broker:broker-1", json.dumps({
        "broker_id": "broker-1", "host": "broker-1", "port": 8000, "status": "active"
    }))
    await mock_redis.sadd("kafkax:brokers", "broker-2")
    await mock_redis.set("kafkax:broker:broker-2", json.dumps({
        "broker_id": "broker-2", "host": "broker-2", "port": 8000, "status": "active"
    }))
    await mock_redis.sadd("kafkax:brokers", "broker-3")
    await mock_redis.set("kafkax:broker:broker-3", json.dumps({
        "broker_id": "broker-3", "host": "broker-3", "port": 8000, "status": "active"
    }))

    # Create a topic with 3 partitions
    topic_repo = TopicRepository(db)
    topic = await topic_repo.create("orders", partition_count=3, broker_ids=["broker-1", "broker-2", "broker-3"])

    # Verify partitions are distributed round-robin
    stmt = select(Partition).where(Partition.topic_id == topic.id).order_by(Partition.partition_number)
    res = await db.execute(stmt)
    partitions = res.scalars().all()
    assert len(partitions) == 3
    assert partitions[0].broker_id == "broker-1"
    assert partitions[1].broker_id == "broker-2"
    assert partitions[2].broker_id == "broker-3"

    # Simulate broker-2 failure
    await mock_redis.delete("kafkax:broker:broker-2")

    # Set up last known state and run cluster monitor once to detect and reassign
    from conftest import TestingSessionLocal
    await mock_redis.set("kafkax:last_known_brokers", json.dumps(["broker-1", "broker-2", "broker-3"]))
    await cluster_monitor_loop(mock_redis, db_session_maker=TestingSessionLocal, once=True)

    # Re-fetch topic partitions from database to verify reassignment using a clean session
    async with TestingSessionLocal() as clean_session:
        stmt = select(Partition).where(Partition.topic_id == topic.id).order_by(Partition.partition_number)
        res = await clean_session.execute(stmt)
        partitions = res.scalars().all()

    # broker-2 is failed, so P1 must have been reassigned (to broker-3 or broker-1 based on round-robin)
    # Available active: broker-1, broker-3
    assert partitions[0].broker_id == "broker-1"
    assert partitions[1].broker_id == "broker-3"  # 1 % 2 = 1 -> broker-3
    assert partitions[2].broker_id == "broker-1"  # 2 % 2 = 0 -> broker-1

@pytest.mark.asyncio
async def test_consumer_group_rebalance_and_failure(db: AsyncSession, mock_redis):
    coordinator = CoordinatorService(mock_redis, db)

    # Create topic in DB
    topic_repo = TopicRepository(db)
    await topic_repo.create("orders", partition_count=3)

    # Join consumer-1
    await coordinator.heartbeat("group-A", "consumer-1", ["orders"])
    
    # Verify assignment for consumer-1 (gets all 3 partitions)
    c1_parts = await coordinator.get_assignments("group-A", "consumer-1", "orders", 3)
    assert sorted(c1_parts) == [0, 1, 2]

    # Join consumer-2
    await coordinator.heartbeat("group-A", "consumer-2", ["orders"])

    # Re-fetch assignments (triggers rebalance since members changed)
    c1_parts = await coordinator.get_assignments("group-A", "consumer-1", "orders", 3)
    c2_parts = await coordinator.get_assignments("group-A", "consumer-2", "orders", 3)

    # Round robin split: P0 -> consumer-1, P1 -> consumer-2, P2 -> consumer-1
    assert sorted(c1_parts) == [0, 2]
    assert sorted(c2_parts) == [1]

    # Simulate consumer-1 failure (heartbeat expires)
    await mock_redis.delete("kafkax:group:group-A:member:consumer-1")

    # Get assignments for consumer-2 (triggers membership cleanup and rebalance)
    c2_parts_after = await coordinator.get_assignments("group-A", "consumer-2", "orders", 3)
    
    # Since consumer-1 is gone, consumer-2 should now own all partitions
    assert sorted(c2_parts_after) == [0, 1, 2]

@pytest.mark.asyncio
async def test_multiple_consumer_groups(db: AsyncSession, mock_redis):
    coordinator = CoordinatorService(mock_redis, db)

    # Create topic
    topic_repo = TopicRepository(db)
    await topic_repo.create("payments", partition_count=2)

    # Group-A joins
    await coordinator.heartbeat("group-A", "consumer-A1", ["payments"])
    await coordinator.heartbeat("group-A", "consumer-A2", ["payments"])

    # Group-B joins
    await coordinator.heartbeat("group-B", "consumer-B1", ["payments"])

    # Verify Group-A assignments (split)
    c_a1 = await coordinator.get_assignments("group-A", "consumer-A1", "payments", 2)
    c_a2 = await coordinator.get_assignments("group-A", "consumer-A2", "payments", 2)
    assert len(c_a1) == 1
    assert len(c_a2) == 1

    # Verify Group-B assignments (consumer-B1 gets all)
    c_b1 = await coordinator.get_assignments("group-B", "consumer-B1", "payments", 2)
    assert sorted(c_b1) == [0, 1]

@pytest.mark.asyncio
async def test_cluster_metadata_api(client: AsyncClient, mock_redis, db: AsyncSession):
    # Set up brokers in mock redis
    await mock_redis.sadd("kafkax:brokers", "broker-1")
    await mock_redis.set("kafkax:broker:broker-1", json.dumps({
        "broker_id": "broker-1", "host": "broker-1", "port": 8000, "status": "active"
    }))
    
    # Create topic & partitions assigned to broker-1
    topic_repo = TopicRepository(db)
    await topic_repo.create("metrics", partition_count=2, broker_ids=["broker-1"])

    # Test summary API
    resp = await client.get("/api/v1/cluster")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["brokers_count"] == 1
    assert data["active_brokers_count"] == 1
    assert data["topics_count"] == 1

    # Test brokers API
    resp = await client.get("/api/v1/cluster/brokers")
    assert resp.status_code == 200
    brokers = resp.json()
    assert len(brokers) == 1
    assert brokers[0]["broker_id"] == "broker-1"

    # Test partitions API
    resp = await client.get("/api/v1/cluster/partitions")
    assert resp.status_code == 200
    partitions = resp.json()
    assert len(partitions) == 2
    assert partitions[0]["topic"] == "metrics"
    assert partitions[0]["owner_broker_id"] == "broker-1"

    # Test consumer groups API
    await mock_redis.sadd("kafkax:groups", "metrics-group")
    await mock_redis.sadd("kafkax:group:metrics-group:members", "c1")
    await mock_redis.set("kafkax:group:metrics-group:member:c1", "active")
    
    resp = await client.get("/api/v1/consumer-groups")
    assert resp.status_code == 200
    assert "metrics-group" in resp.json()

    resp = await client.get("/api/v1/consumer-groups/metrics-group")
    assert resp.status_code == 200
    assert resp.json()["group_id"] == "metrics-group"
    assert "c1" in resp.json()["active_members"]
