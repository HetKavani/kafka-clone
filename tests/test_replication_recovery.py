import pytest
import json
import asyncio
import os
from datetime import datetime
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy import select
from unittest.mock import patch, MagicMock

from app.main import app
from app.core.config import settings
from app.core.database import Base, get_db
from app.core.redis import get_redis
from app.models import Topic, Partition, Event, ConsumerOffset
from app.repositories.topic import TopicRepository
from app.repositories.event import EventRepository
from app.services.producer import ProducerService
from app.services.consumer import ConsumerService
from app.services.coordinator import CoordinatorService
from app.services.cluster import (
    broker_heartbeat_loop,
    cluster_monitor_loop,
    broker_replica_recovery_loop,
    get_partition_owner
)

from conftest import MockRedis

# 1. Multi-Broker Isolated Databases
DB_FILES = {
    "api": "sqlite+aiosqlite:///test_api.db",
    "broker-1": "sqlite+aiosqlite:///test_b1.db",
    "broker-2": "sqlite+aiosqlite:///test_b2.db",
    "broker-3": "sqlite+aiosqlite:///test_b3.db",
}

engines = {k: create_async_engine(v, connect_args={"check_same_thread": False}) for k, v in DB_FILES.items()}
sessionmakers = {k: async_sessionmaker(bind=engines[k], class_=AsyncSession, expire_on_commit=False) for k in DB_FILES}

# Mock httpx.AsyncClient to route requests to the correct mock database engine using host headers
class MockAsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def post(self, url, json=None, headers=None, timeout=None):
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.netloc

        headers = headers or {}
        headers["host"] = host

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            path = parsed.path
            if parsed.query:
                path += f"?{parsed.query}"
            resp = await client.post(path, json=json, headers=headers)
        return resp

    async def get(self, url, timeout=None):
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.netloc

        headers = {"host": host}

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            path = parsed.path
            if parsed.query:
                path += f"?{parsed.query}"
            resp = await client.get(path, headers=headers)
        return resp

@pytest.fixture(scope="function", autouse=True)
async def setup_databases():
    # Setup: Create tables in all databases
    for k, engine in engines.items():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            
    yield
    
    # Teardown: Drop tables and clean up files
    for k, engine in engines.items():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()
        
    for file in ["test_api.db", "test_b1.db", "test_b2.db", "test_b3.db"]:
        if os.path.exists(file):
            try:
                os.remove(file)
            except Exception:
                pass

@pytest.fixture(scope="function")
async def mock_redis() -> MockRedis:
    return MockRedis()

@pytest.fixture(scope="function")
async def client(mock_redis: MockRedis) -> AsyncClient:
    from fastapi import Request

    async def override_get_db(request: Request):
        host = request.headers.get("host", "api")
        if ":" in host:
            host = host.split(":")[0]
            
        b_id = "api"
        if host in DB_FILES:
            b_id = host
        elif settings.BROKER_ID in DB_FILES:
            b_id = settings.BROKER_ID
            
        async with sessionmakers[b_id]() as session:
            try:
                yield session
            finally:
                await session.close()


    async def override_get_redis():
        yield mock_redis

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()

@pytest.mark.asyncio
@patch("httpx.AsyncClient", new=MockAsyncClient)
async def test_partition_replication_and_acks(client: AsyncClient, mock_redis: MockRedis):
    # Register brokers
    for b in ["broker-1", "broker-2", "broker-3"]:
        await mock_redis.sadd("kafkax:brokers", b)
        await mock_redis.set(f"kafkax:broker:{b}", json.dumps({
            "broker_id": b, "host": b, "port": 8000, "status": "active"
        }))

    # Create topic on broker-1
    resp = await client.post("http://broker-1/api/v1/topics/", json={"name": "orders", "partition_count": 1})
    assert resp.status_code == 201

    # Run cluster monitor once to assign partition metadata
    await cluster_monitor_loop(mock_redis, db_session_maker=sessionmakers["broker-1"], once=True)

    # Get partition metadata to verify leader/replicas/isr
    meta_key = "kafkax:topic:orders:partition:0:metadata"
    meta_str = await mock_redis.get(meta_key)
    assert meta_str is not None


    meta = json.loads(meta_str)
    leader = meta["leader"]
    replicas = meta["replicas"]
    assert len(replicas) == 3
    assert leader in replicas

    # Publish with acks=all to the leader
    resp = await client.post(f"http://{leader}/api/v1/topics/orders/publish", json={
        "value": {"amount": 100},
        "acks": "all"
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["offset"] == 0

    # Verify replication: check event exists in follower database
    follower = [r for r in replicas if r != leader][0]
    
    # Read follower local DB
    async with sessionmakers[follower]() as session:
        stmt = select(Event).where(Event.topic == "orders", Event.partition == 0, Event.offset == 0)
        res = await session.execute(stmt)
        event = res.scalar_one_or_none()
        assert event is not None
        assert event.value["amount"] == 100

@pytest.mark.asyncio
@patch("httpx.AsyncClient", new=MockAsyncClient)
async def test_leader_failure_and_election(client: AsyncClient, mock_redis: MockRedis):
    # Register brokers
    for b in ["broker-1", "broker-2", "broker-3"]:
        await mock_redis.sadd("kafkax:brokers", b)
        await mock_redis.set(f"kafkax:broker:{b}", json.dumps({
            "broker_id": b, "host": b, "port": 8000, "status": "active"
        }))

    # Create topic
    resp = await client.post("http://broker-1/api/v1/topics/", json={"name": "logs", "partition_count": 1})
    assert resp.status_code == 201

    # Run cluster monitor once to assign partition metadata
    await cluster_monitor_loop(mock_redis, db_session_maker=sessionmakers["broker-1"], once=True)

    meta_key = "kafkax:topic:logs:partition:0:metadata"
    meta_str = await mock_redis.get(meta_key)
    meta = json.loads(meta_str)
    former_leader = meta["leader"]
    isr = meta["isr"]

    # Simulate leader failure (heartbeat expiry)
    await mock_redis.delete(f"kafkax:broker:{former_leader}")
    
    # Run cluster monitor once to trigger leader election
    await cluster_monitor_loop(mock_redis, db_session_maker=sessionmakers["broker-1"], once=True)

    # Re-fetch metadata
    meta_str = await mock_redis.get(meta_key)
    meta = json.loads(meta_str)
    new_leader = meta["leader"]
    
    assert new_leader != former_leader
    assert new_leader in isr
    assert former_leader not in meta["isr"]

@pytest.mark.asyncio
@patch("httpx.AsyncClient", new=MockAsyncClient)
async def test_replica_recovery(client: AsyncClient, mock_redis: MockRedis):
    # Register brokers
    for b in ["broker-1", "broker-2", "broker-3"]:
        await mock_redis.sadd("kafkax:brokers", b)
        await mock_redis.set(f"kafkax:broker:{b}", json.dumps({
            "broker_id": b, "host": b, "port": 8000, "status": "active"
        }))

    # Create topic
    resp = await client.post("http://broker-1/api/v1/topics/", json={"name": "alerts", "partition_count": 1})
    assert resp.status_code == 201

    # Run cluster monitor once to assign partition metadata
    await cluster_monitor_loop(mock_redis, db_session_maker=sessionmakers["broker-1"], once=True)

    meta_key = "kafkax:topic:alerts:partition:0:metadata"
    meta_str = await mock_redis.get(meta_key)
    meta = json.loads(meta_str)
    leader = meta["leader"]
    follower = [r for r in meta["replicas"] if r != leader][0]

    # Simulate follower failure (remove from ISR)
    meta["isr"].remove(follower)
    await mock_redis.set(meta_key, json.dumps(meta))

    # Publish events to leader while follower is down
    await client.post(f"http://{leader}/api/v1/topics/alerts/publish", json={"value": {"alert": "cpu_high"}, "acks": "leader"})
    await client.post(f"http://{leader}/api/v1/topics/alerts/publish", json={"value": {"alert": "disk_full"}, "acks": "leader"})

    # Verify follower does not have these events yet
    async with sessionmakers[follower]() as session:
        stmt = select(Event).where(Event.topic == "alerts", Event.partition == 0)
        res = await session.execute(stmt)
        events = res.scalars().all()
        assert len(events) == 0

    # Run replica recovery loop on follower to synchronize
    settings.BROKER_ID = follower
    await broker_replica_recovery_loop(mock_redis, db_session_maker=sessionmakers[follower], once=True)
    settings.BROKER_ID = None


    # Verify follower caught up and rejoined ISR
    async with sessionmakers[follower]() as session:
        stmt = select(Event).where(Event.topic == "alerts", Event.partition == 0)
        res = await session.execute(stmt)
        events = res.scalars().all()
        assert len(events) == 2
        assert events[0].value["alert"] == "cpu_high"
        assert events[1].value["alert"] == "disk_full"

    meta_str = await mock_redis.get(meta_key)
    meta = json.loads(meta_str)
    assert follower in meta["isr"]

@pytest.mark.asyncio
@patch("httpx.AsyncClient", new=MockAsyncClient)
async def test_consumer_lag_and_metrics_api(client: AsyncClient, mock_redis: MockRedis):
    # Set up active brokers and metrics
    await mock_redis.sadd("kafkax:brokers", "broker-1")
    await mock_redis.set("kafkax:broker:broker-1", json.dumps({
        "broker_id": "broker-1", "host": "broker-1", "port": 8000, "status": "active"
    }))
    await mock_redis.set("kafkax:metrics:events_published", "10")
    await mock_redis.set("kafkax:metrics:events_consumed", "8")
    await mock_redis.set("kafkax:metrics:publish_time_ms", "20.0")
    await mock_redis.set("kafkax:metrics:consume_count", "4")
    await mock_redis.set("kafkax:metrics:consume_time_ms", "10.0")

    # Create topic and set offsets in Redis on broker-1
    resp = await client.post("http://broker-1/api/v1/topics/", json={"name": "events", "partition_count": 1})
    assert resp.status_code == 201

    # Run cluster monitor once to assign partition metadata
    await cluster_monitor_loop(mock_redis, db_session_maker=sessionmakers["broker-1"], once=True)

    # Set latest partition offset in Redis
    await mock_redis.set("kafkax:topic:events:partition:0:latest_offset", "5")
    # Commit consumer offset
    await mock_redis.set("kafkax:group:group-Y:offset:events:0", "3")
    await mock_redis.sadd("kafkax:groups", "group-Y")
    await mock_redis.sadd("kafkax:group:group-Y:members", "consumer-1")
    await mock_redis.set("kafkax:group:group-Y:member:consumer-1", "active")
    await mock_redis.set("kafkax:group:group-Y:metadata:consumer-1", json.dumps(["events"]))

    # Test Consumer Lag API
    resp = await client.get("http://broker-1/api/v1/consumer-groups/group-Y/lag")
    assert resp.status_code == 200
    lag_data = resp.json()
    assert lag_data["group_id"] == "group-Y"
    assert lag_data["total_lag"] == 2
    assert lag_data["lag_details"][0]["topic"] == "events"
    assert lag_data["lag_details"][0]["lag"] == 2

    # Test Metrics API
    resp = await client.get("http://broker-1/api/v1/metrics")
    assert resp.status_code == 200
    metrics = resp.json()
    assert metrics["events_published"] == 10
    assert metrics["events_consumed"] == 8
    assert metrics["avg_publish_latency_ms"] == 2.0
    assert metrics["avg_consume_latency_ms"] == 2.5
    assert metrics["active_brokers"] == 1
    assert metrics["consumer_lag"] == 2
