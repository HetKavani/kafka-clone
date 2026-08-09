import pytest
import asyncio
from datetime import datetime, timedelta
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_consume_strategies(client: AsyncClient):
    # 1. Create topic and publish messages
    await client.post("/api/v1/topics/", json={"name": "consume-topic", "partition_count": 1})
    
    for i in range(5):
        await client.post(
            "/api/v1/topics/consume-topic/publish", 
            json={"value": f"msg-{i}", "partition": 0, "acks": "leader"}
        )

    # 2. Strategy: earliest
    resp = await client.get("/api/v1/topics/consume-topic/events?partition=0&strategy=earliest")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 5
    assert [d["offset"] for d in data] == [0, 1, 2, 3, 4]

    # 3. Strategy: specific
    resp = await client.get("/api/v1/topics/consume-topic/events?partition=0&strategy=specific&offset=2")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 3
    assert [d["offset"] for d in data] == [2, 3, 4]

    # 4. Strategy: latest
    resp = await client.get("/api/v1/topics/consume-topic/events?partition=0&strategy=latest")
    assert resp.status_code == 200
    assert len(resp.json()) == 0  # No new messages after latest offset

@pytest.mark.asyncio
async def test_committed_offsets_and_resume(client: AsyncClient):
    await client.post("/api/v1/topics/", json={"name": "resume-topic", "partition_count": 1})
    
    for i in range(5):
        await client.post(
            "/api/v1/topics/resume-topic/publish", 
            json={"value": f"msg-{i}", "partition": 0, "acks": "leader"}
        )

    # Commit offset 2 for group "my-group"
    commit_payload = {"topic": "resume-topic", "partition": 0, "offset": 2}
    commit_resp = await client.post("/api/v1/consumers/my-group/offsets/commit", json=commit_payload)
    assert commit_resp.status_code == 200
    assert commit_resp.json()["committed_offset"] == 2

    # Fetch committed offsets
    offset_list = await client.get("/api/v1/consumers/my-group/offsets")
    assert offset_list.status_code == 200
    assert len(offset_list.json()) == 1
    assert offset_list.json()[0]["committed_offset"] == 2

    # Resume from committed (should return messages starting from offset 2)
    resp = await client.get("/api/v1/topics/resume-topic/events?partition=0&strategy=committed&group_id=my-group")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 3
    assert [d["offset"] for d in data] == [2, 3, 4]

@pytest.mark.asyncio
async def test_replay_by_timestamp(client: AsyncClient):
    await client.post("/api/v1/topics/", json={"name": "time-topic", "partition_count": 1})

    # Message 1 (now)
    await client.post(
        "/api/v1/topics/time-topic/publish",
        json={"value": "msg-old", "partition": 0, "acks": "leader"}
    )
    
    # Simulate a timestamp from future/just now
    checkpoint = datetime.utcnow()
    await asyncio.sleep(0.1)

    # Message 2 & 3
    await client.post(
        "/api/v1/topics/time-topic/publish",
        json={"value": "msg-new-1", "partition": 0, "acks": "leader"}
    )
    await client.post(
        "/api/v1/topics/time-topic/publish",
        json={"value": "msg-new-2", "partition": 0, "acks": "leader"}
    )

    # Replay starting from checkpoint timestamp
    checkpoint_str = checkpoint.isoformat() + "Z"
    resp = await client.get(f"/api/v1/topics/time-topic/events?partition=0&strategy=timestamp&timestamp={checkpoint_str}")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert [d["value"] for d in data] == ["msg-new-1", "msg-new-2"]
