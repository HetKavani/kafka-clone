import json
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
import redis.asyncio as aioredis

from app.core.database import get_db
from app.core.redis import get_redis
from app.schemas.event import EventResponse
from app.schemas.offset import ConsumerOffsetCommitRequest, ConsumerOffsetResponse
from app.services.consumer import ConsumerService
from app.services.coordinator import CoordinatorService
from app.repositories.topic import TopicRepository
from app.models import Topic


router = APIRouter()

@router.get("/topics/{topic}/events", response_model=List[EventResponse])
async def poll_events(
    topic: str,
    partition: int = Query(..., ge=0, description="Partition number"),
    strategy: str = Query("earliest", description="Consumption strategy: earliest, latest, specific, committed, timestamp"),
    group_id: Optional[str] = Query(None, description="Consumer group ID (required for 'committed' strategy)"),
    offset: Optional[int] = Query(None, ge=0, description="Offset index (required for 'specific' strategy)"),
    timestamp: Optional[datetime] = Query(None, description="Timestamp ISO format (required for 'timestamp' strategy)"),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    service = ConsumerService(db, redis_client)
    try:
        return await service.fetch_events(
            topic_name=topic,
            partition=partition,
            strategy=strategy,
            group_id=group_id,
            offset=offset,
            timestamp=timestamp,
            limit=limit
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/consumers/{group_id}/offsets/commit", response_model=ConsumerOffsetResponse)
async def commit_offset(
    group_id: str,
    req: ConsumerOffsetCommitRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    service = ConsumerService(db, redis_client)
    return await service.commit_offset(
        group_id=group_id,
        topic=req.topic,
        partition=req.partition,
        offset=req.offset
    )

@router.get("/consumers/{group_id}/offsets", response_model=List[ConsumerOffsetResponse])
async def get_committed_offsets(
    group_id: str,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    service = ConsumerService(db, redis_client)
    return await service.get_committed_offsets(group_id)


@router.post("/consumers/{group_id}/heartbeat", status_code=status.HTTP_204_NO_CONTENT)
async def send_heartbeat(
    group_id: str,
    consumer_id: str = Query(...),
    topics: List[str] = Query(...),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    service = CoordinatorService(redis_client)
    await service.heartbeat(group_id, consumer_id, topics)
    return None

@router.get("/consumers/{group_id}/assignments/{consumer_id}")
async def get_assignments(
    group_id: str,
    consumer_id: str,
    topic: str = Query(...),
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    topic_repo = TopicRepository(db)
    topic_meta = await topic_repo.get_by_name(topic)
    if not topic_meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Topic '{topic}' not found"
        )
        
    coordinator = CoordinatorService(redis_client, db)
    assignments = await coordinator.get_assignments(
        group_id=group_id,
        consumer_id=consumer_id,
        topic_name=topic,
        partition_count=topic_meta.partition_count
    )
    return {"assigned_partitions": assignments}

@router.get("/consumer-groups")
async def list_consumer_groups(
    redis_client: aioredis.Redis = Depends(get_redis)
):
    groups = await redis_client.smembers("kafkax:groups")
    return list(groups)

@router.get("/consumer-groups/{group}")
async def get_consumer_group_details(
    group: str,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    coordinator = CoordinatorService(redis_client, db)
    active_members = await coordinator.get_active_members(group)
    assignments_str = await redis_client.get(f"kafkax:group:{group}:assignments")
    assignments = json.loads(assignments_str) if assignments_str else {}
    return {
        "group_id": group,
        "active_members": active_members,
        "assignments": assignments
    }

@router.get("/consumer-groups/{group}/members")
async def get_consumer_group_members(
    group: str,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    coordinator = CoordinatorService(redis_client, db)
    active_members = await coordinator.get_active_members(group)
    members_info = []
    for m_id in active_members:
        meta_str = await redis_client.get(coordinator._metadata_key(group, m_id))
        topics = json.loads(meta_str) if meta_str else []
        members_info.append({
            "consumer_id": m_id,
            "topics": topics
        })
    return members_info

@router.get("/consumer-groups/{group}/offsets", response_model=List[ConsumerOffsetResponse])
async def get_consumer_group_offsets(
    group: str,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    service = ConsumerService(db, redis_client)
    return await service.get_committed_offsets(group)

@router.get("/consumer-groups/{group}/lag")
async def get_consumer_group_lag(
    group: str,
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    coordinator = CoordinatorService(redis_client, db)
    active_members = await coordinator.get_active_members(group)
    
    topics = set()
    for m_id in active_members:
        meta_str = await redis_client.get(coordinator._metadata_key(group, m_id))
        if meta_str:
            topics.update(json.loads(meta_str))
            
    lag_details = []
    total_lag = 0
    
    from app.repositories.topic import TopicRepository
    topic_repo = TopicRepository(db)
    
    for t_name in topics:
        topic_meta = await topic_repo.get_by_name(t_name)
        if topic_meta:
            for p_num in range(topic_meta.partition_count):
                latest_offset_str = await redis_client.get(f"kafkax:topic:{t_name}:partition:{p_num}:latest_offset")
                latest_offset = int(latest_offset_str) if latest_offset_str else 0
                
                committed_offset_str = await redis_client.get(f"kafkax:group:{group}:offset:{t_name}:{p_num}")
                committed_offset = int(committed_offset_str) if committed_offset_str else 0
                
                lag = max(0, latest_offset - committed_offset)
                total_lag += lag
                
                lag_details.append({
                    "topic": t_name,
                    "partition": p_num,
                    "latest_offset": latest_offset,
                    "committed_offset": committed_offset,
                    "lag": lag
                })
                
    return {
        "group_id": group,
        "lag_details": lag_details,
        "total_lag": total_lag
    }

@router.get("/metrics")
async def get_metrics(
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    events_published_str = await redis_client.get("kafkax:metrics:events_published")
    events_published = int(events_published_str) if events_published_str else 0
    
    events_consumed_str = await redis_client.get("kafkax:metrics:events_consumed")
    events_consumed = int(events_consumed_str) if events_consumed_str else 0

    publish_count = events_published
    publish_time_ms_str = await redis_client.get("kafkax:metrics:publish_time_ms")
    publish_time_ms = float(publish_time_ms_str) if publish_time_ms_str else 0.0
    avg_publish_latency_ms = publish_time_ms / max(1, publish_count)
    
    consume_count_str = await redis_client.get("kafkax:metrics:consume_count")
    consume_count = int(consume_count_str) if consume_count_str else 0
    consume_time_ms_str = await redis_client.get("kafkax:metrics:consume_time_ms")
    consume_time_ms = float(consume_time_ms_str) if consume_time_ms_str else 0.0
    avg_consume_latency_ms = consume_time_ms / max(1, consume_count)

    from app.services.cluster import get_active_brokers
    active_brokers = await get_active_brokers(redis_client)
    active_brokers_count = len(active_brokers)
    
    active_consumers = 0
    all_groups = await redis_client.smembers("kafkax:groups")
    for group_id in all_groups:
        active_members = await redis_client.smembers(f"kafkax:group:{group_id}:members")
        for member_id in list(active_members):
            if await redis_client.exists(f"kafkax:group:{group_id}:member:{member_id}"):
                active_consumers += 1

    stmt = select(Topic).options(selectinload(Topic.partitions))
    res = await db.execute(stmt)
    topics = res.scalars().all()
    
    total_partitions = 0
    under_replicated_partitions = 0
    out_of_sync_replicas_count = 0
    
    for t in topics:
        for p in t.partitions:
            total_partitions += 1
            meta_key = f"kafkax:topic:{t.name}:partition:{p.partition_number}:metadata"
            meta_str = await redis_client.get(meta_key)
            if meta_str:
                meta = json.loads(meta_str)
                replicas = meta.get("replicas", [])
                isr = meta.get("isr", [])
                if len(isr) < len(replicas):
                    under_replicated_partitions += 1
                    out_of_sync_replicas_count += (len(replicas) - len(isr))

    total_consumer_lag = 0
    for group_id in all_groups:
        group_topics = set()
        active_members = await redis_client.smembers(f"kafkax:group:{group_id}:members")
        for member_id in list(active_members):
            if await redis_client.exists(f"kafkax:group:{group_id}:member:{member_id}"):
                meta_str = await redis_client.get(f"kafkax:group:{group_id}:metadata:{member_id}")
                if meta_str:
                    group_topics.update(json.loads(meta_str))
                    
        for t_name in group_topics:
            topic_meta = next((x for x in topics if x.name == t_name), None)
            if topic_meta:
                for p_num in range(topic_meta.partition_count):
                    latest_offset_str = await redis_client.get(f"kafkax:topic:{t_name}:partition:{p_num}:latest_offset")
                    latest = int(latest_offset_str) if latest_offset_str else 0
                    committed_str = await redis_client.get(f"kafkax:group:{group_id}:offset:{t_name}:{p_num}")
                    committed = int(committed_str) if committed_str else 0
                    total_consumer_lag += max(0, latest - committed)

    return {
        "events_published": events_published,
        "events_consumed": events_consumed,
        "avg_publish_latency_ms": round(avg_publish_latency_ms, 2),
        "avg_consume_latency_ms": round(avg_consume_latency_ms, 2),
        "consumer_lag": total_consumer_lag,
        "active_brokers": active_brokers_count,
        "active_consumers": active_consumers,
        "partition_count": total_partitions,
        "replication_status": {
            "under_replicated_partitions": under_replicated_partitions,
            "out_of_sync_replicas_count": out_of_sync_replicas_count,
            "status": "healthy" if under_replicated_partitions == 0 else "degraded"
        }
    }



