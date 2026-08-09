import json
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.core.database import get_db
from app.core.redis import get_redis
from app.schemas.event import EventResponse
from app.schemas.offset import ConsumerOffsetCommitRequest, ConsumerOffsetResponse
from app.services.consumer import ConsumerService
from app.services.coordinator import CoordinatorService
from app.repositories.topic import TopicRepository

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
    db: AsyncSession = Depends(get_db)
):
    service = ConsumerService(db)
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
    db: AsyncSession = Depends(get_db)
):
    service = ConsumerService(db)
    return await service.commit_offset(
        group_id=group_id,
        topic=req.topic,
        partition=req.partition,
        offset=req.offset
    )

@router.get("/consumers/{group_id}/offsets", response_model=List[ConsumerOffsetResponse])
async def get_committed_offsets(
    group_id: str,
    db: AsyncSession = Depends(get_db)
):
    service = ConsumerService(db)
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
    db: AsyncSession = Depends(get_db)
):
    service = ConsumerService(db)
    return await service.get_committed_offsets(group)
