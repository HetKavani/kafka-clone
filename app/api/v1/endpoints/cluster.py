import json
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.core.database import get_db
from app.core.redis import get_redis
from app.models import Topic
from app.schemas.event import EventReplicationRequest, EventResponse
from app.repositories.event import EventRepository
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload


router = APIRouter()

@router.get("")
async def get_cluster_summary(
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    all_brokers = await redis_client.smembers("kafkax:brokers")
    active_count = 0
    for b_id in all_brokers:
        if await redis_client.exists(f"kafkax:broker:{b_id}"):
            active_count += 1
            
    stmt = select(func.count(Topic.id))
    res = await db.execute(stmt)
    topics_count = res.scalar() or 0
    
    return {
        "status": "healthy" if active_count > 0 else "unhealthy",
        "brokers_count": len(all_brokers),
        "active_brokers_count": active_count,
        "topics_count": topics_count
    }

@router.get("/brokers")
async def list_brokers(
    redis_client: aioredis.Redis = Depends(get_redis)
):
    all_brokers = await redis_client.smembers("kafkax:brokers")
    brokers_list = []
    for b_id in all_brokers:
        b_info_str = await redis_client.get(f"kafkax:broker:{b_id}")
        if b_info_str:
            brokers_list.append(json.loads(b_info_str))
        else:
            brokers_list.append({
                "broker_id": b_id,
                "status": "inactive"
            })
    return brokers_list

@router.get("/partitions")
async def list_cluster_partitions(
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    stmt = select(Topic).options(selectinload(Topic.partitions))
    res = await db.execute(stmt)
    topics = res.scalars().all()
    
    partitions_list = []
    for t in topics:
        for p in t.partitions:
            meta_key = f"kafkax:topic:{t.name}:partition:{p.partition_number}:metadata"
            meta_str = await redis_client.get(meta_key)
            
            leader = p.broker_id
            replicas = p.replicas or []
            isr = p.isr or []
            
            if meta_str:
                meta = json.loads(meta_str)
                leader = meta.get("leader", leader)
                replicas = meta.get("replicas", replicas)
                isr = meta.get("isr", isr)
                
            partitions_list.append({
                "topic": t.name,
                "partition": p.partition_number,
                "owner_broker_id": leader,
                "leader": leader,
                "replicas": replicas,
                "isr": isr,
                "next_offset": p.next_offset
            })
    return partitions_list




@router.post("/internal/replicate", status_code=status.HTTP_201_CREATED)
async def replicate_event(
    req: EventReplicationRequest,
    db: AsyncSession = Depends(get_db)
):
    repo = EventRepository(db)
    try:
        event = await repo.replicate_event(
            topic_name=req.topic,
            partition_number=req.partition,
            offset=req.offset,
            key=req.key,
            value=req.value,
            timestamp=req.timestamp,
            producer_id=req.producer_id
        )
        return {"status": "success", "offset": event.offset}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Replication failed: {str(e)}"
        )

@router.get("/internal/topics/{topic}/partitions/{partition}/events", response_model=List[EventResponse])
async def get_partition_events_internal(
    topic: str,
    partition: int,
    start_offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db)
):
    repo = EventRepository(db)
    events = await repo.get_events(
        topic_name=topic,
        partition_number=partition,
        start_offset=start_offset,
        limit=limit
    )
    return events

