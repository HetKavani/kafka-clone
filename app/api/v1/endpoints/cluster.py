import json
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.core.database import get_db
from app.core.redis import get_redis
from app.models import Topic
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
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Topic).options(selectinload(Topic.partitions))
    res = await db.execute(stmt)
    topics = res.scalars().all()
    
    partitions_list = []
    for t in topics:
        for p in t.partitions:
            partitions_list.append({
                "topic": t.name,
                "partition": p.partition_number,
                "owner_broker_id": p.broker_id,
                "next_offset": p.next_offset
            })
    return partitions_list
