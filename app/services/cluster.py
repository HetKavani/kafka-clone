import json
import logging
import asyncio
from typing import List, Optional
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import async_session
from app.models import Topic, Partition

logger = logging.getLogger("kafkax")

# Helper to get active brokers from Redis
async def get_active_brokers(redis_client) -> List[dict]:
    all_brokers = await redis_client.smembers("kafkax:brokers")
    active_brokers = []
    for b_id in all_brokers:
        b_info_str = await redis_client.get(f"kafkax:broker:{b_id}")
        if b_info_str:
            active_brokers.append(json.loads(b_info_str))
    return active_brokers

# Broker heartbeat loop
async def broker_heartbeat_loop(redis_client, once: bool = False):
    logger.info(json.dumps({
        "event": "broker_registration",
        "broker_id": settings.BROKER_ID,
        "host": settings.HOST,
        "port": settings.PORT
    }))
    while True:
        try:
            broker_info = {
                "broker_id": settings.BROKER_ID,
                "host": settings.HOST,
                "port": settings.PORT,
                "status": "active"
            }
            await redis_client.sadd("kafkax:brokers", settings.BROKER_ID)
            await redis_client.set(f"kafkax:broker:{settings.BROKER_ID}", json.dumps(broker_info), ex=10)
            logger.info(json.dumps({
                "event": "broker_heartbeat",
                "broker_id": settings.BROKER_ID
            }))
        except Exception as e:
            logger.error(f"Error in broker heartbeat loop: {e}")
        if once:
            break
        await asyncio.sleep(3)

# Partition reassignment algorithm (round-robin active brokers)
async def reassign_partitions(db: AsyncSession, active_brokers: List[dict]):
    stmt = select(Topic).options(selectinload(Topic.partitions))
    res = await db.execute(stmt)
    topics = res.scalars().all()
    
    if not active_brokers:
        # No active brokers, set broker_id to None
        for topic in topics:
            for p in topic.partitions:
                if p.broker_id is not None:
                    p.broker_id = None
                    logger.info(json.dumps({
                        "event": "partition_assignment",
                        "topic": topic.name,
                        "partition": p.partition_number,
                        "broker_id": None
                    }))
        await db.commit()
        return

    # Sort active brokers to make assignment deterministic
    brokers = sorted(active_brokers, key=lambda x: x["broker_id"])
    
    for topic in topics:
        # Sort partitions
        parts = sorted(topic.partitions, key=lambda x: x.partition_number)
        for idx, p in enumerate(parts):
            assigned_broker = brokers[idx % len(brokers)]
            if p.broker_id != assigned_broker["broker_id"]:
                p.broker_id = assigned_broker["broker_id"]
                logger.info(json.dumps({
                    "event": "partition_assignment",
                    "topic": topic.name,
                    "partition": p.partition_number,
                    "broker_id": p.broker_id
                }))
    await db.commit()

# Cluster monitor loop to detect broker failure
async def cluster_monitor_loop(redis_client, db_session_maker=None, once: bool = False):
    while True:
        try:
            all_brokers = await redis_client.smembers("kafkax:brokers")
            active_brokers = []
            failed_brokers = []
            
            for b_id in all_brokers:
                is_active = await redis_client.exists(f"kafkax:broker:{b_id}")
                if is_active:
                    b_info_str = await redis_client.get(f"kafkax:broker:{b_id}")
                    if b_info_str:
                        active_brokers.append(json.loads(b_info_str))
                else:
                    failed_brokers.append(b_id)
            
            # Clean up failed brokers from the global set
            for b_id in failed_brokers:
                await redis_client.srem("kafkax:brokers", b_id)
                logger.warning(json.dumps({
                    "event": "broker_failure",
                    "broker_id": b_id
                }))
            
            # Retrieve last known brokers to compare set changes (joins/leaves)
            last_known_str = await redis_client.get("kafkax:last_known_brokers")
            last_known = json.loads(last_known_str) if last_known_str else []
            
            current_active_ids = sorted([b["broker_id"] for b in active_brokers])
            
            if failed_brokers or current_active_ids != last_known:
                # Set changed! Reassign and update
                session_factory = db_session_maker if db_session_maker is not None else async_session
                async with session_factory() as session:
                    await reassign_partitions(session, active_brokers)
                
                await redis_client.set("kafkax:last_known_brokers", json.dumps(current_active_ids))
                
                # Clear consumer group assignments to trigger a consumer rebalance
                all_groups = await redis_client.smembers("kafkax:groups")
                for group_id in all_groups:
                    await redis_client.delete(f"kafkax:group:{group_id}:assignments")
                    logger.info(json.dumps({
                        "event": "partition_rebalance",
                        "group_id": group_id,
                        "reason": "cluster_membership_change"
                    }))
                    
        except Exception as e:
            logger.error(f"Error in cluster monitor loop: {e}")
        if once:
            break
        await asyncio.sleep(2)

# Self-healing partition owner lookup
async def get_partition_owner(db: AsyncSession, redis_client, topic_name: str, partition_num: int) -> Optional[dict]:
    # Check partition owner in DB
    stmt = (
        select(Partition)
        .join(Topic)
        .where(Topic.name == topic_name, Partition.partition_number == partition_num)
    )
    res = await db.execute(stmt)
    partition = res.scalar_one_or_none()
    
    if not partition:
        return None
        
    active_brokers = await get_active_brokers(redis_client)
    is_active = any(b["broker_id"] == partition.broker_id for b in active_brokers)
    
    if not partition.broker_id or not is_active:
        # Trigger partition reassignment immediately
        await reassign_partitions(db, active_brokers)
        
        # Clear consumer groups assignments to trigger rebalance
        all_groups = await redis_client.smembers("kafkax:groups")
        for group_id in all_groups:
            await redis_client.delete(f"kafkax:group:{group_id}:assignments")
            
        # Re-fetch partition details
        res = await db.execute(stmt)
        partition = res.scalar_one_or_none()
        
    if partition and partition.broker_id:
        for b in active_brokers:
            if b["broker_id"] == partition.broker_id:
                return b
                
    return None
