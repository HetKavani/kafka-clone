import json
import logging
import asyncio
from typing import List, Optional
from datetime import datetime
from sqlalchemy import select, text
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
import httpx

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
async def reassign_partitions(db: AsyncSession, redis_client, active_brokers: List[dict]):
    stmt = select(Topic).options(selectinload(Topic.partitions))
    res = await db.execute(stmt)
    topics = res.scalars().all()
    
    if not active_brokers:
        for topic in topics:
            for p in topic.partitions:
                if p.broker_id is not None:
                    p.broker_id = None
                    p.replicas = []
                    p.isr = []
                    logger.info(json.dumps({
                        "event": "partition_assignment",
                        "topic": topic.name,
                        "partition": p.partition_number,
                        "broker_id": None
                    }))
        await db.commit()
        return

    brokers = sorted(active_brokers, key=lambda x: x["broker_id"])
    broker_ids = [b["broker_id"] for b in brokers]
    rf = settings.REPLICATION_FACTOR
    
    for topic in topics:
        parts = sorted(topic.partitions, key=lambda x: x.partition_number)
        for p in parts:
            idx = p.partition_number
            assigned_leader = broker_ids[idx % len(broker_ids)]
            
            assigned_replicas = []
            for j in range(min(rf, len(broker_ids))):
                assigned_replicas.append(broker_ids[(idx + j) % len(broker_ids)])
            
            meta_key = f"kafkax:topic:{topic.name}:partition:{p.partition_number}:metadata"
            meta = {
                "leader": assigned_leader,
                "replicas": assigned_replicas,
                "isr": assigned_replicas
            }
            await redis_client.set(meta_key, json.dumps(meta))

            if p.broker_id != assigned_leader or p.replicas != assigned_replicas:
                p.broker_id = assigned_leader
                p.replicas = assigned_replicas
                p.isr = assigned_replicas
                logger.info(json.dumps({
                    "event": "partition_assignment",
                    "topic": topic.name,
                    "partition": p.partition_number,
                    "broker_id": p.broker_id,
                    "replicas": p.replicas,
                    "isr": p.isr
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
                
                # Perform leader election / failure handling for all partitions
                keys = []
                cursor = 0
                while True:
                    cursor, scan_keys = await redis_client.scan(cursor, match="kafkax:topic:*:partition:*:metadata")
                    keys.extend(scan_keys)
                    if cursor == 0:
                        break
                        
                for key in keys:
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                    parts = key_str.split(":")
                    topic_name = parts[2]
                    partition_num = int(parts[4])
                    
                    meta_str = await redis_client.get(key)
                    if not meta_str:
                        continue
                    meta = json.loads(meta_str)
                    
                    isr = meta.get("isr", [])
                    replicas = meta.get("replicas", [])
                    leader = meta.get("leader")
                    
                    changed = False
                    if b_id in isr:
                        isr.remove(b_id)
                        changed = True
                        
                    if leader == b_id:
                        active_isr = [m for m in isr if m != b_id]
                        if active_isr:
                            new_leader = active_isr[0]
                        else:
                            active_brokers_ids = [b["broker_id"] for b in active_brokers]
                            active_reps = [r for r in replicas if r in active_brokers_ids and r != b_id]
                            if active_reps:
                                new_leader = active_reps[0]
                            elif active_brokers_ids:
                                new_leader = active_brokers_ids[0]
                            else:
                                new_leader = None
                        
                        meta["leader"] = new_leader
                        if new_leader and new_leader not in isr:
                            isr.append(new_leader)
                        
                        changed = True
                        logger.info(json.dumps({
                            "event": "leader_election",
                            "topic": topic_name,
                            "partition": partition_num,
                            "former_leader": b_id,
                            "new_leader": new_leader,
                            "isr": isr
                        }))
                        
                    if changed:
                        meta["isr"] = isr
                        await redis_client.set(key, json.dumps(meta))
                        
                        session_factory = db_session_maker if db_session_maker is not None else async_session
                        async with session_factory() as session:
                            stmt_update = (
                                select(Partition)
                                .join(Topic)
                                .where(Topic.name == topic_name, Partition.partition_number == partition_num)
                            )
                            res_update = await session.execute(stmt_update)
                            p_row = res_update.scalar_one_or_none()
                            if p_row:
                                p_row.broker_id = meta["leader"]
                                p_row.replicas = meta["replicas"]
                                p_row.isr = meta["isr"]
                                await session.commit()
            
            # Retrieve last known brokers to compare set changes (joins/leaves)
            last_known_str = await redis_client.get("kafkax:last_known_brokers")
            last_known = json.loads(last_known_str) if last_known_str else []
            current_active_ids = sorted([b["broker_id"] for b in active_brokers])
            
            if failed_brokers or current_active_ids != last_known:
                session_factory = db_session_maker if db_session_maker is not None else async_session
                async with session_factory() as session:
                    await reassign_partitions(session, redis_client, active_brokers)
                
                await redis_client.set("kafkax:last_known_brokers", json.dumps(current_active_ids))
                
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
    meta_key = f"kafkax:topic:{topic_name}:partition:{partition_num}:metadata"
    meta_str = await redis_client.get(meta_key)
    active_brokers = await get_active_brokers(redis_client)
    
    leader_id = None
    if meta_str:
        meta = json.loads(meta_str)
        leader_id = meta.get("leader")
        
    if not leader_id:
        stmt = (
            select(Partition)
            .join(Topic)
            .where(Topic.name == topic_name, Partition.partition_number == partition_num)
        )
        res = await db.execute(stmt)
        partition = res.scalar_one_or_none()
        if not partition:
            return None
        leader_id = partition.broker_id
        
    is_active = any(b["broker_id"] == leader_id for b in active_brokers)
    
    if not leader_id or not is_active:
        if active_brokers:
            active_ids = sorted([b["broker_id"] for b in active_brokers])
            leader_id = active_ids[partition_num % len(active_ids)]
            rf = settings.REPLICATION_FACTOR
            replicas = [active_ids[(partition_num + i) % len(active_ids)] for i in range(min(rf, len(active_ids)))]
            meta = {
                "leader": leader_id,
                "replicas": replicas,
                "isr": replicas
            }
            await redis_client.set(meta_key, json.dumps(meta))
            
            # Update DB locally
            stmt = (
                select(Partition)
                .join(Topic)
                .where(Topic.name == topic_name, Partition.partition_number == partition_num)
            )
            res = await db.execute(stmt)
            p_row = res.scalar_one_or_none()
            if p_row:
                p_row.broker_id = leader_id
                p_row.replicas = replicas
                p_row.isr = replicas
                await db.commit()
                
            all_groups = await redis_client.smembers("kafkax:groups")
            for group_id in all_groups:
                await redis_client.delete(f"kafkax:group:{group_id}:assignments")
        else:
            return None
            
    if leader_id:
        for b in active_brokers:
            if b["broker_id"] == leader_id:
                return b
                
    return None

# Broker replica recovery background loop
async def broker_replica_recovery_loop(redis_client, db_session_maker, once: bool = False):
    current_broker = settings.BROKER_ID
    if not current_broker:
        return

    logger.info(json.dumps({
        "event": "replica_recovery_loop_started",
        "broker_id": current_broker
    }))

    while True:
        try:
            keys = []
            cursor = 0
            while True:
                cursor, scan_keys = await redis_client.scan(cursor, match="kafkax:topic:*:partition:*:metadata")
                keys.extend(scan_keys)
                if cursor == 0:
                    break

            for key in keys:
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                parts = key_str.split(":")
                if len(parts) < 6:
                    continue
                topic_name = parts[2]
                partition_num = int(parts[4])

                meta_str = await redis_client.get(key)
                if not meta_str:
                    continue
                meta = json.loads(meta_str)
                replicas = meta.get("replicas", [])
                isr = meta.get("isr", [])
                leader_id = meta.get("leader")

                if current_broker in replicas and current_broker not in isr:
                    logger.info(json.dumps({
                        "event": "replica_recovery_started",
                        "topic": topic_name,
                        "partition": partition_num,
                        "broker_id": current_broker,
                        "leader": leader_id
                    }))

                    if leader_id == current_broker:
                        meta["isr"] = list(set(isr + [current_broker]))
                        await redis_client.set(key, json.dumps(meta))
                        continue

                    leader_info_str = await redis_client.get(f"kafkax:broker:{leader_id}")
                    if not leader_info_str:
                        continue
                    leader_info = json.loads(leader_info_str)

                    async with db_session_maker() as session:
                        from app.repositories.event import EventRepository
                        repo = EventRepository(session)
                        
                        from app.models import Topic, Partition
                        stmt = select(Partition).join(Topic).where(
                            Topic.name == topic_name,
                            Partition.partition_number == partition_num
                        )
                        res = await session.execute(stmt)
                        partition_row = res.scalar_one_or_none()
                        start_offset = partition_row.next_offset if partition_row else 0

                        url = f"http://{leader_info['host']}:{leader_info['port']}/api/v1/cluster/internal/topics/{topic_name}/partitions/{partition_num}/events?start_offset={start_offset}"
                        async with httpx.AsyncClient() as client:
                            resp = await client.get(url, timeout=5.0)
                            if resp.status_code == 200:
                                events_data = resp.json()
                                if events_data:
                                    for evt_data in events_data:
                                        ts = datetime.fromisoformat(evt_data["timestamp"].replace("Z", "+00:00"))
                                        await repo.replicate_event(
                                            topic_name=topic_name,
                                            partition_number=partition_num,
                                            offset=evt_data["offset"],
                                            key=evt_data["key"],
                                            value=evt_data["value"],
                                            timestamp=ts,
                                            producer_id=evt_data["producer_id"]
                                        )
                                    logger.info(json.dumps({
                                        "event": "replica_replicated_batch",
                                        "topic": topic_name,
                                        "partition": partition_num,
                                        "count": len(events_data)
                                    }))

                        leader_offset_str = await redis_client.get(f"kafkax:topic:{topic_name}:partition:{partition_num}:latest_offset")
                        leader_offset = int(leader_offset_str) if leader_offset_str else 0
                        
                        res = await session.execute(stmt)
                        partition_row = res.scalar_one_or_none()
                        local_offset = partition_row.next_offset if partition_row else 0

                        if local_offset >= leader_offset:
                            meta_str = await redis_client.get(key)
                            if meta_str:
                                meta = json.loads(meta_str)
                                if current_broker not in meta.get("isr", []):
                                    meta["isr"].append(current_broker)
                                    await redis_client.set(key, json.dumps(meta))
                                    logger.info(json.dumps({
                                        "event": "replica_recovery_complete",
                                        "topic": topic_name,
                                        "partition": partition_num,
                                        "broker_id": current_broker,
                                        "current_isr": meta["isr"]
                                    }))
        except Exception as e:
            logger.error(f"Error in replica recovery loop: {e}")
            
        if once:
            break
        await asyncio.sleep(5)

