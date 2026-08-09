import json
import logging
import hashlib
import asyncio
from datetime import datetime
from typing import Optional, List
import redis.asyncio as aioredis
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from app.repositories.topic import TopicRepository
from app.repositories.event import EventRepository
from app.schemas.event import EventPublishRequest, EventPublishResponse
from app.models import Event
from app.core.config import settings

logger = logging.getLogger("kafkax")

class ProducerService:
    # Class-level dictionary of partition locks to serialize writes per partition within the process
    _partition_locks = {}

    def __init__(self, db: AsyncSession, redis_client: aioredis.Redis):
        self.db = db
        self.redis = redis_client
        self.topic_repo = TopicRepository(db)
        self.event_repo = EventRepository(db)

    async def _get_partition(self, topic_name: str, partition_count: int, req: EventPublishRequest) -> int:
        if req.partition is not None:
            if req.partition < 0 or req.partition >= partition_count:
                raise ValueError(f"Partition {req.partition} is out of bounds (0-{partition_count-1})")
            return req.partition

        if req.key is not None:
            hasher = hashlib.md5(req.key.encode("utf-8"))
            hash_val = int(hasher.hexdigest(), 16)
            return hash_val % partition_count

        rr_key = f"kafkax:topic:{topic_name}:rr_counter"
        counter = await self.redis.incr(rr_key)
        return counter % partition_count

    async def _get_or_init_partition_metadata(self, topic_name: str, partition: int) -> dict:
        meta_key = f"kafkax:topic:{topic_name}:partition:{partition}:metadata"
        meta_str = await self.redis.get(meta_key)
        if meta_str:
            return json.loads(meta_str)

        from app.services.cluster import get_active_brokers
        active = await get_active_brokers(self.redis)
        active_ids = sorted([b["broker_id"] for b in active])

        if not active_ids:
            fallback_id = settings.BROKER_ID or "broker-1"
            active_ids = [fallback_id]

        leader = active_ids[partition % len(active_ids)]
        rf = settings.REPLICATION_FACTOR
        replicas = []
        for i in range(min(rf, len(active_ids))):
            replicas.append(active_ids[(partition + i) % len(active_ids)])

        meta = {
            "leader": leader,
            "replicas": replicas,
            "isr": replicas
        }
        await self.redis.set(meta_key, json.dumps(meta))
        return meta

    async def _replicate_to_follower(
        self,
        follower_id: str,
        topic: str,
        partition: int,
        offset: int,
        key: Optional[str],
        value: any,
        timestamp: datetime,
        producer_id: Optional[str]
    ) -> bool:
        b_info_str = await self.redis.get(f"kafkax:broker:{follower_id}")
        if not b_info_str:
            return False
        b_info = json.loads(b_info_str)

        url = f"http://{b_info['host']}:{b_info['port']}/api/v1/cluster/internal/replicate"
        payload = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "key": key,
            "value": value,
            "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp),
            "producer_id": producer_id
        }
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=payload, timeout=2.0)
                if resp.status_code == 201:
                    return True
                logger.error(f"Failed to replicate to follower {follower_id}: {resp.text}")
        except Exception as e:
            logger.error(f"Error replicating to follower {follower_id}: {e}")
        return False

    async def _replicate_to_followers(
        self,
        followers: List[str],
        topic: str,
        partition: int,
        offset: int,
        key: Optional[str],
        value: any,
        timestamp: datetime,
        producer_id: Optional[str]
    ) -> List[tuple[str, bool]]:
        tasks = [self._replicate_to_follower(fid, topic, partition, offset, key, value, timestamp, producer_id) for fid in followers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out = []
        for fid, res in zip(followers, results):
            if isinstance(res, Exception) or not res:
                out.append((fid, False))
            else:
                out.append((fid, True))
        return out

    async def _remove_from_isr(self, topic: str, partition: int, failed_follower: str):
        meta_key = f"kafkax:topic:{topic}:partition:{partition}:metadata"
        meta_str = await self.redis.get(meta_key)
        if meta_str:
            meta = json.loads(meta_str)
            if failed_follower in meta.get("isr", []):
                meta["isr"].remove(failed_follower)
                await self.redis.set(meta_key, json.dumps(meta))
                logger.warning(json.dumps({
                    "event": "isr_update",
                    "topic": topic,
                    "partition": partition,
                    "removed_broker": failed_follower,
                    "current_isr": meta["isr"],
                    "reason": "replication_failure"
                }))

    async def publish(
        self,
        topic_name: str,
        req: EventPublishRequest,
        producer_id: Optional[str] = None
    ) -> EventPublishResponse:
        start_time = datetime.utcnow()
        topic = await self.topic_repo.get_by_name(topic_name)
        if not topic:
            raise ValueError(f"Topic '{topic_name}' not found")

        partition = await self._get_partition(topic_name, topic.partition_count, req)
        meta = await self._get_or_init_partition_metadata(topic_name, partition)
        
        current_broker = settings.BROKER_ID or "broker-1"
        isr = meta.get("isr", [current_broker])
        followers = [f for f in isr if f != current_broker]

        if req.acks == "none":
            session_factory = async_sessionmaker(
                bind=self.db.bind,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            async def _bg_publish():
                lock_key = (topic_name, partition)
                if lock_key not in ProducerService._partition_locks:
                    ProducerService._partition_locks[lock_key] = asyncio.Lock()
                    
                async with ProducerService._partition_locks[lock_key]:
                    async with session_factory() as bg_session:
                        try:
                            bg_topic_repo = TopicRepository(bg_session)
                            bg_topic = await bg_topic_repo.get_by_name(topic_name)
                            bg_event_repo = EventRepository(bg_session)
                            bg_event = await bg_event_repo.publish(
                                topic=bg_topic,
                                partition_number=partition,
                                key=req.key,
                                value=req.value,
                                producer_id=producer_id
                            )
                            await self.redis.set(f"kafkax:topic:{topic_name}:partition:{partition}:latest_offset", bg_event.offset + 1)
                            
                            if followers:
                                await self._replicate_to_followers(
                                    followers=followers,
                                    topic=topic_name,
                                    partition=partition,
                                    offset=bg_event.offset,
                                    key=req.key,
                                    value=req.value,
                                    timestamp=bg_event.timestamp,
                                    producer_id=producer_id
                                )

                            logger.info(json.dumps({
                                "event": "event_publishing",
                                "topic": bg_topic.name,
                                "partition": partition,
                                "offset": bg_event.offset,
                                "producer_id": producer_id,
                                "acks": "none"
                            }))
                        except Exception:
                            pass

            asyncio.create_task(_bg_publish())
            
            duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            await self.redis.incr("kafkax:metrics:events_published")
            await self.redis.incrbyfloat("kafkax:metrics:publish_time_ms", duration_ms)

            return EventPublishResponse(
                topic=topic_name,
                partition=partition,
                offset=-1,
                timestamp=datetime.utcnow()
            )

        elif req.acks == "leader":
            lock_key = (topic_name, partition)
            if lock_key not in ProducerService._partition_locks:
                ProducerService._partition_locks[lock_key] = asyncio.Lock()
                
            async with ProducerService._partition_locks[lock_key]:
                event = await self.event_repo.publish(
                    topic=topic,
                    partition_number=partition,
                    key=req.key,
                    value=req.value,
                    producer_id=producer_id
                )
                await self.redis.set(f"kafkax:topic:{topic_name}:partition:{partition}:latest_offset", event.offset + 1)
                
                if followers:
                    asyncio.create_task(self._replicate_to_followers(
                        followers=followers,
                        topic=topic_name,
                        partition=partition,
                        offset=event.offset,
                        key=req.key,
                        value=req.value,
                        timestamp=event.timestamp,
                        producer_id=producer_id
                    ))

                logger.info(json.dumps({
                    "event": "event_publishing",
                    "topic": event.topic,
                    "partition": event.partition,
                    "offset": event.offset,
                    "producer_id": producer_id,
                    "acks": "leader"
                }))

                duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
                await self.redis.incr("kafkax:metrics:events_published")
                await self.redis.incrbyfloat("kafkax:metrics:publish_time_ms", duration_ms)

                return EventPublishResponse(
                    topic=event.topic,
                    partition=event.partition,
                    offset=event.offset,
                    timestamp=event.timestamp
                )

        else: # acks == "all"
            lock_key = (topic_name, partition)
            if lock_key not in ProducerService._partition_locks:
                ProducerService._partition_locks[lock_key] = asyncio.Lock()
                
            async with ProducerService._partition_locks[lock_key]:
                event = await self.event_repo.publish(
                    topic=topic,
                    partition_number=partition,
                    key=req.key,
                    value=req.value,
                    producer_id=producer_id
                )
                await self.redis.set(f"kafkax:topic:{topic_name}:partition:{partition}:latest_offset", event.offset + 1)
                
                if followers:
                    rep_results = await self._replicate_to_followers(
                        followers=followers,
                        topic=topic_name,
                        partition=partition,
                        offset=event.offset,
                        key=req.key,
                        value=req.value,
                        timestamp=event.timestamp,
                        producer_id=producer_id
                    )
                    
                    for fid, success in rep_results:
                        if not success:
                            await self._remove_from_isr(topic_name, partition, fid)

                logger.info(json.dumps({
                    "event": "event_publishing",
                    "topic": event.topic,
                    "partition": event.partition,
                    "offset": event.offset,
                    "producer_id": producer_id,
                    "acks": "all"
                }))

                duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
                await self.redis.incr("kafkax:metrics:events_published")
                await self.redis.incrbyfloat("kafkax:metrics:publish_time_ms", duration_ms)

                return EventPublishResponse(
                    topic=event.topic,
                    partition=event.partition,
                    offset=event.offset,
                    timestamp=event.timestamp
                )

