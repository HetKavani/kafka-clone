import json
import logging
import hashlib
import asyncio
from datetime import datetime
from typing import Optional
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from app.repositories.topic import TopicRepository
from app.repositories.event import EventRepository
from app.schemas.event import EventPublishRequest, EventPublishResponse
from app.models import Event

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
        # 1. Explicit partition
        if req.partition is not None:
            if req.partition < 0 or req.partition >= partition_count:
                raise ValueError(f"Partition {req.partition} is out of bounds (0-{partition_count-1})")
            return req.partition

        # 2. Key-based hashing (deterministic)
        if req.key is not None:
            hasher = hashlib.md5(req.key.encode("utf-8"))
            hash_val = int(hasher.hexdigest(), 16)
            return hash_val % partition_count

        # 3. Round-Robin using Redis
        rr_key = f"kafkax:topic:{topic_name}:rr_counter"
        counter = await self.redis.incr(rr_key)
        return counter % partition_count

    async def publish(
        self,
        topic_name: str,
        req: EventPublishRequest,
        producer_id: Optional[str] = None
    ) -> EventPublishResponse:
        topic = await self.topic_repo.get_by_name(topic_name)
        if not topic:
            raise ValueError(f"Topic '{topic_name}' not found")

        partition = await self._get_partition(topic_name, topic.partition_count, req)

        # Helper function for execution
        async def _execute_publish(session: AsyncSession) -> Event:
            # We instantiate a fresh repo linked to the provided session
            repo = EventRepository(session)
            return await repo.publish(
                topic=topic,
                partition_number=partition,
                key=req.key,
                value=req.value,
                producer_id=producer_id
            )

        if req.acks == "none":
            # Fire and forget: run the commit in the background using a separate task/connection
            # We bind to self.db.bind to match the active engine (Postgres vs SQLite)
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
                            logger.info(json.dumps({
                                "event": "event_publishing",
                                "topic": bg_topic.name,
                                "partition": partition,
                                "offset": bg_event.offset,
                                "producer_id": producer_id
                            }))
                        except Exception as e:
                            # Log error or handle silently for acks=none
                            pass

            asyncio.create_task(_bg_publish())
            
            # Return immediate response without offset/durable confirm
            return EventPublishResponse(
                topic=topic_name,
                partition=partition,
                offset=-1,
                timestamp=datetime.utcnow()
            )

        else: # acks == "leader"
            lock_key = (topic_name, partition)
            if lock_key not in ProducerService._partition_locks:
                ProducerService._partition_locks[lock_key] = asyncio.Lock()
                
            async with ProducerService._partition_locks[lock_key]:
                # Synchronous wait for write
                event = await _execute_publish(self.db)
                logger.info(json.dumps({
                    "event": "event_publishing",
                    "topic": event.topic,
                    "partition": event.partition,
                    "offset": event.offset,
                    "producer_id": producer_id
                }))
                return EventPublishResponse(
                    topic=event.topic,
                    partition=event.partition,
                    offset=event.offset,
                    timestamp=event.timestamp
                )
