import json
import logging
from datetime import datetime
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.repositories.event import EventRepository
from app.repositories.topic import TopicRepository
from app.repositories.offset import OffsetRepository
from app.models import Event, ConsumerOffset

logger = logging.getLogger("kafkax")

class ConsumerService:
    def __init__(self, db: AsyncSession, redis_client: Optional[aioredis.Redis] = None):
        self.db = db
        self.redis = redis_client
        self.event_repo = EventRepository(db)
        self.topic_repo = TopicRepository(db)
        self.offset_repo = OffsetRepository(db)

    async def fetch_events(
        self,
        topic_name: str,
        partition: int,
        strategy: str,  # "earliest", "latest", "specific", "timestamp", "committed"
        group_id: Optional[str] = None,
        offset: Optional[int] = None,
        timestamp: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Event]:
        start_time = datetime.utcnow()
        topic = await self.topic_repo.get_by_name(topic_name)
        if not topic:
            raise ValueError(f"Topic '{topic_name}' not found")

        # Resolve starting offset based on strategy
        start_offset = 0

        if strategy == "earliest":
            start_offset = 0
            
        elif strategy == "latest":
            start_offset = await self.event_repo.get_partition_offset(topic.id, partition)
            
        elif strategy == "specific":
            if offset is None:
                raise ValueError("Offset must be specified when using 'specific' strategy")
            start_offset = offset
            
        elif strategy == "committed":
            if not group_id:
                raise ValueError("Group ID must be specified when using 'committed' strategy")
            
            offset_val = None
            if self.redis:
                redis_offset_str = await self.redis.get(f"kafkax:group:{group_id}:offset:{topic_name}:{partition}")
                if redis_offset_str is not None:
                    offset_val = int(redis_offset_str)

            if offset_val is None:
                committed = await self.offset_repo.get(group_id, topic_name, partition)
                if committed:
                    offset_val = committed.committed_offset
                else:
                    offset_val = 0
            start_offset = offset_val
                
        elif strategy == "timestamp":
            if not timestamp:
                raise ValueError("Timestamp must be specified when using 'timestamp' strategy")
            events = await self.event_repo.get_events_from_timestamp(
                topic_name=topic_name,
                partition_number=partition,
                timestamp=timestamp,
                limit=limit
            )
        else:
            raise ValueError(f"Unknown consumption strategy: {strategy}")

        if strategy != "timestamp":
            events = await self.event_repo.get_events(
                topic_name=topic_name,
                partition_number=partition,
                start_offset=start_offset,
                limit=limit
            )

        if group_id:
            for event in events:
                logger.info(json.dumps({
                    "event": "event_consumption",
                    "topic": event.topic,
                    "partition": event.partition,
                    "offset": event.offset,
                    "group_id": group_id
                }))

        # Track metrics
        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        if self.redis and events:
            await self.redis.incrby("kafkax:metrics:events_consumed", len(events))
            await self.redis.incr("kafkax:metrics:consume_count")
            await self.redis.incrbyfloat("kafkax:metrics:consume_time_ms", duration_ms)

        return events

    async def commit_offset(self, group_id: str, topic: str, partition: int, offset: int) -> ConsumerOffset:
        # Commit to DB
        db_offset = await self.offset_repo.commit(group_id, topic, partition, offset)
        # Commit to Redis
        if self.redis:
            await self.redis.set(f"kafkax:group:{group_id}:offset:{topic}:{partition}", str(offset))
            await self.redis.sadd("kafkax:groups", group_id)
        return db_offset

    async def get_committed_offsets(self, group_id: str) -> List[ConsumerOffset]:
        return await self.offset_repo.list_by_group(group_id)

