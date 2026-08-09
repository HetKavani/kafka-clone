import json
import logging
from datetime import datetime
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.event import EventRepository
from app.repositories.topic import TopicRepository
from app.repositories.offset import OffsetRepository
from app.models import Event, ConsumerOffset

logger = logging.getLogger("kafkax")

class ConsumerService:
    def __init__(self, db: AsyncSession):
        self.db = db
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
        topic = await self.topic_repo.get_by_name(topic_name)
        if not topic:
            raise ValueError(f"Topic '{topic_name}' not found")

        # Resolve starting offset based on strategy
        start_offset = 0

        if strategy == "earliest":
            start_offset = 0
            
        elif strategy == "latest":
            # Fetch the next offset from partitions table
            start_offset = await self.event_repo.get_partition_offset(topic.id, partition)
            
        elif strategy == "specific":
            if offset is None:
                raise ValueError("Offset must be specified when using 'specific' strategy")
            start_offset = offset
            
        elif strategy == "committed":
            if not group_id:
                raise ValueError("Group ID must be specified when using 'committed' strategy")
            committed = await self.offset_repo.get(group_id, topic_name, partition)
            if committed:
                start_offset = committed.committed_offset
            else:
                # Fallback to earliest if no offset has been committed yet
                start_offset = 0
                
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

        return events

    async def commit_offset(self, group_id: str, topic: str, partition: int, offset: int) -> ConsumerOffset:
        return await self.offset_repo.commit(group_id, topic, partition, offset)

    async def get_committed_offsets(self, group_id: str) -> List[ConsumerOffset]:
        return await self.offset_repo.list_by_group(group_id)
