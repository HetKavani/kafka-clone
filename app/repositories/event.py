from datetime import datetime
from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import Partition, Event, Topic

class EventRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def publish(
        self,
        topic: Topic,
        partition_number: int,
        key: Optional[str],
        value: any,
        producer_id: Optional[str] = None
    ) -> Event:
        # Atomic lock on the partition row using SELECT ... FOR UPDATE
        stmt = (
            select(Partition)
            .where(
                Partition.topic_id == topic.id,
                Partition.partition_number == partition_number
            )
            .with_for_update()
        )
        
        result = await self.db.execute(stmt)
        partition = result.scalar_one_or_none()
        
        if not partition:
            raise ValueError(f"Partition {partition_number} not found for topic {topic.name}")
        
        # Determine offset and update next_offset
        offset = partition.next_offset
        partition.next_offset += 1
        
        # Create the event record
        event = Event(
            topic=topic.name,
            partition=partition_number,
            offset=offset,
            key=key,
            value=value,
            timestamp=datetime.utcnow(),
            producer_id=producer_id
        )
        
        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)
        return event

    async def get_events(
        self,
        topic_name: str,
        partition_number: int,
        start_offset: int,
        limit: int = 100
    ) -> List[Event]:
        stmt = (
            select(Event)
            .where(
                Event.topic == topic_name,
                Event.partition == partition_number,
                Event.offset >= start_offset
            )
            .order_by(Event.offset)
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_events_from_timestamp(
        self,
        topic_name: str,
        partition_number: int,
        timestamp: datetime,
        limit: int = 100
    ) -> List[Event]:
        stmt = (
            select(Event)
            .where(
                Event.topic == topic_name,
                Event.partition == partition_number,
                Event.timestamp >= timestamp
            )
            .order_by(Event.offset)
            .limit(limit)
        )
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def get_partition_offset(self, topic_id: int, partition_number: int) -> int:
        stmt = select(Partition.next_offset).where(
            Partition.topic_id == topic_id,
            Partition.partition_number == partition_number
        )
        result = await self.db.execute(stmt)
        val = result.scalar_one_or_none()
        return val if val is not None else 0
