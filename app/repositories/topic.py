from typing import List, Optional
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import Topic, Partition

class TopicRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_name(self, name: str) -> Optional[Topic]:
        stmt = select(Topic).where(Topic.name == name).options(selectinload(Topic.partitions))
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def list_all(self) -> List[Topic]:
        stmt = select(Topic).options(selectinload(Topic.partitions))
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def create(self, name: str, partition_count: int, broker_ids: Optional[List[str]] = None) -> Topic:
        import json
        import logging
        logger = logging.getLogger("kafkax")
        
        topic = Topic(name=name, partition_count=partition_count)
        self.db.add(topic)
        # Flush to populate the topic.id
        await self.db.flush()
        
        # Create partitions
        for i in range(partition_count):
            broker_id = None
            if broker_ids:
                broker_id = broker_ids[i % len(broker_ids)]
            partition = Partition(topic_id=topic.id, partition_number=i, next_offset=0, broker_id=broker_id)
            self.db.add(partition)
            if broker_id:
                logger.info(json.dumps({
                    "event": "partition_assignment",
                    "topic": name,
                    "partition": i,
                    "broker_id": broker_id
                }))
            
        await self.db.commit()
        # Refresh to populate relationships
        await self.db.refresh(topic)
        return topic

    async def delete(self, name: str) -> bool:
        topic = await self.get_by_name(name)
        if not topic:
            return False
        
        # Delete topic (will cascade delete partitions)
        await self.db.delete(topic)
        await self.db.commit()
        return True
