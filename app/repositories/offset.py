import json
import logging
from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from app.models import ConsumerOffset

logger = logging.getLogger("kafkax")

class OffsetRepository:
    def __init__(self, db: AsyncSession):
        self.db = db


    async def get(self, group_id: str, topic: str, partition: int) -> Optional[ConsumerOffset]:
        stmt = select(ConsumerOffset).where(
            ConsumerOffset.group_id == group_id,
            ConsumerOffset.topic == topic,
            ConsumerOffset.partition == partition
        )
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_group(self, group_id: str) -> List[ConsumerOffset]:
        stmt = select(ConsumerOffset).where(ConsumerOffset.group_id == group_id)
        result = await self.db.execute(stmt)
        return list(result.scalars().all())

    async def commit(self, group_id: str, topic: str, partition: int, offset: int) -> ConsumerOffset:
        # Cross-db compatible upsert logic
        stmt = select(ConsumerOffset).where(
            ConsumerOffset.group_id == group_id,
            ConsumerOffset.topic == topic,
            ConsumerOffset.partition == partition
        )
        result = await self.db.execute(stmt)
        consumer_offset = result.scalar_one_or_none()

        if consumer_offset:
            consumer_offset.committed_offset = offset
            consumer_offset.updated_at = datetime.utcnow()
        else:
            consumer_offset = ConsumerOffset(
                group_id=group_id,
                topic=topic,
                partition=partition,
                committed_offset=offset,
                updated_at=datetime.utcnow()
            )
            self.db.add(consumer_offset)

        await self.db.commit()
        await self.db.refresh(consumer_offset)
        logger.info(json.dumps({
            "event": "offset_commits",
            "group_id": group_id,
            "topic": topic,
            "partition": partition,
            "offset": offset
        }))
        return consumer_offset
