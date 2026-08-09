from datetime import datetime
from typing import Any, Optional
from sqlalchemy import String, Integer, DateTime, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.core.database import Base

class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    offset: Mapped[int] = mapped_column(Integer, nullable=False)
    key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    value: Mapped[Any] = mapped_column(JSON, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    producer_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    __table_args__ = (
        Index("idx_topic_partition_offset", "topic", "partition", "offset", unique=True),
        Index("idx_topic_partition_timestamp", "topic", "partition", "timestamp"),
    )
