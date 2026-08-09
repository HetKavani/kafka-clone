from typing import Optional, Any
from sqlalchemy import Integer, ForeignKey, UniqueConstraint, String, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base

class Partition(Base):
    __tablename__ = "partitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic_id: Mapped[int] = mapped_column(Integer, ForeignKey("topics.id", ondelete="CASCADE"), nullable=False)
    partition_number: Mapped[int] = mapped_column(Integer, nullable=False)
    next_offset: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    broker_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    replicas: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    isr: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)

    topic: Mapped["Topic"] = relationship("Topic", back_populates="partitions")



    __table_args__ = (
        UniqueConstraint("topic_id", "partition_number", name="uq_topic_partition"),
    )
