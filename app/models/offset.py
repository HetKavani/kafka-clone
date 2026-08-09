from datetime import datetime
from sqlalchemy import String, Integer, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.core.database import Base

class ConsumerOffset(Base):
    __tablename__ = "consumer_offsets"

    group_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    topic: Mapped[str] = mapped_column(String(255), primary_key=True)
    partition: Mapped[int] = mapped_column(Integer, primary_key=True)
    committed_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
