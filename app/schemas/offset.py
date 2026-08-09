from datetime import datetime
from pydantic import BaseModel, Field

class ConsumerOffsetCommitRequest(BaseModel):
    topic: str = Field(..., description="Name of the topic")
    partition: int = Field(..., ge=0, description="Partition number")
    offset: int = Field(..., ge=0, description="Committed offset index")

class ConsumerOffsetResponse(BaseModel):
    group_id: str
    topic: str
    partition: int
    committed_offset: int
    updated_at: datetime

    class Config:
        from_attributes = True
