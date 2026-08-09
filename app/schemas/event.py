from datetime import datetime
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator

class EventPublishRequest(BaseModel):
    key: Optional[str] = Field(None, description="Partition key, hashed if partition is not explicitly provided")
    value: Any = Field(..., description="Message payload, must be JSON serializable")
    partition: Optional[int] = Field(None, ge=0, description="Explicit target partition index")
    acks: str = Field("leader", description="Ack settings: none or leader")

    @field_validator("acks")
    @classmethod
    def validate_acks(cls, v: str) -> str:
        if v not in ("none", "leader"):
            raise ValueError("Acks must be either 'none' or 'leader'. 'all' is not supported yet.")
        return v

class EventPublishResponse(BaseModel):
    topic: str
    partition: int
    offset: int
    timestamp: datetime

class EventResponse(BaseModel):
    topic: str
    partition: int
    offset: int
    key: Optional[str] = None
    value: Any
    timestamp: datetime
    producer_id: Optional[str] = None

    class Config:
        from_attributes = True
