from datetime import datetime
from pydantic import BaseModel, Field, field_validator
import re

class TopicCreate(BaseModel):
    name: str = Field(..., description="The unique name of the topic")
    partition_count: int = Field(default=1, ge=1, description="Number of partitions to create")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not re.match(r"^[a-zA-Z0-9_\-\.]+$", v):
            raise ValueError("Topic name can only contain alphanumeric characters, underscores, hyphens, and dots.")
        return v

class TopicResponse(BaseModel):
    id: int
    name: str
    partition_count: int
    created_at: datetime

    class Config:
        from_attributes = True
