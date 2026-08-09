from typing import Dict, Any
from pydantic import BaseModel

class HealthResponse(BaseModel):
    status: str
    details: Dict[str, Any]
