from typing import AsyncGenerator
import redis.asyncio as aioredis
from app.core.config import settings

class RedisManager:
    def __init__(self):
        self.client = None

    def connect(self) -> None:
        self.client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

    async def disconnect(self) -> None:
        if self.client:
            await self.client.close()

redis_manager = RedisManager()

async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    if redis_manager.client is None:
        redis_manager.connect()
    yield redis_manager.client
