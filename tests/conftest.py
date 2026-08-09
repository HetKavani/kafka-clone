import pytest
import asyncio
from typing import AsyncGenerator
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.core.database import Base, get_db
from app.core.redis import get_redis

# 1. In-memory SQLite for testing
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

engine = create_async_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestingSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# 2. Mock Redis Client
class MockRedis:
    def __init__(self):
        self.data = {}
        self.sets = {}
        self.counters = {}

    async def get(self, key: str):
        return self.data.get(key)

    async def set(self, key: str, value: str, ex: int = None):
        self.data[key] = str(value)

    async def exists(self, key: str) -> bool:
        return key in self.data or key in self.sets

    async def delete(self, key: str):
        self.data.pop(key, None)
        self.sets.pop(key, None)
        self.counters.pop(key, None)

    async def incr(self, key: str) -> int:
        val = self.counters.get(key, 0) + 1
        self.counters[key] = val
        self.data[key] = str(val)
        return val

    async def sadd(self, key: str, value: str):
        if key not in self.sets:
            self.sets[key] = set()
        self.sets[key].add(str(value))
        return 1

    async def smembers(self, key: str):
        return self.sets.get(key, set())

    async def srem(self, key: str, value: str):
        if key in self.sets:
            self.sets[key].discard(str(value))
        return 1

    async def ping(self):
        return True

@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="function")
async def db() -> AsyncGenerator[AsyncSession, None]:
    # Setup: Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    async with TestingSessionLocal() as session:
        yield session
        
    # Teardown: Drop tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

@pytest.fixture(scope="function")
async def mock_redis() -> MockRedis:
    return MockRedis()

@pytest.fixture(scope="function")
async def client(db: AsyncSession, mock_redis: MockRedis) -> AsyncGenerator[AsyncClient, None]:
    # Override dependencies
    async def override_get_db():
        async with TestingSessionLocal() as session:
            try:
                yield session
            finally:
                await session.close()

    async def override_get_redis():
        yield mock_redis

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
