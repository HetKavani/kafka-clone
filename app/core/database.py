import json
import logging
from typing import AsyncGenerator
from urllib.parse import urlparse, urlunparse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings

logger = logging.getLogger("kafkax")

async def ensure_postgres_db_exists(database_url: str):
    if not database_url.startswith("postgresql"):
        return
    try:
        parsed = urlparse(database_url)
        db_name = parsed.path.lstrip('/')
        if not db_name or db_name == "postgres":
            return
        
        new_path = "/postgres"
        parsed_base = parsed._replace(path=new_path)
        base_url = urlunparse(parsed_base)
        
        temp_engine = create_async_engine(base_url, isolation_level="AUTOCOMMIT")
        async with temp_engine.connect() as conn:
            result = await conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = :db_name"), {"db_name": db_name})
            exists = result.scalar()
            if not exists:
                # Run raw create database (must not be inside transaction block, hence AUTOCOMMIT)
                await conn.execute(text(f"CREATE DATABASE {db_name}"))
                logger.info(json.dumps({"event": "database_created", "database": db_name}))
        await temp_engine.dispose()
    except Exception as e:
        logger.error(f"Error ensuring database exists: {e}")


# Create async engine. Use pool_pre_ping=True to check connection health.
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    echo=False,
)

async_session = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

class Base(DeclarativeBase):
    pass

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()
