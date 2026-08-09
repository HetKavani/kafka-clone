import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.core.config import settings
from app.core.database import engine, Base, async_session, ensure_postgres_db_exists
from app.core.redis import redis_manager
from app.api.v1.router import api_router
from app.services.cluster import broker_heartbeat_loop, cluster_monitor_loop, broker_replica_recovery_loop

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions
    # 1. Ensure the PostgreSQL database exists
    await ensure_postgres_db_exists(settings.DATABASE_URL)

    # 2. Automatically create PostgreSQL tables if they don't exist and run self-healing migration
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        try:
            await conn.execute(text("ALTER TABLE partitions ADD COLUMN broker_id VARCHAR(255)"))
        except Exception:
            pass
        try:
            await conn.execute(text("ALTER TABLE partitions ADD COLUMN replicas JSON"))
        except Exception:
            pass
        try:
            await conn.execute(text("ALTER TABLE partitions ADD COLUMN isr JSON"))
        except Exception:
            pass

    # 3. Setup Redis Connection Pool
    redis_manager.connect()

    # 4. Start background tasks for cluster management
    app.state.bg_tasks = []
    if settings.BROKER_ID:
        app.state.bg_tasks.append(asyncio.create_task(broker_heartbeat_loop(redis_manager.client)))
        app.state.bg_tasks.append(asyncio.create_task(broker_replica_recovery_loop(redis_manager.client, async_session)))
    app.state.bg_tasks.append(asyncio.create_task(cluster_monitor_loop(redis_manager.client, async_session)))

    yield

    # Shutdown actions
    # 1. Cancel background tasks
    for task in app.state.bg_tasks:
        task.cancel()
    if app.state.bg_tasks:
        await asyncio.gather(*app.state.bg_tasks, return_exceptions=True)

    # 2. Close Redis connection pool
    await redis_manager.disconnect()


app = FastAPI(
    title=settings.PROJECT_NAME,
    description="KafkaX - A Python-based Kafka-inspired single-broker event streaming platform",
    version="1.0.0",
    lifespan=lifespan,
)

# Set CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Mount API v1 router
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/")
async def root():
    return {
        "message": "Welcome to KafkaX API. Go to /docs for API documentation.",
        "project": settings.PROJECT_NAME,
        "version": "1.0.0"
    }
