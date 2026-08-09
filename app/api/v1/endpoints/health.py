from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import redis.asyncio as aioredis

from app.core.database import get_db
from app.core.redis import get_redis
from app.schemas.health import HealthResponse

router = APIRouter()

@router.get("", response_model=HealthResponse)
async def check_general_health(
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    postgres_ok = True
    postgres_detail = "Healthy"
    try:
        await db.execute(text("SELECT 1"))
    except Exception as e:
        postgres_ok = False
        postgres_detail = f"Unhealthy: {str(e)}"

    redis_ok = True
    redis_detail = "Healthy"
    try:
        await redis_client.ping()
    except Exception as e:
        redis_ok = False
        redis_detail = f"Unhealthy: {str(e)}"

    overall = "healthy" if (postgres_ok and redis_ok) else "unhealthy"
    
    return HealthResponse(
        status=overall,
        details={
            "postgres": postgres_detail,
            "redis": redis_detail
        }
    )

@router.get("/postgres", response_model=HealthResponse)
async def check_postgres_health(db: AsyncSession = Depends(get_db)):
    postgres_ok = True
    postgres_detail = "Healthy"
    try:
        await db.execute(text("SELECT 1"))
    except Exception as e:
        postgres_ok = False
        postgres_detail = f"Unhealthy: {str(e)}"

    overall = "healthy" if postgres_ok else "unhealthy"
    
    return HealthResponse(
        status=overall,
        details={"postgres": postgres_detail}
    )

@router.get("/redis", response_model=HealthResponse)
async def check_redis_health(redis_client: aioredis.Redis = Depends(get_redis)):
    redis_ok = True
    redis_detail = "Healthy"
    try:
        await redis_client.ping()
    except Exception as e:
        redis_ok = False
        redis_detail = f"Unhealthy: {str(e)}"

    overall = "healthy" if redis_ok else "unhealthy"
    
    return HealthResponse(
        status=overall,
        details={"redis": redis_detail}
    )
