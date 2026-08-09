import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_health_endpoints(client: AsyncClient):
    # General health check
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"
    assert "postgres" in resp.json()["details"]
    assert "redis" in resp.json()["details"]

    # Postgres specific health check
    resp_pg = await client.get("/api/v1/health/postgres")
    assert resp_pg.status_code == 200
    assert resp_pg.json()["status"] == "healthy"

    # Redis specific health check
    resp_redis = await client.get("/api/v1/health/redis")
    assert resp_redis.status_code == 200
    assert resp_redis.json()["status"] == "healthy"
