from fastapi import APIRouter

from app.api.v1.endpoints import topics, publish, consumers, health, cluster

api_router = APIRouter()

api_router.include_router(topics.router, prefix="/topics", tags=["topics"])
api_router.include_router(publish.router, prefix="/topics", tags=["publish"])
api_router.include_router(consumers.router, tags=["consumers"])
api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(cluster.router, prefix="/cluster", tags=["cluster"])

