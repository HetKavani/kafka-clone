import httpx
import json
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Header, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from app.core.config import settings
from app.core.database import get_db
from app.core.redis import get_redis
from app.schemas.event import EventPublishRequest, EventPublishResponse
from app.services.producer import ProducerService
from app.repositories.topic import TopicRepository
from app.services.cluster import get_partition_owner

router = APIRouter()

@router.post("/{topic}/publish", response_model=EventPublishResponse, status_code=status.HTTP_201_CREATED)
async def publish_event(
    topic: str,
    req: EventPublishRequest,
    routed: bool = Query(False, description="Is this request already routed"),
    x_producer_id: Optional[str] = Header(None, alias="X-Producer-ID"),
    db: AsyncSession = Depends(get_db),
    redis_client: aioredis.Redis = Depends(get_redis)
):
    # Enforce payload size limit
    val_str = json.dumps(req.value)
    if len(val_str.encode("utf-8")) > settings.MAX_PAYLOAD_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Payload size limit exceeded"
        )

    service = ProducerService(db, redis_client)

    try:
        if not routed:
            # Resolve topic
            topic_repo = TopicRepository(db)
            topic_meta = await topic_repo.get_by_name(topic)
            if not topic_meta:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Topic '{topic}' not found"
                )
            
            # Select target partition
            partition = await service._get_partition(topic, topic_meta.partition_count, req)
            
            # Lookup owner
            owner = await get_partition_owner(db, redis_client, topic, partition)
            if owner and owner["broker_id"] != settings.BROKER_ID:
                # Force partition field in request to guarantee consistency on destination broker
                req.partition = partition
                async with httpx.AsyncClient() as client:
                    resp = await client.post(
                        f"http://{owner['host']}:{owner['port']}/api/v1/topics/{topic}/publish?routed=true",
                        json=req.model_dump(),
                        headers={"X-Producer-ID": x_producer_id} if x_producer_id else None,
                        timeout=5.0
                    )
                    if resp.status_code != 201:
                        raise HTTPException(status_code=resp.status_code, detail=resp.text)
                    return EventPublishResponse(**resp.json())
        
        # Local publish if routed or we are the owner
        return await service.publish(
            topic_name=topic,
            req=req,
            producer_id=x_producer_id
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

