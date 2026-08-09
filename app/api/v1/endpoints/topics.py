from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.topic import TopicCreate, TopicResponse
from app.repositories.topic import TopicRepository

router = APIRouter()

@router.post("/", response_model=TopicResponse, status_code=status.HTTP_201_CREATED)
async def create_topic(req: TopicCreate, db: AsyncSession = Depends(get_db)):
    repo = TopicRepository(db)
    existing = await repo.get_by_name(req.name)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Topic '{req.name}' already exists"
        )
    return await repo.create(name=req.name, partition_count=req.partition_count)

@router.get("/", response_model=List[TopicResponse])
async def list_topics(db: AsyncSession = Depends(get_db)):
    repo = TopicRepository(db)
    return await repo.list_all()

@router.get("/{topic}", response_model=TopicResponse)
async def get_topic(topic: str, db: AsyncSession = Depends(get_db)):
    repo = TopicRepository(db)
    res = await repo.get_by_name(topic)
    if not res:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Topic '{topic}' not found"
        )
    return res

@router.delete("/{topic}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_topic(topic: str, db: AsyncSession = Depends(get_db)):
    repo = TopicRepository(db)
    deleted = await repo.delete(topic)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Topic '{topic}' not found"
        )
    return None
