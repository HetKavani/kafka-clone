import json
import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

logger = logging.getLogger("kafkax")

class CoordinatorService:
    def __init__(self, redis_client: aioredis.Redis, db: Optional[AsyncSession] = None):
        self.redis = redis_client
        self.db = db

    def _member_key(self, group_id: str, consumer_id: str) -> str:
        return f"kafkax:group:{group_id}:member:{consumer_id}"

    def _group_set_key(self, group_id: str) -> str:
        return f"kafkax:group:{group_id}:members"

    def _metadata_key(self, group_id: str, consumer_id: str) -> str:
        return f"kafkax:group:{group_id}:metadata:{consumer_id}"

    async def heartbeat(self, group_id: str, consumer_id: str, topics: List[str]) -> None:
        # Check if member already exists to distinguish registration vs heartbeat
        is_registered = await self.redis.exists(self._member_key(group_id, consumer_id))
        
        # Save membership flag with a 10-second TTL
        await self.redis.set(self._member_key(group_id, consumer_id), "active", ex=10)
        # Add to the group members set
        await self.redis.sadd(self._group_set_key(group_id), consumer_id)
        # Save metadata (subscribed topics)
        await self.redis.set(self._metadata_key(group_id, consumer_id), json.dumps(topics), ex=10)
        # Register group ID globally
        await self.redis.sadd("kafkax:groups", group_id)

        if not is_registered:
            logger.info(json.dumps({
                "event": "consumer_registration",
                "group_id": group_id,
                "consumer_id": consumer_id,
                "topics": topics
            }))
        else:
            logger.info(json.dumps({
                "event": "consumer_heartbeat",
                "group_id": group_id,
                "consumer_id": consumer_id
            }))

    async def get_active_members(self, group_id: str) -> List[str]:
        # Get all members in the group set
        all_members = await self.redis.smembers(self._group_set_key(group_id))
        active_members = []
        
        for member_id in list(all_members):
            # Check if heartbeat key still exists
            is_active = await self.redis.exists(self._member_key(group_id, member_id))
            if is_active:
                active_members.append(member_id)
            else:
                # Remove inactive member from set
                await self.redis.srem(self._group_set_key(group_id), member_id)
                await self.redis.delete(self._metadata_key(group_id, member_id))
                
        return sorted(active_members)

    async def get_assignments(
        self,
        group_id: str,
        consumer_id: str,
        topic_name: str,
        partition_count: int
    ) -> List[int]:
        active_members = await self.get_active_members(group_id)
        if consumer_id not in active_members:
            return []

        # Read assignment and check if we need to rebalance
        assignments_str = await self.redis.get(f"kafkax:group:{group_id}:assignments")
        
        # Build current state representation to check if it matches the last rebalance state
        current_state = {}
        for member_id in active_members:
            meta_str = await self.redis.get(self._metadata_key(group_id, member_id))
            current_state[member_id] = json.loads(meta_str) if meta_str else []
            
        last_state_str = await self.redis.get(f"kafkax:group:{group_id}:last_state")
        needs_rebalance = (
            assignments_str is None or
            last_state_str is None or
            json.loads(last_state_str) != current_state
        )
        
        if needs_rebalance:
            # We will run rebalance for the entire group
            new_assignments = {member: {} for member in active_members}
            
            # Collect all unique topics subscribed by group members
            unique_topics = set()
            for topics in current_state.values():
                unique_topics.update(topics)
                
            # Query partition count for each topic
            if self.db:
                from app.repositories.topic import TopicRepository
                topic_repo = TopicRepository(self.db)
                
                for t_name in unique_topics:
                    if t_name == topic_name:
                        t_parts = partition_count
                    else:
                        t_meta = await topic_repo.get_by_name(t_name)
                        t_parts = t_meta.partition_count if t_meta else 0
                        
                    if t_parts > 0:
                        # Get consumers subscribed to t_name
                        subscribers = [m for m, topics in current_state.items() if t_name in topics]
                        subscribers.sort()  # deterministic
                        
                        if subscribers:
                            for part_num in range(t_parts):
                                assigned_member = subscribers[part_num % len(subscribers)]
                                if t_name not in new_assignments[assigned_member]:
                                    new_assignments[assigned_member][t_name] = []
                                new_assignments[assigned_member][t_name].append(part_num)
            else:
                # Fallback: only balance the requested topic_name
                subscribers = [m for m, topics in current_state.items() if topic_name in topics]
                subscribers.sort()
                if subscribers:
                    for part_num in range(partition_count):
                        assigned_member = subscribers[part_num % len(subscribers)]
                        if topic_name not in new_assignments[assigned_member]:
                            new_assignments[assigned_member][topic_name] = []
                        new_assignments[assigned_member][topic_name].append(part_num)
                        
            # Save new assignments and state in Redis
            await self.redis.set(f"kafkax:group:{group_id}:assignments", json.dumps(new_assignments))
            await self.redis.set(f"kafkax:group:{group_id}:last_state", json.dumps(current_state))
            
            logger.info(json.dumps({
                "event": "partition_rebalance",
                "group_id": group_id,
                "reason": "membership_or_subscription_change"
            }))
            logger.info(json.dumps({
                "event": "partition_assignment",
                "group_id": group_id,
                "assignments": new_assignments
            }))
            
            assignments_str = json.dumps(new_assignments)
            
        all_assignments = json.loads(assignments_str)
        member_assignments = all_assignments.get(consumer_id, {})
        return member_assignments.get(topic_name, [])

