import json
import logging
import asyncpg
from config import settings
from datetime import datetime

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        logger.info("Connecting to Database...")
        self.pool = await asyncpg.create_pool(settings.DATABASE_URL)
    
    async def disconnect(self):
        logger.info("Closing Database connection...")
        if self.pool:
            await self.pool.close()
    
    async def insert_event(self, event_dict: dict) -> str :
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                status = await conn.execute("""
                    INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (topic, event_id) DO NOTHING
                """,
                event_dict["topic"],
                event_dict["event_id"],
                datetime.fromisoformat(event_dict["timestamp"]),
                event_dict["source"],
                json.dumps(event_dict["payload"]),
                )

                return "INSERTED" if status == "INSERT 0 1" else "DUPLICATE"
    
    async def get_events(self, topic= None, limit= 100):
        query = "SELECT topic, event_id, timestamp, source FROM processed_events"
        args = []
        if topic:
            query += " WHERE topic = $1"
            args.append(topic)
        query += " ORDER BY timestamp DESC LIMIT $2"
        args.append(limit)

        rows = await self.pool.fetch(query, *args)
        return [dict(row) for row in rows]
    
    async def get_total_count(self):
        return await self.pool.fetchval("SELECT COUNT(*) FROM processed_events")
    
db = Database()