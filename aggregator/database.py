import json
import logging
import asyncpg
from config import settings
from datetime import datetime, timezone

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
        try:
            dt_obj = datetime.fromisoformat(event_dict["timestamp"])

            if dt_obj.tzinfo is not None:
                dt_obj = dt_obj.astimezone(timezone.utc).replace(tzinfo=None)
        except ValueError:
            dt_obj = dt_obj.replace(tzinfo=None)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                status = await conn.execute("""
                    INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (topic, event_id) DO NOTHING
                """,
                event_dict["topic"],
                event_dict["event_id"],
                dt_obj,
                event_dict["source"],
                json.dumps(event_dict["payload"]),
                )

                return "INSERTED" if status == "INSERT 0 1" else "DUPLICATE"
    
    async def get_events(self, topic= None, limit= 100):
        query = "SELECT topic, event_id, timestamp, source FROM processed_events"
        args = []

        if topic:
            query += " WHERE topic = $1::text"
            args.append(topic)

        limit_param_index = len(args) + 1    
        query += f" ORDER BY timestamp DESC LIMIT ${limit_param_index}::int"
        args.append(limit)

        rows = await self.pool.fetch(query, *args)
        result = []

        for row in rows:
            row_dict = dict(row)
            if isinstance(row_dict["timestamp"], datetime):
                row_dict["timestamp"] = row_dict["timestamp"].isoformat()
            result.append(row_dict)

        return result
    
    async def get_total_count(self):
        try:
            return await self.pool.fetchval("SELECT COUNT(*) FROM processed_events")
        except asyncpg.UndefinedTableError:
            return 0
        
db = Database()