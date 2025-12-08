import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from database import db
from broker import broker
from consumer import start_consumer, stats as consumer_stats
from schemas import Event

api_stats = {
    "received": 0,
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    broker.connect()
    
    consumer_task = asyncio.create_task(start_consumer())
    yield

    consumer_task.cancel()
    await db.disconnect()
    await broker.disconnect()

app = FastAPI(lifespan=lifespan, title="Distributed Log Aggregator")

@app.post("/publish", status_code=200)
async def publish_event(event: Event):
    try:
        await broker.publish(event.model_dump_json())
        api_stats["received"] += 1

        return {
            "status": "queued",
            "event_id": event.event_id
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/events")
async def read_events(topic: Optional[str] = None, limit: int = 100):
    return await db.get_events(topic, limit)

@app.get("/stats")
async def read_stats():
    db_count = await db.get_total_count()
    return {
        "uptime_stats" : {
            "total_received": api_stats["received"],
            "duplicates_dropped": consumer_stats["duplicates_dropped"],
        },
        "database_stats": {
            "unique_events": db_count,
        },
    }