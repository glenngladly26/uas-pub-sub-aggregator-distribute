import asyncio
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from database import db
from broker import broker
from consumer import start_consumer, stats as consumer_stats
from schemas import Event

APP_START_TIME = 0
api_stats = {
    "received": 0,
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global APP_START_TIME
    APP_START_TIME = time.time()

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
    total_received = api_stats["received"]
    duplicates = consumer_stats["duplicates_dropped"]
    unique_stored = await db.get_total_count()
    
    current_time = time.time()
    uptime_seconds = current_time - APP_START_TIME
    
    throughput_rps = total_received / uptime_seconds if uptime_seconds > 0 else 0
    
    return {
        "performance_metrics": {
            "uptime_seconds": round(uptime_seconds, 2),
            "throughput_rps": round(throughput_rps, 2),
        },
        "traffic_stats": {
            "total_received_api": total_received,
            "duplicates_dropped_consumer": duplicates,
        },
        "database_stats": {
            "unique_events_stored": unique_stored
        }
    }