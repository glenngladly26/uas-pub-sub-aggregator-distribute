import asyncio
import json
import logging
from broker import broker
from database import db

logger = logging.getLogger(__name__)

stats = {
    "duplicates_dropped": 0,
}

async def start_consumer():
    logger.info("Consumer Worker Started...")
    while True:
        try:
            result = await broker.consume()
            if not result:
                continue
            
            _, raw_data = result
            event_dict = json.loads(raw_data)

            status = await db.insert_event(event_dict)

            if status == "INSERTED":
                logger.info(f"PROCESSED: {event_dict['event_id']}")
            else:
                stats["duplicates_dropped"] += 1
                logger.warning(f"DUPLICATE DROPPED: {event_dict['event_id']}")

        except Exception as e:
            logger.error(f"Consumer Error: {e}")
            await asyncio.sleep(1)