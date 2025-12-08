import asyncio
import logging

import aiohttp
from config import settings
from generator import EventGenerator
from client import AsyncClient

logging.basicConfig(
    level= logging.INFO,
    format= '%(asctime)s - %(message)s'
)
logger = logging.getLogger("Main")

async def wait_for_aggregator():
    url = settings.TARGET_URL.replace("/publish", "/stats")
    logger.info(f"Waiting for Aggregator at {url}...")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=2) as response:
                    if response.status == 200:
                        logger.info("Aggregator is UP")
                        return
            except Exception:
                pass

            logger.info("Aggregator not ready, retrying in 2s...")
            await asyncio.sleep(2)

async def worker(
    session,
    semaphore,
    generator,
    client,
    counter,
):
    async with semaphore:
        event, is_duplicate = generator.create_event()
        success = await client.send_event(
            session,
            settings.TARGET_URL,
            event.model_dump()
        )

        if success and counter % 100 == 0:
            status_msg = "[DUPLICATE]" if is_duplicate else "[NEW]"
            logger.info(
                f"Send {counter}/{settings.TOTAL_EVENTS} {status_msg} - ID: {event.event_id[:8]}..."
            )

async def main():
    await wait_for_aggregator()

    logger.info("Starting Publisher")
    logger.info(f"Target {settings.TOTAL_EVENTS} events | Duplicate Ratio: {settings.DUPLICATE_RATIO}%")

    generator = EventGenerator()
    client = AsyncClient()
    semaphore = asyncio.Semaphore(settings.CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(settings.TOTAL_EVENTS):
            task = asyncio.create_task(
                worker(session, semaphore, generator, client, i+1)
            )
        
        tasks.append(task)
        await asyncio.gather(*tasks)

    logger.info("Publishing Completed")

if __name__ == "__main__":
    asyncio.run(main())