import redis.asyncio as redis
from config import settings
import logging

logger = logging.getLogger(__name__)

class Broker:
    def __init__(self):
        self.client = None
    
    def connect(self):
        logger.info("Connection to Redis Broker...")
        self.client = redis.from_url(settings.BROKER_URL, decode_responses=True)

    async def disconnect(self):
        logger.info("Closing Broker connection...")
        if self.client:
            await self.client.close()
    
    async def publish(self, message: str):
        await self.client.rpush(settings.QUEUE_NAME, message)
    
    async def consume(self):
        return await self.client.blpop(settings.QUEUE_NAME, timeout = 1)

broker = Broker()