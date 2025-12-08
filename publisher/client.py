import logging
import aiohttp

logger = logging.getLogger("PublisherClient")

class AsyncClient:
    async def send_event(
        self,
        session: aiohttp.ClientSession,
        url: str,
        data: dict
    ) -> bool:
        try:
            async with session.post(url, json=data) as response:
                if response.status in [200, 201, 202]:
                    return True
                else:
                    logger.error(f"Failed to sent. Status: {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False