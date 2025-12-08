import asyncio
import httpx
import pytest

BASE_URL = "http://localhost:8080"

@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as ac:
        yield ac