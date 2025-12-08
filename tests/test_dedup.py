import pytest
import asyncio
import uuid
from .utils import generate_event

@pytest.mark.asyncio
async def test_deduplication_logic(client):
    """
    Kirim duplikat berurutan -> hanya 1 yang tersimpan.
    """
    unique_id = str(uuid.uuid4())
    payload = generate_event(topic="test-dedup", event_id=unique_id)

    await client.post("/publish", json=payload)
    
    await asyncio.sleep(0.5) 
    
    await client.post("/publish", json=payload)
    await asyncio.sleep(0.5)

    response = await client.get(f"/events?topic=test-dedup&limit=100")
    events = response.json()
    
    matches = [e for e in events if e["event_id"] == unique_id]
    
    assert len(matches) == 1, f"Gagal! Ditemukan {len(matches)} data, seharusnya 1."