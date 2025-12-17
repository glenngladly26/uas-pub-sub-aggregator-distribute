import pytest
import asyncio
import uuid
from .utils import generate_event

@pytest.mark.asyncio
async def test_stats_consistency(client):
    """Verifikasi struktur JSON /stats"""
    response = await client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    
    assert "database_stats" in data
    assert isinstance(data["database_stats"]["unique_events_stored"], int)

@pytest.mark.asyncio
async def test_persistence_check(client):
    """
    Cek apakah data yang baru dikirim bisa diambil kembali (Read-after-Write).
    """
    unique_id = str(uuid.uuid4())
    payload = generate_event(topic="persistence-check", event_id=unique_id)
    
    await client.post("/publish", json=payload)
    await asyncio.sleep(1)
    
    response = await client.get(f"/events?topic=persistence-check")
    data = response.json()
    
    found = any(e['event_id'] == unique_id for e in data)
    assert found is True, "Data hilang (Persistence check failed)"

@pytest.mark.asyncio
async def test_get_events_empty_topic(client):
    """Get events untuk topik hantu -> list kosong"""
    response = await client.get("/events?topic=hantu-belau")
    assert response.status_code == 200
    assert response.json() == []