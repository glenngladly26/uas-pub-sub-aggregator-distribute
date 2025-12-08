import pytest
import asyncio
import uuid
from .utils import generate_event

@pytest.mark.asyncio
async def test_topic_filtering_isolation(client):
    """
    Test Isolasi Topik:
    Pastikan query ke topic tertentu TIDAK membocorkan data dari topic lain.
    """
    topic_target = "sports"
    topic_noise = "politics"
    
    await client.post("/publish", json=generate_event(topic=topic_target))
    await client.post("/publish", json=generate_event(topic=topic_target))
    
    await client.post("/publish", json=generate_event(topic=topic_noise))
    
    await asyncio.sleep(1)

    response = await client.get(f"/events?topic={topic_target}")
    data = response.json()
    
    assert len(data) >= 2
    for event in data:
        assert event["topic"] == topic_target
        assert event["topic"] != topic_noise

@pytest.mark.asyncio
async def test_get_events_limit_param(client):
    """
    Publish 5 event, tapi request GET dengan limit=3.
    API harus mengembalikan tepat 3 item.
    """
    topic = "test-limit"
    
    for _ in range(5):
        await client.post("/publish", json=generate_event(topic=topic))
    
    await asyncio.sleep(1)
    
    response = await client.get(f"/events?topic={topic}&limit=3")
    data = response.json()
    
    assert len(data) == 3, f"Limit gagal! Diharapkan 3, diterima {len(data)}"