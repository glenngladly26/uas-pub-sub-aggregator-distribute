import pytest
import asyncio
import uuid
import time
from .utils import generate_event

@pytest.mark.asyncio
async def test_concurrency_race_condition(client):
    """
    10 Request dikirim BERSAMAAN (Parallel) dengan ID yang SAMA.
    """
    unique_id = str(uuid.uuid4())
    payload = generate_event(topic="test-race", event_id=unique_id)
  
    tasks = [client.post("/publish", json=payload) for _ in range(10)]
    
    responses = await asyncio.gather(*tasks)
    
    for res in responses:
        assert res.status_code == 200

    await asyncio.sleep(2)

    response = await client.get(f"/events?topic=test-race&limit=50")
    matches = [e for e in response.json() if e["event_id"] == unique_id]
    
    assert len(matches) == 1, f"Race Condition Gagal! Ada {len(matches)} duplikat."

@pytest.mark.asyncio
async def test_stress_execution_time(client):
    """
    Stress Test Kecil:
    Mengirim 100 request individual secara Paralel (Asynchronous).
    """
    payloads = [generate_event(topic="stress-test") for _ in range(100)]
    
    start_time = time.time()
    
    tasks = [client.post("/publish", json=p) for p in payloads]
    
    responses = await asyncio.gather(*tasks)
    
    end_time = time.time()
    duration = end_time - start_time
    
    for res in responses:
        assert res.status_code == 200
    
    print(f"\nTime to send 100 requests (Parallel): {duration:.4f}s")
    
    assert duration < 2.0, "API terlalu lambat menangani 100 request paralel"