import pytest
from .utils import generate_event

@pytest.mark.asyncio
async def test_create_event_valid(client):
    """Test kirim event valid -> harus 200 OK"""
    payload = generate_event()
    response = await client.post("/publish", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "queued"

@pytest.mark.asyncio
async def test_create_event_missing_field(client):
    """Test kirim event cacat (tanpa event_id) -> harus 422"""
    payload = generate_event()
    del payload["event_id"]
    response = await client.post("/publish", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_create_event_invalid_type(client):
    """Test kirim tipe data salah (timestamp rusak) -> 422"""
    payload = generate_event()
    payload["timestamp"] = "bukan-tanggal"
    response = await client.post("/publish", json=payload)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_method_not_allowed(client):
    """Coba DELETE /publish -> 405"""
    response = await client.delete("/publish")
    assert response.status_code == 405