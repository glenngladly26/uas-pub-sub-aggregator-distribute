import uuid
from datetime import datetime, timezone

def generate_event(topic="test-unit", event_id=None):
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "pytest",
        "payload": {
            "status": "testing",
            "value": 123,
        }
    }