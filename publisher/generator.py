import random
import uuid

from datetime import datetime, timezone
from schemas import EventModel
from config import settings

class EventGenerator:
    def __init__(self):
        self.sent_event_ids = []
    
    def _generate_payload(self) -> dict:
        return {
            "amount": random.randint(10, 100),
            "user_id": f"user_{random.randint(1, 100)}",
            "status": random.choice(["ok", "pending", "failed"])
        }
    
    def create_event(self) -> tuple[EventModel, bool]:
        is_duplicate = False

        if self.sent_event_ids and random.random() < settings.DUPLICATE_RATIO:
            event_id = random.choice(self.sent_event_ids)
            is_duplicate = True
        else:
            event_id = str(uuid.uuid4())
            self.sent_event_ids.append(event_id)

        event = EventModel(
            topic= random.choice(settings.TOPICS),
            event_id= event_id,
            timestamp= datetime.now(timezone.utc).isoformat(),
            source= "publisher-simulator",
            payload= self._generate_payload()
        )

        return event, is_duplicate