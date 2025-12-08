from typing import Any, Dict
from pydantic import BaseModel


class EventModel(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]