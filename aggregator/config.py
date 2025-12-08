import os

class Settings:
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/eventdb")
    BROKER_URL: str = os.getenv("BROKER_URL", "redis://broker:6379/0")
    QUEUE_NAME: str = "event_queue"

settings = Settings()