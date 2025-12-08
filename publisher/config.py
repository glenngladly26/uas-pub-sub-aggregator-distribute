import os

class Settings:
    TARGET_URL: str = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
    TOTAL_EVENTS: int = os.getenv("TOTAL_EVENTS", 1000)
    DUPLICATE_RATIO: float = 0.3
    CONCURRENCY: int = 50
    TOPICS: list = ["payment", "order", "user-login", "sensor-suhu", "notifikasi"]

settings = Settings()