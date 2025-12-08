CREATE TABLE IF NOT EXISTS processed_events (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(50) NOT NULL,
    event_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    source VARCHAR(50),
    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
);

CREATE INDEX idx_topic ON processed_events(topic)