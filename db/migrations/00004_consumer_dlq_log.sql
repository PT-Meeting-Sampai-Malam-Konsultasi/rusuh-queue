-- +goose Up
-- One row per stream:booking entry moved to the dead-letter stream (ADR-0013).
CREATE TABLE queue.consumer_dlq_log (
    stream_id   TEXT PRIMARY KEY,
    event_id    TEXT,
    event_type  TEXT,
    deliveries  INTEGER     NOT NULL,
    last_error  TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    moved_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS queue.consumer_dlq_log;
