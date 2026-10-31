-- +goose Up
-- Queue settings per event, written only by adminUpdateQueue (D-2).
CREATE TABLE queue.event_queue_config (
    event_id      TEXT PRIMARY KEY,
    max_slots     INTEGER     NOT NULL DEFAULT 2000 CHECK (max_slots > 0),
    rate_cap      INTEGER     NOT NULL DEFAULT 50   CHECK (rate_cap > 0),
    stream_from   INTEGER     NOT NULL DEFAULT 2000 CHECK (stream_from >= 0),
    state         TEXT        NOT NULL DEFAULT 'open' CHECK (state IN ('open', 'paused', 'closed')),
    updated_by    TEXT        NOT NULL,
    reason        TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS queue.event_queue_config;
