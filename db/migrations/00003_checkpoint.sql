-- +goose Up
-- Written every second by the leader, so seq and served survive a Valkey failover.
CREATE TABLE queue.checkpoint (
    event_id  TEXT PRIMARY KEY,
    seq       BIGINT      NOT NULL CHECK (seq >= 0),
    served    BIGINT      NOT NULL CHECK (served >= 0),
    fence     BIGINT      NOT NULL,
    at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS queue.checkpoint;
