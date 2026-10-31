-- +goose Up
-- The queue service owns this schema alone; core never reads or writes it (ADR-0011).
CREATE SCHEMA IF NOT EXISTS queue;

-- +goose Down
DROP SCHEMA IF EXISTS queue;
