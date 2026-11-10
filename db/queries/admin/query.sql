-- name: GetEventQueueConfig :one
SELECT * FROM queue.event_queue_config
WHERE event_id = $1;

-- name: UpsertEventQueueConfig :one
INSERT INTO queue.event_queue_config (event_id, max_slots, rate_cap, stream_from, state, updated_by, reason)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (event_id) DO UPDATE
SET max_slots = EXCLUDED.max_slots,
    rate_cap = EXCLUDED.rate_cap,
    stream_from = EXCLUDED.stream_from,
    state = EXCLUDED.state,
    updated_by = EXCLUDED.updated_by,
    reason = EXCLUDED.reason,
    updated_at = now()
RETURNING *;
