-- name: LogDeadLetter :exec
INSERT INTO queue.consumer_dlq_log (stream_id, event_id, event_type, deliveries, last_error, payload)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (stream_id) DO UPDATE
SET deliveries = EXCLUDED.deliveries,
    last_error = EXCLUDED.last_error,
    moved_at = now();
