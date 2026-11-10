-- name: GetCheckpoint :one
SELECT * FROM queue.checkpoint
WHERE event_id = $1;

-- name: UpsertCheckpoint :exec
-- A stale leader (lower fence) never overwrites a newer checkpoint.
INSERT INTO queue.checkpoint (event_id, seq, served, fence, at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (event_id) DO UPDATE
SET seq = EXCLUDED.seq,
    served = EXCLUDED.served,
    fence = EXCLUDED.fence,
    at = now()
WHERE queue.checkpoint.fence <= EXCLUDED.fence;
