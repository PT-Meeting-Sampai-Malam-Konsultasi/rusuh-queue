package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/infrastructure/postgres/sqlcgen"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/port/outbound"
)

type checkpoints struct {
	q *sqlcgen.Queries
}

// New builds the outbound adapter implementing outbound.ICheckpoints on
// queue.checkpoint (db/migrations/00003_checkpoint.sql).
func New(db *sql.DB) outbound.ICheckpoints {
	return &checkpoints{q: sqlcgen.New(db)}
}

func (c *checkpoints) Upsert(ctx context.Context, eventID string, seq, served, fence int64) error {
	err := c.q.UpsertCheckpoint(ctx, sqlcgen.UpsertCheckpointParams{EventID: eventID, Seq: seq, Served: served, Fence: fence})
	if err != nil {
		return fmt.Errorf("checkpoint %s: %w", eventID, err)
	}
	return nil
}
