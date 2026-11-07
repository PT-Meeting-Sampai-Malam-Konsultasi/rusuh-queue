package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/infrastructure/postgres/sqlcgen"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/port/outbound"
)

type deadLetters struct {
	q *sqlcgen.Queries
}

func New(db *sql.DB) outbound.IDeadLetterLog {
	return &deadLetters{q: sqlcgen.New(db)}
}

func (d *deadLetters) Log(ctx context.Context, l dto.DeadLetter) error {
	payload := json.RawMessage(l.Payload)
	if !json.Valid(payload) {
		// keep a poison payload readable in the JSONB column
		payload, _ = json.Marshal(l.Payload)
	}
	deliveries := int32(math.MaxInt32)
	if l.Deliveries < math.MaxInt32 {
		deliveries = int32(max(l.Deliveries, 0))
	}
	err := d.q.LogDeadLetter(ctx, sqlcgen.LogDeadLetterParams{
		StreamID:   l.StreamID,
		EventID:    sql.NullString{String: l.EventID, Valid: l.EventID != ""},
		EventType:  sql.NullString{String: l.EventType, Valid: l.EventType != ""},
		Deliveries: deliveries,
		LastError:  l.LastError,
		Payload:    payload,
	})
	if err != nil {
		return fmt.Errorf("dead-letter log %s: %w", l.StreamID, err)
	}
	return nil
}
