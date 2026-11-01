package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/postgres/sqlcgen"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/outbound"
)

type adminRepository struct {
	q *sqlcgen.Queries
}

// New builds the outbound adapter implementing outbound.IAdminRepository on
// queue.event_queue_config (db/migrations/00002_event_queue_config.sql).
func New(db *sql.DB) outbound.IAdminRepository {
	return &adminRepository{q: sqlcgen.New(db)}
}

func (r *adminRepository) GetSettings(ctx context.Context, eventID string) (*entity.Settings, error) {
	row, err := r.q.GetEventQueueConfig(ctx, eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get queue settings %s: %w", eventID, err)
	}
	return toDomain(row), nil
}

func (r *adminRepository) SaveSettings(ctx context.Context, s entity.Settings, by, reason string) error {
	_, err := r.q.UpsertEventQueueConfig(ctx, sqlcgen.UpsertEventQueueConfigParams{
		EventID:    s.EventID,
		MaxSlots:   int32(s.MaxSlots),
		RateCap:    int32(s.RateCap),
		StreamFrom: int32(s.StreamFrom),
		State:      s.State,
		UpdatedBy:  by,
		Reason:     sql.NullString{String: reason, Valid: reason != ""},
	})
	if err != nil {
		return fmt.Errorf("save queue settings %s: %w", s.EventID, err)
	}
	return nil
}

// toDomain converts a persistence row into the module's public domain entity.
func toDomain(row sqlcgen.QueueEventQueueConfig) *entity.Settings {
	return &entity.Settings{
		EventID:    row.EventID,
		MaxSlots:   int64(row.MaxSlots),
		RateCap:    int64(row.RateCap),
		StreamFrom: int64(row.StreamFrom),
		State:      row.State,
	}
}
