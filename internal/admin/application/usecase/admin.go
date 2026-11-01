package usecase

import (
	"context"
	"log/slog"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

func (s *AdminUsecase) QueueSettings(ctx context.Context, eventID string) (entity.Settings, error) {
	saved, err := s.repo.GetSettings(ctx, eventID)
	if err != nil {
		return entity.Settings{}, err
	}
	if saved == nil {
		return entity.DefaultSettings(eventID), nil
	}
	return *saved, nil
}

// UpdateQueue saves first: the controller's sync re-applies the saved settings,
// so a failed Valkey write is repaired within a sync period.
func (s *AdminUsecase) UpdateQueue(ctx context.Context, eventID string, p entity.Patch, by string) ([]byte, error) {
	if !qkeys.ValidEvent(eventID) {
		return nil, entity.ErrUnknownEvent
	}
	live, err := s.queue.Live(ctx, eventID)
	if err != nil {
		return nil, err
	}
	if !live {
		return nil, entity.ErrUnknownEvent
	}
	current, err := s.QueueSettings(ctx, eventID)
	if err != nil {
		return nil, err
	}
	next, err := p.Apply(current)
	if err != nil {
		return nil, err
	}
	if err := s.repo.SaveSettings(ctx, next, by, p.Reason); err != nil {
		return nil, err
	}
	f, err := s.queue.Apply(ctx, next)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, entity.ErrUnknownEvent
	}
	slog.InfoContext(ctx, "queue settings changed", "event", eventID, "by", by, "state", next.State,
		"maxSlots", next.MaxSlots, "rateCap", next.RateCap, "streamFrom", next.StreamFrom, "reason", p.Reason)
	return f, nil
}
