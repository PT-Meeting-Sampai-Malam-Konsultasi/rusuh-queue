package usecase

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
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
