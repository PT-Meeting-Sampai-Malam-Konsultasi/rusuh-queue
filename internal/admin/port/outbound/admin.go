package outbound

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
)

//go:generate mockgen -source=admin.go -destination=../../../../_mock/admin/port/outbound/admin_mock.go -package=mock_outbound

// IAdminRepository is the outbound port for queue settings persistence. It stays
// private to this module (nested under internal/) so other modules can only
// ever depend on the public Service interface in ../../admin.go,
// never on how admin persists its data.
type IAdminRepository interface {
	// GetSettings returns (nil, nil) when no settings are saved for the event.
	GetSettings(ctx context.Context, eventID string) (*entity.Settings, error)
	// SaveSettings records the settings with who changed them and why (audit).
	SaveSettings(ctx context.Context, s entity.Settings, by, reason string) error
}

// IQueueConfig applies settings to a live queue in Valkey.
type IQueueConfig interface {
	Live(ctx context.Context, eventID string) (bool, error)
	// Apply returns the frontier as it stands after the change.
	Apply(ctx context.Context, s entity.Settings) ([]byte, error)
}
