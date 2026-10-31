package inbound

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
)

//go:generate mockgen -source=admin.go -destination=../../../../_mock/admin/port/inbound/admin_mock.go -package=mock_inbound

// IAdminUsecase is the inbound (driving) port of the admin domain:
// what it offers to its HTTP handlers and to the composition root.
type IAdminUsecase interface {
	// QueueSettings returns the saved settings, or DefaultSettings when none are saved.
	QueueSettings(ctx context.Context, eventID string) (entity.Settings, error)
}
