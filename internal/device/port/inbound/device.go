package inbound

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

//go:generate mockgen -source=device.go -destination=../../../../_mock/device/port/inbound/device_mock.go -package=mock_inbound

// IDeviceUsecase is the inbound (driving) port of the device domain:
// what it offers to its HTTP handlers and to the composition root.
type IDeviceUsecase interface {
	Register(ctx context.Context, in entity.RegisterInput) (*entity.Registration, error)
	// JWKS lists the public keys core uses to verify queue's tokens (D-3).
	JWKS() token.JWKS
}
