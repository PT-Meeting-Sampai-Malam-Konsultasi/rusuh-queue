package usecase

import (
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/port/outbound"
)

type DeviceUsecase struct {
	cfg     entity.Config
	human   outbound.IHumanVerifier
	attest  outbound.IAttestor
	limiter outbound.IRateLimiter
	signer  outbound.ISigner
	now     func() time.Time
}

// New builds the concrete Service implementation for device.
func NewDevice(cfg entity.Config, human outbound.IHumanVerifier, att outbound.IAttestor, limiter outbound.IRateLimiter, signer outbound.ISigner) inbound.IDeviceUsecase {
	return &DeviceUsecase{cfg: cfg, human: human, attest: att, limiter: limiter, signer: signer, now: time.Now}
}
