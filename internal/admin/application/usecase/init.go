package usecase

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/outbound"
)

type AdminUsecase struct {
	repo  outbound.IAdminRepository
	queue outbound.IQueueConfig
}

func NewAdmin(repo outbound.IAdminRepository, queue outbound.IQueueConfig) inbound.IAdminUsecase {
	return &AdminUsecase{repo: repo, queue: queue}
}
