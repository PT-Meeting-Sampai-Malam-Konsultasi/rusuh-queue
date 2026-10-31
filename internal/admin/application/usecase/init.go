package usecase

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/outbound"
)

type AdminUsecase struct {
	repo outbound.IAdminRepository
}

// New builds the concrete Service implementation for admin.
func NewAdmin(repo outbound.IAdminRepository) inbound.IAdminUsecase {
	return &AdminUsecase{repo: repo}
}
