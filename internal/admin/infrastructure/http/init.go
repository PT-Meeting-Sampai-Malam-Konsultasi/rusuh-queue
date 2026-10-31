package http

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/inbound"
)

type AdminHandler struct {
	service inbound.IAdminUsecase
}

func NewAdmin(s inbound.IAdminUsecase) *AdminHandler {
	return &AdminHandler{service: s}
}
