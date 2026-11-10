package http

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

type AdminHandler struct {
	service inbound.IAdminUsecase
	auth    *auth.AdminVerifier
}

func NewAdmin(s inbound.IAdminUsecase, v *auth.AdminVerifier) *AdminHandler {
	return &AdminHandler{service: s, auth: v}
}
