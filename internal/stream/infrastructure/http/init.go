package http

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

// StreamHandler serves streamQueue (ADR-0015).
type StreamHandler struct {
	service inbound.IStreamUsecase
	auth    *auth.Verifier
	cfg     entity.Config
}

func NewStream(s inbound.IStreamUsecase, a *auth.Verifier, cfg entity.Config) *StreamHandler {
	return &StreamHandler{service: s, auth: a, cfg: cfg}
}
