package usecase

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/outbound"
)

// NewStream builds the stream hub. Subscriptions start with an event's first
// stream and end with its last, so a task listens only to events it serves.
func NewStream(cfg entity.Config, places outbound.IPlaces, sub outbound.ISubscriber, limiter outbound.IRateLimiter) inbound.IStreamUsecase {
	return &StreamUsecase{cfg: cfg, places: places, sub: sub, limiter: limiter, events: map[string]*eventStreams{}}
}
