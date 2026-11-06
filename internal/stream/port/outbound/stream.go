package outbound

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
)

//go:generate mockgen -source=stream.go -destination=../../../../_mock/stream/port/outbound/stream_mock.go -package=mock_outbound

// IPlaces reads places and frontiers through the queue domain (wired in delivery).
// MyPlace returns the stream domain's errors (entity.ErrMoved, ErrMissed, ...).
type IPlaces interface {
	MyPlace(ctx context.Context, eventID, deviceID string, tokenSeq int64) (dto.Place, error)
	// Frontier returns entity.ErrUnknownEvent when the event has no frontier yet.
	Frontier(ctx context.Context, eventID string) ([]byte, error)
}

// ISubscriber listens to an event's tick channel until ctx ends. onMessage gets
// every frontier, and "" each time the subscription (re)starts: messages sent
// while disconnected are lost, so the caller re-reads the frontier.
type ISubscriber interface {
	Subscribe(ctx context.Context, eventID string, onMessage func(message string))
}

type IRateLimiter interface {
	Hit(ctx context.Context, rule ratelimit.Rule, id string) (ratelimit.Result, error)
}
