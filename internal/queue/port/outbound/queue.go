package outbound

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

//go:generate mockgen -source=queue.go -destination=../../../../_mock/queue/port/outbound/queue_mock.go -package=mock_outbound

// IQueueStore is the outbound port to the event's Valkey keys.
type IQueueStore interface {
	// Join and Place take the turn windows: a missed place is over (join gives a new one).
	Join(ctx context.Context, k qkeys.Event, deviceID, userID string, w dto.Windows) (dto.JoinResult, error)
	// Transfer moves the account's live place, or its claimed lease not yet held, to deviceID (D-13, ADR-0021).
	Transfer(ctx context.Context, k qkeys.Event, deviceID, userID string, w dto.Windows, perEvent int64) (dto.JoinResult, error)
	Place(ctx context.Context, k qkeys.Event, deviceID string, w dto.Windows) (dto.PlaceView, error)
	// Leave ends the device's place; found is false for an unknown event.
	Leave(ctx context.Context, k qkeys.Event, deviceID string) (found bool, err error)
	// Frontier returns nil, nil when the event has no frontier.
	Frontier(ctx context.Context, k qkeys.Event) ([]byte, error)
	Claim(ctx context.Context, k qkeys.Event, in dto.ClaimInput) (dto.ClaimResult, error)
}

// ISigner issues queue and admission tokens (pkg/token).
type ISigner interface {
	Sign(kind token.Kind, claims token.Claims) (string, error)
}

// IAdmissions records an admission in core before the user gets its token
// (registerAdmission). It returns entity.ErrAdmissionUnavailable when core cannot
// be reached, so the claim is retried; any other error is a contract bug.
type IAdmissions interface {
	Register(ctx context.Context, rec dto.AdmissionRecord) error
}

// IRateLimiter counts joins and claims per device (pkg/ratelimit).
type IRateLimiter interface {
	Hit(ctx context.Context, rule ratelimit.Rule, id string) (ratelimit.Result, error)
}
