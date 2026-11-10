package outbound

import (
	"context"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

//go:generate mockgen -source=controller.go -destination=../../../../_mock/controller/port/outbound/controller_mock.go -package=mock_outbound

// IControllerStore is the controller's outbound port to Valkey.
type IControllerStore interface {
	CoreEvents(ctx context.Context) ([]string, error)
	LiveEvents(ctx context.Context) ([]string, error)
	// CoreDocs returns the raw sale and stock JSON; nil when a key is missing.
	CoreDocs(ctx context.Context, k qkeys.Event) (sale, stock []byte, err error)
	Bootstrap(ctx context.Context, k qkeys.Event, a dto.BootstrapArgs) (status, state string, err error)
	AddLive(ctx context.Context, eventID string) error
	RemoveLive(ctx context.Context, eventID string) error
	// Counters returns seq and served; ok is false when the event's keys are gone.
	Counters(ctx context.Context, k qkeys.Event) (seq, served int64, ok bool, err error)
	// Lead takes or renews the event's leadership for ttl.
	Lead(ctx context.Context, k qkeys.Event, task string, ttl time.Duration) (dto.Lead, error)
	// Resign drops the leadership if task holds it.
	Resign(ctx context.Context, k qkeys.Event, task string) error
	Purge(ctx context.Context, k qkeys.Event, fence int64, grace time.Duration, limit int) (dto.PurgeResult, error)
	Tick(ctx context.Context, k qkeys.Event, a dto.TickArgs) (dto.TickResult, error)
	CoreStock(ctx context.Context, k qkeys.Event) ([]byte, error)
	Reconcile(ctx context.Context, k qkeys.Event, fence, available int64) (dto.ReconcileResult, error)
	// Lottery numbers up to batch devices of the pre-open draw list.
	Lottery(ctx context.Context, k qkeys.Event, fence int64, batch int) (dto.LotteryResult, error)
	// Subscribe blocks until ctx ends, calling onEvent per message and per (re)subscribe.
	Subscribe(ctx context.Context, channel string, onEvent func(message string))
}

// ICheckpoints is the outbound port to queue.checkpoint in PostgreSQL.
type ICheckpoints interface {
	Upsert(ctx context.Context, eventID string, seq, served, fence int64) error
}

// ISettings is the narrow slice of the admin usecase the controller needs; delivery passes the admin usecase in.
type ISettings interface {
	QueueSettings(ctx context.Context, eventID string) (entity.Settings, error)
}
