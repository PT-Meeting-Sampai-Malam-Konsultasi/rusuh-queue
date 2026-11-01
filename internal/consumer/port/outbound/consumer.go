package outbound

import (
	"context"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

//go:generate mockgen -source=consumer.go -destination=../../../../_mock/consumer/port/outbound/consumer_mock.go -package=mock_outbound

// IConsumerStore is the consumer's port to Valkey: the stream, its group and the release script.
type IConsumerStore interface {
	EnsureGroup(ctx context.Context) error
	Read(ctx context.Context) ([]dto.Message, error)
	Pending(ctx context.Context, idle time.Duration, count int64) ([]dto.Pending, error)
	// Claim takes over pending entries; entries trimmed from the stream are missing from the result.
	Claim(ctx context.Context, idle time.Duration, ids []string) ([]dto.Message, error)
	Ack(ctx context.Context, ids []string) error
	DeadLetter(ctx context.Context, m dto.Message, reason string) error
	Release(ctx context.Context, k qkeys.Event, a dto.ReleaseArgs) (dto.ReleaseStatus, error)
}

// IDeadLetterLog records dead-lettered entries in queue.consumer_dlq_log.
type IDeadLetterLog interface {
	Log(ctx context.Context, d dto.DeadLetter) error
}
