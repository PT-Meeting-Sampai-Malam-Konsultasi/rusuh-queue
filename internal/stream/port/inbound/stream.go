package inbound

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/dto"
)

//go:generate mockgen -source=stream.go -destination=../../../../_mock/stream/port/inbound/stream_mock.go -package=mock_inbound

// IStream is one open stream: Frames delivers each frontier after Initial, and
// Done closes when the hub ends the stream (dropped as too slow, or draining).
type IStream interface {
	Initial() dto.Frame
	Frames() <-chan dto.Frame
	Done() <-chan struct{}
	Seq() int64
}

// IStreamUsecase is the stream hub (ADR-0015).
type IStreamUsecase interface {
	Open(ctx context.Context, in dto.OpenInput) (IStream, error)
	// Turn reads the device's turn when served passes its seq; the window follows ADR-0021.
	Turn(ctx context.Context, s IStream) (dto.Turn, error)
	Close(s IStream)
	// Drain refuses new streams and ends the open ones, spread over the configured time.
	Drain()
}
