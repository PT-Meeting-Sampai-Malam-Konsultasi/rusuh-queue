package inbound

import (
	"context"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
)

//go:generate mockgen -source=queue.go -destination=../../../../_mock/queue/port/inbound/queue_mock.go -package=mock_inbound

// IQueueUsecase is the inbound (driving) port of the queue domain:
// what it offers to its HTTP handlers and to the composition root.
type IQueueUsecase interface {
	// Join gives the device its place (idempotent) and attaches the account if any.
	Join(ctx context.Context, in entity.JoinInput) (*entity.Place, error)
	// MyPlace returns the device's place after the draw; tokenSeq is the seq in its queue token.
	MyPlace(ctx context.Context, eventID, deviceID string, tokenSeq int64) (*entity.Place, error)
	// Leave ends the device's place (idempotent).
	Leave(ctx context.Context, eventID, deviceID string) error
	// Frontier returns the frontier JSON exactly as the controller wrote it.
	Frontier(ctx context.Context, eventID string) ([]byte, error)
	// Claim reserves up to 3 units for the device's reached turn and signs an
	// admission token for core; a retry returns the same slot.
	// Claim takes userID "" for a guest, refused when an account is required (ADR-0021).
	Claim(ctx context.Context, eventID, deviceID, userID string, tokenSeq int64) (*entity.Admission, error)
}
