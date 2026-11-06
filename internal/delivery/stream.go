package delivery

import (
	"context"
	"errors"
	"net/http"

	vk "github.com/valkey-io/valkey-go"

	queueentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	queueinbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/port/inbound"
	streamusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/application/usecase"
	streamdto "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/dto"
	streamentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	streamhttp "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/infrastructure/http"
	streamvalkey "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/infrastructure/valkey"
	streaminbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

type StreamDeps struct {
	Config  streamentity.Config
	Valkey  vk.Client
	Queue   queueinbound.IQueueUsecase
	Limiter outbound.IRateLimiter
	Auth    *auth.Verifier
}

// RegisterStream wires the stream hub (ADR-0015) and returns it, so shutdown can drain it.
func RegisterStream(mux *http.ServeMux, d StreamDeps) streaminbound.IStreamUsecase {
	hub := streamusecase.NewStream(d.Config, streamPlaces{q: d.Queue}, streamvalkey.New(d.Valkey), d.Limiter)
	streamhttp.RegisterRoutes(mux, streamhttp.NewStream(hub, d.Auth, d.Config))
	return hub
}

// streamPlaces is the stream domain's IPlaces over the queue usecase: the same
// checks as getMyPlace (moved, missed, token seq), with the errors translated.
type streamPlaces struct{ q queueinbound.IQueueUsecase }

func (p streamPlaces) MyPlace(ctx context.Context, eventID, deviceID string, tokenSeq int64) (streamdto.Place, error) {
	pl, err := p.q.MyPlace(ctx, eventID, deviceID, tokenSeq)
	if err != nil {
		return streamdto.Place{}, streamError(err)
	}
	return streamdto.Place{Seq: pl.Seq, Served: pl.Served, TurnExpiresAt: pl.TurnExpiresAt, ServerNow: pl.ServerNow}, nil
}

func (p streamPlaces) Frontier(ctx context.Context, eventID string) ([]byte, error) {
	b, err := p.q.Frontier(ctx, eventID)
	if err != nil {
		return nil, streamError(err)
	}
	return b, nil
}

func streamError(err error) error {
	for queueErr, streamErr := range map[error]error{
		queueentity.ErrUnknownEvent:        streamentity.ErrUnknownEvent,
		queueentity.ErrFrontierUnavailable: streamentity.ErrUnknownEvent,
		queueentity.ErrClosed:              streamentity.ErrClosed,
		queueentity.ErrMoved:               streamentity.ErrMoved,
		queueentity.ErrMissed:              streamentity.ErrMissed,
		queueentity.ErrPlaceExpired:        streamentity.ErrPlaceExpired,
		queueentity.ErrPlaceMismatch:       streamentity.ErrPlaceMismatch,
	} {
		if errors.Is(err, queueErr) {
			return streamErr
		}
	}
	return err
}
