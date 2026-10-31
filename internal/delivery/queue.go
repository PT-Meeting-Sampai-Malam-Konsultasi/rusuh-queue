package delivery

import (
	"net/http"

	vk "github.com/valkey-io/valkey-go"

	queueusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/application/usecase"
	queueentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	queuehttp "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/http"
	queuevalkey "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/valkey"
	queueoutbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

// QueueDeps are the queue domain's dependencies.
type QueueDeps struct {
	Config  queueentity.Config
	Valkey  vk.Client
	Signer  queueoutbound.ISigner
	Limiter queueoutbound.IRateLimiter
	Auth    *auth.Verifier
}

// RegisterQueue wires the Valkey store → usecase → handler and registers join, me, leave, frontier and claim.
func RegisterQueue(mux *http.ServeMux, d QueueDeps) {
	uc := queueusecase.NewQueue(d.Config, queuevalkey.New(d.Valkey), d.Signer, d.Limiter)
	queuehttp.RegisterRoutes(mux, queuehttp.NewQueue(uc, d.Auth))
}
