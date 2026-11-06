package delivery

import (
	"net/http"

	vk "github.com/valkey-io/valkey-go"

	queueusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/application/usecase"
	queueentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/coreapi"
	queuehttp "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/http"
	queuevalkey "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/valkey"
	queueinbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/port/inbound"
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
	// Admissions records admissions in core; nil means off (coreapi.Disabled).
	Admissions queueoutbound.IAdmissions
}

// RegisterQueue wires the Valkey store → usecase → handler and registers join, me, leave, frontier and claim.
// It returns the usecase, which the stream reads places and frontiers through.
func RegisterQueue(mux *http.ServeMux, d QueueDeps) queueinbound.IQueueUsecase {
	admissions := d.Admissions
	if admissions == nil {
		admissions = coreapi.Disabled{}
	}
	uc := queueusecase.NewQueue(d.Config, queuevalkey.New(d.Valkey), d.Signer, d.Limiter, admissions)
	queuehttp.RegisterRoutes(mux, queuehttp.NewQueue(uc, d.Auth))
	return uc
}
