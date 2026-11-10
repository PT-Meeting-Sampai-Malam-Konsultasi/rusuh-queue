package delivery

import (
	"net/http"

	deviceusecase "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/application/usecase"
	deviceentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	devicehttp "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/infrastructure/http"
	deviceoutbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
)

// DeviceDeps are the device domain's outbound adapters, built from pkg/ clients
// (turnstile.Client, attest.Sandbox, ratelimit.Limiter, token.Keyring).
type DeviceDeps struct {
	Config   deviceentity.Config
	Human    deviceoutbound.IHumanVerifier
	Attestor deviceoutbound.IAttestor
	Limiter  deviceoutbound.IRateLimiter
	Signer   deviceoutbound.ISigner
	ClientIP *clientip.Resolver
}

// RegisterDevice wires adapters → usecase → handler and registers registerDevice and getJwks.
func RegisterDevice(mux *http.ServeMux, d DeviceDeps) {
	uc := deviceusecase.NewDevice(d.Config, d.Human, d.Attestor, d.Limiter, d.Signer)
	devicehttp.RegisterRoutes(mux, devicehttp.NewDevice(uc, d.ClientIP))
}
