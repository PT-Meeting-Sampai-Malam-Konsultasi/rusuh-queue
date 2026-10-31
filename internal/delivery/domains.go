package delivery

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	controllerentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/entity"
	deviceentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	queueentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/attest"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/remotejwks"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/turnstile"
)

// wireDomains builds the token, rate-limit and verification clients and wires
// every domain onto mux; long-running routines go to s.background.
func (s *Server) wireDomains(cfg *config.Config, mux *http.ServeMux, ipResolver *clientip.Resolver) error {
	keys, err := loadKeyring(cfg)
	if err != nil {
		return fmt.Errorf("signing keys: %w", err)
	}
	if cfg.TurnstileSecret == "" && cfg.Env != "development" {
		return errors.New("TURNSTILE_SECRET is required outside development")
	}
	if cfg.Attestor != "sandbox" {
		return fmt.Errorf("unknown ATTESTOR %q; only sandbox exists until real attestation (D-6)", cfg.Attestor)
	}
	if cfg.RateWindow < cfg.MissWindow {
		return fmt.Errorf("RATE_WINDOW (%s) must not be shorter than MISS_WINDOW (%s)", cfg.RateWindow, cfg.MissWindow)
	}
	if cfg.CoreJWKSURL == "" {
		slog.Warn("CORE_JWKS_URL empty: joins with an access token are refused until core publishes its keys")
	}
	limiter := ratelimit.New(s.valkey)
	verifier := auth.New(keys, remotejwks.New(cfg.CoreJWKSURL))

	RegisterDevice(mux, DeviceDeps{
		Config: deviceentity.Config{
			TokenTTL:  cfg.DeviceTokenTTL,
			PerIP:     ratelimit.Rule{Scope: "dev-reg:ip", Limit: cfg.RLDevicePerIP, Window: time.Minute},
			PerSubnet: ratelimit.Rule{Scope: "dev-reg:net", Limit: cfg.RLDevicePerSubnet, Window: time.Minute},
		},
		Human:    turnstile.New(cfg.TurnstileSecret, cfg.TurnstileVerifyURL, cfg.TurnstileHostnames),
		Attestor: attest.Sandbox{},
		Limiter:  limiter,
		Signer:   keys,
		ClientIP: ipResolver,
	})

	RegisterQueue(mux, QueueDeps{
		Config: queueentity.Config{
			JoinPerDevice:  ratelimit.Rule{Scope: "join:dev", Limit: cfg.RLJoinPerDevice, Window: time.Minute},
			ClaimPerDevice: ratelimit.Rule{Scope: "claim:dev", Limit: cfg.RLClaimPerDevice, Window: time.Minute},
			OrderWindow:    cfg.OrderWindow,
			MissWindow:     cfg.MissWindow,
		},
		Valkey:  s.valkey,
		Signer:  keys,
		Limiter: limiter,
		Auth:    verifier,
	})

	adminUsecase := NewAdmin(s.db)
	RegisterAdmin(mux, adminUsecase)

	controllerConfig := controllerentity.DefaultConfig()
	controllerConfig.TickEvery = cfg.Tick
	controllerConfig.PurgeGrace = cfg.PurgeGrace
	controllerConfig.HistWindow = cfg.RateWindow
	controllerConfig.PriorOrderTime = cfg.RatePrior
	controllerConfig.InviteWindow = cfg.MissWindow
	controller := NewController(ControllerDeps{
		Config:   controllerConfig,
		DB:       s.db,
		Valkey:   s.valkey,
		Settings: adminUsecase,
	})
	s.background = append(s.background, controller.Run)
	return nil
}

// loadKeyring reads SIGNING_KEYS. Without keys, development gets a throwaway
// key (tokens die with the process); every other environment refuses to start.
func loadKeyring(cfg *config.Config) (*token.Keyring, error) {
	if cfg.SigningKeys == "" {
		if cfg.Env != "development" {
			return nil, token.ErrNoKeys
		}
		k, err := token.GenerateKey("dev-ephemeral")
		if err != nil {
			return nil, err
		}
		slog.Warn("SIGNING_KEYS empty: using an ephemeral development key")
		return token.NewKeyring(k)
	}
	keys, err := token.ParseKeys(cfg.SigningKeys)
	if err != nil {
		return nil, err
	}
	return token.NewKeyring(keys...)
}
