package delivery

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	deviceentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/attest"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
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
	limiter := ratelimit.New(s.valkey)

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
