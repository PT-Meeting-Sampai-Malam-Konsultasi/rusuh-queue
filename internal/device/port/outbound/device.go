package outbound

import (
	"context"
	"net/netip"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

//go:generate mockgen -source=device.go -destination=../../../../_mock/device/port/outbound/device_mock.go -package=mock_outbound

// The device module stores nothing: a device is its signed token. Its outbound
// ports are the checks and the signer, each implemented by a pkg/ client.

// IHumanVerifier checks a web caller (pkg/turnstile). It returns
// turnstile.ErrRejected or turnstile.ErrUnavailable.
type IHumanVerifier interface {
	Verify(ctx context.Context, token string, remoteIP netip.Addr) error
}

// IAttestor checks a mobile app (pkg/attest).
type IAttestor interface {
	Verify(ctx context.Context, platform, installID, attestation string) error
}

// IRateLimiter counts registrations (pkg/ratelimit).
type IRateLimiter interface {
	Hit(ctx context.Context, rule ratelimit.Rule, id string) (ratelimit.Result, error)
}

// ISigner issues device tokens and publishes the public keys (pkg/token).
type ISigner interface {
	Sign(kind token.Kind, claims token.Claims) (string, error)
	JWKS() token.JWKS
}
