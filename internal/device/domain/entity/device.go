package entity

import (
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
)

// Platform of the caller, as in registerDevice's request body.
type Platform string

const (
	PlatformWeb     Platform = "web"
	PlatformAndroid Platform = "android"
	PlatformIOS     Platform = "ios"
)

// RegisterInput is one registerDevice call (FR-04, ADR-0003).
type RegisterInput struct {
	Platform       Platform
	InstallID      string // mobile only
	Attestation    string // mobile only
	TurnstileToken string // web only
	ClientIP       netip.Addr
}

// Registration is the signed device identity returned to the caller.
type Registration struct {
	DeviceID  string
	Token     string
	ExpiresAt time.Time
}

// Config holds the module's tunables (env: DEVICE_TOKEN_TTL, RL_DEVICE_*).
type Config struct {
	TokenTTL  time.Duration
	PerIP     ratelimit.Rule
	PerSubnet ratelimit.Rule
	// SkipVerification turns off Turnstile and attestation; rate limits still apply.
	SkipVerification bool
}

var (
	// ErrInvalidInput: a required field is missing or malformed (400 VALIDATION_FAILED).
	ErrInvalidInput = errors.New("device: invalid input")
	// ErrHumanCheckFailed: Turnstile or attestation said no (400 VALIDATION_FAILED).
	ErrHumanCheckFailed = errors.New("device: human or app check failed")
	// ErrCheckUnavailable: Turnstile or attestation could not answer (503).
	ErrCheckUnavailable = errors.New("device: human or app check unavailable")
)

// RateLimitedError: too many registrations from this IP or subnet (429).
type RateLimitedError struct {
	Scope      string
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("device: rate limited by %s, retry after %s", e.Scope, e.RetryAfter)
}
