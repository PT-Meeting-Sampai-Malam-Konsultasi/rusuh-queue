package config

import (
	"time"
)

type Config struct {
	Port string
	// AdminPort serves the operator API (adminUpdateQueue); never expose it publicly.
	AdminPort    string
	AdminJWKSURL string // company SSO keys for operator tokens (EdDSA)
	AdminIssuer  string
	Env          string

	DatabaseURL string

	ValkeyAddr     string
	ValkeyPassword string

	// SigningKeys: "kid=base64(32-byte Ed25519 seed)", comma-separated, signing key first.
	// Empty is allowed only with ENV=development (an ephemeral key is generated).
	SigningKeys string

	TurnstileSecret    string
	TurnstileVerifyURL string
	TurnstileHostnames []string
	Attestor           string // "sandbox" until real Play Integrity / App Attest (D-6)

	// ClientIPHeader is trusted only from TrustedProxyCIDRs (lab: CF-Connecting-IP + Cloudflare ranges).
	ClientIPHeader    string
	TrustedProxyCIDRs []string
	CORSOrigins       []string

	// CoreJWKSURL: core's public keys for access tokens (D-4), e.g. https://<core>/.well-known/jwks.json
	CoreJWKSURL string
	// CoreInternalURL: core's private base URL for registerAdmission, e.g. http://core-api.internal/v1.
	// Empty turns registration off (claims answer without asking core).
	CoreInternalURL      string
	CoreInternalToken    string        // shared secret for core's internal endpoints
	CoreAdmissionTimeout time.Duration // per attempt; one retry
	RLJoinPerDevice      int64         // joins per minute per device
	// LoginWindow: the turn of a device not logged in before its turn (ADR-0021); ClaimRequiresLogin: claim needs an account.
	LoginWindow            time.Duration
	ClaimRequiresLogin     bool
	PlaceTransfersPerEvent int64 // moves of a place to another device per account per event (D-13); 0 = off
	RLClaimPerDevice       int64 // claims per minute per device

	// Admission (IMPLEMENTATION_PLAN.md "Admission controller").
	Tick        time.Duration // controller tick
	OrderWindow time.Duration // lease and admission token lifetime (FR-17)
	PurgeGrace  time.Duration // units go back this long after a lease ends (FR-18)
	MissWindow  time.Duration // an unclaimed turn is MISSED after this (HiFi: 60 s to enter); also the D-7 invitation window
	RateWindow  time.Duration // ratePerMin window and the history kept for MISSED; must be >= MissWindow
	RatePrior   time.Duration // average claim-to-paid time for the warm-up speed guess; 0 = off
	HoldGrace   time.Duration // a held slot is kept this long past the payment deadline

	ReconcileEvery time.Duration // unreserved is checked against core's stock at least this often
	SnapshotMaxAge time.Duration // an older core:{evt}:stock is not trusted

	DeviceTokenTTL    time.Duration
	RLDevicePerIP     int64 // registrations per minute per IP
	RLDevicePerSubnet int64 // registrations per minute per /24 (IPv4) or /48 (IPv6)

	// ShutdownTimeout bounds the drain after SIGTERM. Keep it below the
	// orchestrator's grace period (lab: 45 s, ECS stopTimeout: 45 s).
	ShutdownTimeout time.Duration

	LogLevel            string
	LogFormat           string
	OtelExporterType    string
	OtelEndpoint        string
	OtelMetricsExporter string // overrides OtelExporterType for metrics; none turns them off
	OtelTracesExporter  string // overrides OtelExporterType for traces
	OtelLogsExporter    string // none (default) keeps logs on stdout only; otlp also ships them to the collector
	// RequestLog writes one log line per HTTP request. Spans and HTTP metrics are recorded either way.
	RequestLog bool
}
