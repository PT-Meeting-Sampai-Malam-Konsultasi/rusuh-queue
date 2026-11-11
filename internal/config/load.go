package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Load reads runtime configuration from the environment and an optional .env file.
//
// Returns an error when a security-sensitive configuration value is invalid.
func Load() (*Config, error) {
	// Load .env file if it exists, ignore error if it doesn't
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	valkeyTLSEnabled, err := getRequiredBool("VALKEY_TLS_ENABLED", false)
	if err != nil {
		return nil, err
	}

	return &Config{
		Port: getEnv("PORT", "8080"),

		AdminPort:    getEnv("ADMIN_PORT", "8081"),
		AdminJWKSURL: os.Getenv("ADMIN_JWKS_URL"),
		AdminIssuer:  getEnv("ADMIN_ISSUER", "rusuh-admin"),
		Env:          getEnv("ENV", "development"),

		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/rusuh?sslmode=disable"),

		ValkeyAddr:       getEnv("VALKEY_ADDR", "localhost:6379"),
		ValkeyPassword:   os.Getenv("VALKEY_PASSWORD"),
		ValkeyTLSEnabled: valkeyTLSEnabled,

		SigningKeys: os.Getenv("SIGNING_KEYS"),

		TurnstileSecret:    os.Getenv("TURNSTILE_SECRET"),
		TurnstileVerifyURL: os.Getenv("TURNSTILE_VERIFY_URL"),
		TurnstileHostnames: getList("TURNSTILE_HOSTNAMES"),
		Attestor:           getEnv("ATTESTOR", "sandbox"),

		ClientIPHeader:    os.Getenv("CLIENT_IP_HEADER"),
		TrustedProxyCIDRs: getList("TRUSTED_PROXY_CIDRS"),
		CORSOrigins:       getList("CORS_ORIGINS"),

		CoreJWKSURL:            os.Getenv("CORE_JWKS_URL"),
		CoreJWKS:               os.Getenv("CORE_JWKS"),
		CoreIssuer:             getEnv("CORE_ISSUER", "rusuh-core"),
		CoreAudience:           getEnv("CORE_AUDIENCE", "rusuh-queue"),
		CoreAccessRequireOTP:   getBool("CORE_ACCESS_REQUIRE_OTP", true),
		CoreInternalURL:        os.Getenv("CORE_INTERNAL_URL"),
		CoreInternalToken:      os.Getenv("CORE_INTERNAL_TOKEN"),
		CoreAdmissionTimeout:   getDuration("CORE_ADMISSION_TIMEOUT", time.Second),
		CorePaymentWindow:      getDuration("CORE_PAYMENT_WINDOW", 10*time.Minute),
		RLJoinPerDevice:        getInt("RL_JOIN_PER_DEVICE", 30),
		PlaceTransfersPerEvent: getInt("PLACE_TRANSFERS_PER_EVENT", 1),
		LoginWindow:            getDuration("LOGIN_WINDOW", 3*time.Minute),
		ClaimRequiresLogin:     getBool("CLAIM_REQUIRES_LOGIN", true),
		RLClaimPerDevice:       getInt("RL_CLAIM_PER_DEVICE", 60),

		Tick:        getDuration("TICK", time.Second),
		OrderWindow: getDuration("ORDER_WINDOW", 5*time.Minute),
		PurgeGrace:  getDuration("PURGE_GRACE", 30*time.Second),
		MissWindow:  getDuration("MISS_WINDOW", time.Minute),
		RateWindow:  getDuration("RATE_WINDOW", 5*time.Minute),
		RatePrior:   getDuration("RATE_PRIOR_ORDER_TIME", 6*time.Minute),
		HoldGrace:   getDuration("HOLD_GRACE", 2*time.Minute),

		ReconcileEvery: getDuration("RECONCILE_EVERY", 10*time.Second),
		SnapshotMaxAge: getDuration("SNAPSHOT_MAX_AGE", 30*time.Second),

		DeviceTokenTTL:    getDuration("DEVICE_TOKEN_TTL", 30*24*time.Hour),
		RLDevicePerIP:     getInt("RL_DEVICE_PER_IP", 10),
		RLDevicePerSubnet: getInt("RL_DEVICE_PER_SUBNET", 100),

		ShutdownTimeout: getDuration("SHUTDOWN_TIMEOUT", 30*time.Second),

		StreamMaxPerTask:  getInt("STREAM_MAX_PER_TASK", 5000),
		StreamLifetime:    getDuration("STREAM_LIFETIME", 10*time.Minute),
		StreamHeartbeat:   getDuration("STREAM_HEARTBEAT", 15*time.Second),
		StreamDrain:       getDuration("STREAM_DRAIN", 25*time.Second),
		RLStreamPerDevice: getInt("RL_STREAM_PER_DEVICE", 12),

		LogLevel:            getEnv("LOG_LEVEL", "info"),
		LogFormat:           getEnv("LOG_FORMAT", "text"),
		OtelExporterType:    getEnv("OTEL_EXPORTER_TYPE", "stdout"),
		OtelEndpoint:        getEnv("OTEL_ENDPOINT", "localhost:4318"),
		ServiceInstanceID:   getEnv("SERVICE_INSTANCE_ID", hostname()),
		OtelMetricsExporter: os.Getenv("OTEL_METRICS_EXPORTER"),
		OtelTracesExporter:  os.Getenv("OTEL_TRACES_EXPORTER"),
		OtelLogsExporter:    getEnv("OTEL_LOGS_EXPORTER", "none"),
		RequestLog:          getBool("REQUEST_LOG", true),
	}, nil
}

// getRequiredBool parses a boolean environment variable and rejects invalid values.
func getRequiredBool(key string, fallback bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	switch v {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("invalid %s: %q must be true or false", key, v)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("Invalid %s=%q, using %s", key, v, fallback)
		return fallback
	}
	return d
}

func getList(key string) []string {
	var out []string
	for v := range strings.SplitSeq(os.Getenv(key), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func getInt(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		log.Printf("Invalid %s=%q, using %d", key, v, fallback)
		return fallback
	}
	return n
}

func getBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Printf("Invalid %s=%q, using %t", key, v, fallback)
		return fallback
	}
	return b
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
