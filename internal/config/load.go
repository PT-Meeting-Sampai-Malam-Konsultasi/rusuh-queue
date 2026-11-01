package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

func Load() *Config {
	// Load .env file if it exists, ignore error if it doesn't
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	return &Config{
		Port: getEnv("PORT", "8080"),
		Env:  getEnv("ENV", "development"),

		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/rusuh?sslmode=disable"),

		ValkeyAddr:     getEnv("VALKEY_ADDR", "localhost:6379"),
		ValkeyPassword: os.Getenv("VALKEY_PASSWORD"),

		SigningKeys: os.Getenv("SIGNING_KEYS"),

		TurnstileSecret:    os.Getenv("TURNSTILE_SECRET"),
		TurnstileVerifyURL: os.Getenv("TURNSTILE_VERIFY_URL"),
		TurnstileHostnames: getList("TURNSTILE_HOSTNAMES"),
		Attestor:           getEnv("ATTESTOR", "sandbox"),

		ClientIPHeader:    os.Getenv("CLIENT_IP_HEADER"),
		TrustedProxyCIDRs: getList("TRUSTED_PROXY_CIDRS"),
		CORSOrigins:       getList("CORS_ORIGINS"),

		CoreJWKSURL:      os.Getenv("CORE_JWKS_URL"),
		RLJoinPerDevice:  getInt("RL_JOIN_PER_DEVICE", 30),
		RLClaimPerDevice: getInt("RL_CLAIM_PER_DEVICE", 60),

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

		LogLevel:         getEnv("LOG_LEVEL", "info"),
		LogFormat:        getEnv("LOG_FORMAT", "text"),
		OtelExporterType: getEnv("OTEL_EXPORTER_TYPE", "stdout"),
		OtelEndpoint:     getEnv("OTEL_ENDPOINT", "localhost:4317"),
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
