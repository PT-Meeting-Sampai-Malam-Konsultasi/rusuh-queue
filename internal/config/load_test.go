package config

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoadReadsTheEnvironment(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("ENV", "production")
	t.Setenv("TURNSTILE_HOSTNAMES", " rusuh.id, ,www.rusuh.id ")
	t.Setenv("LOGIN_WINDOW", "90s")
	t.Setenv("RL_JOIN_PER_DEVICE", "7")
	t.Setenv("CLAIM_REQUIRES_LOGIN", "false")
	t.Setenv("REQUEST_LOG", "0")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != "9090" || c.Env != "production" || c.LoginWindow != 90*time.Second || c.RLJoinPerDevice != 7 ||
		c.ClaimRequiresLogin || c.RequestLog || !slices.Equal(c.TurnstileHostnames, []string{"rusuh.id", "www.rusuh.id"}) {
		t.Fatalf("config = %+v", c)
	}
}

func TestLoadCoreAccessSettings(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.CoreIssuer != "rusuh-core" || c.CoreAudience != "rusuh-queue" || !c.CoreAccessRequireOTP || c.CoreJWKS != "" {
		t.Fatalf("defaults = %+v", c)
	}
	t.Setenv("CORE_JWKS", `{"keys":[]}`)
	t.Setenv("CORE_ISSUER", "rusuh-auth")
	t.Setenv("CORE_AUDIENCE", "rusuh-api")
	t.Setenv("CORE_ACCESS_REQUIRE_OTP", "false")
	if c, err = Load(); err != nil {
		t.Fatal(err)
	}
	if c.CoreIssuer != "rusuh-auth" || c.CoreAudience != "rusuh-api" || c.CoreAccessRequireOTP || c.CoreJWKS != `{"keys":[]}` {
		t.Fatalf("config = %+v", c)
	}
}

func TestLoadFallsBackOnBadValues(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("CORS_ORIGINS", "")
	t.Setenv("LOGIN_WINDOW", "three minutes")
	t.Setenv("RL_JOIN_PER_DEVICE", "-1")
	t.Setenv("RL_CLAIM_PER_DEVICE", "many")
	t.Setenv("CLAIM_REQUIRES_LOGIN", "maybe")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != "8080" || c.LoginWindow != 3*time.Minute || c.RLJoinPerDevice != 30 || c.RLClaimPerDevice != 60 ||
		!c.ClaimRequiresLogin || c.CORSOrigins != nil {
		t.Fatalf("config = %+v", c)
	}
}

func TestLoadServiceInstanceID(t *testing.T) {
	t.Setenv("SERVICE_INSTANCE_ID", "")
	host, _ := os.Hostname()
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := c.ServiceInstanceID; got == "" || got != host {
		t.Fatalf("ServiceInstanceID = %q, want the hostname %q", got, host)
	}

	t.Setenv("SERVICE_INSTANCE_ID", "task-7f3a")
	c, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := c.ServiceInstanceID; got != "task-7f3a" {
		t.Fatalf("ServiceInstanceID = %q, want task-7f3a", got)
	}
}

// TestLoadValkeyTLSEnabled verifies VALKEY_TLS_ENABLED defaults to false and accepts valid values.
func TestLoadValkeyTLSEnabled(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset", want: false},
		{name: "enabled", value: "true", want: true},
		{name: "disabled", value: "false", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("VALKEY_TLS_ENABLED", tt.value)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.ValkeyTLSEnabled != tt.want {
				t.Errorf("ValkeyTLSEnabled = %t, want %t", cfg.ValkeyTLSEnabled, tt.want)
			}
		})
	}
}

// TestLoadRejectsInvalidValkeyTLSEnabled ensures TLS cannot be silently disabled by a typo.
func TestLoadRejectsInvalidValkeyTLSEnabled(t *testing.T) {
	t.Setenv("VALKEY_TLS_ENABLED", "ture")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want invalid VALKEY_TLS_ENABLED error")
	}
	if !strings.Contains(err.Error(), "VALKEY_TLS_ENABLED") {
		t.Errorf("Load() error = %q, want variable name", err)
	}
}
