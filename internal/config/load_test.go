package config

import (
	"slices"
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

	c := Load()
	if c.Port != "9090" || c.Env != "production" || c.LoginWindow != 90*time.Second || c.RLJoinPerDevice != 7 ||
		c.ClaimRequiresLogin || c.RequestLog || !slices.Equal(c.TurnstileHostnames, []string{"rusuh.id", "www.rusuh.id"}) {
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

	c := Load()
	if c.Port != "8080" || c.LoginWindow != 3*time.Minute || c.RLJoinPerDevice != 30 || c.RLClaimPerDevice != 60 ||
		!c.ClaimRequiresLogin || c.CORSOrigins != nil {
		t.Fatalf("config = %+v", c)
	}
}
