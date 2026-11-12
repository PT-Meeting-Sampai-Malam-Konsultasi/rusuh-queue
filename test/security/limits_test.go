package security

import (
	"net/http"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

func wantRateLimited(t *testing.T, what string, code int, retryAfter string, problem api.ErrorCode) {
	t.Helper()
	if code != http.StatusTooManyRequests || problem != api.ErrorCodeRATELIMITED || retryAfter == "" {
		t.Fatalf("%s: %d %s Retry-After %q, want 429 RATE_LIMITED with Retry-After", what, code, problem, retryAfter)
	}
}

// OWASP A04/API4: a device that hammers join is cut off at RL_JOIN_PER_DEVICE,
// and its limit does not lock other devices out.
func TestJoinFloodIsLimitedPerDevice(t *testing.T) {
	e := newEnv(t)
	e.openEvent(t, "evt_flood", 3)
	flooder := map[string]string{"X-Device-Token": e.deviceToken(t, "w_flood")}

	for i := range 30 {
		if rec := e.do("POST", "/v1/queue/evt_flood/join", flooder); rec.Code != http.StatusOK {
			t.Fatalf("join %d: %d %s", i+1, rec.Code, rec.Body)
		}
	}
	rec := e.do("POST", "/v1/queue/evt_flood/join", flooder)
	var code api.ErrorCode
	if rec.Code == http.StatusTooManyRequests {
		code = problemCode(t, rec)
	}
	wantRateLimited(t, "join 31", rec.Code, rec.Header().Get("Retry-After"), code)

	other := map[string]string{"X-Device-Token": e.deviceToken(t, "w_bystander")}
	if rec := e.do("POST", "/v1/queue/evt_flood/join", other); rec.Code != http.StatusOK {
		t.Fatalf("another device during the flood: %d %s", rec.Code, rec.Body)
	}
}

// OWASP A04/API4: claim retries are capped at RL_CLAIM_PER_DEVICE.
func TestClaimFloodIsLimitedPerDevice(t *testing.T) {
	e := newEnv(t)
	k := e.openEvent(t, "evt_cflood", 10)
	c := e.join(t, "evt_cflood", "w_cflood", "acc_cflood")
	testkit.Tick(t, e.client, k, testkit.TickArgs{})

	for i := range 60 {
		if rec := e.claim("evt_cflood", c); rec.Code != http.StatusOK {
			t.Fatalf("claim %d: %d %s", i+1, rec.Code, rec.Body)
		}
	}
	rec := e.claim("evt_cflood", c)
	var code api.ErrorCode
	if rec.Code == http.StatusTooManyRequests {
		code = problemCode(t, rec)
	}
	wantRateLimited(t, "claim 61", rec.Code, rec.Header().Get("Retry-After"), code)
}
