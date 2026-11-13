package security

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

// OWASP A04 (replay): one turn reserves stock once, however many times and
// however fast its claim is sent; every reply names the same slot and lease end.
func TestReplayedClaimsReserveOnce(t *testing.T) {
	e := newEnv(t)
	k := e.openEvent(t, "evt_replay", 10)
	c := e.join(t, "evt_replay", "w_replay", "acc_replay")
	testkit.Tick(t, e.client, k, testkit.TickArgs{})

	const n = 20
	recs := make([]*httptest.ResponseRecorder, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { recs[i] = e.claim("evt_replay", c) })
	}
	wg.Wait()

	first := decodeAdmission(t, recs[0])
	for i, rec := range recs[1:] {
		a := decodeAdmission(t, rec)
		if a.SlotId != first.SlotId || !a.ExpiresAt.Equal(first.ExpiresAt) || a.ReservedUnits != first.ReservedUnits {
			t.Fatalf("claim %d = %+v, want slot %s ending %s", i+1, a, first.SlotId, first.ExpiresAt)
		}
	}
	if left := testkit.Int(t, e.client, k.Unreserved); left != 10-int64(first.ReservedUnits) {
		t.Fatalf("unreserved = %d after %d claims of %d units, want %d", left, n, first.ReservedUnits, 10-first.ReservedUnits)
	}
}

// OWASP A04: replaying a claim after its lease ended gets no new lease and no new
// admission token, so a stale turn cannot be stretched past the order window.
func TestClaimAfterTheLeaseEndedIsRefused(t *testing.T) {
	e := newEnvWith(t, func(c *config.Config) { c.OrderWindow = 500 * time.Millisecond })
	k := e.openEvent(t, "evt_late", 10)
	c := e.join(t, "evt_late", "w_late", "acc_late")
	testkit.Tick(t, e.client, k, testkit.TickArgs{})
	first := decodeAdmission(t, e.claim("evt_late", c))

	time.Sleep(time.Until(first.ExpiresAt) + 100*time.Millisecond)
	rec := e.claim("evt_late", c)
	if rec.Code != http.StatusGone || problemCode(t, rec) != api.ErrorCodeEXPIRED {
		t.Fatalf("claim after the lease: %d %s, want 410 EXPIRED", rec.Code, rec.Body)
	}
	if left := testkit.Int(t, e.client, k.Unreserved); left != 10-int64(first.ReservedUnits) {
		t.Fatalf("unreserved = %d, want %d: the late claim reserved again", left, 10-first.ReservedUnits)
	}
}
