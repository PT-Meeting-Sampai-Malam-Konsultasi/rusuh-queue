package integration

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

// ADR-0021 end to end: a guest's turn waits for the login, the claim needs it,
// and a claimed turn resumes on another device with the same slot and time left.
func TestLoginBeforeClaimThenResumeOnAnotherDevice(t *testing.T) {
	cfg := queueConfig(5*time.Minute, 200*time.Millisecond)
	cfg.LoginWindow = 2 * time.Second
	cfg.ClaimRequiresLogin = true
	e := newQueueEnvOn(t, testkit.Valkey(t), cfg)
	k := testkit.Event(t, e.client, "evt_login", testkit.EventOpts{})
	bearer := "Bearer " + e.core.accessToken(t, "user_lc", []string{"otp"}, "rusuh-queue")
	devPhone, devLaptop := e.deviceToken(t, "m_phone"), e.deviceToken(t, "w_laptop")

	phone := decodePlace(t, e.do("POST", "/v1/queue/evt_login/join", map[string]string{"X-Device-Token": devPhone}))
	testkit.Tick(t, e.client, k, testkit.TickArgs{})
	phoneHeaders := map[string]string{"X-Device-Token": devPhone, "X-Queue-Token": phone.QueueToken}

	if rec := e.do("POST", "/v1/queue/evt_login/claim", phoneHeaders); rec.Code != http.StatusUnauthorized ||
		problemCode(t, rec) != api.ErrorCodeLOGINREQUIRED {
		t.Fatalf("guest claim: %d %s, want 401 LOGIN_REQUIRED", rec.Code, rec.Body)
	}
	time.Sleep(400 * time.Millisecond) // past the 200 ms miss window, inside the guest's login window
	phoneHeaders["Authorization"] = bearer
	rec := e.do("POST", "/v1/queue/evt_login/claim", phoneHeaders)
	var first api.Admission
	if rec.Code != http.StatusOK || json.NewDecoder(rec.Body).Decode(&first) != nil {
		t.Fatalf("claim after the login: %d %s", rec.Code, rec.Body)
	}

	// The phone dies; the user logs in on the laptop while the order window runs.
	laptop := decodePlace(t, e.do("POST", "/v1/queue/evt_login/join", map[string]string{"X-Device-Token": devLaptop, "Authorization": bearer}))
	if laptop.Seq == nil || *laptop.Seq != *phone.Seq || !laptop.Claimed || !laptop.AccountAttached {
		t.Fatalf("laptop join = %+v, want the phone's seq %d with claimed", laptop, *phone.Seq)
	}
	rec = e.do("POST", "/v1/queue/evt_login/claim", map[string]string{
		"X-Device-Token": devLaptop, "X-Queue-Token": laptop.QueueToken, "Authorization": bearer})
	var resumed api.Admission
	if rec.Code != http.StatusOK || json.NewDecoder(rec.Body).Decode(&resumed) != nil {
		t.Fatalf("laptop claim: %d %s", rec.Code, rec.Body)
	}
	if resumed.SlotId != first.SlotId || !resumed.ExpiresAt.Equal(first.ExpiresAt) || resumed.ReservedUnits != first.ReservedUnits {
		t.Fatalf("resumed %+v, want the phone's slot %s ending %s", resumed, first.SlotId, first.ExpiresAt)
	}

	if rec := e.do("POST", "/v1/queue/evt_login/claim", phoneHeaders); rec.Code != http.StatusGone || problemCode(t, rec) != api.ErrorCodeMOVED {
		t.Fatalf("phone claim after the move: %d %s, want 410 MOVED", rec.Code, rec.Body)
	}
}
