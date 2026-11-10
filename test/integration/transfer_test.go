package integration

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

// D-13 / FAQ 9.10: a logged-in user moves a live place to another device. Since
// 5 Nov every logged-in join moves it; {"takeOver": true} is still accepted and ignored.

func (e queueEnv) joinBody(path string, headers map[string]string, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

const takeOver = `{"takeOver":true}`

func TestTakeOverMovesThePlace(t *testing.T) {
	e := newQueueEnv(t)
	k := testkit.Event(t, e.client, "evt_move", testkit.EventOpts{})
	testkit.Joins(t, e.client, k, "w_before", 3) // seq 1-3 belong to others
	bearer := "Bearer " + e.core.accessToken(t, "user_mv", []string{"otp"}, "rusuh-queue")
	devA, devB := e.deviceToken(t, "m_phone"), e.deviceToken(t, "w_laptop")
	path := "/v1/queue/evt_move/join"

	// Device A: join as a guest, then log in (the account is bound to the place).
	a := decodePlace(t, e.do("POST", path, map[string]string{"X-Device-Token": devA}))
	if a2 := decodePlace(t, e.do("POST", path, map[string]string{"X-Device-Token": devA, "Authorization": bearer})); !a2.AccountAttached || *a2.Seq != *a.Seq {
		t.Fatalf("attach on A: %+v", a2)
	}

	// Device B: logging in moves the place with its number, without asking.
	rec := e.do("POST", path, map[string]string{"X-Device-Token": devB, "Authorization": bearer})
	if rec.Code != http.StatusOK {
		t.Fatalf("logged-in join on B: %d %s", rec.Code, rec.Body)
	}
	b := decodePlace(t, rec)
	if *b.Seq != *a.Seq || !b.AccountAttached {
		t.Fatalf("B got %+v, want seq %d attached", b, *a.Seq)
	}
	// takeOver again is idempotent: B already holds the place.
	if again := decodePlace(t, e.joinBody(path, map[string]string{"X-Device-Token": devB, "Authorization": bearer}, takeOver)); *again.Seq != *a.Seq {
		t.Fatalf("repeat takeOver moved the place: %+v", again)
	}

	// Device A's old queue token is now MOVED, on me and on claim.
	aHeaders := map[string]string{"X-Device-Token": devA, "X-Queue-Token": a.QueueToken}
	if rec := e.do("GET", "/v1/queue/evt_move/me", aHeaders); rec.Code != http.StatusGone || problemCode(t, rec) != api.ErrorCodeMOVED {
		t.Fatalf("me on A: %d %s, want 410 MOVED", rec.Code, rec.Body)
	}
	testkit.Tick(t, e.client, k, testkit.TickArgs{}) // admit everyone, so a claim would otherwise succeed
	if rec := e.do("POST", "/v1/queue/evt_move/claim", aHeaders); rec.Code != http.StatusGone || problemCode(t, rec) != api.ErrorCodeMOVED {
		t.Fatalf("claim on A: %d %s, want 410 MOVED", rec.Code, rec.Body)
	}
	// B claims the moved place.
	if rec := e.do("POST", "/v1/queue/evt_move/claim", map[string]string{"X-Device-Token": devB, "X-Queue-Token": b.QueueToken}); rec.Code != http.StatusOK {
		t.Fatalf("claim on B: %d %s", rec.Code, rec.Body)
	}
}

func TestTakeOverOncePerEvent(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_once", testkit.EventOpts{})
	bearer := "Bearer " + e.core.accessToken(t, "user_once", []string{"otp"}, "rusuh-queue")
	devA, devB := e.deviceToken(t, "m_a"), e.deviceToken(t, "w_b")
	path := "/v1/queue/evt_once/join"

	a := decodePlace(t, e.do("POST", path, map[string]string{"X-Device-Token": devA, "Authorization": bearer}))
	if rec := e.joinBody(path, map[string]string{"X-Device-Token": devB, "Authorization": bearer}, takeOver); rec.Code != http.StatusOK {
		t.Fatalf("first move: %d %s", rec.Code, rec.Body)
	}
	// Device A logs in again and tries to take the place back: the cap is 1.
	rec := e.joinBody(path, map[string]string{"X-Device-Token": devA, "Authorization": bearer}, takeOver)
	if rec.Code != http.StatusConflict || problemCode(t, rec) != api.ErrorCodeACCOUNTHASPLACEELSEWHERE {
		t.Fatalf("second move: %d %s, want 409", rec.Code, rec.Body)
	}
	_ = a
}

func TestTakeOverKeepsTheBetterPlace(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_best", testkit.EventOpts{})
	bearer := "Bearer " + e.core.accessToken(t, "user_best", []string{"otp"}, "rusuh-queue")
	devA, devB := e.deviceToken(t, "m_late"), e.deviceToken(t, "w_early")
	path := "/v1/queue/evt_best/join"

	b := decodePlace(t, e.do("POST", path, map[string]string{"X-Device-Token": devB}))                          // guest, earlier number
	a := decodePlace(t, e.do("POST", path, map[string]string{"X-Device-Token": devA, "Authorization": bearer})) // account's place, later
	moved := decodePlace(t, e.joinBody(path, map[string]string{"X-Device-Token": devB, "Authorization": bearer}, takeOver))
	if *moved.Seq != *b.Seq || *b.Seq >= *a.Seq {
		t.Fatalf("kept seq %d, want B's own lower seq %d (A had %d)", *moved.Seq, *b.Seq, *a.Seq)
	}
	if rec := e.do("GET", "/v1/queue/evt_best/me", map[string]string{"X-Device-Token": devA, "X-Queue-Token": a.QueueToken}); problemCode(t, rec) != api.ErrorCodeMOVED {
		t.Fatalf("A's place should be ended as MOVED: %d %s", rec.Code, rec.Body)
	}
}

func TestTakeOverBeforeTheDraw(t *testing.T) {
	e := newQueueEnv(t)
	k := testkit.Event(t, e.client, "evt_premove", testkit.EventOpts{OpensIn: time.Hour})
	bearer := "Bearer " + e.core.accessToken(t, "user_pre", []string{"otp"}, "rusuh-queue")
	devA, devB := e.deviceToken(t, "m_pa"), e.deviceToken(t, "w_pb")
	path := "/v1/queue/evt_premove/join"

	e.do("POST", path, map[string]string{"X-Device-Token": devA, "Authorization": bearer})
	rec := e.joinBody(path, map[string]string{"X-Device-Token": devB, "Authorization": bearer}, takeOver)
	if p := decodePlace(t, rec); rec.Code != http.StatusOK || p.Seq != nil {
		t.Fatalf("takeOver before the draw: %d %s, want 200 with seq null", rec.Code, rec.Body)
	}
	members, err := e.client.Do(context.Background(), e.client.B().Smembers().Key(k.Pre).Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != "w_pb" {
		t.Fatalf("draw list %v, want only w_pb", members)
	}
}

func TestTakeOverIgnoredForGuestsAndWhenOff(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_noop", testkit.EventOpts{})
	path := "/v1/queue/evt_noop/join"

	// A guest asking to take over just gets its own place.
	if rec := e.joinBody(path, map[string]string{"X-Device-Token": e.deviceToken(t, "w_guest")}, takeOver); rec.Code != http.StatusOK {
		t.Fatalf("guest takeOver: %d %s", rec.Code, rec.Body)
	}
	if rec := e.joinBody(path, map[string]string{"X-Device-Token": e.deviceToken(t, "w_guest")}, `{"takeover":1}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: %d, want 400", rec.Code)
	}

	// PLACE_TRANSFERS_PER_EVENT=0 turns moving off: the 409 stays.
	cfg := queueConfig(5*time.Minute, 5*time.Minute)
	cfg.TransfersPerEvent = 0
	off := newQueueEnvOn(t, e.client, cfg)
	bearer := "Bearer " + off.core.accessToken(t, "user_off", []string{"otp"}, "rusuh-queue")
	off.do("POST", path, map[string]string{"X-Device-Token": off.deviceToken(t, "m_off_a"), "Authorization": bearer})
	rec := off.joinBody(path, map[string]string{"X-Device-Token": off.deviceToken(t, "w_off_b"), "Authorization": bearer}, takeOver)
	if rec.Code != http.StatusConflict {
		t.Fatalf("moving off: %d %s, want 409", rec.Code, rec.Body)
	}
}

// A claimed turn moves while its order window runs (ADR-0021, login_claim_test.go).
// Once the lease ended there is nothing to move: B gets a normal join at the back,
// and A's old claim is EXPIRED.
func TestTakeOverDoesNotMoveAnEndedClaim(t *testing.T) {
	e := newQueueEnvOn(t, testkit.Valkey(t), queueConfig(100*time.Millisecond, 5*time.Minute))
	k := testkit.Event(t, e.client, "evt_claimed", testkit.EventOpts{})
	bearer := "Bearer " + e.core.accessToken(t, "user_cl", []string{"otp"}, "rusuh-queue")
	devA, devB := e.deviceToken(t, "m_ca"), e.deviceToken(t, "w_cb")
	path := "/v1/queue/evt_claimed/join"

	a := decodePlace(t, e.do("POST", path, map[string]string{"X-Device-Token": devA, "Authorization": bearer}))
	testkit.Tick(t, e.client, k, testkit.TickArgs{})
	if rec := e.do("POST", "/v1/queue/evt_claimed/claim", map[string]string{"X-Device-Token": devA, "X-Queue-Token": a.QueueToken}); rec.Code != http.StatusOK {
		t.Fatalf("claim on A: %d %s", rec.Code, rec.Body)
	}
	time.Sleep(150 * time.Millisecond) // the 100 ms order window ends

	b := decodePlace(t, e.do("POST", path, map[string]string{"X-Device-Token": devB, "Authorization": bearer}))
	if *b.Seq == *a.Seq || b.Claimed {
		t.Fatalf("an ended claim moved to B: %+v", b)
	}
	if rec := e.do("POST", "/v1/queue/evt_claimed/claim", map[string]string{"X-Device-Token": devA, "X-Queue-Token": a.QueueToken}); rec.Code != http.StatusGone {
		t.Fatalf("A's claim after its lease ended: %d %s, want 410", rec.Code, rec.Body)
	}
}
