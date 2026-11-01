package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

// admissionEnv runs the controller and the queue HTTP module on one Valkey,
// with a short order window (lease) and miss window so the 5-minute rules can
// be seen in seconds.
type admissionEnv struct {
	queueEnv
}

const (
	testOrderWindow = 800 * time.Millisecond
	testMissWindow  = time.Second
)

func newAdmissionEnv(t *testing.T) admissionEnv {
	t.Helper()
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	cfg := fastController()
	cfg.PurgeGrace = 200 * time.Millisecond
	cfg.HistWindow = testMissWindow
	startControllerWith(t, client, db, cfg)
	time.Sleep(300 * time.Millisecond) // subscriptions
	return admissionEnv{newQueueEnvOn(t, client, queueConfig(testOrderWindow, testMissWindow))}
}

// publish makes core's documents for an open sale and nudges, then waits for the queue.
func (e admissionEnv) publish(t *testing.T, id string, available int64) {
	t.Helper()
	testkit.CoreSale(t, e.client, sale(id, -time.Minute, "published"), stock(id, available))
	testkit.Publish(t, e.client, qkeys.CoreEventsChanged)
	eventually(t, id+" open", func() bool { return cfgState(e.client, id) == "open" })
}

type joined struct {
	id, devToken, queueToken string
	seq                      int64
}

func (e admissionEnv) join(t *testing.T, eventID, id string) joined {
	t.Helper()
	d := joined{id: id, devToken: e.deviceToken(t, id)}
	p := decodePlace(t, e.do("POST", "/v1/queue/"+eventID+"/join", map[string]string{"X-Device-Token": d.devToken}))
	d.queueToken, d.seq = p.QueueToken, *p.Seq
	return d
}

func (e admissionEnv) claim(eventID string, d joined) *httptest.ResponseRecorder {
	return e.do("POST", "/v1/queue/"+eventID+"/claim", map[string]string{"X-Device-Token": d.devToken, "X-Queue-Token": d.queueToken})
}

// claimUntil retries a claim like the client does on NOT_YET and returns the first other answer.
func (e admissionEnv) claimUntil(t *testing.T, eventID string, d joined, within time.Duration) (*httptest.ResponseRecorder, time.Duration) {
	t.Helper()
	start := time.Now()
	for {
		rec := e.claim(eventID, d)
		if rec.Code != 409 || problemCode(t, rec) != api.ErrorCodeNOTYET || time.Since(start) > within {
			return rec, time.Since(start)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// M3 verify, FR-05: with no queue, join → claim succeeds within one tick.
// The admission token verifies the way core will check it (tokens.md).
func TestJoinThenClaimWithinOneTick(t *testing.T) {
	e := newAdmissionEnv(t)
	e.publish(t, "evt_now", 100)

	d := e.join(t, "evt_now", "w_now1")
	rec, took := e.claimUntil(t, "evt_now", d, 2*time.Second)
	if rec.Code != 200 {
		t.Fatalf("claim %d %s", rec.Code, rec.Body)
	}
	if took > 2*fastController().TickEvery {
		t.Fatalf("admitted after %s, want within one tick (%s)", took, fastController().TickEvery)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q", rec.Header().Get("Cache-Control"))
	}
	var adm api.Admission
	if err := json.NewDecoder(rec.Body).Decode(&adm); err != nil {
		t.Fatal(err)
	}
	if adm.ReservedUnits != 3 || adm.LoginRequired == nil || !*adm.LoginRequired || adm.SlotId == "" {
		t.Fatalf("admission = %+v", adm)
	}
	if d := adm.ExpiresAt.Sub(adm.ServerNow); d != testOrderWindow {
		t.Fatalf("expiresAt - serverNow = %s, want the order window", d)
	}

	var c token.AdmissionClaims
	if err := e.keys.Verify(token.KindAdmission, adm.AdmissionToken, token.AudienceCore, &c); err != nil {
		t.Fatalf("admission token: %v", err)
	}
	if c.Event != "evt_now" || c.Seq != 1 || c.Slot != adm.SlotId || c.Reserved != 3 || c.Subject != "w_now1" ||
		!c.ExpiresAt.Equal(adm.ExpiresAt.Truncate(time.Second)) {
		t.Fatalf("claims = %+v", c)
	}
	if err := e.keys.Verify(token.KindAdmission, adm.AdmissionToken, token.AudienceQueue, &c); err == nil {
		t.Fatal("admission token accepted for queue's own audience")
	}

	// A retry (lost response) returns the same slot.
	again := e.claim("evt_now", d)
	var adm2 api.Admission
	_ = json.NewDecoder(again.Body).Decode(&adm2)
	if again.Code != 200 || adm2.SlotId != adm.SlotId {
		t.Fatalf("retry %d slot %q, want %q", again.Code, adm2.SlotId, adm.SlotId)
	}
}

// M3 verify: a lease left alone past its end and the purge grace gives its
// units back; the old place gets EXPIRED; the next user is admitted.
func TestLeaseExpiryAdmitsNext(t *testing.T) {
	e := newAdmissionEnv(t)
	e.publish(t, "evt_exp", 3)
	k := qkeys.For("evt_exp")

	a := e.join(t, "evt_exp", "w_expA")
	b := e.join(t, "evt_exp", "w_expB")
	if rec, _ := e.claimUntil(t, "evt_exp", a, 2*time.Second); rec.Code != 200 {
		t.Fatalf("a claim %d %s", rec.Code, rec.Body)
	}
	// Every unit is reserved by a: b waits, the queue is paused.
	rec := e.claim("evt_exp", b)
	if rec.Code != 409 || problemCode(t, rec) != api.ErrorCodeNOTYET || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("b claim %d %s", rec.Code, rec.Body)
	}
	eventually(t, "paused", func() bool { return cfgState(e.client, "evt_exp") == "paused" })

	// Lease end (0.8 s) + grace (0.2 s) + one tick.
	eventually(t, "units back", func() bool { return testkit.Int(t, e.client, k.Unreserved) > 0 })
	if rec := e.claim("evt_exp", a); rec.Code != 410 || problemCode(t, rec) != api.ErrorCodeEXPIRED {
		t.Fatalf("a after expiry %d %s", rec.Code, rec.Body)
	}
	rec, _ = e.claimUntil(t, "evt_exp", b, 2*time.Second)
	if rec.Code != 200 {
		t.Fatalf("b claim %d %s", rec.Code, rec.Body)
	}
	// a rejoins at the back.
	if again := e.join(t, "evt_exp", "w_expA"); again.seq != 3 {
		t.Fatalf("a rejoined at seq %d, want 3", again.seq)
	}
}

// M3 verify: a device that comes back after the miss window without claiming gets MISSED.
func TestMissedTurn(t *testing.T) {
	e := newAdmissionEnv(t)
	e.publish(t, "evt_miss", 100)
	d := e.join(t, "evt_miss", "w_miss1")

	eventually(t, "turn reached", func() bool { return testkit.Int(t, e.client, qkeys.For("evt_miss").Served) >= 1 })
	time.Sleep(testMissWindow + 300*time.Millisecond)
	if rec := e.claim("evt_miss", d); rec.Code != 410 || problemCode(t, rec) != api.ErrorCodeMISSED {
		t.Fatalf("claim %d %s", rec.Code, rec.Body)
	}
	me := e.do("GET", "/v1/queue/evt_miss/me", map[string]string{"X-Device-Token": d.devToken, "X-Queue-Token": d.queueToken})
	if me.Code != 410 || problemCode(t, me) != api.ErrorCodeMISSED {
		t.Fatalf("me %d %s", me.Code, me.Body)
	}
	// "Antre lagi": a new number at the back.
	if again := e.join(t, "evt_miss", "w_miss1"); again.seq != 2 {
		t.Fatalf("rejoin seq = %d, want 2", again.seq)
	}
}

// QueuePlace carries what the waiting room draws: served (progress track),
// opensAt (pre-queue countdown), turnExpiresAt (60 s to enter), serverNow.
func TestPlaceCarriesTurnAndClock(t *testing.T) {
	e := newAdmissionEnv(t)
	e.publish(t, "evt_turn", 100)
	devTok := e.deviceToken(t, "w_turn1")
	var raw map[string]any
	rec := e.do("POST", "/v1/queue/evt_turn/join", map[string]string{"X-Device-Token": devTok})
	_ = json.Unmarshal(rec.Body.Bytes(), &raw)
	if raw["turnExpiresAt"] != nil || raw["serverNow"] == nil || raw["opensAt"] == nil || raw["served"] == nil {
		t.Fatalf("join body = %v", raw)
	}
	qt, _ := raw["queueToken"].(string)

	eventually(t, "turn reached", func() bool { return testkit.Int(t, e.client, qkeys.For("evt_turn").Served) >= 1 })
	me := e.do("GET", "/v1/queue/evt_turn/me", map[string]string{"X-Device-Token": devTok, "X-Queue-Token": qt})
	var p struct {
		TurnExpiresAt *time.Time `json:"turnExpiresAt"`
		ServerNow     time.Time  `json:"serverNow"`
		Served        int64      `json:"served"`
	}
	_ = json.Unmarshal(me.Body.Bytes(), &p)
	if me.Code != 200 || p.TurnExpiresAt == nil || p.Served < 1 {
		t.Fatalf("me %d %s", me.Code, me.Body)
	}
	if left := p.TurnExpiresAt.Sub(p.ServerNow); left <= 0 || left > testMissWindow {
		t.Fatalf("turn ends in %s, want within the miss window", left)
	}
}

// leaveQueue: 204 (twice), the place is over, and a new join goes to the back.
func TestLeaveQueue(t *testing.T) {
	e := newAdmissionEnv(t)
	e.publish(t, "evt_bye", 100)
	d := e.join(t, "evt_bye", "w_bye1")
	h := map[string]string{"X-Device-Token": d.devToken, "X-Queue-Token": d.queueToken}

	if rec := e.do("DELETE", "/v1/queue/evt_bye/me", map[string]string{"X-Device-Token": d.devToken}); rec.Code != 401 {
		t.Fatalf("without queue token: %d", rec.Code)
	}
	for range 2 {
		if rec := e.do("DELETE", "/v1/queue/evt_bye/me", h); rec.Code != 204 {
			t.Fatalf("leave %d %s", rec.Code, rec.Body)
		}
	}
	if rec := e.do("GET", "/v1/queue/evt_bye/me", h); rec.Code != 410 || problemCode(t, rec) != api.ErrorCodeEXPIRED {
		t.Fatalf("me after leave %d %s", rec.Code, rec.Body)
	}
	if again := e.join(t, "evt_bye", "w_bye1"); again.seq != 2 {
		t.Fatalf("rejoin seq = %d, want 2", again.seq)
	}
}

func TestClaimErrors(t *testing.T) {
	e := newAdmissionEnv(t)
	e.publish(t, "evt_cerr", 100)
	d := e.join(t, "evt_cerr", "w_cerr1")
	other := e.join(t, "evt_cerr", "w_cerr2")

	for name, h := range map[string]map[string]string{
		"no tokens":            nil,
		"no queue token":       {"X-Device-Token": d.devToken},
		"queue token of other": {"X-Device-Token": d.devToken, "X-Queue-Token": other.queueToken},
	} {
		if rec := e.do("POST", "/v1/queue/evt_cerr/claim", h); rec.Code != 401 {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body)
		}
	}
	if rec := e.claim("evt_nope", d); rec.Code != 401 {
		t.Errorf("token for another event: %d", rec.Code)
	}
	ctx := context.Background()
	e.client.Do(ctx, e.client.B().Hset().Key(qkeys.For("evt_cerr").Cfg).FieldValue().FieldValue("state", "closed").Build())
	if rec := e.claim("evt_cerr", d); rec.Code != 410 || problemCode(t, rec) != api.ErrorCodeQUEUECLOSED {
		t.Errorf("closed: %d %s", rec.Code, rec.Body)
	}
}

// M3 verify: two controllers on one event → one leader, one fence; the other
// takes over within a tick when the leader stops (resign), and only the
// newest fence writes the checkpoint.
func TestTwoControllersOneLeader(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	cfgA, cfgB := fastController(), fastController()
	cfgA.TaskID, cfgB.TaskID = "task-a", "task-b"
	stopA := startControllerWith(t, client, db, cfgA)
	time.Sleep(300 * time.Millisecond)
	k := qkeys.For("evt_two")
	testkit.CoreSale(t, client, sale("evt_two", -time.Minute, "published"), stock("evt_two", 10000))
	testkit.Publish(t, client, qkeys.CoreEventsChanged)
	eventually(t, "a leads", func() bool { return leader(client, k) == "task-a" })
	startControllerWith(t, client, db, cfgB)

	time.Sleep(time.Second) // five ticks of both
	if l, f := leader(client, k), testkit.Int(t, client, k.Fence); l != "task-a" || f != 1 {
		t.Fatalf("leader %q fence %d, want task-a with fence 1 (no flapping)", l, f)
	}
	eventually(t, "checkpoint fence 1", func() bool { return checkpointFence(db, "evt_two") == 1 })

	stopA()
	took := time.Now()
	eventually(t, "b leads", func() bool { return leader(client, k) == "task-b" })
	if d := time.Since(took); d > 3*cfgB.TickEvery {
		t.Fatalf("takeover after %s, want within about one tick", d)
	}
	if f := testkit.Int(t, client, k.Fence); f != 2 {
		t.Fatalf("fence = %d, want 2", f)
	}
	eventually(t, "checkpoint fence 2", func() bool { return checkpointFence(db, "evt_two") == 2 })
}

func leader(client vk.Client, k qkeys.Event) string {
	s, _ := client.Do(context.Background(), client.B().Get().Key(k.Leader).Build()).ToString()
	return s
}

func checkpointFence(db *sql.DB, eventID string) int64 {
	var fence int64
	_ = db.QueryRow(`SELECT fence FROM queue.checkpoint WHERE event_id = $1`, eventID).Scan(&fence)
	return fence
}

// P1: devices that join before saleOpensAt are drawn at opening, the queue
// opens, and a client claims with its pre-draw queue token (seq 0).
func TestPreopenJoinersAreDrawnAndClaim(t *testing.T) {
	e := newAdmissionEnv(t)
	const id = "evt_pre"
	testkit.CoreSale(t, e.client, sale(id, 700*time.Millisecond, "published"), stock(id, 100))
	testkit.Publish(t, e.client, qkeys.CoreEventsChanged)
	eventually(t, id+" preopen", func() bool { return cfgState(e.client, id) == "preopen" })

	devs := make([]joined, 3)
	for i := range devs {
		devs[i] = joined{id: "w_pre" + strconv.Itoa(i), devToken: e.deviceToken(t, "w_pre"+strconv.Itoa(i))}
		p := decodePlace(t, e.do("POST", "/v1/queue/"+id+"/join", map[string]string{"X-Device-Token": devs[i].devToken}))
		if p.Seq != nil {
			t.Fatalf("seq %d before the draw, want null", *p.Seq)
		}
		devs[i].queueToken = p.QueueToken
	}

	eventually(t, id+" open", func() bool { return cfgState(e.client, id) == "open" })
	seqs := map[int]bool{}
	for _, d := range devs {
		rec, _ := e.claimUntil(t, id, d, 3*time.Second)
		if rec.Code != 200 {
			t.Fatalf("%s claim %d %s", d.id, rec.Code, rec.Body)
		}
		var adm api.Admission
		if err := json.NewDecoder(rec.Body).Decode(&adm); err != nil {
			t.Fatal(err)
		}
		var c token.AdmissionClaims
		if err := e.keys.Verify(token.KindAdmission, adm.AdmissionToken, token.AudienceCore, &c); err != nil {
			t.Fatal(err)
		}
		seqs[int(c.Seq)] = true
	}
	if len(seqs) != 3 || !seqs[1] || !seqs[2] || !seqs[3] {
		t.Fatalf("admission seqs %v, want 1, 2 and 3", seqs)
	}
}
