package valkey_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

var tickArgs = dto.TickArgs{HistWindow: 5 * time.Minute, InviteWindow: 30 * time.Second}

func set(t *testing.T, client vk.Client, key string, v int64) {
	t.Helper()
	if err := client.Do(context.Background(), client.B().Set().Key(key).Value(strconv.FormatInt(v, 10)).Build()).Error(); err != nil {
		t.Fatal(err)
	}
}

// leaseSlots fills n slots with leases that end in an hour.
func leaseSlots(t *testing.T, client vk.Client, k qkeys.Event, n int) {
	t.Helper()
	ends := time.Now().Add(time.Hour).UnixMilli()
	for i := range n {
		if err := client.Do(context.Background(), client.B().Zadd().Key(k.Active).ScoreMember().
			ScoreMember(float64(ends), "s_fill"+strconv.Itoa(i)).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
}

func lead(t *testing.T, s outbound.IControllerStore, k qkeys.Event, task string) dto.Lead {
	t.Helper()
	l, err := s.Lead(context.Background(), k, task, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// Advance rule (IMPLEMENTATION_PLAN.md rusuh_tick_v1) on one tick.
func TestTickAdvanceRule(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)

	for _, tc := range []struct {
		name        string
		opts        testkit.EventOpts
		waiting     int // devices in the queue (seq)
		resv, held  int64
		active      int
		hold        bool
		wantAdvance int64
		wantState   string
		wantReason  api.FrontierReason
	}{
		{name: "empty queue", wantAdvance: 0, wantState: "open"},
		{name: "fewer waiting than the rate cap", waiting: 10, wantAdvance: 10, wantState: "open"},
		{name: "rate cap", opts: testkit.EventOpts{Available: 1000, RateCap: 50}, waiting: 120, wantAdvance: 50, wantState: "open"},
		{name: "slots full", opts: testkit.EventOpts{MaxSlots: 5}, waiting: 10, active: 5, wantAdvance: 0, wantState: "open"},
		{name: "two slots free", opts: testkit.EventOpts{MaxSlots: 5}, waiting: 10, active: 3, wantAdvance: 2, wantState: "open"},
		{name: "stock for three users", opts: testkit.EventOpts{Available: 9}, waiting: 10, wantAdvance: 3, wantState: "open"},
		{name: "two units left: one more user", opts: testkit.EventOpts{Available: 2}, waiting: 10, wantAdvance: 1, wantState: "open"},
		{name: "one unit left: one more user", opts: testkit.EventOpts{Available: 1}, waiting: 10, wantAdvance: 1, wantState: "open"},
		{name: "everything reserved: paused", opts: testkit.EventOpts{Available: 1}, waiting: 10, resv: 3, wantAdvance: 0, wantState: "paused", wantReason: "stock"},
		{name: "everything held: paused", opts: testkit.EventOpts{Available: 1}, waiting: 10, held: 2, wantAdvance: 0, wantState: "paused", wantReason: "stock"},
		{name: "operator pause", waiting: 10, hold: true, wantAdvance: 0, wantState: "paused", wantReason: "operator"},
		{name: "sold out: closed", opts: testkit.EventOpts{Available: 1}, waiting: 10, wantAdvance: 0, wantState: "closed", wantReason: "sold_out"},
		{name: "cancelled stays closed", opts: testkit.EventOpts{Cancelled: true}, waiting: 0, wantAdvance: 0, wantState: "closed", wantReason: "cancelled"},
		{name: "before opening", opts: testkit.EventOpts{OpensIn: time.Hour}, wantAdvance: 0, wantState: "preopen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := testkit.Event(t, client, "evt_"+strconv.Itoa(int(time.Now().UnixNano())), tc.opts)
			testkit.Joins(t, client, k, "w_d", tc.waiting)
			if tc.name == "everything reserved: paused" || tc.name == "everything held: paused" || tc.name == "sold out: closed" {
				set(t, client, k.Unreserved, 0)
			}
			set(t, client, k.Resv, tc.resv)
			set(t, client, k.Held, tc.held)
			leaseSlots(t, client, k, tc.active)
			if tc.hold {
				client.Do(context.Background(), client.B().Hset().Key(k.Cfg).FieldValue().FieldValue("hold", "1").Build())
			}

			l := lead(t, s, k, "task-a")
			r, err := s.Tick(context.Background(), k, dto.TickArgs{Fence: l.Fence, HistWindow: tickArgs.HistWindow, InviteWindow: tickArgs.InviteWindow})
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != dto.TickOK || r.Advance != tc.wantAdvance || r.State != tc.wantState {
				t.Fatalf("tick = %s advance %d state %s, want advance %d state %s", r.Status, r.Advance, r.State, tc.wantAdvance, tc.wantState)
			}
			if r.Served != tc.wantAdvance || testkit.Int(t, client, k.Served) != tc.wantAdvance {
				t.Fatalf("served = %d", r.Served)
			}
			var f api.Frontier
			if err := json.Unmarshal(r.Frontier, &f); err != nil || string(f.State) != tc.wantState || int64(f.Served) != tc.wantAdvance {
				t.Fatalf("frontier = %s (%v)", r.Frontier, err)
			}
			var gotReason api.FrontierReason
			if f.Reason != nil {
				gotReason = *f.Reason
			}
			if gotReason != tc.wantReason || f.OpensAt == nil || *f.OpensAt == 0 {
				t.Fatalf("frontier reason %q opensAt %v, want reason %q (HiFi: Penuh sementara vs organiser pause)", gotReason, f.OpensAt, tc.wantReason)
			}
			if tc.wantState == "closed" || tc.wantState == "paused" {
				if st, _ := client.Do(context.Background(), client.B().Hget().Key(k.Cfg).Field("state").Build()).ToString(); st != tc.wantState {
					t.Fatalf("cfg state = %s, want %s (join must see it)", st, tc.wantState)
				}
			}
		})
	}
}

// D-7: invited users who have not claimed yet hold provisional slots, so
// served cannot run ahead of the slot cap; a claim frees the provisional slot
// for a real one, and an invitation older than the window stops counting.
func TestTickInvitedPendingHoldsSlots(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	k := testkit.Event(t, client, "evt_d7", testkit.EventOpts{MaxSlots: 5})
	testkit.Joins(t, client, k, "w_d", 20)
	l := lead(t, s, k, "task-a")
	args := dto.TickArgs{Fence: l.Fence, HistWindow: 5 * time.Minute, InviteWindow: 400 * time.Millisecond}

	tick := func() dto.TickResult {
		t.Helper()
		r, err := s.Tick(context.Background(), k, args)
		if err != nil || r.Status != dto.TickOK {
			t.Fatalf("tick = %+v, %v", r, err)
		}
		return r
	}
	if r := tick(); r.Advance != 5 || r.Pending != 5 {
		t.Fatalf("first tick = advance %d pending %d, want 5/5", r.Advance, r.Pending)
	}
	if r := tick(); r.Advance != 0 {
		t.Fatalf("second tick advance = %d, want 0 while 5 invitations are pending", r.Advance)
	}

	// Two of them claim: 2 leases, 3 pending, still no room.
	for i := range 2 {
		if st, _ := testkit.Claim(t, client, k, "w_d"+strconv.Itoa(i), int64(i+1), time.Hour); st != "OK" {
			t.Fatalf("claim = %s", st)
		}
	}
	if r := tick(); r.Advance != 0 || r.Pending != 3 {
		t.Fatalf("after 2 claims = advance %d pending %d, want 0/3", r.Advance, r.Pending)
	}

	// The 3 unclaimed invitations age out: 3 slots open again.
	time.Sleep(500 * time.Millisecond)
	if r := tick(); r.Advance != 3 {
		t.Fatalf("after the invitation window advance = %d, want 3 (5 slots - 2 leases)", r.Advance)
	}
}

// ratePerMin comes from served over the history window; the frontier is
// published on the tick channel.
func TestTickRateAndPublish(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	k := testkit.Event(t, client, "evt_rate", testkit.EventOpts{Available: 10000, RateCap: 10})
	testkit.Joins(t, client, k, "w_d", 100)
	l := lead(t, s, k, "task-a")

	got := make(chan string, 16)
	go valkeyx.Subscribe(t.Context(), client, k.Tick, func(m string) {
		if m != "" {
			got <- m
		}
	})
	time.Sleep(300 * time.Millisecond)

	var last dto.TickResult
	for range 3 {
		r, err := s.Tick(context.Background(), k, dto.TickArgs{Fence: l.Fence, HistWindow: 5 * time.Minute, InviteWindow: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		last = r
		time.Sleep(600 * time.Millisecond)
	}
	var f api.Frontier
	_ = json.Unmarshal(last.Frontier, &f)
	// 30 admitted in ~1.2 s since the first admission → about 1,500 a minute.
	if f.Served != 30 || f.RatePerMin == nil || *f.RatePerMin < 900 || *f.RatePerMin > 2400 {
		t.Fatalf("frontier = %s", last.Frontier)
	}

	select {
	case m := <-got:
		var pf api.Frontier
		if err := json.Unmarshal([]byte(m), &pf); err != nil || pf.Event != "evt_rate" {
			t.Fatalf("published %q (%v)", m, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no frontier on the tick channel")
	}
}

// seedHist writes a 10 s admission history ending now (Valkey's clock): one
// entry in the middle of each 1 s slice, 10 a second for five slices, then 20
// a second, starting from served 1000. Nobody waits, so the tick advances 0
// and the frontier's speed comes only from this history: ratePerMin 900, slice
// speeds 5 x 600 and 5 x 1200 a minute. The first admission was started ago.
func seedHist(t *testing.T, client vk.Client, k qkeys.Event, started time.Duration) {
	t.Helper()
	ctx := context.Background()
	tm, err := client.Do(ctx, client.B().Time().Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	sec, _ := strconv.ParseInt(tm[0], 10, 64)
	usec, _ := strconv.ParseInt(tm[1], 10, 64)
	now := sec*1000 + usec/1000
	served := int64(1000)
	for b := range int64(10) {
		served += 10
		if b >= 5 {
			served += 10
		}
		at := now - 10_000 + b*1000 + 500
		member := strconv.FormatInt(at, 10) + ":" + strconv.FormatInt(served, 10)
		if err := client.Do(ctx, client.B().Zadd().Key(k.Hist).ScoreMember().ScoreMember(float64(served), member).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	set(t, client, k.Served, served)
	set(t, client, k.Seq, served)
	if err := client.Do(ctx, client.B().Hset().Key(k.Cfg).FieldValue().
		FieldValue("histStart", strconv.FormatInt(now-started.Milliseconds(), 10)).
		FieldValue("histFloor", "1000").Build()).Error(); err != nil {
		t.Fatal(err)
	}
}

// rateLow / rateHigh / rateSource (FR-09, architecture 08): the range comes from
// the slice speeds, and a warm-up guess maxSlots / prior replaces the measured
// speed for one order time after the first admission, then blends out.
func TestTickRateRange(t *testing.T) {
	for _, tc := range []struct {
		name            string
		started, prior  time.Duration
		rate, low, high int // within 2% (blend depends on the clock)
		source          api.FrontierRateSource
	}{
		// no guess: measured 900, p25 600, p75 1200
		{"measured", 20 * time.Second, 0, 900, 600, 1200, api.Measured},
		// guess 2000 slots / 1 h = 33/min for the first hour: wide 0.6x..1.5x band
		{"warm-up", 2 * time.Minute, time.Hour, 33, 20, 50, api.Prior},
		// half way through the blend minute: (900 + 12000) / 2, still the wide band
		{"blending", 40 * time.Second, 10 * time.Second, 6450, 3870, 9675, api.Prior},
		// blend over: measured, and the warm-up slices are long out of the window
		{"after warm-up", 80 * time.Second, 10 * time.Second, 900, 600, 1200, api.Measured},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := testkit.Valkey(t)
			s := valkey.New(client)
			k := testkit.Event(t, client, "evt_range", testkit.EventOpts{Available: 10000})
			seedHist(t, client, k, tc.started)
			l := lead(t, s, k, "task-a")
			r, err := s.Tick(context.Background(), k, dto.TickArgs{
				Fence: l.Fence, HistWindow: 10 * time.Second, InviteWindow: time.Second, PriorOrderTime: tc.prior,
			})
			if err != nil || r.Advance != 0 {
				t.Fatalf("tick = %+v, %v", r, err)
			}
			var f api.Frontier
			if err := json.Unmarshal(r.Frontier, &f); err != nil || f.RatePerMin == nil || f.RateLow == nil || f.RateHigh == nil || f.RateSource == nil {
				t.Fatalf("frontier = %s (%v)", r.Frontier, err)
			}
			near := func(got, want int) bool { return got >= want*98/100 && got <= want*102/100+1 }
			if !near(*f.RatePerMin, tc.rate) || !near(*f.RateLow, tc.low) || !near(*f.RateHigh, tc.high) || *f.RateSource != tc.source {
				t.Fatalf("frontier = %s, want rate %d low %d high %d source %s", r.Frontier, tc.rate, tc.low, tc.high, tc.source)
			}
		})
	}
}

// No admission in the window and no guess: every speed field is 0, so clients
// hide the estimate instead of dividing by zero.
func TestTickRateZero(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	k := testkit.Event(t, client, "evt_zero", testkit.EventOpts{})
	l := lead(t, s, k, "task-a")
	r, err := s.Tick(context.Background(), k, dto.TickArgs{Fence: l.Fence, HistWindow: time.Minute, InviteWindow: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var f api.Frontier
	_ = json.Unmarshal(r.Frontier, &f)
	if f.RatePerMin == nil || *f.RatePerMin != 0 || *f.RateLow != 0 || *f.RateHigh != 0 || *f.RateSource != api.Measured {
		t.Fatalf("frontier = %s", r.Frontier)
	}
}

// Two leaders at once: the newest fence wins; the old leader's scripts are
// refused and change nothing.
func TestLeaderFencing(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_fence", testkit.EventOpts{})
	testkit.Joins(t, client, k, "w_d", 10)

	a := lead(t, s, k, "task-a")
	if !a.Leading || !a.Acquired || a.Fence != 1 {
		t.Fatalf("a = %+v", a)
	}
	if b := lead(t, s, k, "task-b"); b.Leading {
		t.Fatalf("b leads while a holds the lease: %+v", b)
	}
	if again := lead(t, s, k, "task-a"); !again.Leading || again.Acquired || again.Fence != 1 {
		t.Fatalf("renewal = %+v", again)
	}

	// a stalls past its lease (simulated by the key expiring); b takes over.
	client.Do(ctx, client.B().Del().Key(k.Leader).Build())
	b := lead(t, s, k, "task-b")
	if !b.Leading || !b.Acquired || b.Fence != 2 {
		t.Fatalf("b = %+v", b)
	}

	// a wakes up and still believes it leads with fence 1.
	if r, _ := s.Tick(ctx, k, dto.TickArgs{Fence: a.Fence, HistWindow: time.Minute, InviteWindow: time.Second}); r.Status != dto.TickStale {
		t.Fatalf("stale tick = %+v", r)
	}
	if p, _ := s.Purge(ctx, k, a.Fence, 0, 10); !p.Stale {
		t.Fatalf("stale purge = %+v", p)
	}
	if served := testkit.Int(t, client, k.Served); served != 0 {
		t.Fatalf("served = %d after the stale tick, want 0", served)
	}
	if l := lead(t, s, k, "task-a"); l.Leading {
		t.Fatal("a renews b's lease")
	}

	// Resign only removes one's own lease.
	_ = s.Resign(ctx, k, "task-a")
	if l := lead(t, s, k, "task-b"); !l.Leading || l.Acquired {
		t.Fatalf("a's resign removed b's lease: %+v", l)
	}
	_ = s.Resign(ctx, k, "task-b")
	if l := lead(t, s, k, "task-a"); !l.Leading || l.Fence != 3 {
		t.Fatalf("after b resigns a = %+v", l)
	}
}

// Purge: units of a lease that ended more than the grace ago go back, the
// lease is kept as purged, and the place cannot claim again.
func TestPurgeReturnsUnits(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_purge", testkit.EventOpts{Available: 10})
	testkit.Joins(t, client, k, "w_d", 2)
	l := lead(t, s, k, "task-a")
	if r, _ := s.Tick(ctx, k, dto.TickArgs{Fence: l.Fence, HistWindow: time.Minute, InviteWindow: time.Second}); r.Served != 2 {
		t.Fatalf("served = %d", r.Served)
	}

	if st, n := testkit.Claim(t, client, k, "w_d0", 1, 200*time.Millisecond); st != "OK" || n != 3 {
		t.Fatalf("claim = %s %d", st, n)
	}
	if st, _ := testkit.Claim(t, client, k, "w_d1", 2, time.Hour); st != "OK" {
		t.Fatalf("claim = %s", st)
	}
	if u := testkit.Int(t, client, k.Unreserved); u != 4 {
		t.Fatalf("unreserved = %d, want 4", u)
	}

	// Within the grace nothing is purged.
	time.Sleep(300 * time.Millisecond)
	if p, _ := s.Purge(ctx, k, l.Fence, time.Second, 500); p.Purged != 0 {
		t.Fatalf("purged %d inside the grace", p.Purged)
	}
	// After it, only the ended lease is.
	p, err := s.Purge(ctx, k, l.Fence, 50*time.Millisecond, 500)
	if err != nil || p.Purged != 1 {
		t.Fatalf("purge = %+v, %v", p, err)
	}
	if u, r := testkit.Int(t, client, k.Unreserved), testkit.Int(t, client, k.Resv); u != 7 || r != 3 {
		t.Fatalf("unreserved %d resv %d, want 7 and 3", u, r)
	}
	if n, _ := client.Do(ctx, client.B().Zcard().Key(k.Active).Build()).AsInt64(); n != 1 {
		t.Fatalf("active = %d, want 1", n)
	}
	lease, _ := client.Do(ctx, client.B().Hget().Key(k.Leases).Field("s_w_d0_1").Build()).ToString()
	if lease == "" || lease[len(lease)-len("|purged|")-13:len(lease)-13] != "|purged|" {
		t.Fatalf("lease = %q, want status purged", lease)
	}
	if st, _ := testkit.Claim(t, client, k, "w_d0", 1, time.Hour); st != "EXPIRED" {
		t.Fatalf("claim after purge = %s, want EXPIRED", st)
	}
	// A second purge finds nothing.
	if p, _ := s.Purge(ctx, k, l.Fence, 0, 500); p.Purged != 0 {
		t.Fatalf("second purge = %+v", p)
	}
}

// P1 / NFR-05: devices that joined before opening get every number once, in random order.
func TestLotteryNumbersEveryPreopenDevice(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	opens := time.Now().Add(3 * time.Second)
	k := testkit.Event(t, client, "evt_draw", testkit.EventOpts{OpensIn: time.Until(opens)})
	const n = 1000
	testkit.Joins(t, client, k, "w_d", n)
	l := lead(t, s, k, "task-a")

	if r, err := s.Lottery(ctx, k, l.Fence, 300); err != nil || r.Status != dto.LotteryWait || r.Left != n {
		t.Fatalf("before opensAt: %+v %v, want WAIT with %d left", r, err, n)
	}
	time.Sleep(time.Until(opens) + 100*time.Millisecond)
	if r, err := s.Lottery(ctx, k, l.Fence+1, 300); err != nil || r.Status != dto.LotteryStale {
		t.Fatalf("stale fence: %+v %v", r, err)
	}

	var drawn int64
	for {
		r, err := s.Lottery(ctx, k, l.Fence, 300)
		if err != nil || r.Status != dto.LotteryOK {
			t.Fatalf("draw: %+v %v", r, err)
		}
		drawn += r.Drawn
		if r.Left == 0 {
			break
		}
	}
	if drawn != n || testkit.Int(t, client, k.Seq) != n {
		t.Fatalf("drawn %d seq %d, want %d", drawn, testkit.Int(t, client, k.Seq), n)
	}

	seen := make(map[int64]bool, n)
	inJoinOrder := 0
	for i := range n {
		v, err := client.Do(ctx, client.B().Hget().Key(k.Devs).Field("w_d"+strconv.Itoa(i)).Build()).AsInt64()
		if err != nil || v < 1 || v > n || seen[v] {
			t.Fatalf("device %d: seq %d %v (duplicate or out of range)", i, v, err)
		}
		seen[v] = true
		if v == int64(i+1) {
			inJoinOrder++
		}
	}
	if inJoinOrder > n/10 {
		t.Fatalf("%d of %d devices kept their join order; the draw is not random", inJoinOrder, n)
	}

	r := testkit.Tick(t, client, k, testkit.TickArgs{})
	if r[0] != "OK" || r[3] != "open" {
		t.Fatalf("tick after the draw: %v, want open", r)
	}
}

// A device that left the draw list before the draw gets no number.
func TestLotterySkipsDevicesThatLeft(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_draw_left", testkit.EventOpts{OpensIn: 200 * time.Millisecond})
	testkit.Joins(t, client, k, "w_l", 3)
	if err := valkeyx.FCall(ctx, client, "rusuh_leave_v1", []string{k.Cfg, k.Devs, k.Pre}, "w_l1").Error(); err != nil {
		t.Fatal(err)
	}
	l := lead(t, s, k, "task-a")
	time.Sleep(300 * time.Millisecond)

	r, err := s.Lottery(ctx, k, l.Fence, 100)
	if err != nil || r.Drawn != 2 || r.Left != 0 {
		t.Fatalf("draw: %+v %v, want 2 drawn", r, err)
	}
	if v, _ := client.Do(ctx, client.B().Hexists().Key(k.Devs).Field("w_l1").Build()).AsBool(); v {
		t.Fatal("the device that left still has a place")
	}
}
