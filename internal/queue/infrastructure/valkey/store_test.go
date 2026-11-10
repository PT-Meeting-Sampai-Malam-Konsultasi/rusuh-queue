package valkey_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

// M2 verify: 10,000 concurrent joins from one device get one seq.
func TestConcurrentJoinsOneDevice(t *testing.T) {
	client := testkit.Valkey(t)
	k := testkit.Event(t, client, "evt_one", testkit.EventOpts{})
	s := valkey.New(client)

	seqs := make(chan int64, 10000)
	var wg sync.WaitGroup
	for range 10000 {
		wg.Go(func() {
			r, err := s.Join(context.Background(), k, "w_same", "", dto.Windows{Miss: miss})
			if err != nil || r.Status != dto.JoinOK {
				t.Errorf("join = %+v, %v", r, err)
				return
			}
			seqs <- r.Seq
		})
	}
	wg.Wait()
	close(seqs)
	for seq := range seqs {
		if seq != 1 {
			t.Fatalf("seq = %d, want 1 for every call", seq)
		}
	}
	if n := counter(t, client, k.Seq); n != 1 {
		t.Fatalf("q:seq = %d, want 1", n)
	}
}

// M2 verify: 10,000 devices get 1…10,000 with no gaps or repeats.
func TestConcurrentJoinsManyDevices(t *testing.T) {
	client := testkit.Valkey(t)
	k := testkit.Event(t, client, "evt_many", testkit.EventOpts{})
	s := valkey.New(client)

	const n = 10000
	got := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			r, err := s.Join(context.Background(), k, fmt.Sprintf("w_dev%05d", i), "", dto.Windows{Miss: miss})
			if err != nil || r.Status != dto.JoinOK {
				t.Errorf("join %d = %+v, %v", i, r, err)
				return
			}
			got[i] = r.Seq
		})
	}
	wg.Wait()
	sort.Slice(got, func(a, b int) bool { return got[a] < got[b] })
	for i, seq := range got {
		if seq != int64(i+1) {
			t.Fatalf("sorted seq[%d] = %d, want %d (gap or repeat)", i, seq, i+1)
		}
	}
}

func TestJoinRules(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_rules", testkit.EventOpts{})

	t.Run("account on a second device is refused", func(t *testing.T) {
		if r, _ := s.Join(ctx, k, "w_phone", "user1", dto.Windows{Miss: miss}); r.Status != dto.JoinOK || !r.Attached {
			t.Fatalf("first = %+v", r)
		}
		if r, _ := s.Join(ctx, k, "w_laptop", "user1", dto.Windows{Miss: miss}); r.Status != dto.JoinAccountElsewhere {
			t.Fatalf("second device = %+v, want ACCOUNT_ELSEWHERE", r)
		}
	})

	t.Run("guest attaches an account later without moving", func(t *testing.T) {
		guest, _ := s.Join(ctx, k, "w_guest", "", dto.Windows{Miss: miss})
		if guest.Attached {
			t.Fatal("guest must not be attached")
		}
		logged, _ := s.Join(ctx, k, "w_guest", "user2", dto.Windows{Miss: miss})
		if logged.Seq != guest.Seq || !logged.Attached {
			t.Fatalf("after login = %+v, want same seq %d and attached", logged, guest.Seq)
		}
		again, _ := s.Join(ctx, k, "w_guest", "", dto.Windows{Miss: miss})
		if !again.Attached {
			t.Fatal("a guest call on an attached place must report accountAttached")
		}
	})

	t.Run("another account on an attached device is refused", func(t *testing.T) {
		if r, _ := s.Join(ctx, k, "w_guest", "user3", dto.Windows{Miss: miss}); r.Status != dto.JoinDeviceOtherAccount {
			t.Fatalf("got %+v, want DEVICE_OTHER_ACCOUNT", r)
		}
	})

	t.Run("a used place rejoins at the back", func(t *testing.T) {
		first, _ := s.Join(ctx, k, "w_used", "", dto.Windows{Miss: miss})
		hset(t, client, k.Devs, "w_used", fmt.Sprintf("x:%d", first.Seq))
		again, _ := s.Join(ctx, k, "w_used", "", dto.Windows{Miss: miss})
		if again.Status != dto.JoinOK || again.Seq <= first.Seq {
			t.Fatalf("rejoin = %+v, want a new seq after %d", again, first.Seq)
		}
	})

	t.Run("the account moves once its old place is used", func(t *testing.T) {
		phone, _ := s.Join(ctx, k, "w_phone", "user1", dto.Windows{Miss: miss})
		hset(t, client, k.Devs, "w_phone", fmt.Sprintf("x:%d", phone.Seq))
		if r, _ := s.Join(ctx, k, "w_laptop", "user1", dto.Windows{Miss: miss}); r.Status != dto.JoinOK {
			t.Fatalf("laptop after phone's place ended = %+v", r)
		}
	})

	t.Run("keys created by join expire with the event", func(t *testing.T) {
		for _, key := range []string{k.Devs, k.Accts} {
			ttl, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
			if err != nil || ttl < int64(time.Hour/time.Millisecond) {
				t.Fatalf("%s ttl = %d, %v; want the event's expiry", key, ttl, err)
			}
		}
	})
}

func TestJoinStates(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()

	pre := testkit.Event(t, client, "evt_pre", testkit.EventOpts{OpensIn: time.Hour})
	r, _ := s.Join(ctx, pre, "w_early", "", dto.Windows{Miss: miss})
	if r.Status != dto.JoinPre {
		t.Fatalf("preopen join = %+v, want PRE", r)
	}
	if again, _ := s.Join(ctx, pre, "w_early", "", dto.Windows{Miss: miss}); again.Status != dto.JoinPre {
		t.Fatalf("second preopen join = %+v", again)
	}
	members, _ := client.Do(ctx, client.B().Smembers().Key(pre.Pre).Build()).AsStrSlice()
	if len(members) != 1 || members[0] != "w_early" {
		t.Fatalf("pre set = %v", members)
	}

	closed := testkit.Event(t, client, "evt_closed", testkit.EventOpts{Cancelled: true})
	if r, _ := s.Join(ctx, closed, "w_x", "", dto.Windows{Miss: miss}); r.Status != dto.JoinClosed {
		t.Fatalf("closed join = %+v", r)
	}
	if r, _ := s.Join(ctx, qkeys.For("evt_missing"), "w_x", "", dto.Windows{Miss: miss}); r.Status != dto.JoinNoEvent {
		t.Fatalf("missing event = %+v", r)
	}
}

func TestPlace(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_place", testkit.EventOpts{})

	r, _ := s.Join(ctx, k, "w_a", "user9", dto.Windows{Miss: miss})
	v, err := s.Place(ctx, k, "w_a", dto.Windows{Miss: miss})
	if err != nil || !v.Found || v.State != "open" || v.Value != fmt.Sprint(r.Seq) || !v.Attached || v.EndsAt.IsZero() {
		t.Fatalf("place = %+v, %v", v, err)
	}
	if v, _ := s.Place(ctx, k, "w_nobody", dto.Windows{Miss: miss}); !v.Found || v.Value != "" {
		t.Fatalf("no place = %+v", v)
	}
	if v, _ := s.Place(ctx, qkeys.For("evt_missing"), "w_a", dto.Windows{Miss: miss}); v.Found {
		t.Fatalf("missing event = %+v", v)
	}
}

func counter(t *testing.T, client vk.Client, key string) int64 {
	t.Helper()
	n, err := client.Do(context.Background(), client.B().Get().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	return n
}

func hset(t *testing.T, client vk.Client, key, field, value string) {
	t.Helper()
	if err := client.Do(context.Background(), client.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build()).Error(); err != nil {
		t.Fatalf("hset: %v", err)
	}
}

// miss is the production turn window (MISS_WINDOW, "60 detik untuk masuk").
const miss = time.Minute

func claimIn(device string, seq int64, slot string) dto.ClaimInput {
	return dto.ClaimInput{DeviceID: device, TokenSeq: seq, SlotID: slot, OrderWindow: 5 * time.Minute, Windows: dto.Windows{Miss: 5 * time.Minute}}
}

// M3 verify: 1,000 concurrent claims for 2 units → exactly 2 units reserved,
// everyone else NO_STOCK; and with 10 slots, exactly 10 leases, the rest FULL.
func TestClaimRace(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)

	for _, tc := range []struct {
		name                  string
		opts                  testkit.EventOpts
		wantOK, wantUnits     int64
		wantRefusal           dto.ClaimStatus
		wantUnreserved, wantR int64
	}{
		{name: "2 units", opts: testkit.EventOpts{Available: 2}, wantOK: 1, wantUnits: 2, wantRefusal: dto.ClaimNoStock, wantUnreserved: 0, wantR: 2},
		{name: "10 slots", opts: testkit.EventOpts{Available: 10000, MaxSlots: 10}, wantOK: 10, wantUnits: 30, wantRefusal: dto.ClaimFull, wantUnreserved: 9970, wantR: 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := testkit.Event(t, client, fmt.Sprintf("evt_race%d", tc.wantOK), tc.opts)
			const n = 1000
			testkit.Joins(t, client, k, "w_r", n)
			hset(t, client, k.Cfg, "state", "open")
			if err := client.Do(context.Background(), client.B().Set().Key(k.Served).Value(fmt.Sprint(n)).Build()).Error(); err != nil {
				t.Fatal(err)
			}

			var ok, units, refused atomic.Int64
			var wg sync.WaitGroup
			for i := range n {
				wg.Go(func() {
					r, err := s.Claim(context.Background(), k, claimIn(fmt.Sprintf("w_r%d", i), int64(i+1), fmt.Sprintf("s_%d", i)))
					switch {
					case err != nil:
						t.Errorf("claim: %v", err)
					case r.Status == dto.ClaimOK:
						ok.Add(1)
						units.Add(int64(r.Units))
					case r.Status == tc.wantRefusal:
						refused.Add(1)
					default:
						t.Errorf("claim status = %s", r.Status)
					}
				})
			}
			wg.Wait()
			if ok.Load() != tc.wantOK || units.Load() != tc.wantUnits || refused.Load() != n-tc.wantOK {
				t.Fatalf("ok %d units %d refused %d", ok.Load(), units.Load(), refused.Load())
			}
			if u, r := testkit.Int(t, client, k.Unreserved), testkit.Int(t, client, k.Resv); u != tc.wantUnreserved || r != tc.wantR {
				t.Fatalf("unreserved %d resv %d, want %d and %d", u, r, tc.wantUnreserved, tc.wantR)
			}
		})
	}
}

// Claim checks run in the documented order; a retried claim returns the same lease.
func TestClaimRules(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_claim", testkit.EventOpts{Available: 10})
	testkit.Joins(t, client, k, "w_c", 3)

	if r, _ := s.Claim(ctx, k, claimIn("w_c0", 1, "s_a")); r.Status != dto.ClaimNotYet {
		t.Fatalf("before served = %s, want NOT_YET", r.Status)
	}
	testkit.Tick(t, client, k, testkit.TickArgs{})

	if r, _ := s.Claim(ctx, k, claimIn("w_c0", 2, "s_a")); r.Status != dto.ClaimExpired {
		t.Fatalf("token seq of another place = %s, want EXPIRED", r.Status)
	}
	if r, _ := s.Claim(ctx, k, claimIn("w_nobody", 1, "s_a")); r.Status != dto.ClaimExpired {
		t.Fatalf("device without a place = %s, want EXPIRED", r.Status)
	}

	first, err := s.Claim(ctx, k, claimIn("w_c0", 1, "s_a"))
	if err != nil || first.Status != dto.ClaimOK || first.SlotID != "s_a" || first.Units != 3 || first.Attached {
		t.Fatalf("claim = %+v, %v", first, err)
	}
	if d := first.EndsAt.Sub(first.ServerNow); d != 5*time.Minute {
		t.Fatalf("lease = %s, want the 5-minute order window", d)
	}
	again, _ := s.Claim(ctx, k, claimIn("w_c0", 1, "s_b"))
	if again.Status != dto.ClaimOK || again.SlotID != "s_a" || !again.EndsAt.Equal(first.EndsAt) {
		t.Fatalf("retry = %+v, want the same lease", again)
	}
	if u := testkit.Int(t, client, k.Unreserved); u != 7 {
		t.Fatalf("unreserved = %d after a retried claim, want 7", u)
	}

	short := claimIn("w_c1", 2, "s_c")
	short.OrderWindow = 100 * time.Millisecond
	if r, _ := s.Claim(ctx, k, short); r.Status != dto.ClaimOK {
		t.Fatalf("claim = %s", r.Status)
	}
	time.Sleep(150 * time.Millisecond)
	if r, _ := s.Claim(ctx, k, claimIn("w_c1", 2, "s_d")); r.Status != dto.ClaimExpired {
		t.Fatalf("after the lease ended = %s, want EXPIRED", r.Status)
	}

	hset(t, client, k.Cfg, "state", "closed")
	if r, _ := s.Claim(ctx, k, claimIn("w_c2", 3, "s_e")); r.Status != dto.ClaimClosed {
		t.Fatalf("closed = %s", r.Status)
	}
}

// MISSED: the turn was reached longer ago than the miss window, seen either
// in the history or, once the history was trimmed, below its floor.
func TestClaimMissed(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_miss", testkit.EventOpts{Available: 100, RateCap: 1})
	testkit.Joins(t, client, k, "w_m", 4)

	testkit.Tick(t, client, k, testkit.TickArgs{}) // served 1
	time.Sleep(300 * time.Millisecond)
	in := claimIn("w_m0", 1, "s_a")
	in.Windows.Miss = 200 * time.Millisecond
	if r, _ := s.Claim(ctx, k, in); r.Status != dto.ClaimMissed {
		t.Fatalf("claim 300 ms after the turn with a 200 ms window = %s, want MISSED", r.Status)
	}

	testkit.Tick(t, client, k, testkit.TickArgs{}) // served 2
	testkit.Tick(t, client, k, testkit.TickArgs{}) // served 3
	time.Sleep(300 * time.Millisecond)
	// The trim drops every old entry but the newest (served 3), so the floor is 2.
	testkit.Tick(t, client, k, testkit.TickArgs{HistWindow: 200 * time.Millisecond}) // served 4
	if r, _ := s.Claim(ctx, k, claimIn("w_m1", 2, "s_b")); r.Status != dto.ClaimMissed {
		t.Fatalf("seq below the trimmed history = %s, want MISSED", r.Status)
	}
	if r, _ := s.Claim(ctx, k, claimIn("w_m3", 4, "s_c")); r.Status != dto.ClaimOK {
		t.Fatalf("fresh turn = %s, want OK", r.Status)
	}
}

// HiFi "Giliran terlewat" → "Antre lagi": a turn not claimed within the miss
// window is over; joining again gives a new number at the back, and the
// account may queue on another device.
func TestMissedPlaceRejoinsAtBack(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_rejoin", testkit.EventOpts{})
	short := 200 * time.Millisecond

	first, _ := s.Join(ctx, k, "w_phone", "user1", dto.Windows{Miss: short})
	if first.Seq != 1 || !first.TurnEnds.IsZero() {
		t.Fatalf("join = %+v, want seq 1 and no turn yet", first)
	}
	testkit.Tick(t, client, k, testkit.TickArgs{})
	reached, _ := s.Join(ctx, k, "w_phone", "user1", dto.Windows{Miss: short})
	if reached.Seq != 1 || reached.Served != 1 || reached.TurnEnds.IsZero() {
		t.Fatalf("after the turn = %+v, want seq 1 with turnEnds", reached)
	}
	if d := reached.TurnEnds.Sub(reached.ServerNow); d <= 0 || d > short {
		t.Fatalf("turn ends in %s, want within the %s window", d, short)
	}

	time.Sleep(300 * time.Millisecond)
	v, _ := s.Place(ctx, k, "w_phone", dto.Windows{Miss: short})
	if !v.Missed || v.Value != "1" {
		t.Fatalf("place = %+v, want missed", v)
	}
	if r, _ := s.Join(ctx, k, "w_laptop", "user1", dto.Windows{Miss: short}); r.Status != dto.JoinOK || r.Seq != 2 {
		t.Fatalf("account on another device after a missed turn = %+v, want a new place", r)
	}
	if r, _ := s.Join(ctx, k, "w_other", "", dto.Windows{Miss: short}); r.Seq != 3 {
		t.Fatalf("seq = %d", r.Seq)
	}
	// With the production window the same place is still live.
	if v, _ := s.Place(ctx, k, "w_phone", dto.Windows{Miss: miss}); v.Missed {
		t.Fatal("missed within the production 60 s window")
	}
}

// HiFi "Keluar dari antrian": leave ends the place (open) or the draw entry (preopen).
func TestLeave(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()

	k := testkit.Event(t, client, "evt_leave", testkit.EventOpts{})
	_, _ = s.Join(ctx, k, "w_a", "user1", dto.Windows{Miss: miss})
	for range 2 { // idempotent
		if found, err := s.Leave(ctx, k, "w_a"); err != nil || !found {
			t.Fatalf("leave = %v, %v", found, err)
		}
	}
	if v, _ := s.Place(ctx, k, "w_a", dto.Windows{Miss: miss}); v.Value != "x:1" {
		t.Fatalf("place after leave = %q", v.Value)
	}
	if r, _ := s.Join(ctx, k, "w_b", "user1", dto.Windows{Miss: miss}); r.Status != dto.JoinOK {
		t.Fatalf("account on another device after leaving = %s", r.Status)
	}
	if r, _ := s.Join(ctx, k, "w_a", "", dto.Windows{Miss: miss}); r.Seq != 3 {
		t.Fatalf("rejoin seq = %d, want 3 (back of the queue)", r.Seq)
	}

	pre := testkit.Event(t, client, "evt_leave_pre", testkit.EventOpts{OpensIn: time.Hour})
	_, _ = s.Join(ctx, pre, "w_p", "", dto.Windows{Miss: miss})
	_, _ = s.Leave(ctx, pre, "w_p")
	if n, _ := client.Do(ctx, client.B().Scard().Key(pre.Pre).Build()).AsInt64(); n != 0 {
		t.Fatalf("draw list = %d after leaving, want 0", n)
	}
	if found, _ := s.Leave(ctx, qkeys.For("evt_missing"), "w_a"); found {
		t.Fatal("leave on an unknown event reports found")
	}
}
