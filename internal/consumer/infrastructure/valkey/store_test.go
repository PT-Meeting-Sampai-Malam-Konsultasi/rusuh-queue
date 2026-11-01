package valkey_test

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

type counters struct{ unreserved, resv, held int64 }

func read(t *testing.T, client vk.Client, k qkeys.Event) counters {
	t.Helper()
	return counters{testkit.Int(t, client, k.Unreserved), testkit.Int(t, client, k.Resv), testkit.Int(t, client, k.Held)}
}

type releaser struct {
	t     *testing.T
	s     outbound.IConsumerStore
	k     qkeys.Event
	n     int
	grace time.Duration
}

func (r *releaser) apply(typ, slot string, units int) dto.ReleaseStatus {
	r.t.Helper()
	r.n++
	return r.applyID("ev"+strconv.Itoa(r.n), typ, slot, units)
}

func (r *releaser) applyID(id, typ, slot string, units int) dto.ReleaseStatus {
	r.t.Helper()
	a := dto.ReleaseArgs{ID: id, EventID: r.k.ID, Type: typ, SlotID: slot, Units: units, HoldGrace: r.grace}
	if typ == "held" {
		a.PaymentDeadline = time.Now().Add(10 * time.Minute)
	}
	st, err := r.s.Release(context.Background(), r.k, a)
	if err != nil {
		r.t.Fatal(err)
	}
	return st
}

func purge(t *testing.T, client vk.Client, k qkeys.Event) {
	t.Helper()
	if err := client.Do(context.Background(), client.B().Set().Key(k.Fence).Value("1").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := valkeyx.FCall(context.Background(), client, "rusuh_purge_v1",
		[]string{k.Cfg, k.Unreserved, k.Resv, k.Active, k.Leases, k.Fence}, "1", "0", "500").Error(); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseRules(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client, entity.DefaultConfig())

	for _, tc := range []struct {
		name  string
		steps func(r *releaser, slot string)
		want  counters
	}{
		{"held 2 of 3: one unit back", func(r *releaser, slot string) { r.apply("held", slot, 2) }, counters{8, 0, 2}},
		{"held then paid: sold", func(r *releaser, slot string) {
			r.apply("held", slot, 2)
			r.apply("paid", slot, 2)
		}, counters{8, 0, 0}},
		{"held then expired: units back", func(r *releaser, slot string) {
			r.apply("held", slot, 2)
			r.apply("expired", slot, 2)
		}, counters{10, 0, 0}},
		{"paid before held: same as held then paid", func(r *releaser, slot string) {
			r.apply("paid", slot, 2)
			if st := r.apply("held", slot, 2); st != dto.ReleaseUnknownSlot {
				r.t.Fatalf("late held = %s, want UNKNOWN_SLOT", st)
			}
		}, counters{8, 0, 0}},
		{"held after the purge takes its units back", func(r *releaser, slot string) {
			purge(r.t, client, r.k)
			r.apply("held", slot, 2)
		}, counters{8, 0, 2}},
		{"held for more units than reserved is capped", func(r *releaser, slot string) { r.apply("held", slot, 3) }, counters{7, 0, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := testkit.Event(t, client, "evt_rel", testkit.EventOpts{Available: 10})
			slot, units := testkit.Admit(t, client, k, "w_rel", 50*time.Millisecond)
			if units != 3 {
				t.Fatalf("reserved %d, want 3", units)
			}
			if c := read(t, client, k); c != (counters{7, 3, 0}) {
				t.Fatalf("after claim %+v", c)
			}
			if tc.name == "held after the purge takes its units back" {
				time.Sleep(100 * time.Millisecond)
			}
			tc.steps(&releaser{t: t, s: s, k: k, grace: 2 * time.Minute}, slot)
			if c := read(t, client, k); c != tc.want {
				t.Fatalf("counters %+v, want %+v", c, tc.want)
			}
			if err := client.Do(context.Background(), client.B().Flushall().Build()).Error(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client, entity.DefaultConfig())
	k := testkit.Event(t, client, "evt_dup", testkit.EventOpts{Available: 10})
	slot, _ := testkit.Admit(t, client, k, "w_dup", time.Minute)
	r := &releaser{t: t, s: s, k: k, grace: 2 * time.Minute}

	for i := range 5 {
		st := r.applyID("same-id", "held", slot, 2)
		if (i == 0) != (st == dto.ReleaseApplied) || (i > 0 && st != dto.ReleaseDuplicate) {
			t.Fatalf("delivery %d: %s", i+1, st)
		}
	}
	if c := read(t, client, k); c != (counters{8, 0, 2}) {
		t.Fatalf("counters %+v after 5 deliveries, want one change", c)
	}
	if st := r.apply("held", "s_nobody", 1); st != dto.ReleaseUnknownSlot {
		t.Fatalf("unknown slot: %s", st)
	}
	if st, _ := s.Release(context.Background(), qkeys.For("evt_none"), dto.ReleaseArgs{ID: "x", Type: "paid", SlotID: slot, Units: 1}); st != dto.ReleaseNoEvent {
		t.Fatalf("unknown event: %s", st)
	}
}

// A held lease keeps its slot past the order window: the purge leaves it alone.
func TestPurgeLeavesHeldLeases(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client, entity.DefaultConfig())
	k := testkit.Event(t, client, "evt_keep", testkit.EventOpts{Available: 10})
	slot, _ := testkit.Admit(t, client, k, "w_keep", 50*time.Millisecond)
	(&releaser{t: t, s: s, k: k, grace: 2 * time.Minute}).apply("held", slot, 2)
	time.Sleep(100 * time.Millisecond)
	purge(t, client, k)
	if c := read(t, client, k); c != (counters{8, 0, 2}) {
		t.Fatalf("counters %+v after purge, want the hold kept", c)
	}
}

// Whatever the order of claims, holds, payments, expiries and purges, no counter
// goes negative and every unit is accounted for once nothing is in flight:
// unreserved + resv + held + sold == stock.
func TestReleaseConservesUnits(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client, entity.DefaultConfig())
	const stock = 40
	for run := range 5 {
		rng := rand.New(rand.NewPCG(uint64(run), 7))
		k := testkit.Event(t, client, "evt_prop"+strconv.Itoa(run), testkit.EventOpts{Available: stock})
		r := &releaser{t: t, s: s, k: k, grace: time.Minute}
		var sold int64
		type lease struct {
			slot     string
			units    int
			held     bool
			finished bool
		}
		var leases []*lease
		for i := range 60 {
			if c := read(t, client, k); c.unreserved > 0 && rng.IntN(3) == 0 {
				slot, units := testkit.Admit(t, client, k, "w_p"+strconv.Itoa(run)+"_"+strconv.Itoa(i), 30*time.Millisecond)
				leases = append(leases, &lease{slot: slot, units: int(units)})
				continue
			}
			if len(leases) == 0 {
				continue
			}
			l := leases[rng.IntN(len(leases))]
			if l.finished {
				continue
			}
			n := 1 + rng.IntN(l.units)
			switch rng.IntN(4) {
			case 0:
				if !l.held && r.apply("held", l.slot, n) == dto.ReleaseApplied {
					l.held, l.units = true, n
				}
			case 1:
				if r.apply("paid", l.slot, n) == dto.ReleaseApplied {
					if !l.held {
						l.units = n
					}
					sold += int64(l.units)
					l.finished = true
				}
			case 2:
				if r.apply("expired", l.slot, n) == dto.ReleaseApplied {
					l.finished = true
				}
			case 3:
				time.Sleep(40 * time.Millisecond)
				purge(t, client, k)
			}
			c := read(t, client, k)
			if c.unreserved < 0 || c.resv < 0 || c.held < 0 {
				t.Fatalf("run %d step %d: negative counter %+v", run, i, c)
			}
		}
		for _, l := range leases {
			if !l.finished {
				r.apply("expired", l.slot, l.units)
			}
		}
		time.Sleep(40 * time.Millisecond)
		purge(t, client, k)
		c := read(t, client, k)
		if got := c.unreserved + c.resv + c.held + sold; got != stock || c.resv != 0 || c.held != 0 {
			t.Fatalf("run %d: %+v sold %d: total %d, want %d with nothing reserved or held", run, c, sold, got, stock)
		}
	}
}
