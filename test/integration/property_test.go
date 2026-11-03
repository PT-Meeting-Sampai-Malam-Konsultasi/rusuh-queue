package integration

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	vk "github.com/valkey-io/valkey-go"
	"pgregory.net/rapid"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

// M4 verify (NFR-04, NFR-02): random joins, ticks, claims, purges and core's
// holds, payments and expiries, with duplicate deliveries, never break the
// stock books. After every step, with no event in flight:
//
//	unreserved + resv == available in core's PostgreSQL (reserved units count as available there)
//	held == units core holds and has not paid or expired
//	unreserved, resv, held >= 0 (without the scripts' clamp to 0 ever firing)
//
// core's PostgreSQL is modelled: a hold needs n <= available (CHECK available >= 0),
// held units leave available, paid units stay sold, expired units come back.

type propLease struct {
	device string
	units  int    // reserved at the claim
	held   int    // units core holds (0 = no booking yet)
	state  string // reserved | held | done (paid or expired)
}

type propEvent struct {
	id, typ, slot string
	units         int
}

type stockMachine struct {
	client    vk.Client
	k         qkeys.Event
	available int64 // core's PostgreSQL available
	heldUnits int64 // core's held units
	devices   []string
	seqs      map[string]int64
	claimed   map[string]bool
	leases    map[string]*propLease
	slots     []string
	events    []propEvent
	n         int
}

func (m *stockMachine) fcall(t *rapid.T, fn string, keys []string, args ...string) vk.ValkeyResult {
	r := valkeyx.FCall(context.Background(), m.client, fn, keys, args...)
	if err := r.Error(); err != nil {
		t.Fatalf("%s: %v", fn, err)
	}
	return r
}

func (m *stockMachine) get(t *rapid.T, key string) int64 {
	n, err := m.client.Do(context.Background(), m.client.B().Get().Key(key).Build()).AsInt64()
	if err != nil && !vk.IsValkeyNil(err) {
		t.Fatalf("get %s: %v", key, err)
	}
	return n
}

func (m *stockMachine) Join(t *rapid.T) {
	m.n++
	dev := "w_p" + strconv.Itoa(m.n)
	vals, _ := m.fcall(t, "rusuh_join_v1", []string{m.k.Cfg, m.k.Seq, m.k.Devs, m.k.Pre, m.k.Accts, m.k.Served, m.k.Hist}, dev, "", "300000").ToArray()
	if st, _ := vals[0].ToString(); st == "OK" {
		seq, _ := vals[1].AsInt64()
		m.devices = append(m.devices, dev)
		m.seqs[dev] = seq
	}
}

func (m *stockMachine) Tick(t *rapid.T) {
	m.fcall(t, "rusuh_tick_v1",
		[]string{m.k.Cfg, m.k.Seq, m.k.Served, m.k.Unreserved, m.k.Resv, m.k.Held, m.k.Active, m.k.Hist, m.k.Inv, m.k.Claimed, m.k.Pre, m.k.Frontier, m.k.Fence},
		"1", m.k.ID, m.k.Tick, "300000", "30000", "0")
}

func (m *stockMachine) Claim(t *rapid.T) {
	if len(m.devices) == 0 {
		t.Skip("no device")
	}
	dev := rapid.SampledFrom(m.devices).Draw(t, "device")
	if m.claimed[dev] {
		t.Skip("already claimed")
	}
	// A 0 ms order window ends the lease at once, so the next purge takes it.
	window := rapid.SampledFrom([]string{"0", "300000"}).Draw(t, "orderWindowMs")
	slot := fmt.Sprintf("s_%s", dev)
	vals, _ := m.fcall(t, "rusuh_claim_v1",
		[]string{m.k.Cfg, m.k.Served, m.k.Devs, m.k.Unreserved, m.k.Resv, m.k.Active, m.k.Leases, m.k.Hist, m.k.Claimed, m.k.Accts},
		dev, strconv.FormatInt(m.seqs[dev], 10), slot, window, "300000").ToArray()
	if st, _ := vals[0].ToString(); st == "OK" {
		units, _ := vals[2].AsInt64()
		m.claimed[dev] = true
		m.leases[slot] = &propLease{device: dev, units: int(units), state: "reserved"}
		m.slots = append(m.slots, slot)
	}
}

func (m *stockMachine) Purge(t *rapid.T) {
	vals, _ := m.fcall(t, "rusuh_purge_v1", []string{m.k.Cfg, m.k.Unreserved, m.k.Resv, m.k.Active, m.k.Leases, m.k.Fence}, "1", "0", "500").ToArray()
	if n, _ := vals[1].AsInt64(); n > 0 {
		purges.Add(n)
	}
}

// purges counts leases the purge ended across the run, to show the path is exercised.
var purges atomic.Int64

// leaseStatus reads queue's view of a lease (reserved | held | purged | "" when dropped).
func (m *stockMachine) leaseStatus(t *rapid.T, slot string) string {
	raw, err := m.client.Do(context.Background(), m.client.B().Hget().Key(m.k.Leases).Field(slot).Build()).ToString()
	if vk.IsValkeyNil(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("lease %s: %v", slot, err)
	}
	var d, st string
	var seq, units, ends int64
	if _, err := fmt.Sscanf(stringsReplacePipes(raw), "%s %d %d %s %d", &d, &seq, &units, &st, &ends); err != nil {
		t.Fatalf("lease %q: %v", raw, err)
	}
	return st
}

func stringsReplacePipes(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] == '|' {
			b[i] = ' '
		}
	}
	return string(b)
}

func (m *stockMachine) release(t *rapid.T, ev propEvent, deadlineMs string) string {
	st, err := m.fcall(t, "rusuh_release_v1",
		[]string{m.k.Cfg, m.k.Unreserved, m.k.Resv, m.k.Held, m.k.Active, m.k.Leases, m.k.Done},
		ev.id, ev.typ, ev.slot, strconv.Itoa(ev.units), deadlineMs, "120000").ToString()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func (m *stockMachine) pick(t *rapid.T, want string) (string, *propLease) {
	var cands []string
	for _, s := range m.slots {
		if m.leases[s].state == want {
			cands = append(cands, s)
		}
	}
	if len(cands) == 0 {
		t.Skip("no " + want + " lease")
	}
	s := rapid.SampledFrom(cands).Draw(t, "slot")
	return s, m.leases[s]
}

// Hold: core books n of the reserved units. core refuses a hold once the
// admission token's exp (the lease end) has passed, and its event reaches
// queue before the purge grace, so a lease queue already purged is not held.
func (m *stockMachine) Hold(t *rapid.T) {
	slot, l := m.pick(t, "reserved")
	if m.leaseStatus(t, slot) != "reserved" {
		t.Skip("lease ended before core's hold")
	}
	n := rapid.IntRange(1, l.units).Draw(t, "units")
	if int64(n) > m.available {
		t.Skip("core's CHECK (available >= 0) refuses the hold")
	}
	m.available -= int64(n)
	m.heldUnits += int64(n)
	l.held, l.state = n, "held"
	ev := propEvent{id: "e_h_" + slot, typ: "held", slot: slot, units: n}
	m.events = append(m.events, ev)
	if st := m.release(t, ev, "9999999999999"); st != "APPLIED" {
		t.Fatalf("held: %s", st)
	}
}

func (m *stockMachine) Pay(t *rapid.T) {
	slot, l := m.pick(t, "held")
	m.heldUnits -= int64(l.held)
	l.state = "done"
	ev := propEvent{id: "e_p_" + slot, typ: "paid", slot: slot, units: l.held}
	m.events = append(m.events, ev)
	if st := m.release(t, ev, "0"); st != "APPLIED" {
		t.Fatalf("paid: %s", st)
	}
}

func (m *stockMachine) Expire(t *rapid.T) {
	slot, l := m.pick(t, "held")
	m.available += int64(l.held)
	m.heldUnits -= int64(l.held)
	l.state = "done"
	ev := propEvent{id: "e_x_" + slot, typ: "expired", slot: slot, units: l.held}
	m.events = append(m.events, ev)
	if st := m.release(t, ev, "0"); st != "APPLIED" {
		t.Fatalf("expired: %s", st)
	}
}

// Redeliver: at-least-once delivery repeats an event; it must change nothing.
func (m *stockMachine) Redeliver(t *rapid.T) {
	if len(m.events) == 0 {
		t.Skip("no event yet")
	}
	ev := rapid.SampledFrom(m.events).Draw(t, "event")
	if st := m.release(t, ev, "9999999999999"); st != "DUPLICATE" {
		t.Fatalf("redelivered %s: %s, want DUPLICATE", ev.id, st)
	}
}

func (m *stockMachine) Check(t *rapid.T) {
	unres, resv, held := m.get(t, m.k.Unreserved), m.get(t, m.k.Resv), m.get(t, m.k.Held)
	if unres < 0 || resv < 0 || held < 0 {
		t.Fatalf("negative counter: unreserved %d, resv %d, held %d", unres, resv, held)
	}
	if held != m.heldUnits {
		t.Fatalf("held = %d, core holds %d", held, m.heldUnits)
	}
	if unres+resv != m.available {
		t.Fatalf("unreserved %d + resv %d = %d, core's available = %d", unres, resv, unres+resv, m.available)
	}
}

func TestStockBooksProperty(t *testing.T) {
	client := testkit.Valkey(t)
	run := 0
	rapid.Check(t, func(rt *rapid.T) {
		run++
		stock := rapid.Int64Range(1, 12).Draw(rt, "stock")
		k := testkit.Event(t, client, "evt_prop_"+strconv.Itoa(run), testkit.EventOpts{
			Available: stock,
			MaxSlots:  rapid.Int64Range(1, 4).Draw(rt, "maxSlots"),
			RateCap:   rapid.Int64Range(1, 3).Draw(rt, "rateCap"),
		})
		if err := client.Do(context.Background(), client.B().Set().Key(k.Fence).Value("1").Build()).Error(); err != nil {
			rt.Fatal(err)
		}
		m := &stockMachine{client: client, k: k, available: stock,
			seqs: map[string]int64{}, claimed: map[string]bool{}, leases: map[string]*propLease{}}
		rt.Repeat(rapid.StateMachineActions(m))
	})
	t.Logf("leases purged across all runs: %d", purges.Load())
	if purges.Load() == 0 {
		t.Fatal("the purge path was never exercised")
	}
}
