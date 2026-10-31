package testkit

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/coredocs"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

// EventOpts describe a test sale; zero values give an open sale of 100 units,
// 2000 slots and a rate cap of 50 per tick.
type EventOpts struct {
	Available int64
	OpensIn   time.Duration // > 0 makes the event preopen
	Cancelled bool
	MaxSlots  int64
	RateCap   int64
}

// Event sets up an event's queue keys with rusuh_bootstrap_v1, as the controller does.
func Event(t *testing.T, client valkey.Client, id string, o EventOpts) qkeys.Event {
	t.Helper()
	if o.Available == 0 && !o.Cancelled {
		o.Available = 100
	}
	if o.MaxSlots == 0 {
		o.MaxSlots = 2000
	}
	if o.RateCap == 0 {
		o.RateCap = 50
	}
	now := time.Now()
	opens := now.Add(o.OpensIn)
	if o.OpensIn == 0 {
		opens = now.Add(-time.Minute)
	}
	ends := now.Add(2 * time.Hour)
	ms := func(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }
	cancelled := "0"
	if o.Cancelled {
		cancelled = "1"
	}

	k := qkeys.For(id)
	err := valkeyx.FCall(context.Background(), client, "rusuh_bootstrap_v1",
		[]string{k.Cfg, k.Seq, k.Served, k.Unreserved, k.Resv, k.Frontier},
		ms(now), ms(opens), ms(ends), ms(ends.Add(24*time.Hour)), cancelled,
		strconv.FormatInt(o.Available, 10), strconv.FormatInt(o.MaxSlots, 10), strconv.FormatInt(o.RateCap, 10), "2000", `{"event":"`+id+`","state":"open","served":0,"ts":0}`,
	).Error()
	if err != nil {
		t.Fatalf("bootstrap %s: %v", id, err)
	}
	return k
}

// CoreSale writes the documents core publishes for a sale (D-1, D-2) and lists the event.
func CoreSale(t *testing.T, client valkey.Client, sale coredocs.Sale, stock coredocs.Stock) {
	t.Helper()
	ctx := context.Background()
	k := qkeys.For(sale.EventID)
	sb, _ := json.Marshal(sale)
	tb, _ := json.Marshal(stock)
	b := client.B()
	for _, r := range client.DoMulti(ctx,
		b.Set().Key(k.CoreSale).Value(string(sb)).Build(),
		b.Set().Key(k.CoreStock).Value(string(tb)).Build(),
		b.Sadd().Key(qkeys.CoreEvents).Member(sale.EventID).Build(),
	) {
		if err := r.Error(); err != nil {
			t.Fatalf("write core docs: %v", err)
		}
	}
}

// Publish sends a sharded pub/sub nudge, as core does after changing a document.
func Publish(t *testing.T, client valkey.Client, channel string) {
	t.Helper()
	if err := client.Do(context.Background(), client.B().Spublish().Channel(channel).Message("1").Build()).Error(); err != nil {
		t.Fatalf("spublish %s: %v", channel, err)
	}
}

// TickArgs default to the production windows when zero.
type TickArgs struct {
	HistWindow     time.Duration // 5 min
	InviteWindow   time.Duration // 30 s
	PriorOrderTime time.Duration // 0: no warm-up guess
}

// Tick runs one rusuh_tick_v1 as the leader would, taking fence 1 if no
// leader ever ran. It returns the reply as strings.
func Tick(t *testing.T, client valkey.Client, k qkeys.Event, a TickArgs) []string {
	t.Helper()
	ctx := context.Background()
	if a.HistWindow == 0 {
		a.HistWindow = 5 * time.Minute
	}
	if a.InviteWindow == 0 {
		a.InviteWindow = 30 * time.Second
	}
	if err := client.Do(ctx, client.B().Set().Key(k.Fence).Value("1").Nx().Build()).Error(); err != nil && !valkey.IsValkeyNil(err) {
		t.Fatalf("fence: %v", err)
	}
	fence, _ := client.Do(ctx, client.B().Get().Key(k.Fence).Build()).ToString()
	vals, err := valkeyx.FCall(ctx, client, "rusuh_tick_v1",
		[]string{k.Cfg, k.Seq, k.Served, k.Unreserved, k.Resv, k.Held, k.Active, k.Hist, k.Inv, k.Claimed, k.Pre, k.Frontier, k.Fence},
		fence, k.ID, k.Tick, strconv.FormatInt(a.HistWindow.Milliseconds(), 10), strconv.FormatInt(a.InviteWindow.Milliseconds(), 10),
		strconv.FormatInt(a.PriorOrderTime.Milliseconds(), 10),
	).ToArray()
	if err != nil {
		t.Fatalf("tick %s: %v", k.ID, err)
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		if s, err := v.ToString(); err == nil {
			out[i] = s
		} else if n, err := v.AsInt64(); err == nil {
			out[i] = strconv.FormatInt(n, 10)
		}
	}
	return out
}

// Joins gives n new devices ("<prefix>0".."<prefix>n-1") their places with rusuh_join_v1.
func Joins(t *testing.T, client valkey.Client, k qkeys.Event, prefix string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		if err := valkeyx.FCall(ctx, client, "rusuh_join_v1", []string{k.Cfg, k.Seq, k.Devs, k.Pre, k.Accts, k.Served, k.Hist},
			prefix+strconv.Itoa(i), "", "60000").Error(); err != nil {
			t.Fatalf("join: %v", err)
		}
	}
}

// Claim runs rusuh_claim_v1 for a device's seq and returns the status and units.
func Claim(t *testing.T, client valkey.Client, k qkeys.Event, device string, seq int64, orderWindow time.Duration) (string, int64) {
	t.Helper()
	vals, err := valkeyx.FCall(context.Background(), client, "rusuh_claim_v1",
		[]string{k.Cfg, k.Served, k.Devs, k.Unreserved, k.Resv, k.Active, k.Leases, k.Hist, k.Claimed, k.Accts},
		device, strconv.FormatInt(seq, 10), "s_"+device+"_"+strconv.FormatInt(seq, 10),
		strconv.FormatInt(orderWindow.Milliseconds(), 10), strconv.FormatInt((5*time.Minute).Milliseconds(), 10),
	).ToArray()
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	status, _ := vals[0].ToString()
	units, _ := vals[2].AsInt64()
	return status, units
}

// Int reads an integer key (0 when missing).
func Int(t *testing.T, client valkey.Client, key string) int64 {
	t.Helper()
	n, err := client.Do(context.Background(), client.B().Get().Key(key).Build()).AsInt64()
	if err != nil && !valkey.IsValkeyNil(err) {
		t.Fatalf("get %s: %v", key, err)
	}
	return n
}
