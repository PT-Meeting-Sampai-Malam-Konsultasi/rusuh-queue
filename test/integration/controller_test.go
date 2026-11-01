package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/delivery"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/coredocs"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

// fastController is the production configuration sped up for tests: no
// discovery timer (nudges only), 200 ms ticks, a 1 s leader lease.
func fastController() entity.Config {
	c := entity.DefaultConfig()
	c.DiscoverEvery = time.Hour
	c.CheckpointEvery = 200 * time.Millisecond
	c.TickEvery = 200 * time.Millisecond
	c.LeaderTTL = time.Second
	return c
}

func startController(t *testing.T, client vk.Client, db *sql.DB) {
	t.Helper()
	startControllerWith(t, client, db, fastController())
}

// startControllerWith runs a controller until the test ends; the returned
// function stops it earlier (as SIGTERM would).
func startControllerWith(t *testing.T, client vk.Client, db *sql.DB, cfg entity.Config) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	m := delivery.NewController(delivery.ControllerDeps{
		Config:   cfg,
		DB:       db,
		Valkey:   client,
		Settings: delivery.NewAdmin(db, client),
	})
	go func() { _ = m.Run(ctx); close(done) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func sale(id string, opensIn time.Duration, status string) coredocs.Sale {
	now := time.Now().UTC()
	return coredocs.Sale{
		V: 1, EventID: id, Status: status,
		WaitingRoomOpensAt: now.Add(-time.Hour), SaleOpensAt: now.Add(opensIn), EndsAt: now.Add(3 * time.Hour), UpdatedAt: now,
	}
}

func stock(id string, available int64) coredocs.Stock {
	return coredocs.Stock{V: 1, EventID: id, Available: available, Version: 1, TS: time.Now().UTC()}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func cfgState(client vk.Client, id string) string {
	s, _ := client.Do(context.Background(), client.B().Hget().Key(qkeys.For(id).Cfg).Field("state").Build()).ToString()
	return s
}

// core publishes an event (documents + nudge) → queue sets it up within a second,
// with no timer help (DiscoverEvery is one hour).
func TestControllerSetsUpPublishedEvent(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	startController(t, client, db)
	time.Sleep(300 * time.Millisecond) // let the subscription start

	testkit.CoreSale(t, client, sale("evt_pub", -time.Minute, "published"), stock("evt_pub", 10000))
	testkit.Publish(t, client, qkeys.CoreEventsChanged)

	eventually(t, "cfg state open", func() bool { return cfgState(client, "evt_pub") == "open" })
	k := qkeys.For("evt_pub")
	ctx := context.Background()

	unreserved, _ := client.Do(ctx, client.B().Get().Key(k.Unreserved).Build()).AsInt64()
	if unreserved != 10000 {
		t.Fatalf("unreserved = %d, want 10000 from core's stock", unreserved)
	}
	maxSlots, _ := client.Do(ctx, client.B().Hget().Key(k.Cfg).Field("maxSlots").Build()).AsInt64()
	if maxSlots != 2000 {
		t.Fatalf("maxSlots = %d, want the default 2000", maxSlots)
	}
	raw, _ := client.Do(ctx, client.B().Get().Key(k.Frontier).Build()).AsBytes()
	var f api.Frontier
	if err := json.Unmarshal(raw, &f); err != nil || f.State != api.QueueStateOpen || f.Sellable == nil || *f.Sellable != 10000 {
		t.Fatalf("frontier = %s (%v)", raw, err)
	}
	ttl, _ := client.Do(ctx, client.B().Pttl().Key(k.Cfg).Build()).AsInt64()
	if ttl < int64((26*time.Hour)/time.Millisecond) {
		t.Fatalf("cfg ttl = %dms, want sale end + 1 day", ttl)
	}
	members, _ := client.Do(ctx, client.B().Smembers().Key(qkeys.LiveEvents).Build()).AsStrSlice()
	if len(members) != 1 || members[0] != "evt_pub" {
		t.Fatalf("q:events = %v", members)
	}

	// Checkpoint reaches PostgreSQL.
	eventually(t, "checkpoint row", func() bool {
		var seq, served int64
		return db.QueryRow(`SELECT seq, served FROM queue.checkpoint WHERE event_id = 'evt_pub'`).Scan(&seq, &served) == nil
	})
}

// A cancelled sale closes the queue on core's per-event nudge.
func TestControllerClosesCancelledSale(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	startController(t, client, db)
	time.Sleep(300 * time.Millisecond)

	testkit.CoreSale(t, client, sale("evt_cxl", time.Hour, "published"), stock("evt_cxl", 50))
	testkit.Publish(t, client, qkeys.CoreEventsChanged)
	eventually(t, "preopen", func() bool { return cfgState(client, "evt_cxl") == "preopen" })
	time.Sleep(300 * time.Millisecond) // per-event subscription

	testkit.CoreSale(t, client, sale("evt_cxl", time.Hour, "cancelled"), stock("evt_cxl", 50))
	testkit.Publish(t, client, qkeys.For("evt_cxl").CoreChanged)
	eventually(t, "closed after cancel", func() bool { return cfgState(client, "evt_cxl") == "closed" })
}

// Settings saved by an operator win over the defaults.
func TestControllerUsesSavedSettings(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	if _, err := db.Exec(`INSERT INTO queue.event_queue_config (event_id, max_slots, rate_cap, stream_from, state, updated_by)
		VALUES ('evt_cfg', 500, 20, 0, 'open', 'test')`); err != nil {
		t.Fatal(err)
	}
	startController(t, client, db)
	time.Sleep(300 * time.Millisecond)

	testkit.CoreSale(t, client, sale("evt_cfg", -time.Minute, "published"), stock("evt_cfg", 3))
	testkit.Publish(t, client, qkeys.CoreEventsChanged)
	eventually(t, "cfg", func() bool { return cfgState(client, "evt_cfg") == "open" })

	vals, _ := client.Do(context.Background(), client.B().Hmget().Key(qkeys.For("evt_cfg").Cfg).Field("maxSlots", "rateCap", "streamFrom").Build()).AsStrSlice()
	if vals[0] != "500" || vals[1] != "20" || vals[2] != "0" {
		t.Fatalf("cfg = %v, want 500/20/0", vals)
	}
}

// Missing or malformed documents never create a queue.
func TestControllerIgnoresBadDocuments(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	startController(t, client, db)
	time.Sleep(300 * time.Millisecond)

	bad := sale("evt_bad", -time.Minute, "published")
	bad.EndsAt = bad.SaleOpensAt.Add(-time.Minute) // ends before it opens
	testkit.CoreSale(t, client, bad, stock("evt_bad", 10))
	testkit.Publish(t, client, qkeys.CoreEventsChanged)
	time.Sleep(time.Second)
	if s := cfgState(client, "evt_bad"); s != "" {
		t.Fatalf("state = %q, want no queue for a bad sale document", s)
	}
}

// P3: core's stock changes after setup; the leader corrects unreserved after a
// nudge, and ignores a snapshot that is too old.
func TestControllerReconcilesStock(t *testing.T) {
	client := testkit.Valkey(t)
	startController(t, client, testkit.Postgres(t))
	time.Sleep(300 * time.Millisecond)
	const id = "evt_rcl"
	k := qkeys.For(id)
	testkit.CoreSale(t, client, sale(id, -time.Minute, "published"), stock(id, 100))
	testkit.Publish(t, client, qkeys.CoreEventsChanged)
	eventually(t, "set up", func() bool { return cfgState(client, id) == "open" })

	old := stock(id, 40)
	old.TS = time.Now().Add(-time.Minute).UTC()
	testkit.CoreSale(t, client, sale(id, -time.Minute, "published"), old)
	for range 5 {
		testkit.Publish(t, client, k.CoreChanged)
		time.Sleep(250 * time.Millisecond)
	}
	if n := testkit.Int(t, client, k.Unreserved); n != 100 {
		t.Fatalf("unreserved %d from a stale snapshot, want 100", n)
	}

	testkit.CoreSale(t, client, sale(id, -time.Minute, "published"), stock(id, 120))
	eventually(t, "corrected", func() bool {
		testkit.Publish(t, client, k.CoreChanged)
		return testkit.Int(t, client, k.Unreserved) == 120
	})
}
