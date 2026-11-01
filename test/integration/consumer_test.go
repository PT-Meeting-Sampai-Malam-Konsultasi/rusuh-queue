package integration

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	vk "github.com/valkey-io/valkey-go"

	consumerentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/delivery"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

func fastConsumer(name string) consumerentity.Config {
	c := consumerentity.DefaultConfig()
	c.Consumer = name
	c.Block = 100 * time.Millisecond
	c.RetryEvery = 100 * time.Millisecond
	c.RetryIdle = 200 * time.Millisecond
	return c
}

func startConsumer(t *testing.T, client vk.Client, db *sql.DB, cfg consumerentity.Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c := delivery.NewConsumer(delivery.ConsumerDeps{Config: cfg, DB: db, Valkey: client})
	go func() { _ = c.Run(ctx); close(done) }()
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { cancel(); <-done }) })
	time.Sleep(200 * time.Millisecond) // the group exists before the test publishes
}

func pendingCount(client vk.Client) int64 {
	vals, _ := client.Do(context.Background(), client.B().Xpending().Key("stream:booking").Group("queue").Build()).ToArray()
	if len(vals) == 0 {
		return -1
	}
	n, _ := vals[0].AsInt64()
	return n
}

// P2: held, paid and expired from core change the counters, each once, and
// everything is acknowledged.
func TestConsumerAppliesBookingEvents(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	startConsumer(t, client, db, fastConsumer("task-a"))
	k := testkit.Event(t, client, "evt_cons", testkit.EventOpts{Available: 10})
	slotA, _ := testkit.Admit(t, client, k, "w_consumerAAAAAAAAAA", time.Minute)
	slotB, _ := testkit.Admit(t, client, k, "w_consumerBBBBBBBBBB", time.Minute)
	deadline := time.Now().Add(10 * time.Minute).UTC()

	held := testkit.BookingEvent{ID: "o1", Type: "booking.held", EventID: k.ID, SlotID: slotA, DeviceID: "w_consumerAAAAAAAAAA", Units: 2, PaymentDeadline: deadline}
	for range 5 {
		testkit.PublishBooking(t, client, held)
	}
	testkit.PublishBooking(t, client, testkit.BookingEvent{ID: "o2", Type: "booking.paid", EventID: k.ID, SlotID: slotA, DeviceID: "w_consumerAAAAAAAAAA", Units: 2})
	testkit.PublishBooking(t, client, testkit.BookingEvent{ID: "o3", Type: "booking.held", EventID: k.ID, SlotID: slotB, DeviceID: "w_consumerBBBBBBBBBB", Units: 1, PaymentDeadline: deadline})
	testkit.PublishBooking(t, client, testkit.BookingEvent{ID: "o4", Type: "booking.expired", EventID: k.ID, SlotID: slotB, DeviceID: "w_consumerBBBBBBBBBB", Units: 1})

	// 10 units, two claims of 3: A holds 2 and pays (1 back), B holds 1 (2 back) and expires (1 back).
	eventually(t, "counters", func() bool {
		return testkit.Int(t, client, k.Unreserved) == 8 && testkit.Int(t, client, k.Resv) == 0 && testkit.Int(t, client, k.Held) == 0
	})
	eventually(t, "all acked", func() bool { return pendingCount(client) == 0 })
}

// A payload that can never be valid goes to the dead-letter stream and the log at once.
func TestConsumerDeadLettersPoison(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	startConsumer(t, client, db, fastConsumer("task-a"))
	id := testkit.PublishRaw(t, client, "booking.held", `{"v":1,"id":"bad"`)

	eventually(t, "dead-lettered", func() bool {
		n, _ := client.Do(context.Background(), client.B().Xlen().Key("stream:booking:dlq").Build()).AsInt64()
		return n == 1 && pendingCount(client) == 0
	})
	var deliveries int
	if err := db.QueryRow(`SELECT deliveries FROM queue.consumer_dlq_log WHERE stream_id = $1`, id).Scan(&deliveries); err != nil || deliveries != 1 {
		t.Fatalf("dlq log: deliveries %d, %v", deliveries, err)
	}
}

// An entry another task took and never acknowledged is retried; after five
// deliveries it goes to the dead-letter stream.
func TestConsumerRetriesThenDeadLetters(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	ctx := context.Background()
	b := client.B()
	if err := client.Do(ctx, b.XgroupCreate().Key("stream:booking").Group("queue").Id("$").Mkstream().Build()).Error(); err != nil {
		t.Fatal(err)
	}
	k := testkit.Event(t, client, "evt_retry", testkit.EventOpts{Available: 10})
	slot, _ := testkit.Admit(t, client, k, "w_retryAAAAAAAAAAAAA", time.Minute)
	ok := testkit.PublishBooking(t, client, testkit.BookingEvent{ID: "r1", Type: "booking.held", EventID: k.ID, SlotID: slot,
		DeviceID: "w_retryAAAAAAAAAAAAA", Units: 2, PaymentDeadline: time.Now().Add(10 * time.Minute).UTC()})
	stuck := testkit.PublishBooking(t, client, testkit.BookingEvent{ID: "r2", Type: "booking.expired", EventID: k.ID, SlotID: "s_other",
		DeviceID: "w_retryAAAAAAAAAAAAA", Units: 1})

	// A task that crashed: read both, never acknowledged; the second one already delivered five times.
	if err := client.Do(ctx, b.Xreadgroup().Group("queue", "dead-task").Count(10).Streams().Key("stream:booking").Id(">").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(ctx, b.Xclaim().Key("stream:booking").Group("queue").Consumer("dead-task").MinIdleTime("0").Id(stuck).Retrycount(5).Build()).Error(); err != nil {
		t.Fatal(err)
	}

	startConsumer(t, client, db, fastConsumer("task-b"))
	eventually(t, "retried hold applied", func() bool { return testkit.Int(t, client, k.Held) == 2 })
	eventually(t, "the stuck one dead-lettered", func() bool {
		var n int
		_ = db.QueryRow(`SELECT count(*) FROM queue.consumer_dlq_log WHERE stream_id = $1`, stuck).Scan(&n)
		return n == 1 && pendingCount(client) == 0
	})
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM queue.consumer_dlq_log WHERE stream_id = $1`, ok).Scan(&n)
	if n != 0 {
		t.Fatal("the good entry was dead-lettered")
	}
}
