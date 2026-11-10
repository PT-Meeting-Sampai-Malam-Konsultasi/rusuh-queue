package postgres_test

import (
	"context"
	"math"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/infrastructure/postgres"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

func TestLogKeepsPoisonPayloadsReadable(t *testing.T) {
	db := testkit.Postgres(t)
	log := postgres.New(db)
	ctx := context.Background()

	for _, d := range []dto.DeadLetter{
		{StreamID: "1-0", EventID: "evt1", EventType: "booking.paid", Deliveries: 5, LastError: "too many deliveries", Payload: `{"n":1}`},
		{StreamID: "2-0", Deliveries: math.MaxInt64, LastError: "invalid", Payload: "{not json"},
	} {
		if err := log.Log(ctx, d); err != nil {
			t.Fatal(err)
		}
	}

	var payload string
	var deliveries int64
	var eventID *string
	if err := db.QueryRow(`SELECT payload::text, deliveries, event_id FROM queue.consumer_dlq_log WHERE stream_id = '2-0'`).Scan(&payload, &deliveries, &eventID); err != nil {
		t.Fatal(err)
	}
	if payload != `"{not json"` || deliveries != math.MaxInt32 || eventID != nil {
		t.Fatalf("row = %s %d %v", payload, deliveries, eventID)
	}

	_ = db.Close()
	if err := log.Log(ctx, dto.DeadLetter{StreamID: "3-0", Payload: "{}"}); err == nil {
		t.Fatal("closed database accepted the log")
	}
}
