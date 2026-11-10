package postgres_test

import (
	"context"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/infrastructure/postgres"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

// A stale leader (lower fence) never overwrites a newer checkpoint.
func TestCheckpointFenceGuard(t *testing.T) {
	db := testkit.Postgres(t)
	c := postgres.New(db)
	ctx := context.Background()

	for _, w := range []struct{ seq, served, fence int64 }{
		{10, 5, 1},
		{20, 15, 2}, // new leader
		{11, 6, 1},  // old leader wakes up
		{21, 16, 2},
	} {
		if err := c.Upsert(ctx, "evt_cp", w.seq, w.served, w.fence); err != nil {
			t.Fatal(err)
		}
	}
	var seq, served, fence int64
	if err := db.QueryRow(`SELECT seq, served, fence FROM queue.checkpoint WHERE event_id = 'evt_cp'`).Scan(&seq, &served, &fence); err != nil {
		t.Fatal(err)
	}
	if seq != 21 || served != 16 || fence != 2 {
		t.Fatalf("checkpoint = %d/%d fence %d, want 21/16 fence 2", seq, served, fence)
	}
}
