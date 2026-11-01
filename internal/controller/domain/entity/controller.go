package entity

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"time"
)

// Config holds the controller's timers (IMPLEMENTATION_PLAN.md "Admission controller").
type Config struct {
	// DiscoverEvery re-reads core:events and every live event's sale and stock
	// even without a nudge (lost pub/sub messages are only a delay).
	DiscoverEvery time.Duration
	// CheckpointEvery: the leader writes seq and served to PostgreSQL (Valkey failover recovery).
	CheckpointEvery time.Duration

	// TaskID names this task in q:{evt}:leader; unique per process.
	TaskID string
	// TickEvery is the admission tick (TICK). A tick that starts more than half
	// a period late is skipped, never queued.
	TickEvery time.Duration
	// LeaderTTL: a leader that stops renewing loses the event after this long.
	LeaderTTL time.Duration
	// PurgeGrace: a lease's units go back this long after its order window ends (PURGE_GRACE).
	PurgeGrace time.Duration
	// InviteWindow: invitations younger than this and not claimed yet count as
	// provisional slots (D-7). Same as the queue's miss window: after it an
	// unclaimed invitation is MISSED and no longer needs a slot.
	InviteWindow time.Duration
	// HistWindow is both the ratePerMin window and how far back the claim
	// script can see when a seq was reached (RATE_WINDOW, FR-09).
	HistWindow time.Duration
	// PriorOrderTime is the average time from claim to paid, for the speed
	// guess maxSlots / PriorOrderTime the frontier shows during warm-up
	// (RATE_PRIOR_ORDER_TIME, rateSource "prior"); 0 turns the guess off.
	PriorOrderTime time.Duration
	// DrawBatch and DrawBudget bound the pre-open draw per tick: batches of
	// DrawBatch devices until the list is empty or DrawBudget is spent.
	DrawBatch  int
	DrawBudget time.Duration
}

func DefaultConfig() Config {
	return Config{
		DiscoverEvery:   30 * time.Second,
		CheckpointEvery: time.Second,
		TaskID:          NewTaskID(),
		TickEvery:       time.Second,
		LeaderTTL:       3 * time.Second,
		PurgeGrace:      30 * time.Second,
		InviteWindow:    time.Minute,
		HistWindow:      5 * time.Minute,
		PriorOrderTime:  6 * time.Minute,
		DrawBatch:       2000,
		DrawBudget:      300 * time.Millisecond,
	}
}

// NewTaskID returns "<hostname>-<random>": a restarted pod with the same
// name is still a different leader.
func NewTaskID() string {
	host, _ := os.Hostname()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}
