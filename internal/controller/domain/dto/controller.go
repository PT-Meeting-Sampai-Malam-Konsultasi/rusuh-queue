package dto

import (
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
)

// BootstrapArgs are rusuh_bootstrap_v1's arguments.
type BootstrapArgs struct {
	Now             time.Time
	OpensAt         time.Time
	EndsAt          time.Time
	ExpireAt        time.Time
	Cancelled       bool
	Available       int64
	Settings        entity.Settings
	InitialFrontier []byte
}

// Lead is rusuh_lead_v1's answer for one event.
type Lead struct {
	Leading  bool
	Fence    int64
	Acquired bool // a new lease: the fence was bumped
}

// TickArgs are rusuh_tick_v1's arguments.
type TickArgs struct {
	Fence        int64
	HistWindow   time.Duration
	InviteWindow time.Duration
	// PriorOrderTime drives the warm-up guess for the speed (RATE_PRIOR_ORDER_TIME); 0 turns it off.
	PriorOrderTime time.Duration
}

// TickStatus: TickOK, TickStale (another leader holds a newer fence) or TickNoEvent.
type TickStatus string

const (
	TickOK      TickStatus = "OK"
	TickStale   TickStatus = "STALE"
	TickNoEvent TickStatus = "NO_EVENT"
)

type TickResult struct {
	Status   TickStatus
	Frontier []byte
	Advance  int64
	State    string
	Seq      int64
	Served   int64
	Pending  int64 // invited, not claimed yet (D-7)
}

type PurgeResult struct {
	Stale  bool
	Purged int64
	Stuck  int64 // held leases past their end (core's to resolve; alarm)
}
