package dto

import (
	"time"
)

// JoinStatus is rusuh_join_v1's status.
type JoinStatus string

const (
	JoinOK                 JoinStatus = "OK"
	JoinPre                JoinStatus = "PRE"
	JoinClosed             JoinStatus = "CLOSED"
	JoinNoEvent            JoinStatus = "NO_EVENT"
	JoinAccountElsewhere   JoinStatus = "ACCOUNT_ELSEWHERE"
	JoinDeviceOtherAccount JoinStatus = "DEVICE_OTHER_ACCOUNT"
	// rusuh_transfer_v1 only (D-13).
	JoinNothingToMove JoinStatus = "NOTHING" // no live place on another device: run a normal join
	JoinTransferLimit JoinStatus = "LIMIT"   // the account already moved its place as often as allowed
)

// Turn is what the client needs around the place: served when answering, the
// sale opening, the end of the turn (zero until reached) and Valkey's clock.
type Turn struct {
	Served    int64
	OpensAt   time.Time
	TurnEnds  time.Time
	ServerNow time.Time
}

type JoinResult struct {
	Status   JoinStatus
	Seq      int64
	Attached bool
	EndsAt   time.Time
	Turn
}

// ClaimStatus is rusuh_claim_v1's status.
type ClaimStatus string

const (
	ClaimOK      ClaimStatus = "OK"
	ClaimNoEvent ClaimStatus = "NO_EVENT"
	ClaimClosed  ClaimStatus = "CLOSED"
	ClaimExpired ClaimStatus = "EXPIRED"
	ClaimNotYet  ClaimStatus = "NOT_YET"
	ClaimMissed  ClaimStatus = "MISSED"
	ClaimFull    ClaimStatus = "FULL"
	ClaimNoStock ClaimStatus = "NO_STOCK"
)

// ClaimInput: SlotID is a fresh id, used only when a new lease is made.
type ClaimInput struct {
	DeviceID    string
	TokenSeq    int64
	SlotID      string
	OrderWindow time.Duration
	MissWindow  time.Duration
}

type ClaimResult struct {
	Status    ClaimStatus
	SlotID    string
	Units     int
	EndsAt    time.Time // lease end, Valkey time
	Attached  bool
	ServerNow time.Time
}

// PlaceView is what Valkey holds for one device in one event.
type PlaceView struct {
	Found    bool   // the event exists
	State    string // cfg.state
	Value    string // devs[device]: "<seq>" | "pre" | "x:<seq>" | "m:<seq>" / "m:pre" (moved) | "" (no place)
	Attached bool
	EndsAt   time.Time
	Missed   bool // reached and not claimed within the miss window
	Turn
}
