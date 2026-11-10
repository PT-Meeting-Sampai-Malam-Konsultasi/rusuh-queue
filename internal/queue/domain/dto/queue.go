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
	// JoinClaimed: the account's claimed, not yet held lease moved here (ADR-0021); claim at once.
	JoinClaimed JoinStatus = "CLAIMED"
)

// Windows are the turn lengths (ADR-0021): Miss for a device that was logged in
// before its turn was reached, Login for every other device.
type Windows struct {
	Miss  time.Duration
	Login time.Duration
}

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
	// With a user (ADR-0021).
	ClaimDeviceOtherAccount ClaimStatus = "DEVICE_OTHER_ACCOUNT"
	ClaimAccountElsewhere   ClaimStatus = "ACCOUNT_ELSEWHERE"
)

// ClaimInput: SlotID is a fresh id, used only when a new lease is made.
// UserID, when set, is attached to the device by the claim.
type ClaimInput struct {
	DeviceID    string
	UserID      string
	TokenSeq    int64
	SlotID      string
	OrderWindow time.Duration
	Windows     Windows
}

type ClaimResult struct {
	Status    ClaimStatus
	SlotID    string
	Units     int
	EndsAt    time.Time // lease end, Valkey time
	Attached  bool
	ServerNow time.Time
}

// AdmissionRecord is what core stores per slot (registerAdmission).
type AdmissionRecord struct {
	Token         string
	EventID       string
	DeviceID      string
	UserID        string
	Seq           int64
	SlotID        string
	ReservedUnits int
	ExpiresAt     time.Time
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
