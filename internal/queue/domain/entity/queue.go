package entity

import (
	"errors"
	"fmt"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
)

// Config holds the module's tunables (env: RL_JOIN_PER_DEVICE, RL_CLAIM_PER_DEVICE,
// ORDER_WINDOW, MISS_WINDOW).
type Config struct {
	JoinPerDevice  ratelimit.Rule
	ClaimPerDevice ratelimit.Rule
	// OrderWindow is the lease and admission token lifetime (FR-17, ADR-0005: 5 min).
	OrderWindow time.Duration
	// MissWindow: a turn not claimed this long after it was reached is MISSED
	// ("60 detik untuk masuk"); the place is then over and join gives a new one.
	// Must not exceed the controller's HistWindow (RATE_WINDOW).
	MissWindow time.Duration
	// LoginWindow is the turn of a device not logged in before its turn was reached
	// (ADR-0021, env LOGIN_WINDOW: 3 min); MissWindow applies to the others.
	LoginWindow time.Duration
	// ClaimRequiresLogin refuses claims without an access token (ADR-0021, env CLAIM_REQUIRES_LOGIN).
	ClaimRequiresLogin bool
	// TransfersPerEvent is how often an account may move its place to another
	// device in one event (D-13, env PLACE_TRANSFERS_PER_EVENT); 0 turns moving off.
	TransfersPerEvent int64
}

// RateLimitedError: the device joins too often (429).
type RateLimitedError struct{ RetryAfter time.Duration }

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("queue: join rate limited, retry after %s", e.RetryAfter)
}

// Place is a device's position in one event's queue (QueuePlace in the API).
type Place struct {
	EventID         string
	State           string // preopen | open | paused | closed
	Seq             *int64 // nil while waiting for the pre-sale draw
	Served          int64
	OpensAt         time.Time
	TurnExpiresAt   *time.Time // set once seq <= served
	ServerNow       time.Time
	QueueToken      string
	AccountAttached bool
	// Claimed: the account's claimed lease moved to this device (ADR-0021); claim at once.
	Claimed bool
}

// Admission is a claimed turn (Admission in the API).
type Admission struct {
	Token         string
	SlotID        string
	ReservedUnits int
	LoginRequired bool // a guest logs in inside the order window (ADR-0006)
	ExpiresAt     time.Time
	ServerNow     time.Time
}

// JoinInput is one joinQueue call. UserID is empty for a guest.
type JoinInput struct {
	EventID  string
	DeviceID string
	UserID   string
}

var (
	ErrUnknownEvent       = errors.New("queue: unknown event")
	ErrClosed             = errors.New("queue: closed, sold out")
	ErrAccountElsewhere   = errors.New("queue: account holds a place on another device")
	ErrDeviceOtherAccount = errors.New("queue: this place belongs to another account")
	ErrPlaceExpired       = errors.New("queue: place used or expired; rejoin")
	ErrPlaceMismatch      = errors.New("queue: queue token does not match the device's place")
	// ErrMoved: the account moved this place to another device (D-13).
	ErrMoved = errors.New("queue: place moved to another device")
	// ErrTransferLimit: the account already moved its place as often as allowed.
	ErrTransferLimit       = errors.New("queue: place already moved to another device")
	ErrFrontierUnavailable = errors.New("queue: frontier not written yet")

	// Claim refusals (FR-08). ErrPlaceExpired and ErrClosed are also returned.
	ErrNotYet  = errors.New("queue: turn not reached yet")
	ErrFull    = errors.New("queue: every slot is in use")
	ErrNoStock = errors.New("queue: every unit is reserved or held")
	ErrMissed  = errors.New("queue: turn passed without a claim")
	// ErrLoginRequired: claimTurn needs an access token (ADR-0021).
	ErrLoginRequired = errors.New("queue: log in to claim the turn")
	// ErrAdmissionUnavailable: the lease is made but core did not record the
	// token, so the user does not get it yet (503, retry gives the same slot).
	ErrAdmissionUnavailable = errors.New("queue: core could not record the admission")
)
