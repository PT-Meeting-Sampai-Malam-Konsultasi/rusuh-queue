package dto

import "time"

// Message is one stream entry; Payload is the raw JSON of its "payload" field.
type Message struct {
	ID      string
	Payload string
}

// Pending is one entry the group delivered but nobody acknowledged.
type Pending struct {
	ID         string
	Deliveries int64
}

type ReleaseArgs struct {
	ID              string
	EventID         string
	Type            string
	SlotID          string
	Units           int
	PaymentDeadline time.Time
	HoldGrace       time.Duration
}

type ReleaseStatus string

const (
	ReleaseApplied     ReleaseStatus = "APPLIED"
	ReleaseDuplicate   ReleaseStatus = "DUPLICATE"
	ReleaseUnknownSlot ReleaseStatus = "UNKNOWN_SLOT"
	ReleaseNoEvent     ReleaseStatus = "NO_EVENT"
)

type DeadLetter struct {
	StreamID   string
	EventID    string
	EventType  string
	Deliveries int64
	LastError  string
	Payload    string
}
