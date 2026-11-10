package entity

import (
	"errors"
	"time"
)

type Config struct {
	Stream    string
	DLQStream string
	Group     string
	// Consumer names this task in the group; unique per process.
	Consumer string
	Batch    int64
	Block    time.Duration
	// Every RetryEvery, entries pending longer than RetryIdle are claimed and
	// retried; after MaxDeliveries they go to the dead-letter stream.
	RetryEvery    time.Duration
	RetryIdle     time.Duration
	MaxDeliveries int64
	// HoldGrace keeps a held slot past the payment deadline (booking.v1 schema).
	HoldGrace time.Duration
}

func DefaultConfig() Config {
	return Config{
		Stream:        "stream:booking",
		DLQStream:     "stream:booking:dlq",
		Group:         "queue",
		Batch:         100,
		Block:         time.Second,
		RetryEvery:    10 * time.Second,
		RetryIdle:     30 * time.Second,
		MaxDeliveries: 5,
		HoldGrace:     2 * time.Minute,
	}
}

type EventType string

const (
	BookingHeld    EventType = "booking.held"
	BookingPaid    EventType = "booking.paid"
	BookingExpired EventType = "booking.expired"
)

// BookingEvent is one stream:booking payload (rusuh-documentation/api/events/booking.v1.schema.json).
type BookingEvent struct {
	V               int       `json:"v"`
	ID              string    `json:"id"`
	Type            EventType `json:"type"`
	EventID         string    `json:"eventId"`
	SlotID          string    `json:"slotId"`
	DeviceID        string    `json:"deviceId"`
	BookingID       string    `json:"bookingId"`
	Units           int       `json:"units"`
	PaymentDeadline time.Time `json:"paymentDeadline"`
	OccurredAt      time.Time `json:"occurredAt"`
}

// ErrInvalidEvent marks a payload that can never succeed; it goes to the dead-letter stream at once.
var ErrInvalidEvent = errors.New("consumer: invalid booking event")
