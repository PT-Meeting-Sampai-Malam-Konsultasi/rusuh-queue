// Package coredocs are the JSON documents core writes into Valkey for queue
// to read (D-1, D-2; rusuh-documentation/api/shared-keys.md). queue never
// writes them; cmd/fakecore and tests do, to stand in for core.
package coredocs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const Version = 1

// Sale is core:{evt}:sale.
type Sale struct {
	V                  int       `json:"v"`
	EventID            string    `json:"eventId"`
	Status             string    `json:"status"` // published | cancelled
	WaitingRoomOpensAt time.Time `json:"waitingRoomOpensAt"`
	SaleOpensAt        time.Time `json:"saleOpensAt"`
	EndsAt             time.Time `json:"endsAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// Stock is core:{evt}:stock: units still available in PostgreSQL
// (reserved units count as available there; held and sold ones do not).
type Stock struct {
	V         int       `json:"v"`
	EventID   string    `json:"eventId"`
	Available int64     `json:"available"`
	Version   int64     `json:"version"` // increases on every change
	TS        time.Time `json:"ts"`
}

var ErrInvalid = errors.New("coredocs: invalid document")

func ParseSale(b []byte, eventID string) (Sale, error) {
	var s Sale
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("%w: sale: %w", ErrInvalid, err)
	}
	switch {
	case s.V != Version:
		return s, fmt.Errorf("%w: sale v=%d", ErrInvalid, s.V)
	case s.EventID != eventID:
		return s, fmt.Errorf("%w: sale for %q under %q", ErrInvalid, s.EventID, eventID)
	case s.Status != "published" && s.Status != "cancelled":
		return s, fmt.Errorf("%w: sale status %q", ErrInvalid, s.Status)
	case s.SaleOpensAt.IsZero() || !s.EndsAt.After(s.SaleOpensAt):
		return s, fmt.Errorf("%w: sale needs saleOpensAt < endsAt", ErrInvalid)
	}
	return s, nil
}

func ParseStock(b []byte, eventID string) (Stock, error) {
	var s Stock
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("%w: stock: %w", ErrInvalid, err)
	}
	switch {
	case s.V != Version:
		return s, fmt.Errorf("%w: stock v=%d", ErrInvalid, s.V)
	case s.EventID != eventID:
		return s, fmt.Errorf("%w: stock for %q under %q", ErrInvalid, s.EventID, eventID)
	case s.Available < 0:
		return s, fmt.Errorf("%w: stock available %d", ErrInvalid, s.Available)
	}
	return s, nil
}
