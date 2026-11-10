package entity

import (
	"errors"
	"fmt"
)

// Settings are the queue knobs for one event (OPS-01), stored in
// queue.event_queue_config and changed only by adminUpdateQueue (M5).
type Settings struct {
	EventID    string
	MaxSlots   int64
	RateCap    int64 // admissions per second
	StreamFrom int64 // 0 = polling only (ADR-0015)
	State      string
}

// DefaultSettings apply until an operator saves settings for an event
// (Solution Architecture: 2,000 slots, 50/s, SSE from 2,000).
func DefaultSettings(eventID string) Settings {
	return Settings{EventID: eventID, MaxSlots: 2000, RateCap: 50, StreamFrom: 2000, State: "open"}
}

// Opt is an optional field of a partial update; Set is false when the caller left it out.
type Opt[T any] struct {
	V   T
	Set bool
}

func Some[T any](v T) Opt[T] { return Opt[T]{V: v, Set: true} }

// Patch is an operator's change (adminUpdateQueue); unset fields keep their value.
type Patch struct {
	State      Opt[string]
	MaxSlots   Opt[int64]
	RateCap    Opt[int64]
	StreamFrom Opt[int64]
	Reason     string
}

var (
	ErrInvalidSettings = errors.New("admin: invalid queue settings")
	ErrUnknownEvent    = errors.New("admin: unknown event")
)

// Apply returns s with p's fields, or ErrInvalidSettings.
func (p Patch) Apply(s Settings) (Settings, error) {
	if p.State.Set {
		s.State = p.State.V
	}
	if p.MaxSlots.Set {
		s.MaxSlots = p.MaxSlots.V
	}
	if p.RateCap.Set {
		s.RateCap = p.RateCap.V
	}
	if p.StreamFrom.Set {
		s.StreamFrom = p.StreamFrom.V
	}
	switch {
	case s.State != "open" && s.State != "paused" && s.State != "closed":
		return s, fmt.Errorf("%w: state must be open, paused or closed", ErrInvalidSettings)
	case s.MaxSlots < 1 || s.MaxSlots > 1<<31-1:
		return s, fmt.Errorf("%w: maxConcurrentSlots must be at least 1", ErrInvalidSettings)
	case s.RateCap < 1 || s.RateCap > 1<<31-1:
		return s, fmt.Errorf("%w: rateCapPerSecond must be at least 1", ErrInvalidSettings)
	case s.StreamFrom < 0 || s.StreamFrom > 1<<31-1:
		return s, fmt.Errorf("%w: streamFrom must not be negative", ErrInvalidSettings)
	}
	return s, nil
}
