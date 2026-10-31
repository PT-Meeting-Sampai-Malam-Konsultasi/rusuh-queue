package entity

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
