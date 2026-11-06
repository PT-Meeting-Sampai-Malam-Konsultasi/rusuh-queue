// Package dto holds the values the stream domain passes through its ports.
package dto

import "time"

// Place is the part of a device's place the stream needs (from the queue domain).
// Seq is nil while the place waits for the pre-open draw.
type Place struct {
	Seq           *int64
	Served        int64
	TurnExpiresAt *time.Time
	ServerNow     time.Time
}

// Frame is one frontier: Data is the JSON as written by the controller, sent to
// the client unchanged; the other fields are read from it.
type Frame struct {
	Data       []byte
	State      string
	Served     int64
	StreamFrom int64
	TS         int64
}

// Turn is the data of the "turn" event (QueueTurn in the API).
type Turn struct {
	EventID       string
	Seq           int64
	Served        int64
	TurnExpiresAt time.Time
	ServerNow     time.Time
}

type OpenInput struct {
	EventID  string
	DeviceID string
	TokenSeq int64
}
