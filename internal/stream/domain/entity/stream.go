// Package entity holds the stream domain's rules (ADR-0015).
package entity

import (
	"errors"
	"fmt"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
)

// Config holds the stream tunables (env STREAM_MAX_PER_TASK, STREAM_LIFETIME,
// STREAM_HEARTBEAT, STREAM_DRAIN, RL_STREAM_PER_DEVICE).
type Config struct {
	MaxPerTask int
	// Lifetime is how long a stream lives before the server closes it, ±20% jitter,
	// so reconnects spread streams over new tasks.
	Lifetime  time.Duration
	Heartbeat time.Duration
	// Buffer is the frames a stream may fall behind before it is dropped: a slow
	// client never blocks the hub.
	Buffer int
	// DrainOver spreads the closing of open streams after SIGTERM. Keep it below
	// SHUTDOWN_TIMEOUT so the server can finish.
	DrainOver        time.Duration
	ConnectPerDevice ratelimit.Rule
}

func DefaultConfig() Config {
	return Config{
		MaxPerTask:       5000,
		Lifetime:         10 * time.Minute,
		Heartbeat:        15 * time.Second,
		Buffer:           4,
		DrainOver:        25 * time.Second,
		ConnectPerDevice: ratelimit.Rule{Scope: "stream:dev", Limit: 12, Window: time.Minute},
	}
}

var (
	ErrTooFar        = errors.New("stream: position above streamFrom, or streaming off")
	ErrFull          = errors.New("stream: this task holds its maximum of streams")
	ErrDraining      = errors.New("stream: the task is shutting down")
	ErrUnknownEvent  = errors.New("stream: unknown event")
	ErrClosed        = errors.New("stream: queue closed")
	ErrMoved         = errors.New("stream: place moved to another device")
	ErrMissed        = errors.New("stream: turn passed without a claim")
	ErrPlaceExpired  = errors.New("stream: place used or expired")
	ErrPlaceMismatch = errors.New("stream: queue token does not match the device's place")
)

// RateLimitedError: the device opens streams too often (429).
type RateLimitedError struct{ RetryAfter time.Duration }

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("stream: connect rate limited, retry after %s", e.RetryAfter)
}
