// Package usecase is the stream hub (ADR-0015): one subscription per event per
// task, fanned out to that event's local streams.
package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/outbound"
)

type StreamUsecase struct {
	cfg     entity.Config
	places  outbound.IPlaces
	sub     outbound.ISubscriber
	limiter outbound.IRateLimiter

	mu       sync.Mutex
	events   map[string]*eventStreams
	total    int
	draining bool
}

type eventStreams struct {
	streams map[*stream]struct{}
	cancel  context.CancelFunc
}

type stream struct {
	eventID, deviceID string
	tokenSeq, seq     int64
	initial           dto.Frame
	frames            chan dto.Frame
	done              chan struct{}
	once              sync.Once
	lastTS            int64 // guarded by the hub's mu
}

func (s *stream) Initial() dto.Frame       { return s.initial }
func (s *stream) Frames() <-chan dto.Frame { return s.frames }
func (s *stream) Done() <-chan struct{}    { return s.done }
func (s *stream) Seq() int64               { return s.seq }
func (s *stream) end()                     { s.once.Do(func() { close(s.done) }) }

func (h *StreamUsecase) Open(ctx context.Context, in dto.OpenInput) (inbound.IStream, error) {
	if h.isDraining() {
		return nil, entity.ErrDraining
	}
	if r, err := h.limiter.Hit(ctx, h.cfg.ConnectPerDevice, in.DeviceID); err != nil {
		slog.WarnContext(ctx, "stream rate limit unavailable, allowing", "error", err)
	} else if !r.Allowed {
		return nil, &entity.RateLimitedError{RetryAfter: r.RetryAfter}
	}

	p, err := h.places.MyPlace(ctx, in.EventID, in.DeviceID, in.TokenSeq)
	if err != nil {
		return nil, err
	}
	if p.Seq == nil {
		return nil, entity.ErrTooFar // waiting for the draw: no position yet, keep polling
	}
	raw, err := h.places.Frontier(ctx, in.EventID)
	if err != nil {
		return nil, err
	}
	f, err := parseFrame(raw)
	if err != nil {
		return nil, err
	}
	if f.State == "closed" {
		return nil, entity.ErrClosed
	}
	if f.StreamFrom <= 0 || *p.Seq-f.Served > f.StreamFrom {
		return nil, entity.ErrTooFar
	}

	s := &stream{
		eventID: in.EventID, deviceID: in.DeviceID, tokenSeq: in.TokenSeq, seq: *p.Seq,
		initial: f, lastTS: f.TS,
		frames: make(chan dto.Frame, h.cfg.Buffer), done: make(chan struct{}),
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.draining {
		return nil, entity.ErrDraining
	}
	if h.total >= h.cfg.MaxPerTask {
		return nil, entity.ErrFull
	}
	es := h.events[in.EventID]
	if es == nil {
		subCtx, cancel := context.WithCancel(context.Background())
		es = &eventStreams{streams: map[*stream]struct{}{}, cancel: cancel}
		h.events[in.EventID] = es
		go h.sub.Subscribe(subCtx, in.EventID, func(m string) { h.onMessage(subCtx, in.EventID, m) })
	}
	es.streams[s] = struct{}{}
	h.total++
	return s, nil
}

// onMessage fans one frontier out without blocking: a stream whose buffer is
// full is dropped (the client reconnects or polls). "" means the subscription
// (re)started, so the frontier key is read again; frames not newer than what a
// stream already has are skipped.
func (h *StreamUsecase) onMessage(ctx context.Context, eventID, m string) {
	data := []byte(m)
	if m == "" {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		raw, err := h.places.Frontier(rctx, eventID)
		cancel()
		if err != nil {
			return
		}
		data = raw
	}
	f, err := parseFrame(data)
	if err != nil {
		slog.WarnContext(ctx, "stream: bad frontier message", "event", eventID, "error", err)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	es := h.events[eventID]
	if es == nil {
		return
	}
	for s := range es.streams {
		if f.TS <= s.lastTS {
			continue
		}
		select {
		case s.frames <- f:
			s.lastTS = f.TS
		default:
			h.remove(es, s)
			s.end()
		}
	}
	h.release(eventID, es)
}

func (h *StreamUsecase) Turn(ctx context.Context, is inbound.IStream) (dto.Turn, error) {
	s := is.(*stream)
	p, err := h.places.MyPlace(ctx, s.eventID, s.deviceID, s.tokenSeq)
	if err != nil {
		return dto.Turn{}, err
	}
	t := dto.Turn{EventID: s.eventID, Seq: s.seq, Served: p.Served, ServerNow: p.ServerNow}
	if p.TurnExpiresAt != nil {
		t.TurnExpiresAt = *p.TurnExpiresAt
	}
	return t, nil
}

func (h *StreamUsecase) Close(is inbound.IStream) {
	s := is.(*stream)
	h.mu.Lock()
	if es := h.events[s.eventID]; es != nil {
		h.remove(es, s)
		h.release(s.eventID, es)
	}
	h.mu.Unlock()
	s.end()
}

func (h *StreamUsecase) Drain() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.draining = true
	for _, es := range h.events {
		for s := range es.streams {
			var d time.Duration
			if h.cfg.DrainOver > 0 {
				d = rand.N(h.cfg.DrainOver)
			}
			time.AfterFunc(d, s.end)
		}
	}
}

// Lifetime returns one stream's lifetime: the configured one ±20%.
func Lifetime(base time.Duration) time.Duration {
	return time.Duration(float64(base) * (0.8 + 0.4*rand.Float64()))
}

func (h *StreamUsecase) isDraining() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.draining
}

// remove and release run with mu held.
func (h *StreamUsecase) remove(es *eventStreams, s *stream) {
	if _, ok := es.streams[s]; ok {
		delete(es.streams, s)
		h.total--
	}
}

func (h *StreamUsecase) release(eventID string, es *eventStreams) {
	if len(es.streams) == 0 {
		es.cancel()
		delete(h.events, eventID)
	}
}

func parseFrame(raw []byte) (dto.Frame, error) {
	var v struct {
		State      string `json:"state"`
		Served     int64  `json:"served"`
		StreamFrom int64  `json:"streamFrom"`
		TS         int64  `json:"ts"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return dto.Frame{}, fmt.Errorf("stream: frontier: %w", err)
	}
	return dto.Frame{Data: raw, State: v.State, Served: v.Served, StreamFrom: v.StreamFrom, TS: v.TS}, nil
}
