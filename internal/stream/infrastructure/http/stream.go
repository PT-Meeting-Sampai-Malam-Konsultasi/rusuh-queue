package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	stdhttp "net/http"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/application/usecase"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/problem"
)

// writeTimeout bounds each write: the server has no WriteTimeout, because a
// stream lives for minutes, so a dead client is caught per write instead.
const writeTimeout = 10 * time.Second

// StreamQueue is streamQueue: GET /v1/queue/{eventId}/stream (FR-07, ADR-0015).
func (h *StreamHandler) StreamQueue(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	eventID := r.PathValue("eventId")
	deviceID, err := h.auth.Device(r)
	if err != nil {
		problem.Write(w, stdhttp.StatusUnauthorized, api.ErrorCodeUNAUTHORIZED, "Unauthorized", "A valid X-Device-Token is required")
		return
	}
	claims, err := h.auth.Place(r, eventID, deviceID)
	if err != nil {
		problem.Write(w, stdhttp.StatusUnauthorized, api.ErrorCodeUNAUTHORIZED, "Unauthorized",
			"A valid X-Queue-Token for this device and event is required")
		return
	}
	s, err := h.service.Open(r.Context(), dto.OpenInput{EventID: eventID, DeviceID: deviceID, TokenSeq: claims.Seq})
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer h.service.Close(s)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(stdhttp.StatusOK)
	out := &sse{w: w, rc: stdhttp.NewResponseController(w)}

	if !out.send("retry: 3000\n\n") || !h.frame(r, out, s, s.Initial()) {
		return
	}
	heartbeat := time.NewTicker(h.cfg.Heartbeat)
	defer heartbeat.Stop()
	lifetime := time.NewTimer(usecase.Lifetime(h.cfg.Lifetime))
	defer lifetime.Stop()
	for {
		select {
		case f := <-s.Frames():
			if !h.frame(r, out, s, f) {
				return
			}
		case <-heartbeat.C:
			if !out.send(": hb\n\n") {
				return
			}
		case <-lifetime.C:
			return
		case <-s.Done():
			return
		case <-r.Context().Done():
			return
		}
	}
}

// frame sends one frontier, then the turn once served reaches the seq. It
// reports whether the stream goes on.
func (h *StreamHandler) frame(r *stdhttp.Request, out *sse, s inbound.IStream, f dto.Frame) bool {
	if !out.send(fmt.Sprintf("event: frontier\nid: %d\ndata: %s\n\n", f.TS, f.Data)) {
		return false
	}
	if f.State == "closed" {
		return false
	}
	if f.Served < s.Seq() {
		return true
	}
	t, err := h.service.Turn(r.Context(), s)
	if err != nil {
		// the place moved or ended meanwhile: the client finds out on claimTurn
		slog.InfoContext(r.Context(), "stream: no turn to send", "error", err)
		return false
	}
	data, _ := json.Marshal(api.QueueTurn{
		Event: t.EventID, Seq: int(t.Seq), Served: int(t.Served),
		TurnExpiresAt: t.TurnExpiresAt.UTC(), ServerNow: t.ServerNow.UTC(),
	})
	out.send(fmt.Sprintf("event: turn\ndata: %s\n\n", data))
	return false
}

type sse struct {
	w  stdhttp.ResponseWriter
	rc *stdhttp.ResponseController
}

func (s *sse) send(text string) bool {
	_ = s.rc.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := s.w.Write([]byte(text)); err != nil {
		return false
	}
	return s.rc.Flush() == nil
}

func writeError(w stdhttp.ResponseWriter, r *stdhttp.Request, err error) {
	var limited *entity.RateLimitedError
	switch {
	case errors.As(err, &limited):
		problem.RetryAfter(w, stdhttp.StatusTooManyRequests, api.ErrorCodeRATELIMITED, "Too many requests",
			max(1, int(math.Ceil(limited.RetryAfter.Seconds()))))
	case errors.Is(err, entity.ErrTooFar):
		problem.Write(w, stdhttp.StatusConflict, api.ErrorCodeTOOFAR, "Still far from the front",
			"Your position is above streamFrom; keep polling the frontier")
	case errors.Is(err, entity.ErrFull), errors.Is(err, entity.ErrDraining):
		// the client polls meanwhile and tries the stream again later (ADR-0015 fallback)
		problem.RetryAfter(w, stdhttp.StatusServiceUnavailable, api.ErrorCodeSTREAMFULL, "No stream right now; keep polling", 5)
	case errors.Is(err, entity.ErrClosed):
		problem.Write(w, stdhttp.StatusGone, api.ErrorCodeQUEUECLOSED, "Sold out", "The queue is closed")
	case errors.Is(err, entity.ErrMoved):
		problem.Write(w, stdhttp.StatusGone, api.ErrorCodeMOVED, "Your place moved to another device", "")
	case errors.Is(err, entity.ErrMissed):
		problem.Write(w, stdhttp.StatusGone, api.ErrorCodeMISSED, "Your turn has passed", "Join again to queue at the back")
	case errors.Is(err, entity.ErrPlaceExpired):
		problem.Write(w, stdhttp.StatusGone, api.ErrorCodeEXPIRED, "Your place has ended", "Join again to queue at the back")
	case errors.Is(err, entity.ErrUnknownEvent):
		problem.Write(w, stdhttp.StatusNotFound, api.ErrorCodeNOTFOUND, "Unknown event", "")
	case errors.Is(err, entity.ErrPlaceMismatch):
		problem.Write(w, stdhttp.StatusUnauthorized, api.ErrorCodeUNAUTHORIZED, "Unauthorized",
			"The queue token does not match this device's place")
	default:
		slog.ErrorContext(r.Context(), "stream request failed", "error", err)
		problem.Write(w, stdhttp.StatusInternalServerError, api.ErrorCodeINTERNALERROR, "Internal error", "")
	}
}
