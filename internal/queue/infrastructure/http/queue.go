package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	stdhttp "net/http"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/problem"
)

// frontierTTL: each task answers frontier requests from memory for 250 ms, so
// even uncached traffic (a CDN miss storm) costs Valkey ~4 reads/s per event per task.
const frontierTTL = 250 * time.Millisecond

type cachedFrontier struct {
	body []byte
	at   time.Time
}

// placeResponse is QueuePlace; seq is null (not omitted) while waiting for
// the draw, and turnExpiresAt is null until the turn is reached.
type placeResponse struct {
	EventID         string     `json:"eventId"`
	State           string     `json:"state"`
	Seq             *int64     `json:"seq"`
	Served          int64      `json:"served"`
	OpensAt         time.Time  `json:"opensAt"`
	TurnExpiresAt   *time.Time `json:"turnExpiresAt"`
	ServerNow       time.Time  `json:"serverNow"`
	QueueToken      string     `json:"queueToken"`
	AccountAttached bool       `json:"accountAttached"`
}

// JoinQueue is joinQueue: POST /v1/queue/{eventId}/join (FR-04, FR-10, BR-07).
func (h *QueueHandler) JoinQueue(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	deviceID, err := h.auth.Device(r)
	if err != nil {
		unauthorized(w, "A valid X-Device-Token is required")
		return
	}
	userID, err := h.auth.Account(r.Context(), r)
	if err != nil {
		unauthorized(w, "The access token is not valid; log in again or join as a guest")
		return
	}

	place, err := h.service.Join(r.Context(), entity.JoinInput{EventID: r.PathValue("eventId"), DeviceID: deviceID, UserID: userID})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writePlace(w, place)
}

// GetMyPlace is getMyPlace: GET /v1/queue/{eventId}/me (FR-06, after the draw).
func (h *QueueHandler) GetMyPlace(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	eventID := r.PathValue("eventId")
	deviceID, err := h.auth.Device(r)
	if err != nil {
		unauthorized(w, "A valid X-Device-Token is required")
		return
	}
	claims, err := h.auth.Place(r, eventID, deviceID)
	if err != nil {
		unauthorized(w, "A valid X-Queue-Token for this device and event is required")
		return
	}

	place, err := h.service.MyPlace(r.Context(), eventID, deviceID, claims.Seq)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writePlace(w, place)
}

// GetFrontier is getFrontier: GET /v1/queue/{eventId}/frontier (FR-06, FR-07, NFR-06).
// Same bytes for every device; CloudFront caches it for 1 s.
func (h *QueueHandler) GetFrontier(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	eventID := r.PathValue("eventId")
	body, err := h.cachedFrontier(r, eventID)
	if err != nil {
		if errors.Is(err, entity.ErrUnknownEvent) {
			w.Header().Set("Cache-Control", "public, max-age=1")
		}
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=1")
	_, _ = w.Write(body)
}

// ClaimTurn is claimTurn: POST /v1/queue/{eventId}/claim (FR-08, FR-17, ADR-0004).
func (h *QueueHandler) ClaimTurn(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	eventID := r.PathValue("eventId")
	deviceID, err := h.auth.Device(r)
	if err != nil {
		unauthorized(w, "A valid X-Device-Token is required")
		return
	}
	claims, err := h.auth.Place(r, eventID, deviceID)
	if err != nil {
		unauthorized(w, "A valid X-Queue-Token for this device and event is required")
		return
	}

	adm, err := h.service.Claim(r.Context(), eventID, deviceID, claims.Seq)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	loginRequired := adm.LoginRequired
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(api.Admission{
		AdmissionToken: adm.Token,
		SlotId:         adm.SlotID,
		ReservedUnits:  adm.ReservedUnits,
		LoginRequired:  &loginRequired,
		ExpiresAt:      adm.ExpiresAt.UTC(),
		ServerNow:      adm.ServerNow.UTC(),
	})
}

func (h *QueueHandler) cachedFrontier(r *stdhttp.Request, eventID string) ([]byte, error) {
	h.mu.RLock()
	c, ok := h.frontier[eventID]
	h.mu.RUnlock()
	if ok && h.now().Sub(c.at) < frontierTTL {
		return c.body, nil
	}
	v, err, _ := h.group.Do(eventID, func() (any, error) {
		body, err := h.service.Frontier(r.Context(), eventID)
		if err != nil {
			return nil, err
		}
		h.mu.Lock()
		h.frontier[eventID] = cachedFrontier{body: body, at: h.now()}
		h.mu.Unlock()
		return body, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (h *QueueHandler) writeError(w stdhttp.ResponseWriter, r *stdhttp.Request, err error) {
	var limited *entity.RateLimitedError
	switch {
	case errors.As(err, &limited):
		problem.RetryAfter(w, stdhttp.StatusTooManyRequests, api.ErrorCodeRATELIMITED, "Too many requests", seconds(limited.RetryAfter))
	case errors.Is(err, entity.ErrUnknownEvent):
		problem.Write(w, stdhttp.StatusNotFound, api.ErrorCodeNOTFOUND, "Unknown event", "")
	case errors.Is(err, entity.ErrClosed):
		problem.Write(w, stdhttp.StatusGone, api.ErrorCodeQUEUECLOSED, "Sold out", "The queue is closed because every ticket is sold")
	case errors.Is(err, entity.ErrAccountElsewhere):
		problem.Write(w, stdhttp.StatusConflict, api.ErrorCodeACCOUNTHASPLACEELSEWHERE, "Already queueing on another device", "")
	case errors.Is(err, entity.ErrDeviceOtherAccount):
		problem.Write(w, stdhttp.StatusConflict, api.ErrorCodeACCOUNTHASPLACEELSEWHERE, "This place belongs to another account", "")
	case errors.Is(err, entity.ErrPlaceExpired):
		problem.Write(w, stdhttp.StatusGone, api.ErrorCodeEXPIRED, "Your place has ended", "Join again to queue at the back")
	case errors.Is(err, entity.ErrNotYet):
		problem.RetryAfter(w, stdhttp.StatusConflict, api.ErrorCodeNOTYET, "Your turn has not come yet", 1)
	case errors.Is(err, entity.ErrFull):
		// 1-2 s with jitter, so refused claimers do not come back in one burst
		problem.RetryAfter(w, stdhttp.StatusConflict, api.ErrorCodeFULL, "Every slot is in use", 1+rand.IntN(2))
	case errors.Is(err, entity.ErrNoStock):
		problem.Write(w, stdhttp.StatusConflict, api.ErrorCodeNOSTOCK, "No tickets free right now",
			"Every ticket is reserved or held; the queue resumes if some come back")
	case errors.Is(err, entity.ErrMissed):
		problem.Write(w, stdhttp.StatusGone, api.ErrorCodeMISSED, "Your turn has passed", "Join again to queue at the back")
	case errors.Is(err, entity.ErrPlaceMismatch):
		unauthorized(w, "The queue token does not match this device's place")
	default:
		slog.ErrorContext(r.Context(), "queue request failed", "error", err)
		problem.Write(w, stdhttp.StatusInternalServerError, api.ErrorCodeINTERNALERROR, "Internal error", "")
	}
}

func writePlace(w stdhttp.ResponseWriter, p *entity.Place) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	resp := placeResponse{
		EventID:         p.EventID,
		State:           p.State,
		Seq:             p.Seq,
		Served:          p.Served,
		OpensAt:         p.OpensAt.UTC(),
		ServerNow:       p.ServerNow.UTC(),
		QueueToken:      p.QueueToken,
		AccountAttached: p.AccountAttached,
	}
	if p.TurnExpiresAt != nil {
		t := p.TurnExpiresAt.UTC()
		resp.TurnExpiresAt = &t
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func unauthorized(w stdhttp.ResponseWriter, detail string) {
	problem.Write(w, stdhttp.StatusUnauthorized, api.ErrorCodeUNAUTHORIZED, "Unauthorized", detail)
}

func seconds(d time.Duration) int {
	return max(1, int(math.Ceil(d.Seconds())))
}

// LeaveQueue is leaveQueue: DELETE /v1/queue/{eventId}/me ("Keluar dari antrian").
func (h *QueueHandler) LeaveQueue(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	eventID := r.PathValue("eventId")
	deviceID, err := h.auth.Device(r)
	if err != nil {
		unauthorized(w, "A valid X-Device-Token is required")
		return
	}
	if _, err := h.auth.Place(r, eventID, deviceID); err != nil {
		unauthorized(w, "A valid X-Queue-Token for this device and event is required")
		return
	}
	if err := h.service.Leave(r.Context(), eventID, deviceID); err != nil {
		h.writeError(w, r, err)
		return
	}
	w.WriteHeader(stdhttp.StatusNoContent)
}
