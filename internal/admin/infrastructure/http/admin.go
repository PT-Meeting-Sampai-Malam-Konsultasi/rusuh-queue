package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	stdhttp "net/http"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/problem"
)

const maxBody = 16 << 10

// UpdateQueue is adminUpdateQueue: PATCH /admin/events/{eventId}/queue.
func (h *AdminHandler) UpdateQueue(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	by, err := h.auth.Operator(r.Context(), r)
	switch {
	case errors.Is(err, auth.ErrForbidden):
		problem.Write(w, stdhttp.StatusForbidden, api.ErrorCodeUNAUTHORIZED, "Forbidden", "An operator token with MFA is required")
		return
	case err != nil:
		problem.Write(w, stdhttp.StatusUnauthorized, api.ErrorCodeUNAUTHORIZED, "Unauthorized", "A valid operator token is required")
		return
	}

	var body api.AdminUpdateQueueJSONRequestBody
	dec := json.NewDecoder(stdhttp.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		problem.Write(w, stdhttp.StatusBadRequest, api.ErrorCodeVALIDATIONFAILED, "Invalid body", err.Error())
		return
	}

	f, err := h.service.UpdateQueue(r.Context(), r.PathValue("eventId"), patch(body), by)
	switch {
	case err == nil:
	case errors.Is(err, entity.ErrInvalidSettings):
		problem.Write(w, stdhttp.StatusBadRequest, api.ErrorCodeVALIDATIONFAILED, "Invalid settings", err.Error())
		return
	case errors.Is(err, entity.ErrUnknownEvent):
		problem.Write(w, stdhttp.StatusNotFound, api.ErrorCodeNOTFOUND, "Unknown event", "The event has no live queue")
		return
	default:
		slog.ErrorContext(r.Context(), "adminUpdateQueue failed", "error", err)
		problem.Write(w, stdhttp.StatusInternalServerError, api.ErrorCodeINTERNALERROR, "Internal error", "")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(f)
}

func patch(b api.AdminUpdateQueueJSONRequestBody) entity.Patch {
	var p entity.Patch
	if b.State != nil {
		p.State = entity.Some(string(*b.State))
	}
	if b.MaxConcurrentSlots != nil {
		p.MaxSlots = entity.Some(int64(*b.MaxConcurrentSlots))
	}
	if b.RateCapPerSecond != nil {
		p.RateCap = entity.Some(int64(*b.RateCapPerSecond))
	}
	if b.StreamFrom != nil {
		p.StreamFrom = entity.Some(int64(*b.StreamFrom))
	}
	if b.Reason != nil {
		p.Reason = *b.Reason
	}
	return p
}
