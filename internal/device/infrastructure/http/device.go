package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	stdhttp "net/http"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/problem"
)

const maxBody = 16 << 10

type registerResponse struct {
	DeviceToken string    `json:"deviceToken"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

// RegisterDevice is registerDevice: POST /v1/devices (FR-04, NFR-07).
func (h *DeviceHandler) RegisterDevice(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	var body api.RegisterDeviceJSONBody
	dec := json.NewDecoder(stdhttp.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		problem.Write(w, stdhttp.StatusBadRequest, api.ErrorCodeVALIDATIONFAILED, "Invalid request body", "")
		return
	}

	in := entity.RegisterInput{
		Platform:       entity.Platform(body.Platform),
		TurnstileToken: r.Header.Get("X-Turnstile-Token"),
		ClientIP:       h.ip.IP(r),
	}
	if body.InstallId != nil {
		in.InstallID = *body.InstallId
	}
	if body.Attestation != nil {
		in.Attestation = *body.Attestation
	}

	reg, err := h.service.Register(r.Context(), in)
	var limited *entity.RateLimitedError
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(stdhttp.StatusCreated)
		_ = json.NewEncoder(w).Encode(registerResponse{DeviceToken: reg.Token, ExpiresAt: reg.ExpiresAt})
	case errors.As(err, &limited):
		problem.RetryAfter(w, stdhttp.StatusTooManyRequests, api.ErrorCodeRATELIMITED, "Too many registrations", seconds(limited.RetryAfter))
	case errors.Is(err, entity.ErrInvalidInput):
		problem.Write(w, stdhttp.StatusBadRequest, api.ErrorCodeVALIDATIONFAILED, "Invalid device registration", err.Error())
	case errors.Is(err, entity.ErrHumanCheckFailed):
		problem.Write(w, stdhttp.StatusBadRequest, api.ErrorCodeVALIDATIONFAILED, "Verification failed", "Turnstile or app attestation was not accepted")
	case errors.Is(err, entity.ErrCheckUnavailable):
		slog.WarnContext(r.Context(), "device check unavailable", "error", err)
		problem.RetryAfter(w, stdhttp.StatusServiceUnavailable, api.ErrorCodeVERIFICATIONUNAVAILABLE, "Verification is busy, try again", 2)
	default:
		slog.ErrorContext(r.Context(), "register device failed", "error", err)
		problem.Write(w, stdhttp.StatusInternalServerError, api.ErrorCodeINTERNALERROR, "Internal error", "")
	}
}

// GetJwks is getJwks: GET /.well-known/jwks.json (D-3).
func (h *DeviceHandler) GetJwks(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_ = json.NewEncoder(w).Encode(h.service.JWKS())
}

func seconds(d time.Duration) int {
	return max(1, int(math.Ceil(d.Seconds())))
}
