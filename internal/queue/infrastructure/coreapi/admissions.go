// Package coreapi records admissions in core (registerAdmission) before the
// user gets the token.
package coreapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
)

const attempts = 2

type Admissions struct {
	base   string
	secret string
	client *http.Client
}

// New calls core at base (CORE_INTERNAL_URL, core's origin, e.g. http://core:3000);
// timeout bounds each attempt.
func New(base, secret string, timeout time.Duration) *Admissions {
	return &Admissions{base: strings.TrimRight(base, "/"), secret: secret, client: &http.Client{Timeout: timeout}}
}

type admission struct {
	AdmissionToken string    `json:"admission_token"`
	UserID         string    `json:"user_id"`
	EventID        string    `json:"event_id"`
	SlotID         string    `json:"slot_id"`
	DeviceID       string    `json:"device_id"`
	MaxTickets     int       `json:"max_tickets"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// Register is safe to repeat: core keys the record by admission token and overwrites it.
func (a *Admissions) Register(ctx context.Context, rec dto.AdmissionRecord) error {
	payload, err := json.Marshal(admission{
		AdmissionToken: rec.Token, UserID: rec.UserID, EventID: rec.EventID, SlotID: rec.SlotID, DeviceID: rec.DeviceID,
		MaxTickets: rec.ReservedUnits, ExpiresAt: rec.ExpiresAt.UTC(),
	})
	if err != nil {
		return fmt.Errorf("coreapi: encode admission: %w", err)
	}
	endpoint := a.base + "/v1/core/admission"

	var last error
	for range attempts {
		status, err := a.post(ctx, endpoint, payload)
		switch {
		case err == nil && status >= 200 && status < 300:
			return nil
		case err == nil && status < 500:
			return fmt.Errorf("coreapi: registerAdmission answered %d", status)
		case err == nil:
			last = fmt.Errorf("registerAdmission answered %d", status)
		default:
			last = err
		}
		if ctx.Err() != nil {
			break
		}
	}
	return fmt.Errorf("%w: %v", entity.ErrAdmissionUnavailable, last)
}

func (a *Admissions) post(ctx context.Context, endpoint string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Service-Key", a.secret)
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	return resp.StatusCode, nil
}

// Disabled is used when CORE_INTERNAL_URL is empty: claims behave as before
// registerAdmission existed (lab, fakecore without the endpoint).
type Disabled struct{}

func (Disabled) Register(context.Context, dto.AdmissionRecord) error { return nil }
