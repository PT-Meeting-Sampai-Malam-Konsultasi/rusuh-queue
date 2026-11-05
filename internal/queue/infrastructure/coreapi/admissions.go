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
	"net/url"
	"strings"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
)

const attempts = 2

type Admissions struct {
	base   string
	secret string
	client *http.Client
}

// New calls core at base (CORE_INTERNAL_URL, e.g. http://core-api.internal/v1);
// timeout bounds each attempt.
func New(base, secret string, timeout time.Duration) *Admissions {
	return &Admissions{base: strings.TrimRight(base, "/"), secret: secret, client: &http.Client{Timeout: timeout}}
}

// Register is safe to repeat: core keys the record by slot and keeps the latest token.
func (a *Admissions) Register(ctx context.Context, rec dto.AdmissionRecord) error {
	body := api.AdmissionRecord{
		Token: rec.Token, EventId: rec.EventID, DeviceId: rec.DeviceID, Seq: rec.Seq,
		ReservedUnits: rec.ReservedUnits, ExpiresAt: rec.ExpiresAt.UTC(),
	}
	if rec.UserID != "" {
		body.UserId = &rec.UserID
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("coreapi: encode admission: %w", err)
	}
	endpoint := a.base + "/internal/admissions/" + url.PathEscape(rec.SlotID)

	var last error
	for range attempts {
		status, err := a.put(ctx, endpoint, payload)
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

func (a *Admissions) put(ctx context.Context, endpoint string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.secret)
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
