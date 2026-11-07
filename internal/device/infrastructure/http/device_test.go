package http

import (
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_inbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/device/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

func newMux(t *testing.T) (*stdhttp.ServeMux, *mock_inbound.MockIDeviceUsecase) {
	svc := mock_inbound.NewMockIDeviceUsecase(gomock.NewController(t))
	ip, err := clientip.New("", nil)
	if err != nil {
		t.Fatal(err)
	}
	mux := stdhttp.NewServeMux()
	RegisterRoutes(mux, NewDevice(svc, ip))
	return mux, svc
}

func post(mux *stdhttp.ServeMux, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(stdhttp.MethodPost, "/v1/devices", strings.NewReader(body))
	req.RemoteAddr = "203.0.113.7:4242"
	req.Header.Set("X-Turnstile-Token", "ts")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestRegisterDevice(t *testing.T) {
	mux, svc := newMux(t)
	expires := time.Date(2026, 12, 13, 0, 0, 0, 0, time.UTC)
	svc.EXPECT().Register(gomock.Any(), entity.RegisterInput{
		Platform: "android", InstallID: "install-0123456789", Attestation: "att", TurnstileToken: "ts",
		ClientIP: netip.MustParseAddr("203.0.113.7"),
	}).Return(&entity.Registration{DeviceID: "m_1", Token: "dt", ExpiresAt: expires}, nil)

	w := post(mux, `{"platform":"android","installId":"install-0123456789","attestation":"att"}`)
	var got registerResponse
	_ = json.NewDecoder(w.Body).Decode(&got)
	if w.Code != stdhttp.StatusCreated || got.DeviceToken != "dt" || !got.ExpiresAt.Equal(expires) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status %d body %+v", w.Code, got)
	}
}

func TestRegisterDeviceRefusals(t *testing.T) {
	mux, svc := newMux(t)
	if w := post(mux, `{"platform":"web","extra":1}`); w.Code != stdhttp.StatusBadRequest {
		t.Fatalf("bad body: %d", w.Code)
	}
	for err, want := range map[error]struct {
		status     int
		retryAfter string
	}{
		&entity.RateLimitedError{RetryAfter: 2500 * time.Millisecond}: {429, "3"},
		entity.ErrInvalidInput:     {400, ""},
		entity.ErrHumanCheckFailed: {400, ""},
		entity.ErrCheckUnavailable: {503, "2"},
		errors.New("boom"):         {500, ""},
	} {
		svc.EXPECT().Register(gomock.Any(), gomock.Any()).Return(nil, err)
		w := post(mux, `{"platform":"web"}`)
		if w.Code != want.status || w.Header().Get("Retry-After") != want.retryAfter {
			t.Errorf("%v: status %d, Retry-After %q", err, w.Code, w.Header().Get("Retry-After"))
		}
	}
}

func TestGetJwks(t *testing.T) {
	mux, svc := newMux(t)
	svc.EXPECT().JWKS().Return(token.JWKS{})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(stdhttp.MethodGet, "/.well-known/jwks.json", nil))
	if w.Code != stdhttp.StatusOK || w.Header().Get("Cache-Control") != "public, max-age=300" {
		t.Fatalf("status %d", w.Code)
	}
}
