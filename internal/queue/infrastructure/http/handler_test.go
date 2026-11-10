package http

import (
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_inbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/queue/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

type rig struct {
	mux     *stdhttp.ServeMux
	h       *QueueHandler
	service *mock_inbound.MockIQueueUsecase
	tokens  testkit.Tokens
}

func newRig(t *testing.T) rig {
	tk := testkit.NewTokens(t)
	svc := mock_inbound.NewMockIQueueUsecase(gomock.NewController(t))
	h := NewQueue(svc, tk.Verifier())
	mux := stdhttp.NewServeMux()
	RegisterRoutes(mux, h)
	return rig{mux: mux, h: h, service: svc, tokens: tk}
}

func (r rig) do(method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	var req *stdhttp.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.mux.ServeHTTP(w, req)
	return w
}

func (r rig) device(t *testing.T) map[string]string {
	return map[string]string{"X-Device-Token": r.tokens.Device(t, "w_1")}
}

func (r rig) placed(t *testing.T, seq int64) map[string]string {
	h := r.device(t)
	h["X-Queue-Token"] = r.tokens.Queue(t, "w_1", "evt1", seq)
	return h
}

func code(t *testing.T, w *httptest.ResponseRecorder) api.ErrorCode {
	t.Helper()
	var p struct{ Code api.ErrorCode }
	if err := json.NewDecoder(w.Body).Decode(&p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	return p.Code
}

func place(seq int64) *entity.Place {
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.FixedZone("WIB", 7*3600))
	turn := now.Add(5 * time.Minute)
	return &entity.Place{
		EventID: "evt1", State: "open", Seq: &seq, Served: 3, OpensAt: now, TurnExpiresAt: &turn,
		ServerNow: now, QueueToken: "qt", AccountAttached: true,
	}
}

func TestJoinQueue(t *testing.T) {
	r := newRig(t)
	r.service.EXPECT().Join(gomock.Any(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1", UserID: "user_1"}).Return(place(4), nil)
	h := r.device(t)
	h["Authorization"] = "Bearer " + r.tokens.Core.Access(t, "user_1")
	w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/join", `{"takeOver":true}`, h)

	var got api.QueuePlace
	_ = json.NewDecoder(w.Body).Decode(&got)
	if w.Code != stdhttp.StatusOK || w.Header().Get("Cache-Control") != "no-store" || *got.Seq != 4 ||
		got.TurnExpiresAt == nil || got.TurnExpiresAt.Location() != time.UTC || got.QueueToken != "qt" {
		t.Fatalf("status %d, body %+v", w.Code, got)
	}

	r.service.EXPECT().Join(gomock.Any(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1"}).Return(&entity.Place{EventID: "evt1", State: "preopen"}, nil)
	w = r.do(stdhttp.MethodPost, "/v1/queue/evt1/join", "", r.device(t))
	if w.Code != stdhttp.StatusOK || !strings.Contains(w.Body.String(), `"seq":null`) || !strings.Contains(w.Body.String(), `"turnExpiresAt":null`) {
		t.Fatalf("guest join: %d %s", w.Code, w.Body)
	}
}

func TestJoinQueueRefusals(t *testing.T) {
	r := newRig(t)
	if w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/join", "", nil); w.Code != stdhttp.StatusUnauthorized {
		t.Fatalf("no device token: %d", w.Code)
	}
	if w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/join", `{"unknown":1}`, r.device(t)); w.Code != stdhttp.StatusBadRequest || code(t, w) != api.ErrorCodeVALIDATIONFAILED {
		t.Fatalf("bad body: %d", w.Code)
	}

	h := r.device(t)
	h["Authorization"] = "Basic abc"
	if w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/join", "", h); w.Code != stdhttp.StatusUnauthorized || code(t, w) != api.ErrorCodeUNAUTHORIZED {
		t.Fatalf("bad access token: %d", w.Code)
	}

	r.tokens.Core.Down.Store(true)
	h["Authorization"] = "Bearer " + r.tokens.Core.Access(t, "user_1")
	w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/join", "", h)
	if w.Code != stdhttp.StatusServiceUnavailable || w.Header().Get("Retry-After") != "5" || code(t, w) != api.ErrorCodeVERIFICATIONUNAVAILABLE {
		t.Fatalf("core down: %d", w.Code)
	}

	r.service.EXPECT().Join(gomock.Any(), gomock.Any()).Return(nil, entity.ErrClosed)
	if w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/join", "", r.device(t)); w.Code != stdhttp.StatusGone {
		t.Fatalf("closed: %d", w.Code)
	}
}

func TestGetMyPlace(t *testing.T) {
	r := newRig(t)
	r.service.EXPECT().MyPlace(gomock.Any(), "evt1", "w_1", int64(9)).Return(place(9), nil)
	if w := r.do(stdhttp.MethodGet, "/v1/queue/evt1/me", "", r.placed(t, 9)); w.Code != stdhttp.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	r.service.EXPECT().MyPlace(gomock.Any(), "evt1", "w_1", int64(9)).Return(nil, entity.ErrPlaceMismatch)
	if w := r.do(stdhttp.MethodGet, "/v1/queue/evt1/me", "", r.placed(t, 9)); w.Code != stdhttp.StatusUnauthorized {
		t.Fatalf("mismatch: %d", w.Code)
	}
}

func TestPlaceTokensAreRequired(t *testing.T) {
	r := newRig(t)
	other := r.device(t)
	other["X-Queue-Token"] = r.tokens.Queue(t, "w_1", "evt2", 1)
	for _, ep := range []struct{ method, path string }{
		{stdhttp.MethodGet, "/v1/queue/evt1/me"},
		{stdhttp.MethodPost, "/v1/queue/evt1/claim"},
		{stdhttp.MethodDelete, "/v1/queue/evt1/me"},
	} {
		if w := r.do(ep.method, ep.path, "", nil); w.Code != stdhttp.StatusUnauthorized {
			t.Errorf("%s %s without device: %d", ep.method, ep.path, w.Code)
		}
		if w := r.do(ep.method, ep.path, "", r.device(t)); w.Code != stdhttp.StatusUnauthorized {
			t.Errorf("%s %s without queue token: %d", ep.method, ep.path, w.Code)
		}
		if w := r.do(ep.method, ep.path, "", other); w.Code != stdhttp.StatusUnauthorized {
			t.Errorf("%s %s with another event's token: %d", ep.method, ep.path, w.Code)
		}
	}
}

func TestGetFrontierIsCachedBriefly(t *testing.T) {
	r := newRig(t)
	now := time.Unix(1_800_000_000, 0)
	r.h.now = func() time.Time { return now }
	r.service.EXPECT().Frontier(gomock.Any(), "evt1").Return([]byte(`{"served":1}`), nil)
	for range 2 {
		w := r.do(stdhttp.MethodGet, "/v1/queue/evt1/frontier", "", nil)
		if w.Code != stdhttp.StatusOK || w.Body.String() != `{"served":1}` || w.Header().Get("Cache-Control") != "public, max-age=1" {
			t.Fatalf("status %d body %s", w.Code, w.Body)
		}
	}

	now = now.Add(frontierTTL)
	r.service.EXPECT().Frontier(gomock.Any(), "evt1").Return([]byte(`{"served":2}`), nil)
	if w := r.do(stdhttp.MethodGet, "/v1/queue/evt1/frontier", "", nil); w.Body.String() != `{"served":2}` {
		t.Fatalf("stale frontier %s", w.Body)
	}

	r.service.EXPECT().Frontier(gomock.Any(), "nope").Return(nil, entity.ErrUnknownEvent)
	w := r.do(stdhttp.MethodGet, "/v1/queue/nope/frontier", "", nil)
	if w.Code != stdhttp.StatusNotFound || w.Header().Get("Cache-Control") != "public, max-age=1" {
		t.Fatalf("unknown event: %d %q", w.Code, w.Header().Get("Cache-Control"))
	}
	r.service.EXPECT().Frontier(gomock.Any(), "evt2").Return(nil, errors.New("valkey down"))
	if w := r.do(stdhttp.MethodGet, "/v1/queue/evt2/frontier", "", nil); w.Code != stdhttp.StatusInternalServerError || w.Header().Get("Cache-Control") == "public, max-age=1" {
		t.Fatalf("failure: %d", w.Code)
	}
}

func TestClaimTurn(t *testing.T) {
	r := newRig(t)
	expires := time.Date(2026, 11, 13, 10, 5, 0, 0, time.FixedZone("WIB", 7*3600))
	r.service.EXPECT().Claim(gomock.Any(), "evt1", "w_1", "user_1", int64(9)).Return(&entity.Admission{
		Token: "adm", SlotID: "s_1", ReservedUnits: 3, ExpiresAt: expires, ServerNow: expires.Add(-5 * time.Minute),
	}, nil)
	h := r.placed(t, 9)
	h["Authorization"] = "Bearer " + r.tokens.Core.Access(t, "user_1")
	w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/claim", "", h)
	var got api.Admission
	_ = json.NewDecoder(w.Body).Decode(&got)
	if w.Code != stdhttp.StatusOK || got.AdmissionToken != "adm" || got.SlotId != "s_1" || got.ReservedUnits != 3 ||
		got.LoginRequired == nil || *got.LoginRequired || !got.ExpiresAt.Equal(expires) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status %d, body %+v", w.Code, got)
	}

	h["Authorization"] = "Bearer x"
	if w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/claim", "", h); w.Code != stdhttp.StatusUnauthorized {
		t.Fatalf("bad access token: %d", w.Code)
	}

	r.service.EXPECT().Claim(gomock.Any(), "evt1", "w_1", "", int64(9)).Return(nil, entity.ErrNotYet)
	if w := r.do(stdhttp.MethodPost, "/v1/queue/evt1/claim", "", r.placed(t, 9)); w.Code != stdhttp.StatusConflict || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("not yet: %d", w.Code)
	}
}

func TestLeaveQueue(t *testing.T) {
	r := newRig(t)
	r.service.EXPECT().Leave(gomock.Any(), "evt1", "w_1").Return(nil)
	if w := r.do(stdhttp.MethodDelete, "/v1/queue/evt1/me", "", r.placed(t, 2)); w.Code != stdhttp.StatusNoContent {
		t.Fatalf("status %d", w.Code)
	}
	r.service.EXPECT().Leave(gomock.Any(), "evt1", "w_1").Return(entity.ErrUnknownEvent)
	if w := r.do(stdhttp.MethodDelete, "/v1/queue/evt1/me", "", r.placed(t, 2)); w.Code != stdhttp.StatusNotFound {
		t.Fatalf("unknown: %d", w.Code)
	}
}

func TestWriteErrorMapsEveryDomainError(t *testing.T) {
	for _, tc := range []struct {
		err        error
		status     int
		code       api.ErrorCode
		retryAfter string
	}{
		{&entity.RateLimitedError{RetryAfter: 1500 * time.Millisecond}, 429, api.ErrorCodeRATELIMITED, "2"},
		{&entity.RateLimitedError{}, 429, api.ErrorCodeRATELIMITED, "1"},
		{entity.ErrUnknownEvent, 404, api.ErrorCodeNOTFOUND, ""},
		{entity.ErrClosed, 410, api.ErrorCodeQUEUECLOSED, ""},
		{entity.ErrAccountElsewhere, 409, api.ErrorCodeACCOUNTHASPLACEELSEWHERE, ""},
		{entity.ErrTransferLimit, 409, api.ErrorCodeACCOUNTHASPLACEELSEWHERE, ""},
		{entity.ErrMoved, 410, api.ErrorCodeMOVED, ""},
		{entity.ErrDeviceOtherAccount, 409, api.ErrorCodeACCOUNTHASPLACEELSEWHERE, ""},
		{entity.ErrPlaceExpired, 410, api.ErrorCodeEXPIRED, ""},
		{entity.ErrNotYet, 409, api.ErrorCodeNOTYET, "1"},
		{entity.ErrNoStock, 409, api.ErrorCodeNOSTOCK, ""},
		{entity.ErrMissed, 410, api.ErrorCodeMISSED, ""},
		{entity.ErrPlaceMismatch, 401, api.ErrorCodeUNAUTHORIZED, ""},
		{errors.New("boom"), 500, api.ErrorCodeINTERNALERROR, ""},
	} {
		w := httptest.NewRecorder()
		NewQueue(nil, nil).writeError(w, httptest.NewRequest(stdhttp.MethodGet, "/", nil), tc.err)
		if w.Code != tc.status || code(t, w) != tc.code || w.Header().Get("Retry-After") != tc.retryAfter {
			t.Errorf("%v: status %d, Retry-After %q", tc.err, w.Code, w.Header().Get("Retry-After"))
		}
	}

	w := httptest.NewRecorder()
	NewQueue(nil, nil).writeError(w, httptest.NewRequest(stdhttp.MethodGet, "/", nil), entity.ErrFull)
	if ra := w.Header().Get("Retry-After"); w.Code != 409 || (ra != "1" && ra != "2") {
		t.Fatalf("full: status %d, Retry-After %q", w.Code, ra)
	}
}
