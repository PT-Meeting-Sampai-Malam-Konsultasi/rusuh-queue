package http

import (
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/mock/gomock"

	mock_inbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/admin/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

const issuer = "rusuh-admin"

type rig struct {
	mux     *stdhttp.ServeMux
	service *mock_inbound.MockIAdminUsecase
	sso     *testkit.Issuer
}

func newRig(t *testing.T) rig {
	sso := testkit.NewIssuer(t)
	svc := mock_inbound.NewMockIAdminUsecase(gomock.NewController(t))
	mux := stdhttp.NewServeMux()
	RegisterRoutes(mux, NewAdmin(svc, auth.NewAdmin(sso.URL(), issuer)))
	return rig{mux: mux, service: svc, sso: sso}
}

func (r rig) patch(tok, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(stdhttp.MethodPatch, "/admin/events/evt1/queue", strings.NewReader(body))
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	w := httptest.NewRecorder()
	r.mux.ServeHTTP(w, req)
	return w
}

func (r rig) operator(t *testing.T, amr ...string) string {
	return r.sso.Token(t, issuer, "op_1", amr, auth.AdminAudience)
}

func TestUpdateQueue(t *testing.T) {
	r := newRig(t)
	r.service.EXPECT().UpdateQueue(gomock.Any(), "evt1", entity.Patch{
		State: entity.Some("paused"), MaxSlots: entity.Some(int64(100)), RateCap: entity.Some(int64(5)),
		StreamFrom: entity.Some(int64(0)), Reason: "incident",
	}, "op_1").Return([]byte(`{"state":"paused"}`), nil)
	w := r.patch(r.operator(t, "pwd", "mfa"), `{"state":"paused","maxConcurrentSlots":100,"rateCapPerSecond":5,"streamFrom":0,"reason":"incident"}`)
	if w.Code != stdhttp.StatusOK || w.Body.String() != `{"state":"paused"}` || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status %d body %s", w.Code, w.Body)
	}
}

func TestUpdateQueueRefusals(t *testing.T) {
	r := newRig(t)
	mfa := r.operator(t, "mfa")
	if w := r.patch("", `{}`); w.Code != stdhttp.StatusUnauthorized {
		t.Errorf("no token: %d", w.Code)
	}
	if w := r.patch(r.operator(t, "pwd"), `{}`); w.Code != stdhttp.StatusForbidden {
		t.Errorf("no mfa: %d", w.Code)
	}
	if w := r.patch(mfa, `{"state":`); w.Code != stdhttp.StatusBadRequest {
		t.Errorf("bad body: %d", w.Code)
	}

	for err, want := range map[error]int{
		entity.ErrInvalidSettings: stdhttp.StatusBadRequest,
		entity.ErrUnknownEvent:    stdhttp.StatusNotFound,
		errors.New("boom"):        stdhttp.StatusInternalServerError,
	} {
		r.service.EXPECT().UpdateQueue(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, err)
		if w := r.patch(mfa, `{"state":"open"}`); w.Code != want {
			t.Errorf("%v: %d, want %d", err, w.Code, want)
		}
	}

	r2 := newRig(t)
	r2.sso.Down.Store(true)
	if w := r2.patch(r2.operator(t, "mfa"), `{}`); w.Code != stdhttp.StatusServiceUnavailable {
		t.Errorf("sso down: %d", w.Code)
	}
}
