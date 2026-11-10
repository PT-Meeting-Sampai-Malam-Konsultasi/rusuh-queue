package http

import (
	"encoding/json"
	"fmt"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
)

func TestUnrecordedAdmissionIsRetryable(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(stdhttp.MethodPost, "/v1/queue/evt1/claim", nil)
	err := fmt.Errorf("%w: core answered 502", entity.ErrAdmissionUnavailable)

	NewQueue(nil, nil).writeError(w, r, err)

	var p struct{ Code api.ErrorCode }
	_ = json.NewDecoder(w.Body).Decode(&p)
	if w.Code != stdhttp.StatusServiceUnavailable || w.Header().Get("Retry-After") != "2" || p.Code != api.ErrorCodeADMISSIONUNAVAILABLE {
		t.Fatalf("status %d, Retry-After %q, code %q", w.Code, w.Header().Get("Retry-After"), p.Code)
	}
}

func TestClaimWithoutLoginAsksForIt(t *testing.T) {
	w := httptest.NewRecorder()
	NewQueue(nil, nil).writeError(w, httptest.NewRequest(stdhttp.MethodPost, "/v1/queue/evt1/claim", nil), entity.ErrLoginRequired)

	var p struct{ Code api.ErrorCode }
	_ = json.NewDecoder(w.Body).Decode(&p)
	if w.Code != stdhttp.StatusUnauthorized || p.Code != api.ErrorCodeLOGINREQUIRED {
		t.Fatalf("status %d, code %q", w.Code, p.Code)
	}
}

func TestPlaceTellsTheAppToClaimAMovedTurn(t *testing.T) {
	w := httptest.NewRecorder()
	seq := int64(7)
	writePlace(w, &entity.Place{EventID: "evt1", State: "open", Seq: &seq, Claimed: true})

	var got api.QueuePlace
	_ = json.NewDecoder(w.Body).Decode(&got)
	if got.Claimed == nil || !*got.Claimed {
		t.Fatalf("claimed = %v, want true", got.Claimed)
	}
}
