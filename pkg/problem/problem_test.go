package problem

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
)

func TestRetryAfter(t *testing.T) {
	rec := httptest.NewRecorder()
	RetryAfter(rec, http.StatusConflict, api.ErrorCodeFULL, "All slots are taken", 2)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want 2", got)
	}

	var p api.Problem
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Code != api.ErrorCodeFULL || p.Status != http.StatusConflict || p.Detail != nil {
		t.Fatalf("problem = %+v", p)
	}
}
