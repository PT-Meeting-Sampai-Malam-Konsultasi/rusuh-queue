package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReady(t *testing.T) {
	ok := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("down") }

	tests := []struct {
		name   string
		checks map[string]Check
		drain  bool
		want   int
	}{
		{"all checks pass", map[string]Check{"postgres": ok, "valkey": ok}, false, http.StatusOK},
		{"one check fails", map[string]Check{"postgres": ok, "valkey": down}, false, http.StatusServiceUnavailable},
		{"draining", map[string]Check{"postgres": ok}, true, http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(tt.checks)
			if tt.drain {
				h.Drain()
			}
			mux := http.NewServeMux()
			h.RegisterRoutes(mux)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.want, rec.Body)
			}
		})
	}
}

func TestLiveWhileDraining(t *testing.T) {
	h := New(nil)
	h.Drain()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
