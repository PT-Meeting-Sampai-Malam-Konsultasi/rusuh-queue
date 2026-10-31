// Package health serves liveness and readiness. Readiness fails while the
// process drains after SIGTERM, so the load balancer stops sending new
// requests and SSE streams before the server closes.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// Check reports whether one dependency is usable.
type Check func(ctx context.Context) error

type Handler struct {
	checks   map[string]Check
	draining atomic.Bool
	timeout  time.Duration
}

func New(checks map[string]Check) *Handler {
	return &Handler{checks: checks, timeout: time.Second}
}

// Drain marks the process as shutting down; /readyz answers 503 from now on.
func (h *Handler) Drain() { h.draining.Store(true) }

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.Live)
	mux.HandleFunc("GET /readyz", h.Ready)
}

// Live answers 200 while the process runs.
func (h *Handler) Live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready answers 200 only when not draining and every check passes.
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	failed := map[string]string{}
	for name, check := range h.checks {
		if err := check(ctx); err != nil {
			failed[name] = err.Error()
		}
	}
	if len(failed) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "unavailable", "failed": failed})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
