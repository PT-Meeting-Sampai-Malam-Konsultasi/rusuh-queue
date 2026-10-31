package http

import (
	stdhttp "net/http"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/problem"
)

// StreamQueue is streamQueue: GET /v1/queue/{eventId}/stream (FR-07, ADR-0015).
func (h *StreamHandler) StreamQueue(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	// TODO(M5): implement streamQueue (IMPLEMENTATION_PLAN.md).
	problem.NotImplemented(w, "streamQueue")
}
