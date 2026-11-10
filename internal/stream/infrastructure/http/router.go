package http

import stdhttp "net/http"

// RegisterRoutes registers the stream routes on mux.
func RegisterRoutes(mux *stdhttp.ServeMux, h *StreamHandler) {
	mux.HandleFunc("GET /v1/queue/{eventId}/stream", h.StreamQueue)
}
