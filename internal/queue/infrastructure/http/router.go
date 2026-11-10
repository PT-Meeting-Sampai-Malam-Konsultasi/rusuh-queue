package http

import stdhttp "net/http"

// RegisterRoutes registers the queue routes on mux.
func RegisterRoutes(mux *stdhttp.ServeMux, h *QueueHandler) {
	mux.HandleFunc("POST /v1/queue/{eventId}/join", h.JoinQueue)
	mux.HandleFunc("GET /v1/queue/{eventId}/me", h.GetMyPlace)
	mux.HandleFunc("GET /v1/queue/{eventId}/frontier", h.GetFrontier)
	mux.HandleFunc("POST /v1/queue/{eventId}/claim", h.ClaimTurn)
	mux.HandleFunc("DELETE /v1/queue/{eventId}/me", h.LeaveQueue)
}
