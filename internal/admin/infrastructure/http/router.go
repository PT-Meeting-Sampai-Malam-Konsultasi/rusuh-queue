package http

import stdhttp "net/http"

// RegisterRoutes registers the admin routes on mux.
func RegisterRoutes(mux *stdhttp.ServeMux, h *AdminHandler) {
	mux.HandleFunc("PATCH /admin/events/{eventId}/queue", h.UpdateQueue)
}
