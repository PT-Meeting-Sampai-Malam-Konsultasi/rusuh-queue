package http

import stdhttp "net/http"

// RegisterRoutes registers the device routes on mux.
func RegisterRoutes(mux *stdhttp.ServeMux, h *DeviceHandler) {
	mux.HandleFunc("POST /v1/devices", h.RegisterDevice)
	mux.HandleFunc("GET /.well-known/jwks.json", h.GetJwks)
}
