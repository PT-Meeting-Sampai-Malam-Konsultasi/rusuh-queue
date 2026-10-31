package delivery

import (
	"net/http"

	streamhttp "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/infrastructure/http"
)

// RegisterStream registers streamQueue (a 501 stub until the SSE hub, M5).
func RegisterStream(mux *http.ServeMux) {
	streamhttp.RegisterRoutes(mux, streamhttp.NewStream())
}
