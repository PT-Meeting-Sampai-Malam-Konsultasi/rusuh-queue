package delivery

import (
	"net/http"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
)

// wireDomains builds the token, rate-limit and verification clients and wires
// every domain onto mux; long-running routines go to s.background.
func (s *Server) wireDomains(cfg *config.Config, mux *http.ServeMux, ipResolver *clientip.Resolver) error {
	return nil
}
