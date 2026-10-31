// Package httpmw holds HTTP middleware shared by every module.
package httpmw

import (
	"net/http"
	"slices"
	"strings"
)

// Headers the web app may send to queue (api/openapi.yaml security schemes).
const corsAllowHeaders = "Content-Type, Authorization, X-Device-Token, X-Queue-Token, X-Turnstile-Token, Last-Event-ID"

// CORS allows only the listed origins (exact match, no wildcard) and answers
// preflight requests. Requests from other origins get no CORS headers, so the
// browser blocks them (OWASP A05; H1 assessment finding).
func CORS(origins []string, next http.Handler) http.Handler {
	allowed := make([]string, 0, len(origins))
	for _, o := range origins {
		if o = strings.TrimRight(strings.TrimSpace(o), "/"); o != "" && o != "*" {
			allowed = append(allowed, o)
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		ok := origin != "" && slices.Contains(allowed, origin)
		if origin != "" {
			w.Header().Add("Vary", "Origin")
		}
		if ok {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Expose-Headers", "Retry-After")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			if ok {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
				w.Header().Set("Access-Control-Max-Age", "600")
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
