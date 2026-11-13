// Package httpmw holds HTTP middleware shared by every module.
package httpmw

import (
	"net/http"
	"slices"
	"strings"
)

// Headers the web app may send to queue (api/openapi.yaml security schemes).
const corsAllowHeaders = "Content-Type, Authorization, X-Device-Token, X-Queue-Token, X-Turnstile-Token, X-Request-ID, Last-Event-ID"

// wildcardOrigin is a "<scheme>://*.<host>[:port]" entry: it matches the origins
// whose host is exactly one label in front of suffix.
type wildcardOrigin struct{ scheme, suffix string }

// parseWildcard reports whether entry is a valid one-label wildcard. The bare "*",
// "https://*" and a "*" anywhere but the leading host label are not (OWASP A05).
func parseWildcard(entry string) (wildcardOrigin, bool) {
	scheme, hostport, ok := strings.Cut(entry, "://")
	if !ok || scheme == "" {
		return wildcardOrigin{}, false
	}
	suffix, ok := strings.CutPrefix(hostport, "*.")
	if !ok || strings.ContainsAny(suffix, "*/?#@") || !strings.Contains(suffix, ".") {
		return wildcardOrigin{}, false
	}
	return wildcardOrigin{scheme, suffix}, true
}

func (w wildcardOrigin) matches(origin string) bool {
	scheme, hostport, ok := strings.Cut(origin, "://")
	if !ok || scheme != w.scheme {
		return false
	}
	label, ok := strings.CutSuffix(hostport, "."+w.suffix)
	return ok && isLabel(label)
}

func isLabel(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && c != '-' {
			return false
		}
	}
	return true
}

// CORS allows only the listed origins and answers preflight requests. An entry
// matches exactly, or as a one-label wildcard ("https://*.example.com"); a bare "*"
// is never honoured. Requests from other origins get no CORS headers, so the
// browser blocks them (OWASP A05; H1 assessment finding).
func CORS(origins []string, next http.Handler) http.Handler {
	allowed := make([]string, 0, len(origins))
	var wildcards []wildcardOrigin
	for _, o := range origins {
		o = strings.TrimRight(strings.TrimSpace(o), "/")
		switch {
		case o == "":
		case strings.Contains(o, "*"):
			if w, ok := parseWildcard(o); ok {
				wildcards = append(wildcards, w)
			}
		default:
			allowed = append(allowed, o)
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		ok := origin != "" && (slices.Contains(allowed, origin) ||
			slices.ContainsFunc(wildcards, func(w wildcardOrigin) bool { return w.matches(origin) }))
		if origin != "" {
			w.Header().Add("Vary", "Origin")
		}
		if ok {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Expose-Headers", "Retry-After")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			if ok {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
				w.Header().Set("Access-Control-Max-Age", "600")
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
