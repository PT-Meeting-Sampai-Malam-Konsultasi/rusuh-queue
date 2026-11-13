package httpmw

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestCORS(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := CORS([]string{"https://rusuh-app.bingbing.my.id/", "*"}, next)

	tests := []struct {
		name       string
		method     string
		origin     string
		preflight  bool
		wantStatus int
		wantACAO   string
	}{
		{"allowed origin", "POST", "https://rusuh-app.bingbing.my.id", false, http.StatusTeapot, "https://rusuh-app.bingbing.my.id"},
		{"foreign origin gets no CORS header", "POST", "https://evil.example", false, http.StatusTeapot, ""},
		{"wildcard in config is ignored", "POST", "https://other.example", false, http.StatusTeapot, ""},
		{"no origin (mobile, curl)", "POST", "", false, http.StatusTeapot, ""},
		{"preflight allowed", "OPTIONS", "https://rusuh-app.bingbing.my.id", true, http.StatusNoContent, "https://rusuh-app.bingbing.my.id"},
		{"preflight foreign", "OPTIONS", "https://evil.example", true, http.StatusNoContent, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/v1/queue/devices", nil)
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			if tt.preflight {
				req.Header.Set("Access-Control-Request-Method", "POST")
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != tt.wantACAO {
				t.Fatalf("ACAO = %q, want %q", got, tt.wantACAO)
			}
			if tt.preflight && tt.wantACAO == "" && rec.Header().Get("Access-Control-Allow-Headers") != "" {
				t.Fatal("foreign preflight must not get allowed headers")
			}
		})
	}
}

// Every method the public API serves must pass the browser's preflight;
// leaveQueue is DELETE /v1/queue/{eventId}/me.
func TestCORSPreflightAllowsEveryAPIMethod(t *testing.T) {
	h := CORS([]string{"https://rusuh-app.bingbing.my.id"}, http.NotFoundHandler())
	for _, m := range []string{"GET", "POST", "DELETE"} {
		req := httptest.NewRequest("OPTIONS", "/v1/queue/evt/me", nil)
		req.Header.Set("Origin", "https://rusuh-app.bingbing.my.id")
		req.Header.Set("Access-Control-Request-Method", m)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		allowed := strings.Split(rec.Header().Get("Access-Control-Allow-Methods"), ", ")
		if !slices.Contains(allowed, m) {
			t.Errorf("preflight for %s: Allow-Methods = %v", m, allowed)
		}
	}
}

func TestCORSWildcardOrigins(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := CORS([]string{
		"https://rusuh-tiket.web.id", "https://*.rusuh-web-staging.pages.dev", "http://localhost:3000",
		"https://*.ported.example:8443", "*", "https://*", "https://a.*.example.com", "https://*.com", "*.example.org",
	}, next)

	tests := []struct {
		origin string
		want   bool
	}{
		{"https://rusuh-tiket.web.id", true},
		{"http://localhost:3000", true},
		{"https://pr-42.rusuh-web-staging.pages.dev", true},
		{"https://PR-42.rusuh-web-staging.pages.dev", true},
		{"https://rusuh-web-staging.pages.dev", false},
		{"https://a.b.rusuh-web-staging.pages.dev", false},
		{"https://pr-42.rusuh-web-staging.pages.dev.evil.com", false},
		{"https://evilrusuh-web-staging.pages.dev", false},
		{"https://.rusuh-web-staging.pages.dev", false},
		{"https://pr_42.rusuh-web-staging.pages.dev", false},
		{"https://pr-42.rusuh-web-staging.pages.dev:8443", false},
		{"http://pr-42.rusuh-web-staging.pages.dev", false},
		{"https://x.ported.example:8443", true},
		{"https://x.ported.example", false},
		{"https://x.ported.example:9000", false},
		{"https://anything.com", false},
		{"https://a.b.example.com", false},
		{"https://x.example.org", false},
		{"https://other.example", false},
	}
	for _, tt := range tests {
		for _, preflight := range []bool{false, true} {
			method := "POST"
			if preflight {
				method = "OPTIONS"
			}
			req := httptest.NewRequest(method, "/v1/queue/devices", nil)
			req.Header.Set("Origin", tt.origin)
			if preflight {
				req.Header.Set("Access-Control-Request-Method", "POST")
				req.Header.Set("Access-Control-Request-Headers", "X-Request-ID")
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			want := ""
			if tt.want {
				want = tt.origin
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != want {
				t.Errorf("%s preflight=%t: ACAO = %q, want %q", tt.origin, preflight, got, want)
			}
			hdrs := rec.Header().Get("Access-Control-Allow-Headers")
			if preflight && tt.want != strings.Contains(hdrs, "X-Request-ID") {
				t.Errorf("%s: Allow-Headers = %q", tt.origin, hdrs)
			}
		}
	}
}
