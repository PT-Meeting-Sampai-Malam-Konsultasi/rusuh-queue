package security

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// OWASP A05 (the H1 finding): only the app's origin may read responses or send
// the token headers; any other origin, "null" or a look-alike gets no CORS headers.
func TestCORSAllowsOnlyTheAppOrigin(t *testing.T) {
	e := newEnv(t)
	e.openEvent(t, "evt_cors", 3)

	preflight := func(origin string) *httptest.ResponseRecorder {
		return e.do("OPTIONS", "/v1/queue/evt_cors/claim", map[string]string{
			"Origin": origin, "Access-Control-Request-Method": "POST",
			"Access-Control-Request-Headers": "x-device-token, x-queue-token, authorization",
		})
	}

	for _, origin := range []string{
		"https://evil.example",
		"null",
		"https://rusuh-app.bingbing.my.id.evil.example",
		"https://evil-rusuh-app.bingbing.my.id",
		"http://rusuh-app.bingbing.my.id",
	} {
		for name, rec := range map[string]*httptest.ResponseRecorder{
			"preflight": preflight(origin),
			"GET":       e.do("GET", "/v1/queue/evt_cors/frontier", map[string]string{"Origin": origin}),
		} {
			for _, h := range []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Headers", "Access-Control-Allow-Methods", "Access-Control-Allow-Credentials"} {
				if v := rec.Header().Get(h); v != "" {
					t.Errorf("%s from %s: %s = %q", name, origin, h, v)
				}
			}
		}
	}

	rec := preflight(appOrigin)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != appOrigin {
		t.Fatalf("app preflight Allow-Origin = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("Allow-Credentials = %q: tokens travel in headers, cookies must never be sent", got)
	}
	allowed := strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers"))
	for _, h := range []string{"x-device-token", "x-queue-token", "authorization"} {
		if !strings.Contains(allowed, h) {
			t.Fatalf("app preflight does not allow %s: %q", h, allowed)
		}
	}
}

// OWASP A01/A05: the operator API listens only on the private admin port; the
// public handler does not route it at all.
func TestAdminAPIIsNotOnThePublicListener(t *testing.T) {
	e := newEnv(t)
	for _, method := range []string{"PATCH", "GET", "POST"} {
		rec := e.do(method, "/admin/events/evt_x/queue", map[string]string{"Authorization": "Bearer x"})
		if rec.Code != http.StatusNotFound {
			t.Errorf("public %s /admin/events/evt_x/queue = %d, want 404", method, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	e.admin.ServeHTTP(rec, httptest.NewRequest("PATCH", "/admin/events/evt_x/queue", strings.NewReader(`{"state":"closed"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("admin listener without a token = %d, want 401", rec.Code)
	}
}

// OWASP A02: responses that carry tokens are never stored by a browser or CDN.
func TestTokenResponsesAreNotCached(t *testing.T) {
	e := newEnv(t)
	e.openEvent(t, "evt_cache", 3)
	device := e.deviceToken(t, "w_cache")
	rec := e.do("POST", "/v1/queue/evt_cache/join", map[string]string{"X-Device-Token": device})
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("join: %d Cache-Control %q, want no-store", rec.Code, rec.Header().Get("Cache-Control"))
	}
}
