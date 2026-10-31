// Package integration runs modules over HTTP against real Valkey (Testcontainers).
package integration

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/delivery"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/attest"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/turnstile"
)

// fakeSiteverify accepts only the token "good".
func fakeSiteverify(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.PostForm.Get("response") == "good" {
			_, _ = fmt.Fprint(w, `{"success":true,"hostname":"rusuh-app.bingbing.my.id"}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"success":false,"error-codes":["invalid-input-response"]}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newDeviceMux(t *testing.T) *http.ServeMux {
	t.Helper()
	vk := testkit.Valkey(t)
	k, err := token.GenerateKey("2026-10")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := token.NewKeyring(k)
	if err != nil {
		t.Fatal(err)
	}
	ips, err := clientip.New("CF-Connecting-IP", []string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	delivery.RegisterDevice(mux, delivery.DeviceDeps{
		Config: entity.Config{
			TokenTTL:  30 * 24 * time.Hour,
			PerIP:     ratelimit.Rule{Scope: "dev-reg:ip", Limit: 10, Window: time.Minute},
			PerSubnet: ratelimit.Rule{Scope: "dev-reg:net", Limit: 100, Window: time.Minute},
		},
		Human:    turnstile.New("secret", fakeSiteverify(t).URL, []string{"rusuh-app.bingbing.my.id"}),
		Attestor: attest.Sandbox{},
		Limiter:  ratelimit.New(vk),
		Signer:   keys,
		ClientIP: ips,
	})
	return mux
}

func register(mux http.Handler, clientIP, turnstileToken, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/devices", strings.NewReader(body))
	req.RemoteAddr = "10.1.2.3:443" // a trusted proxy (Cloudflare in the lab)
	req.Header.Set("CF-Connecting-IP", clientIP)
	req.Header.Set("Content-Type", "application/json")
	if turnstileToken != "" {
		req.Header.Set("X-Turnstile-Token", turnstileToken)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func problemCode(t *testing.T, rec *httptest.ResponseRecorder) api.ErrorCode {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("Content-Type = %q, body %s", ct, rec.Body)
	}
	var p api.Problem
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	return p.Code
}

// M1 verify: registerDevice refuses a bad Turnstile token.
func TestRegisterDeviceRefusesBadTurnstile(t *testing.T) {
	mux := newDeviceMux(t)
	for _, tok := range []string{"", "forged"} {
		rec := register(mux, "203.0.113.7", tok, `{"platform":"web"}`)
		if rec.Code != http.StatusBadRequest || problemCode(t, rec) != api.ErrorCodeVALIDATIONFAILED {
			t.Fatalf("token %q: status %d", tok, rec.Code)
		}
	}
}

// M1 verify: the 11th registration a minute from one IP is limited.
func TestRegisterDeviceLimitsEleventhPerIP(t *testing.T) {
	mux := newDeviceMux(t)
	for i := 1; i <= 10; i++ {
		if rec := register(mux, "203.0.113.7", "good", `{"platform":"web"}`); rec.Code != http.StatusCreated {
			t.Fatalf("request %d: status %d, body %s", i, rec.Code, rec.Body)
		}
	}
	rec := register(mux, "203.0.113.7", "good", `{"platform":"web"}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("11th: status %d, want 429", rec.Code)
	}
	retry, err := strconv.Atoi(rec.Header().Get("Retry-After"))
	if err != nil || retry < 1 || retry > 60 {
		t.Fatalf("Retry-After = %q", rec.Header().Get("Retry-After"))
	}
	if problemCode(t, rec) != api.ErrorCodeRATELIMITED {
		t.Fatal("want RATE_LIMITED")
	}

	// A neighbour on another IP is not blocked by this IP's limit.
	if rec := register(mux, "203.0.113.8", "good", `{"platform":"web"}`); rec.Code != http.StatusCreated {
		t.Fatalf("other IP: status %d", rec.Code)
	}
}

// The spoofed header from an untrusted peer must not dodge the limit.
func TestRegisterDeviceIgnoresSpoofedClientIP(t *testing.T) {
	mux := newDeviceMux(t)
	for i := range 11 {
		req := httptest.NewRequest(http.MethodPost, "/v1/devices", strings.NewReader(`{"platform":"web"}`))
		req.RemoteAddr = "198.51.100.9:5555" // not a trusted proxy
		req.Header.Set("CF-Connecting-IP", fmt.Sprintf("203.0.113.%d", i+1))
		req.Header.Set("X-Turnstile-Token", "good")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if i == 10 && rec.Code != http.StatusTooManyRequests {
			t.Fatalf("11th spoofed request: status %d, want 429", rec.Code)
		}
	}
}

// core's view: verify a device token with nothing but the published JWKS.
func TestDeviceTokenVerifiesWithPublishedJWKS(t *testing.T) {
	mux := newDeviceMux(t)

	rec := register(mux, "203.0.113.7", "good", `{"platform":"web"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	var out struct {
		DeviceToken string    `json:"deviceToken"`
		ExpiresAt   time.Time `json:"expiresAt"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	jw := httptest.NewRecorder()
	mux.ServeHTTP(jw, httptest.NewRequest(http.MethodGet, "/.well-known/jwks.json", nil))
	if jw.Code != http.StatusOK || jw.Header().Get("Cache-Control") != "public, max-age=300" {
		t.Fatalf("jwks status %d, cache %q", jw.Code, jw.Header().Get("Cache-Control"))
	}
	var set api.Jwks
	if err := json.NewDecoder(jw.Body).Decode(&set); err != nil || len(set.Keys) != 1 {
		t.Fatalf("jwks = %+v, err %v", set, err)
	}
	x, err := base64.RawURLEncoding.DecodeString(set.Keys[0].X)
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := jwt.Parse(out.DeviceToken, func(tk *jwt.Token) (any, error) {
		if tk.Header["kid"] != set.Keys[0].Kid || tk.Header["typ"] != "device+jwt" {
			return nil, fmt.Errorf("header = %v", tk.Header)
		}
		return ed25519.PublicKey(x), nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithAudience("rusuh-queue"), jwt.WithIssuer("rusuh-queue"))
	if err != nil || !parsed.Valid {
		t.Fatalf("token does not verify with the JWKS key: %v", err)
	}
}

func TestRegisterDeviceRejectsBadBodies(t *testing.T) {
	mux := newDeviceMux(t)
	for _, body := range []string{
		`not json`,
		`{"platform":"web","admin":true}`,        // unknown field
		`{"platform":"symbian"}`,                 // bad platform
		`{"platform":"android","installId":"x"}`, // short install id, no attestation
		`{"platform":"web","pad":"` + strings.Repeat("a", 20<<10) + `"}`, // over 16 KB
	} {
		if rec := register(mux, "203.0.113.50", "good", body); rec.Code != http.StatusBadRequest {
			t.Errorf("body %.40q: status %d, want 400", body, rec.Code)
		}
	}
}

func TestRegisterMobileSandbox(t *testing.T) {
	mux := newDeviceMux(t)
	rec := register(mux, "203.0.113.60", "", `{"platform":"ios","installId":"9b2f6c1e-4a7d-4f0e-a1b2-c3d4e5f60718","attestation":"app-attest-blob"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
}
