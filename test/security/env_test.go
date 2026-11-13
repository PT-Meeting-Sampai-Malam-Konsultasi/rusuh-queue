// Package security attacks the public API the way an outside caller would.
// delivery.New wires the handler, CORS and every token check with production
// settings (core's inline JWKS, iss rusuh-auth / aud rusuh-api, login before
// claim), on real Valkey and PostgreSQL (Testcontainers; skipped without Docker).
package security

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/delivery"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

const (
	appOrigin    = "https://rusuh-app.bingbing.my.id"
	coreKid      = "akun-202611-01"
	coreIssuer   = "rusuh-auth"
	coreAudience = "rusuh-api"
)

type env struct {
	public http.Handler
	admin  http.Handler
	client vk.Client
	keys   *token.Keyring
	core   ed25519.PrivateKey
}

func newEnv(t *testing.T) env {
	return newEnvWith(t, func(*config.Config) {})
}

func newEnvWith(t *testing.T, tweak func(*config.Config)) env {
	t.Helper()
	client := testkit.Valkey(t)

	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	signingKeys := "k1=" + base64.StdEncoding.EncodeToString(seed)
	ring, err := token.ParseKeys(signingKeys)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := token.NewKeyring(ring...)
	if err != nil {
		t.Fatal(err)
	}
	corePub, corePriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("VALKEY_TLS_ENABLED", "false")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Env = "production"
	cfg.DatabaseURL = testkit.PostgresDSN(t)
	cfg.ValkeyAddr = testkit.ValkeyAddr(t)
	cfg.ValkeyPassword = ""
	cfg.SigningKeys = signingKeys
	cfg.TurnstileSecret = "turnstile-secret"
	cfg.CORSOrigins = []string{appOrigin}
	cfg.CoreJWKSURL = ""
	cfg.CoreJWKS = fmt.Sprintf(`{"keys":[{"kty":"OKP","crv":"Ed25519","kid":%q,"alg":"EdDSA","use":"sig","x":%q}]}`,
		coreKid, base64.RawURLEncoding.EncodeToString(corePub))
	cfg.CoreIssuer, cfg.CoreAudience, cfg.CoreAccessRequireOTP = coreIssuer, coreAudience, false
	cfg.ClaimRequiresLogin = true
	cfg.CoreInternalURL = ""
	cfg.AdminJWKSURL = testkit.NewIssuer(t).URL()
	cfg.ClientIPHeader = ""
	cfg.TrustedProxyCIDRs = nil
	cfg.RequestLog = false
	cfg.ShutdownTimeout = 45 * time.Second
	tweak(cfg)

	s, err := delivery.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return env{public: s.Handler, admin: s.AdminHandler, client: client, keys: keys, core: corePriv}
}

func (e env) do(method, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.public.ServeHTTP(rec, req)
	return rec
}

func (e env) deviceToken(t *testing.T, device string) string {
	t.Helper()
	raw, err := e.keys.Sign(token.KindDevice, &token.DeviceClaims{Platform: "web", RegisteredClaims: jwt.RegisteredClaims{
		Subject: device, Audience: jwt.ClaimStrings{token.AudienceQueue}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// accessToken signs a token the way core's auth module does (iss rusuh-auth, aud rusuh-api, no amr).
func (e env) accessToken(t *testing.T, user string) string {
	t.Helper()
	now := time.Now()
	return signAccess(t, e.core, coreKid, jwt.SigningMethodEdDSA, auth.AccessClaims{RegisteredClaims: jwt.RegisteredClaims{
		Issuer: coreIssuer, Subject: user, Audience: jwt.ClaimStrings{coreAudience},
		IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
	}})
}

func signAccess(t *testing.T, key any, kid string, method jwt.SigningMethod, c auth.AccessClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(method, c)
	tok.Header["kid"] = kid
	raw, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// openEvent sets up an open sale with the given stock, as the controller would.
func (e env) openEvent(t *testing.T, id string, available int64) qkeys.Event {
	t.Helper()
	return testkit.Event(t, e.client, id, testkit.EventOpts{Available: available})
}

type place struct {
	Seq        *int64 `json:"seq"`
	QueueToken string `json:"queueToken"`
}

type caller struct {
	device, deviceToken, queueToken, bearer string
	seq                                     int64
}

func (c caller) headers() map[string]string {
	h := map[string]string{"X-Device-Token": c.deviceToken, "X-Queue-Token": c.queueToken}
	if c.bearer != "" {
		h["Authorization"] = "Bearer " + c.bearer
	}
	return h
}

// join gives a logged-in device a place on event.
func (e env) join(t *testing.T, event, device, user string) caller {
	t.Helper()
	c := caller{device: device, deviceToken: e.deviceToken(t, device), bearer: e.accessToken(t, user)}
	rec := e.do("POST", "/v1/queue/"+event+"/join", map[string]string{"X-Device-Token": c.deviceToken, "Authorization": "Bearer " + c.bearer})
	if rec.Code != http.StatusOK {
		t.Fatalf("join %s on %s: %d %s", device, event, rec.Code, rec.Body)
	}
	var p place
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil || p.Seq == nil {
		t.Fatalf("join body: %v %s", err, rec.Body)
	}
	c.queueToken, c.seq = p.QueueToken, *p.Seq
	return c
}

func (e env) claim(event string, c caller) *httptest.ResponseRecorder {
	return e.do("POST", "/v1/queue/"+event+"/claim", c.headers())
}

func decodeAdmission(t *testing.T, rec *httptest.ResponseRecorder) api.Admission {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("claim: %d %s", rec.Code, rec.Body)
	}
	var a api.Admission
	if err := json.NewDecoder(rec.Body).Decode(&a); err != nil {
		t.Fatal(err)
	}
	return a
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

func seqCount(t *testing.T, e env, k qkeys.Event) int64 {
	t.Helper()
	n, err := e.client.Do(context.Background(), e.client.B().Get().Key(k.Seq).Build()).AsInt64()
	if err != nil && !vk.IsValkeyNil(err) {
		t.Fatal(err)
	}
	return n
}
