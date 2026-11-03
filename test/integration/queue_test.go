package integration

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/delivery"

	"github.com/golang-jwt/jwt/v5"
	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/remotejwks"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

// fakeCore signs access tokens the way core will (tokens.md, D-4) and serves its JWKS.
type fakeCore struct {
	priv ed25519.PrivateKey
	srv  *httptest.Server
	down atomic.Bool // answer 503, as if core were unreachable
}

func newFakeCore(t *testing.T) *fakeCore {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	c := &fakeCore{priv: priv}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if c.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintf(w, `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"core-1","alg":"EdDSA","use":"sig","x":%q}]}`,
			base64.RawURLEncoding.EncodeToString(pub))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *fakeCore) accessToken(t *testing.T, user string, amr []string, aud string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, auth.AccessClaims{AMR: amr, RegisteredClaims: jwt.RegisteredClaims{
		Issuer: "rusuh-core", Subject: user, Audience: jwt.ClaimStrings{aud},
		IssuedAt: jwt.NewNumericDate(time.Now()), ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
	}})
	tok.Header["kid"] = "core-1"
	raw, err := tok.SignedString(c.priv)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type queueEnv struct {
	mux    *http.ServeMux
	client vk.Client
	keys   *token.Keyring
	core   *fakeCore
}

// queueConfig is the production configuration with a given order and miss window.
func queueConfig(orderWindow, missWindow time.Duration) entity.Config {
	return entity.Config{
		JoinPerDevice:  ratelimit.Rule{Scope: "join:dev", Limit: 30, Window: time.Minute},
		ClaimPerDevice: ratelimit.Rule{Scope: "claim:dev", Limit: 60, Window: time.Minute},
		OrderWindow:    orderWindow,
		MissWindow:     missWindow,
	}
}

func newQueueEnv(t *testing.T) queueEnv {
	t.Helper()
	return newQueueEnvOn(t, testkit.Valkey(t), queueConfig(5*time.Minute, 5*time.Minute))
}

func newQueueEnvOn(t *testing.T, client vk.Client, cfg entity.Config) queueEnv {
	t.Helper()
	k, _ := token.GenerateKey("k1")
	keys, _ := token.NewKeyring(k)
	core := newFakeCore(t)

	mux := http.NewServeMux()
	delivery.RegisterQueue(mux, delivery.QueueDeps{
		Config:  cfg,
		Valkey:  client,
		Signer:  keys,
		Limiter: ratelimit.New(client),
		Auth:    auth.New(keys, remotejwks.New(core.srv.URL)),
	})
	return queueEnv{mux: mux, client: client, keys: keys, core: core}
}

func (e queueEnv) deviceToken(t *testing.T, id string) string {
	t.Helper()
	raw, err := e.keys.Sign(token.KindDevice, &token.DeviceClaims{Platform: "web", RegisteredClaims: jwt.RegisteredClaims{
		Subject: id, Audience: jwt.ClaimStrings{token.AudienceQueue}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (e queueEnv) do(method, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec
}

type placeBody struct {
	EventID         string `json:"eventId"`
	State           string `json:"state"`
	Seq             *int64 `json:"seq"`
	QueueToken      string `json:"queueToken"`
	AccountAttached bool   `json:"accountAttached"`
}

func decodePlace(t *testing.T, rec *httptest.ResponseRecorder) placeBody {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	var p placeBody
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestJoinMeFrontierFlow(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_flow", testkit.EventOpts{})
	dev := map[string]string{"X-Device-Token": e.deviceToken(t, "w_flow1")}

	p := decodePlace(t, e.do("POST", "/v1/queue/evt_flow/join", dev))
	if p.Seq == nil || *p.Seq != 1 || p.State != "open" || p.AccountAttached {
		t.Fatalf("join = %+v", p)
	}
	again := decodePlace(t, e.do("POST", "/v1/queue/evt_flow/join", dev))
	if *again.Seq != 1 {
		t.Fatalf("refresh changed the place: %d", *again.Seq)
	}

	var qc token.QueueClaims
	if err := e.keys.Verify(token.KindQueue, p.QueueToken, token.AudienceQueue, &qc); err != nil || qc.Seq != 1 || qc.Event != "evt_flow" || qc.Subject != "w_flow1" {
		t.Fatalf("queue token = %+v, %v", qc, err)
	}

	me := decodePlace(t, e.do("GET", "/v1/queue/evt_flow/me", map[string]string{"X-Device-Token": dev["X-Device-Token"], "X-Queue-Token": p.QueueToken}))
	if *me.Seq != 1 {
		t.Fatalf("me = %+v", me)
	}

	fr := e.do("GET", "/v1/queue/evt_flow/frontier", nil)
	if fr.Code != 200 || fr.Header().Get("Cache-Control") != "public, max-age=1" {
		t.Fatalf("frontier %d %q", fr.Code, fr.Header().Get("Cache-Control"))
	}
	var f api.Frontier
	if err := json.NewDecoder(fr.Body).Decode(&f); err != nil || f.Event != "evt_flow" {
		t.Fatalf("frontier = %+v, %v", f, err)
	}
}

func TestJoinPreopenReturnsNullSeq(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_early", testkit.EventOpts{OpensIn: time.Hour})
	rec := e.do("POST", "/v1/queue/evt_early/join", map[string]string{"X-Device-Token": e.deviceToken(t, "w_early1")})
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	var raw map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&raw)
	if v, ok := raw["seq"]; !ok || v != nil || raw["state"] != "preopen" {
		t.Fatalf("body = %v; want \"seq\": null while waiting for the draw", raw)
	}
}

func TestJoinErrors(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_err", testkit.EventOpts{})
	testkit.Event(t, e.client, "evt_soldout", testkit.EventOpts{Cancelled: true})
	devA := e.deviceToken(t, "w_errA")

	other, _ := token.GenerateKey("k1")
	otherRing, _ := token.NewKeyring(other)
	forged, _ := otherRing.Sign(token.KindDevice, &token.DeviceClaims{RegisteredClaims: jwt.RegisteredClaims{
		Subject: "w_errA", Audience: jwt.ClaimStrings{token.AudienceQueue}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}})

	tests := []struct {
		name    string
		path    string
		headers map[string]string
		status  int
		code    api.ErrorCode
	}{
		{"no device token", "/v1/queue/evt_err/join", nil, 401, api.ErrorCodeUNAUTHORIZED},
		{"forged device token", "/v1/queue/evt_err/join", map[string]string{"X-Device-Token": forged}, 401, api.ErrorCodeUNAUTHORIZED},
		{"bad bearer is not treated as a guest", "/v1/queue/evt_err/join", map[string]string{"X-Device-Token": devA, "Authorization": "Bearer junk"}, 401, api.ErrorCodeUNAUTHORIZED},
		{"bearer without amr=otp", "/v1/queue/evt_err/join", map[string]string{"X-Device-Token": devA, "Authorization": "Bearer " + e.core.accessToken(t, "u1", []string{"pwd"}, "rusuh-queue")}, 401, api.ErrorCodeUNAUTHORIZED},
		{"bearer for another audience", "/v1/queue/evt_err/join", map[string]string{"X-Device-Token": devA, "Authorization": "Bearer " + e.core.accessToken(t, "u1", []string{"otp"}, "rusuh-core")}, 401, api.ErrorCodeUNAUTHORIZED},
		{"unknown event", "/v1/queue/evt_nope/join", map[string]string{"X-Device-Token": devA}, 404, api.ErrorCodeNOTFOUND},
		{"bad event id", "/v1/queue/a%7Bb/join", map[string]string{"X-Device-Token": devA}, 404, api.ErrorCodeNOTFOUND},
		{"closed queue", "/v1/queue/evt_soldout/join", map[string]string{"X-Device-Token": devA}, 410, api.ErrorCodeQUEUECLOSED},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := e.do("POST", tt.path, tt.headers)
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d (%s)", rec.Code, tt.status, rec.Body)
			}
			if got := problemCode(t, rec); got != tt.code {
				t.Fatalf("code %s, want %s", got, tt.code)
			}
		})
	}
}

// FAQ 9.9: when core's JWKS cannot be loaded, a logged-in join is 503 with
// Retry-After (the token was not checked), and guest joins are not affected.
func TestJoinWhenCoreKeysUnavailable(t *testing.T) {
	e := newQueueEnv(t)
	e.core.down.Store(true)
	testkit.Event(t, e.client, "evt_corekeys", testkit.EventOpts{})
	bearer := "Bearer " + e.core.accessToken(t, "user_7", []string{"otp"}, "rusuh-queue")

	rec := e.do("POST", "/v1/queue/evt_corekeys/join", map[string]string{"X-Device-Token": e.deviceToken(t, "w_k1"), "Authorization": bearer})
	if rec.Code != http.StatusServiceUnavailable || problemCode(t, rec) != api.ErrorCodeVERIFICATIONUNAVAILABLE {
		t.Fatalf("logged-in join: status %d (%s), want 503 VERIFICATION_UNAVAILABLE", rec.Code, rec.Body)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("503 without Retry-After")
	}

	guest := e.do("POST", "/v1/queue/evt_corekeys/join", map[string]string{"X-Device-Token": e.deviceToken(t, "w_k2")})
	if guest.Code != http.StatusOK {
		t.Fatalf("guest join: status %d (%s), want 200", guest.Code, guest.Body)
	}
}

// M2 verify: a second device for the same account gets 409 (BR-07, Q-51).
func TestAccountAttachAndSecondDevice(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_acct", testkit.EventOpts{})
	bearer := "Bearer " + e.core.accessToken(t, "user_42", []string{"otp"}, "rusuh-queue")

	guest := decodePlace(t, e.do("POST", "/v1/queue/evt_acct/join", map[string]string{"X-Device-Token": e.deviceToken(t, "w_phone")}))
	logged := decodePlace(t, e.do("POST", "/v1/queue/evt_acct/join", map[string]string{"X-Device-Token": e.deviceToken(t, "w_phone"), "Authorization": bearer}))
	if *logged.Seq != *guest.Seq || !logged.AccountAttached {
		t.Fatalf("login moved the place or did not attach: guest %d, logged %+v", *guest.Seq, logged)
	}

	rec := e.do("POST", "/v1/queue/evt_acct/join", map[string]string{"X-Device-Token": e.deviceToken(t, "w_laptop"), "Authorization": bearer})
	if rec.Code != http.StatusConflict || problemCode(t, rec) != api.ErrorCodeACCOUNTHASPLACEELSEWHERE {
		t.Fatalf("second device: status %d", rec.Code)
	}
}

func TestMyPlaceChecksTheQueueToken(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_me", testkit.EventOpts{})
	testkit.Event(t, e.client, "evt_me2", testkit.EventOpts{})
	devA, devB := e.deviceToken(t, "w_meA"), e.deviceToken(t, "w_meB")
	pa := decodePlace(t, e.do("POST", "/v1/queue/evt_me/join", map[string]string{"X-Device-Token": devA}))

	for name, h := range map[string]map[string]string{
		"no queue token":                {"X-Device-Token": devA},
		"another device's queue token":  {"X-Device-Token": devB, "X-Queue-Token": pa.QueueToken},
		"device token as a queue token": {"X-Device-Token": devA, "X-Queue-Token": devA},
	} {
		if rec := e.do("GET", "/v1/queue/evt_me/me", h); rec.Code != 401 {
			t.Errorf("%s: status %d, want 401", name, rec.Code)
		}
	}
	if rec := e.do("GET", "/v1/queue/evt_me2/me", map[string]string{"X-Device-Token": devA, "X-Queue-Token": pa.QueueToken}); rec.Code != 401 {
		t.Errorf("queue token for another event: status %d, want 401", rec.Code)
	}

	// The place is used (claimed and expired) → 410 EXPIRED, rejoin at the back.
	_ = e.client.Do(t.Context(), e.client.B().Hset().Key("q:{evt_me}:devs").FieldValue().FieldValue("w_meA", "x:1").Build())
	if rec := e.do("GET", "/v1/queue/evt_me/me", map[string]string{"X-Device-Token": devA, "X-Queue-Token": pa.QueueToken}); rec.Code != 410 || problemCode(t, rec) != api.ErrorCodeEXPIRED {
		t.Fatalf("used place: status %d", rec.Code)
	}
}

func TestFrontierUnknownEvent(t *testing.T) {
	e := newQueueEnv(t)
	rec := e.do("GET", "/v1/queue/evt_none/frontier", nil)
	if rec.Code != 404 || rec.Header().Get("Cache-Control") != "public, max-age=1" {
		t.Fatalf("status %d, cache %q", rec.Code, rec.Header().Get("Cache-Control"))
	}
}

func TestJoinRateLimitPerDevice(t *testing.T) {
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_rl", testkit.EventOpts{})
	dev := map[string]string{"X-Device-Token": e.deviceToken(t, "w_hammer")}
	for i := 1; i <= 30; i++ {
		if rec := e.do("POST", "/v1/queue/evt_rl/join", dev); rec.Code != 200 {
			t.Fatalf("join %d: %d", i, rec.Code)
		}
	}
	if rec := e.do("POST", "/v1/queue/evt_rl/join", dev); rec.Code != 429 || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("31st: status %d", rec.Code)
	}
}

// M2 verify: join p95 < 200 ms locally at 2,000 joins/s (3 s, 6,000 devices).
func TestJoinLatencyAt2000PerSecond(t *testing.T) {
	if testing.Short() {
		t.Skip("load test")
	}
	e := newQueueEnv(t)
	testkit.Event(t, e.client, "evt_load", testkit.EventOpts{})
	srv := httptest.NewServer(e.mux)
	defer srv.Close()

	const rate, seconds = 2000, 3
	tokens := make([]string, rate*seconds)
	for i := range tokens {
		tokens[i] = e.deviceToken(t, fmt.Sprintf("w_load%05d", i))
	}
	httpc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512}}

	lat := make([]time.Duration, len(tokens))
	var failures int
	var mu sync.Mutex
	var wg sync.WaitGroup
	tick := time.NewTicker(time.Second / rate)
	defer tick.Stop()
	for i := range tokens {
		<-tick.C
		wg.Go(func() {
			req, _ := http.NewRequest("POST", srv.URL+"/v1/queue/evt_load/join", nil)
			req.Header.Set("X-Device-Token", tokens[i])
			start := time.Now()
			resp, err := httpc.Do(req)
			lat[i] = time.Since(start)
			if err != nil || resp.StatusCode != 200 {
				mu.Lock()
				failures++
				mu.Unlock()
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
		})
	}
	wg.Wait()

	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	p50, p95, p99 := lat[len(lat)/2], lat[len(lat)*95/100], lat[len(lat)*99/100]
	t.Logf("joins=%d failures=%d p50=%s p95=%s p99=%s", len(lat), failures, p50, p95, p99)
	if failures > 0 || p95 > 200*time.Millisecond {
		t.Fatalf("failures=%d p95=%s, want 0 and < 200ms", failures, p95)
	}
	seq, _ := e.client.Do(t.Context(), e.client.B().Get().Key("q:{evt_load}:seq").Build()).AsInt64()
	if seq != int64(len(tokens)) {
		t.Fatalf("seq = %d, want %d", seq, len(tokens))
	}
}
