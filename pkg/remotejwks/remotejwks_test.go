package remotejwks

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type core struct {
	keys    map[string]ed25519.PrivateKey
	fetches atomic.Int32
	down    atomic.Bool  // answer 503, as if core were unreachable
	delay   atomic.Int64 // nanoseconds to wait before answering
	srv     *httptest.Server
}

func newCore(t *testing.T, kids ...string) *core {
	t.Helper()
	c := &core{keys: map[string]ed25519.PrivateKey{}}
	for _, k := range kids {
		c.add(k)
	}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		c.fetches.Add(1)
		time.Sleep(time.Duration(c.delay.Load()))
		if c.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var parts []string
		for kid, priv := range c.keys {
			x := base64.RawURLEncoding.EncodeToString(priv.Public().(ed25519.PublicKey))
			parts = append(parts, fmt.Sprintf(`{"kty":"OKP","crv":"Ed25519","kid":%q,"x":%q}`, kid, x))
		}
		_, _ = fmt.Fprintf(w, `{"keys":[%s]}`, strings.Join(parts, ","))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *core) add(kid string) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	c.keys[kid] = priv
}

func (c *core) sign(t *testing.T, kid string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{Subject: "u1", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))})
	tok.Header["kid"] = kid
	raw, err := tok.SignedString(c.keys[kid])
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestVerifyCachesKeys(t *testing.T) {
	c := newCore(t, "a")
	v := New(c.srv.URL)
	for range 5 {
		var claims jwt.RegisteredClaims
		if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
			t.Fatal(err)
		}
	}
	if n := c.fetches.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1 (cached)", n)
	}
}

func TestRotationRefetchesOncePerMinute(t *testing.T) {
	c := newCore(t, "a")
	v := New(c.srv.URL)
	now := time.Now()
	v.now = func() time.Time { return now }

	var claims jwt.RegisteredClaims
	if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
		t.Fatal(err)
	}

	c.add("b") // core rotates
	tokB := c.sign(t, "b")
	if err := v.Verify(context.Background(), tokB, &claims); !errors.Is(err, ErrInvalid) {
		t.Fatalf("within a minute of the last fetch the new kid is not refetched yet: err = %v", err)
	}
	now = now.Add(MinRefresh + time.Second)
	if err := v.Verify(context.Background(), tokB, &claims); err != nil {
		t.Fatalf("after MinRefresh the new kid should be fetched: %v", err)
	}
	if n := c.fetches.Load(); n != 2 {
		t.Fatalf("fetches = %d, want 2", n)
	}
}

func TestRefusesHS256AndUnconfigured(t *testing.T) {
	c := newCore(t, "a")
	v := New(c.srv.URL)
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))})
	tok.Header["kid"] = "a"
	raw, _ := tok.SignedString([]byte(c.keys["a"].Public().(ed25519.PublicKey)))
	var claims jwt.RegisteredClaims
	if err := v.Verify(context.Background(), raw, &claims); !errors.Is(err, ErrInvalid) {
		t.Fatalf("HS256: err = %v", err)
	}
	if err := New("").Verify(context.Background(), c.sign(t, "a"), &claims); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unconfigured: err = %v", err)
	}
}

// waitFetches waits until no refresh is in flight and returns the fetch count.
func waitFetches(t *testing.T, c *core, v *Verifier, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		v.mu.Lock()
		idle := v.inflight == nil
		v.mu.Unlock()
		if n := c.fetches.Load(); idle && n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fetches = %d, want %d", c.fetches.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestColdStartWithCoreDownIsUnavailableNotInvalid(t *testing.T) {
	c := newCore(t, "a")
	c.down.Store(true)
	v := New(c.srv.URL)
	now := time.Now()
	v.now = func() time.Time { return now }

	var claims jwt.RegisteredClaims
	err := v.Verify(context.Background(), c.sign(t, "a"), &claims)
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrUnavailable only", err)
	}
	// Within MinRefresh the failure is remembered: no second fetch.
	for range 5 {
		if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("err = %v, want ErrUnavailable", err)
		}
	}
	if n := c.fetches.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1 (backoff after a failure)", n)
	}

	c.down.Store(false)
	now = now.Add(MinRefresh + time.Second)
	if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
		t.Fatalf("after the backoff core is back: %v", err)
	}
}

func TestStaleKeysServeWhileCoreIsDown(t *testing.T) {
	c := newCore(t, "a")
	v := New(c.srv.URL)
	now := time.Now()
	v.now = func() time.Time { return now }
	var claims jwt.RegisteredClaims
	if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
		t.Fatal(err)
	}

	c.down.Store(true)
	now = now.Add(TTL + time.Second)
	for range 20 {
		if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
			t.Fatalf("stale keys must still verify: %v", err)
		}
	}
	waitFetches(t, c, v, 2) // one background refresh, not one per request

	now = now.Add(MinRefresh + time.Second)
	if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
		t.Fatal(err)
	}
	waitFetches(t, c, v, 3) // the next try waits for the backoff
}

func TestStaleRefreshDoesNotBlockTheRequest(t *testing.T) {
	c := newCore(t, "a")
	v := New(c.srv.URL)
	now := time.Now()
	v.now = func() time.Time { return now }
	var claims jwt.RegisteredClaims
	if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
		t.Fatal(err)
	}

	c.delay.Store(int64(500 * time.Millisecond))
	now = now.Add(TTL + time.Second)
	start := time.Now()
	if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Fatalf("Verify took %v: the refresh ran in the request path", took)
	}
	waitFetches(t, c, v, 2)
}

func TestConcurrentColdStartFetchesOnce(t *testing.T) {
	c := newCore(t, "a")
	c.delay.Store(int64(100 * time.Millisecond))
	v := New(c.srv.URL)
	raw := c.sign(t, "a")

	errs := make(chan error, 50)
	for range 50 {
		go func() {
			var claims jwt.RegisteredClaims
			errs <- v.Verify(context.Background(), raw, &claims)
		}()
	}
	for range 50 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if n := c.fetches.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}
}

func TestWarmLoadsKeysBeforeTheFirstRequest(t *testing.T) {
	c := newCore(t, "a")
	v := New(c.srv.URL)
	v.Warm()
	waitFetches(t, c, v, 1)
	var claims jwt.RegisteredClaims
	if err := v.Verify(context.Background(), c.sign(t, "a"), &claims); err != nil {
		t.Fatal(err)
	}
	if n := c.fetches.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}
}
