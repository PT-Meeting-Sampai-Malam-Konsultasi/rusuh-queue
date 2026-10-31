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
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{Subject: "u1", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))})
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
