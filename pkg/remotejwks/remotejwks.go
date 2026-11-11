// Package remotejwks verifies JWTs signed by another service (core's access
// tokens, D-4) with that service's published JWKS.
//
// Keys are cached so that the other service is not in the request path:
//   - A known kid always verifies from the cache, even when the cache is older
//     than TTL. A stale cache starts one background refresh and does not wait for it.
//   - An unknown kid (a rotation in core) or an empty cache waits for one fetch.
//     Concurrent callers share that fetch.
//   - Fetches start at most once per MinRefresh, successful or not, so an
//     unreachable JWKS costs one attempt a minute, not one per request.
//   - When the keys cannot be loaded, Verify returns ErrUnavailable (the caller
//     answers 503), never ErrInvalid (401).
//
// NewStatic takes the JWKS document itself (core passes its keys by env, not by
// URL) and never fetches.
package remotejwks

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrInvalid means the token is bad: wrong signature, claims or algorithm.
	ErrInvalid = errors.New("remotejwks: token invalid")
	// ErrUnavailable means the keys could not be loaded, so the token was not checked.
	ErrUnavailable = errors.New("remotejwks: keys unavailable")
)

const (
	TTL        = 5 * time.Minute
	MinRefresh = time.Minute
)

type Verifier struct {
	url    string
	static bool
	http   *http.Client
	now    func() time.Time

	mu       sync.Mutex
	keys     map[string]ed25519.PublicKey
	fetched  time.Time     // last successful fetch
	tried    time.Time     // last fetch attempt, successful or not
	lastErr  error         // error of the last attempt, nil after a success
	inflight chan struct{} // closed when the running fetch ends; nil when none runs
}

func New(url string) *Verifier {
	return &Verifier{url: url, http: &http.Client{Timeout: 2 * time.Second}, now: time.Now}
}

func NewStatic(jwks string) (*Verifier, error) {
	keys, err := parseKeys(strings.NewReader(jwks))
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, errors.New("remotejwks: no Ed25519 key with a kid in the JWKS")
	}
	return &Verifier{static: true, keys: keys, now: time.Now}, nil
}

func (v *Verifier) Configured() bool { return v != nil && (v.url != "" || v.static) }

// Warm starts loading the keys in the background, so the first request after
// startup does not wait for them.
func (v *Verifier) Warm() {
	if !v.Configured() || v.static {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.inflight == nil {
		v.startLocked(v.now())
	}
}

// Verify checks an EdDSA token against the remote keys and fills claims;
// opts add the issuer, audience and other checks. It returns ErrInvalid for a
// bad token and ErrUnavailable when the keys could not be loaded.
func (v *Verifier) Verify(ctx context.Context, raw string, claims jwt.Claims, opts ...jwt.ParserOption) error {
	if !v.Configured() {
		return fmt.Errorf("%w: no JWKS configured", ErrInvalid)
	}
	opts = append([]jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30 * time.Second),
		jwt.WithTimeFunc(v.now),
	}, opts...)
	_, err := jwt.NewParser(opts...).ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		return v.key(ctx, kid)
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrUnavailable):
		return err
	default:
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
}

func (v *Verifier) key(ctx context.Context, kid string) (ed25519.PublicKey, error) {
	if v.static {
		if k, ok := v.keys[kid]; ok {
			return k, nil
		}
		return nil, fmt.Errorf("remotejwks: unknown kid %q", kid)
	}
	v.mu.Lock()
	now := v.now()
	if k, ok := v.keys[kid]; ok {
		if now.Sub(v.fetched) > TTL && v.inflight == nil && now.Sub(v.tried) >= MinRefresh {
			v.startLocked(now) // refresh in the background; this request uses the cached key
		}
		v.mu.Unlock()
		return k, nil
	}

	// Unknown kid or no keys yet: wait for a fetch, if one runs or may start.
	wait := v.inflight
	if wait == nil && (v.tried.IsZero() || now.Sub(v.tried) >= MinRefresh) {
		wait = v.startLocked(now)
	}
	v.mu.Unlock()
	if wait != nil {
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, ctx.Err())
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	if v.keys == nil || v.lastErr != nil {
		// The kid may be valid; we could not load the keys to tell.
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, v.lastErr)
	}
	return nil, fmt.Errorf("remotejwks: unknown kid %q", kid)
}

// startLocked starts one fetch in the background and returns a channel that is
// closed when it ends. The fetch does not use a request's context, so a caller
// that gives up does not cancel it for the others. v.mu must be held.
func (v *Verifier) startLocked(now time.Time) chan struct{} {
	done := make(chan struct{})
	v.tried, v.inflight = now, done
	go func() {
		keys, err := v.fetch(context.Background())
		v.mu.Lock()
		defer v.mu.Unlock()
		if err != nil {
			v.lastErr = err
			slog.Warn("remotejwks: refresh failed, keeping cached keys", "url", v.url, "cached", len(v.keys), "error", err)
		} else {
			v.keys, v.fetched, v.lastErr = keys, v.now(), nil
		}
		v.inflight = nil
		close(done)
	}()
	return done
}

func (v *Verifier) fetch(ctx context.Context) (map[string]ed25519.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("remotejwks: fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remotejwks: fetch: status %d", resp.StatusCode)
	}
	return parseKeys(http.MaxBytesReader(nil, resp.Body, 64<<10))
}

func parseKeys(r io.Reader) (map[string]ed25519.PublicKey, error) {
	var set struct {
		Keys []struct {
			Kty, Crv, X, Kid string
		} `json:"keys"`
	}
	if err := json.NewDecoder(r).Decode(&set); err != nil {
		return nil, fmt.Errorf("remotejwks: decode: %w", err)
	}
	keys := make(map[string]ed25519.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kty != "OKP" || k.Crv != "Ed25519" || k.Kid == "" {
			continue
		}
		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			continue
		}
		keys[k.Kid] = ed25519.PublicKey(x)
	}
	return keys, nil
}
