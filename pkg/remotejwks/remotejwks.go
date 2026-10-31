// Package remotejwks verifies JWTs signed by another service (core's access
// tokens, D-4) with that service's published JWKS. Keys are cached; an
// unknown kid triggers at most one refetch per MinRefresh, so a key rotation
// in core reaches queue without a deploy.
package remotejwks

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var ErrInvalid = errors.New("remotejwks: token invalid")

const (
	TTL        = 5 * time.Minute
	MinRefresh = time.Minute
)

type Verifier struct {
	url  string
	http *http.Client

	mu      sync.Mutex
	keys    map[string]ed25519.PublicKey
	fetched time.Time
	now     func() time.Time
}

func New(url string) *Verifier {
	return &Verifier{url: url, http: &http.Client{Timeout: 2 * time.Second}, now: time.Now}
}

// Configured reports whether a JWKS URL is set.
func (v *Verifier) Configured() bool { return v != nil && v.url != "" }

// Verify checks an EdDSA token against the remote keys and fills claims;
// opts add the issuer, audience and other checks.
func (v *Verifier) Verify(ctx context.Context, raw string, claims jwt.Claims, opts ...jwt.ParserOption) error {
	if !v.Configured() {
		return fmt.Errorf("%w: no JWKS URL configured", ErrInvalid)
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
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func (v *Verifier) key(ctx context.Context, kid string) (ed25519.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	now := v.now()
	stale := now.Sub(v.fetched) > TTL
	if k, ok := v.keys[kid]; ok && !stale {
		return k, nil
	}
	if stale || now.Sub(v.fetched) > MinRefresh {
		if err := v.fetch(ctx); err != nil && v.keys == nil {
			return nil, err
		}
	}
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	return nil, fmt.Errorf("remotejwks: unknown kid %q", kid)
}

func (v *Verifier) fetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.url, nil)
	if err != nil {
		return err
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return fmt.Errorf("remotejwks: fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remotejwks: fetch: status %d", resp.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kty, Crv, X, Kid string
		} `json:"keys"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 64<<10)).Decode(&set); err != nil {
		return fmt.Errorf("remotejwks: decode: %w", err)
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
	v.keys = keys
	v.fetched = v.now()
	return nil
}
