// Package token issues and verifies the EdDSA (Ed25519) JWTs that queue signs:
// device, queue and admission tokens (ADR-0003, ADR-0004, D-3). Every token
// carries its kind in the `typ` header and its key in `kid`, so one kind can
// never be replayed as another and keys can rotate. The contract for core is
// rusuh-documentation/api/tokens.md.
package token

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Kind is the token type, sent in the JWT `typ` header.
type Kind string

const (
	KindDevice    Kind = "device+jwt"
	KindQueue     Kind = "queue+jwt"
	KindAdmission Kind = "admission+jwt"
)

const (
	// Issuer is the `iss` of every token queue signs.
	Issuer = "rusuh-queue"
	// AudienceQueue is the `aud` of device and queue tokens (only queue reads them).
	AudienceQueue = "rusuh-queue"
	// AudienceCore is the `aud` of admission tokens (core reads them).
	AudienceCore = "rusuh-core"

	// Leeway is the clock skew allowed on exp, nbf and iat.
	Leeway = 30 * time.Second
)

var (
	ErrInvalid   = errors.New("token invalid")
	ErrNoKeys    = errors.New("token: key ring is empty")
	ErrWrongKind = errors.New("token: wrong kind")
)

// Key is one Ed25519 signing key and its id.
type Key struct {
	ID      string
	Private ed25519.PrivateKey
}

// Keyring signs with its first key and verifies with any of its keys, so a
// rotation lists the new key first and keeps the old one until its tokens expire.
type Keyring struct {
	keys []Key
	byID map[string]ed25519.PublicKey
	now  func() time.Time
}

// NewKeyring builds a ring; the first key signs.
func NewKeyring(keys ...Key) (*Keyring, error) {
	if len(keys) == 0 {
		return nil, ErrNoKeys
	}
	byID := make(map[string]ed25519.PublicKey, len(keys))
	for _, k := range keys {
		if k.ID == "" {
			return nil, errors.New("token: key without id")
		}
		if len(k.Private) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("token: key %q is not an Ed25519 private key", k.ID)
		}
		if _, dup := byID[k.ID]; dup {
			return nil, fmt.Errorf("token: duplicate key id %q", k.ID)
		}
		byID[k.ID] = k.Private.Public().(ed25519.PublicKey)
	}
	return &Keyring{keys: keys, byID: byID, now: time.Now}, nil
}

// ParseKeys reads "kid=base64(32-byte Ed25519 seed)" entries separated by commas,
// the format of the SIGNING_KEYS setting.
func ParseKeys(s string) ([]Key, error) {
	var keys []Key
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, enc, ok := strings.Cut(part, "=")
		if !ok || id == "" {
			return nil, fmt.Errorf("token: entry %q is not kid=seed", redact(part))
		}
		seed, err := base64.StdEncoding.DecodeString(enc)
		if err != nil || len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("token: key %q needs a base64 %d-byte seed", id, ed25519.SeedSize)
		}
		keys = append(keys, Key{ID: id, Private: ed25519.NewKeyFromSeed(seed)})
	}
	if len(keys) == 0 {
		return nil, ErrNoKeys
	}
	return keys, nil
}

// GenerateKey makes a random key, for tests and local development only.
func GenerateKey(id string) (Key, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Key{}, err
	}
	return Key{ID: id, Private: priv}, nil
}

// Sign issues a token of the given kind. The claims' Issuer, IssuedAt and ID
// are filled in; the caller sets Subject, Audience and ExpiresAt.
func (k *Keyring) Sign(kind Kind, claims Claims) (string, error) {
	rc := claims.registered()
	now := k.now()
	rc.Issuer = Issuer
	rc.IssuedAt = jwt.NewNumericDate(now)
	if rc.ID == "" {
		id, err := randomID()
		if err != nil {
			return "", err
		}
		rc.ID = id
	}
	if rc.ExpiresAt == nil || len(rc.Audience) == 0 {
		return "", errors.New("token: exp and aud are required")
	}

	signer := k.keys[0]
	t := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	t.Header["typ"] = string(kind)
	t.Header["kid"] = signer.ID
	return t.SignedString(signer.Private)
}

// Verify checks signature, kind, issuer, audience and times, and fills claims.
// Only EdDSA is accepted, whatever the token's header says.
func (k *Keyring) Verify(kind Kind, raw, audience string, claims Claims) error {
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
		jwt.WithIssuer(Issuer),
		jwt.WithAudience(audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(Leeway),
		jwt.WithTimeFunc(k.now),
	)
	_, err := parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		if typ, _ := t.Header["typ"].(string); typ != string(kind) {
			return nil, ErrWrongKind
		}
		kid, _ := t.Header["kid"].(string)
		pub, ok := k.byID[kid]
		if !ok {
			return nil, fmt.Errorf("token: unknown kid %q", kid)
		}
		return pub, nil
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func redact(s string) string {
	if id, _, ok := strings.Cut(s, "="); ok {
		return id + "=…"
	}
	return "…"
}
