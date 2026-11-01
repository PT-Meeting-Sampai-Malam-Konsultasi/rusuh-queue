// Package auth reads and verifies the tokens on a queue request:
// X-Device-Token, X-Queue-Token (both signed by queue) and the optional
// Authorization: Bearer access token signed by core (D-4).
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/remotejwks"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

var (
	ErrMissing   = errors.New("auth: token missing")
	ErrInvalid   = errors.New("auth: token invalid")
	ErrForbidden = errors.New("auth: token lacks the required authentication")
)

const (
	// CoreIssuer and the audience queue expects on core's access tokens
	// (rusuh-documentation/api/tokens.md, "Access tokens queue reads").
	CoreIssuer     = "rusuh-core"
	AccessAudience = "rusuh-queue"
)

// AccessClaims is the part of core's access token queue uses.
type AccessClaims struct {
	AMR []string `json:"amr"`
	jwt.RegisteredClaims
}

type Verifier struct {
	keys *token.Keyring
	core *remotejwks.Verifier
}

func New(keys *token.Keyring, core *remotejwks.Verifier) *Verifier {
	return &Verifier{keys: keys, core: core}
}

// Device returns the caller's device id from X-Device-Token.
func (v *Verifier) Device(r *http.Request) (string, error) {
	raw := r.Header.Get("X-Device-Token")
	if raw == "" {
		return "", ErrMissing
	}
	var c token.DeviceClaims
	if err := v.keys.Verify(token.KindDevice, raw, token.AudienceQueue, &c); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if c.Subject == "" {
		return "", ErrInvalid
	}
	return c.Subject, nil
}

// Place returns the claims of X-Queue-Token, which must belong to deviceID and event.
func (v *Verifier) Place(r *http.Request, event, deviceID string) (token.QueueClaims, error) {
	var c token.QueueClaims
	raw := r.Header.Get("X-Queue-Token")
	if raw == "" {
		return c, ErrMissing
	}
	if err := v.keys.Verify(token.KindQueue, raw, token.AudienceQueue, &c); err != nil {
		return c, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if c.Subject != deviceID || c.Event != event {
		return c, fmt.Errorf("%w: queue token belongs to another device or event", ErrInvalid)
	}
	return c, nil
}

// Account returns the user id from an optional Authorization: Bearer token.
// No header → ("", nil). A header that does not verify → ErrInvalid: a caller
// who sends a token must not be silently treated as a guest.
func (v *Verifier) Account(ctx context.Context, r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", nil
	}
	raw, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || raw == "" {
		return "", fmt.Errorf("%w: Authorization must be Bearer", ErrInvalid)
	}
	var c AccessClaims
	err := v.core.Verify(ctx, raw, &c,
		jwt.WithIssuer(CoreIssuer),
		jwt.WithAudience(AccessAudience),
		jwt.WithIssuedAt(),
	)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if c.Subject == "" || !slices.Contains(c.AMR, "otp") {
		return "", fmt.Errorf("%w: access token without sub or amr=otp", ErrInvalid)
	}
	return c.Subject, nil
}
