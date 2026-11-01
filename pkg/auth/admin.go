package auth

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/remotejwks"
)

// AdminAudience is the aud an operator token for queue's admin API must carry.
const AdminAudience = "rusuh-queue-admin"

// AdminVerifier checks the operator's bearer token (adminAuth: company SSO with MFA).
type AdminVerifier struct {
	issuer string
	keys   *remotejwks.Verifier
}

func NewAdmin(jwksURL, issuer string) *AdminVerifier {
	return &AdminVerifier{issuer: issuer, keys: remotejwks.New(jwksURL)}
}

// Configured is false when no ADMIN_JWKS_URL is set; every admin call is then refused.
func (v *AdminVerifier) Configured() bool { return v.keys.Configured() }

// Operator returns the operator's id from Authorization: Bearer. ErrMissing and
// ErrInvalid mean 401; ErrForbidden means the token is valid but without MFA.
func (v *AdminVerifier) Operator(ctx context.Context, r *http.Request) (string, error) {
	raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || raw == "" {
		return "", ErrMissing
	}
	var c AccessClaims
	err := v.keys.Verify(ctx, raw, &c, jwt.WithIssuer(v.issuer), jwt.WithAudience(AdminAudience), jwt.WithIssuedAt())
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if c.Subject == "" {
		return "", fmt.Errorf("%w: no sub", ErrInvalid)
	}
	if !slices.Contains(c.AMR, "mfa") {
		return "", ErrForbidden
	}
	return c.Subject, nil
}
