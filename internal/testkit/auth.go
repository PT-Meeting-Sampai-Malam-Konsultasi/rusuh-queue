package testkit

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/remotejwks"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

// Issuer is a fake external issuer (core or the operators' SSO) serving its JWKS over HTTP.
type Issuer struct {
	priv ed25519.PrivateKey
	srv  *httptest.Server
	// Down makes the JWKS endpoint answer 503, as if the issuer were unreachable.
	Down atomic.Bool
}

func NewIssuer(t *testing.T) *Issuer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	is := &Issuer{priv: priv}
	is.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if is.Down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintf(w, `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"ext-1","alg":"EdDSA","use":"sig","x":%q}]}`,
			base64.RawURLEncoding.EncodeToString(pub))
	}))
	t.Cleanup(is.srv.Close)
	return is
}

// URL is the issuer's JWKS URL.
func (is *Issuer) URL() string { return is.srv.URL }

// Token signs an access token as iss for sub with the given amr and aud.
func (is *Issuer) Token(t *testing.T, iss, sub string, amr []string, aud string) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, auth.AccessClaims{AMR: amr, RegisteredClaims: jwt.RegisteredClaims{
		Issuer: iss, Subject: sub, Audience: jwt.ClaimStrings{aud},
		IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
	}})
	tok.Header["kid"] = "ext-1"
	raw, err := tok.SignedString(is.priv)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Access signs a core access token for user, logged in with an OTP.
func (is *Issuer) Access(t *testing.T, user string) string {
	return is.Token(t, auth.CoreIssuer, user, []string{"otp"}, auth.AccessAudience)
}

// Tokens signs queue's own device and queue tokens.
type Tokens struct {
	Keys *token.Keyring
	Core *Issuer
}

func NewTokens(t *testing.T) Tokens {
	t.Helper()
	k, err := token.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := token.NewKeyring(k)
	if err != nil {
		t.Fatal(err)
	}
	return Tokens{Keys: keys, Core: NewIssuer(t)}
}

// Verifier checks tokens the way the handlers do, with core's keys from tk.Core.
func (tk Tokens) Verifier() *auth.Verifier {
	return auth.New(tk.Keys, remotejwks.New(tk.Core.URL()))
}

func (tk Tokens) sign(t *testing.T, kind token.Kind, c token.Claims) string {
	t.Helper()
	raw, err := tk.Keys.Sign(kind, c)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func registered(sub string) jwt.RegisteredClaims {
	return jwt.RegisteredClaims{Subject: sub, Audience: jwt.ClaimStrings{token.AudienceQueue}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}
}

func (tk Tokens) Device(t *testing.T, device string) string {
	return tk.sign(t, token.KindDevice, &token.DeviceClaims{Platform: "web", RegisteredClaims: registered(device)})
}

func (tk Tokens) Queue(t *testing.T, device, event string, seq int64) string {
	return tk.sign(t, token.KindQueue, &token.QueueClaims{Event: event, Seq: seq, RegisteredClaims: registered(device)})
}
