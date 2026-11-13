package security

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

// publishedKey reads queue's own public key from its JWKS, as an attacker can.
func publishedKey(t *testing.T, e env) []byte {
	t.Helper()
	rec := e.do("GET", "/.well-known/jwks.json", nil)
	var set struct {
		Keys []struct{ X string } `json:"keys"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&set); err != nil || len(set.Keys) == 0 {
		t.Fatalf("jwks: %d %s", rec.Code, rec.Body)
	}
	x, err := base64.RawURLEncoding.DecodeString(set.Keys[0].X)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func forge(t *testing.T, method jwt.SigningMethod, key any, kind token.Kind, kid string, c jwt.Claims) string {
	t.Helper()
	tok := jwt.NewWithClaims(method, c)
	tok.Header["typ"], tok.Header["kid"] = string(kind), kid
	raw, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func deviceClaims(device string, exp time.Time, aud string) *token.DeviceClaims {
	return &token.DeviceClaims{Platform: "web", RegisteredClaims: jwt.RegisteredClaims{
		Issuer: token.Issuer, Subject: device, Audience: jwt.ClaimStrings{aud},
		IssuedAt: jwt.NewNumericDate(time.Now()), ExpiresAt: jwt.NewNumericDate(exp),
	}}
}

// OWASP A02/A07: no forged, expired or re-purposed token gets a place in the queue.
func TestForgedDeviceTokensGetNoPlace(t *testing.T) {
	e := newEnv(t)
	k := e.openEvent(t, "evt_forge", 3)
	victim := e.join(t, "evt_forge", "w_victim", "acc_victim")
	testkit.Tick(t, e.client, k, testkit.TickArgs{})
	admission := decodeAdmission(t, e.claim("evt_forge", victim)).AdmissionToken
	before := seqCount(t, e, k)

	_, attacker, _ := ed25519.GenerateKey(rand.Reader)
	hour := time.Now().Add(time.Hour)
	tampered := func() string {
		parts := strings.Split(e.deviceToken(t, "w_attacker"), ".")
		payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
		parts[1] = base64.RawURLEncoding.EncodeToString([]byte(strings.Replace(string(payload), "w_attacker", "w_victim", 1)))
		return strings.Join(parts, ".")
	}

	for name, raw := range map[string]string{
		"own key under queue's kid": forge(t, jwt.SigningMethodEdDSA, attacker, token.KindDevice, "k1",
			deviceClaims("w_attacker", hour, token.AudienceQueue)),
		"alg none": forge(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, token.KindDevice, "k1",
			deviceClaims("w_attacker", hour, token.AudienceQueue)),
		"HS256 keyed with the published public key": forge(t, jwt.SigningMethodHS256, publishedKey(t, e), token.KindDevice, "k1",
			deviceClaims("w_attacker", hour, token.AudienceQueue)),
		"expired beyond the leeway": func() string {
			raw, err := e.keys.Sign(token.KindDevice, deviceClaims("w_attacker", time.Now().Add(-token.Leeway-time.Minute), token.AudienceQueue))
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}(),
		"audience of core": func() string {
			raw, err := e.keys.Sign(token.KindDevice, deviceClaims("w_attacker", hour, token.AudienceCore))
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}(),
		"payload changed to the victim": tampered(),
		"queue token":                   victim.queueToken,
		"admission token":               admission,
		"core access token":             victim.bearer,
		"garbage":                       "not.a.jwt",
	} {
		rec := e.do("POST", "/v1/queue/evt_forge/join", map[string]string{"X-Device-Token": raw})
		if rec.Code != http.StatusUnauthorized || problemCode(t, rec) != api.ErrorCodeUNAUTHORIZED {
			t.Errorf("%s: %d %s, want 401 UNAUTHORIZED", name, rec.Code, rec.Body)
		}
	}
	if after := seqCount(t, e, k); after != before {
		t.Fatalf("seq %d → %d: a forged token got a place", before, after)
	}
}

// OWASP A01 (IDOR): another device cannot read, claim, leave or stream a place,
// and a queue token of one event does not open another.
func TestPlaceTokenIsBoundToItsDeviceAndEvent(t *testing.T) {
	e := newEnv(t)
	e.openEvent(t, "evt_a", 3)
	e.openEvent(t, "evt_b", 3)
	victim := e.join(t, "evt_a", "w_victim", "acc_victim")
	attacker := e.join(t, "evt_a", "w_attacker", "acc_attacker")
	attackerB := e.join(t, "evt_b", "w_attacker", "acc_attacker")

	stolen := attacker
	stolen.queueToken = victim.queueToken
	otherEvent := attacker
	otherEvent.queueToken = attackerB.queueToken

	for name, c := range map[string]caller{"victim's queue token": stolen, "queue token of evt_b": otherEvent} {
		for _, r := range []struct{ method, path string }{
			{"GET", "/v1/queue/evt_a/me"},
			{"POST", "/v1/queue/evt_a/claim"},
			{"DELETE", "/v1/queue/evt_a/me"},
			{"GET", "/v1/queue/evt_a/stream"},
		} {
			rec := e.do(r.method, r.path, c.headers())
			if rec.Code != http.StatusUnauthorized || problemCode(t, rec) != api.ErrorCodeUNAUTHORIZED {
				t.Errorf("%s on %s %s: %d %s, want 401", name, r.method, r.path, rec.Code, rec.Body)
			}
		}
	}

	rec := e.do("GET", "/v1/queue/evt_a/me", victim.headers())
	var p place
	if rec.Code != http.StatusOK || json.NewDecoder(rec.Body).Decode(&p) != nil || p.Seq == nil || *p.Seq != victim.seq {
		t.Fatalf("victim's place after the attempts: %d %s, want seq %d", rec.Code, rec.Body, victim.seq)
	}
}

// OWASP A09/A04: a refused token learns nothing about why (no key ids, no parser errors).
func TestRefusedTokenRevealsNoDetail(t *testing.T) {
	e := newEnv(t)
	e.openEvent(t, "evt_leak", 3)
	_, attacker, _ := ed25519.GenerateKey(rand.Reader)
	raw := forge(t, jwt.SigningMethodEdDSA, attacker, token.KindDevice, "k9", deviceClaims("w_x", time.Now().Add(time.Hour), token.AudienceQueue))

	rec := e.do("POST", "/v1/queue/evt_leak/join", map[string]string{"X-Device-Token": raw})
	body := strings.ToLower(rec.Body.String())
	for _, leak := range []string{"k9", "kid", "signature", "ed25519", "malformed", "unverifiable", "crypto"} {
		if strings.Contains(body, leak) {
			t.Errorf("401 body mentions %q: %s", leak, rec.Body)
		}
	}
}
