package token

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func newRing(t *testing.T, ids ...string) *Keyring {
	t.Helper()
	var keys []Key
	for _, id := range ids {
		k, err := GenerateKey(id)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	ring, err := NewKeyring(keys...)
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

func deviceClaims(exp time.Duration) *DeviceClaims {
	return &DeviceClaims{
		Platform: "web",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "w_device1",
			Audience:  jwt.ClaimStrings{AudienceQueue},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(exp)),
		},
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	ring := newRing(t, "k1")
	raw, err := ring.Sign(KindDevice, deviceClaims(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	var got DeviceClaims
	if err := ring.Verify(KindDevice, raw, AudienceQueue, &got); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.Subject != "w_device1" || got.Platform != "web" || got.Issuer != Issuer || got.ID == "" {
		t.Fatalf("claims = %+v", got)
	}
}

// TestVerifyRefuses covers the forged and confused tokens from the M1 verify step.
func TestVerifyRefuses(t *testing.T) {
	ring := newRing(t, "k1")
	other := newRing(t, "k1") // same kid, different key

	good, err := ring.Sign(KindDevice, deviceClaims(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		raw      func(t *testing.T) string
		kind     Kind
		audience string
	}{
		{"signed by another key with the same kid", func(t *testing.T) string {
			raw, err := other.Sign(KindDevice, deviceClaims(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}, KindDevice, AudienceQueue},
		{"alg none", func(t *testing.T) string {
			tok := jwt.NewWithClaims(jwt.SigningMethodNone, deviceClaims(time.Hour))
			tok.Header["typ"], tok.Header["kid"] = string(KindDevice), "k1"
			raw, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}, KindDevice, AudienceQueue},
		{"HS256 with the public key as secret", func(t *testing.T) string {
			pub := ring.byID["k1"]
			tok := jwt.NewWithClaims(jwt.SigningMethodHS256, deviceClaims(time.Hour))
			tok.Header["typ"], tok.Header["kid"] = string(KindDevice), "k1"
			raw, err := tok.SignedString([]byte(pub))
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}, KindDevice, AudienceQueue},
		{"expired beyond the leeway", func(t *testing.T) string {
			raw, err := ring.Sign(KindDevice, deviceClaims(-Leeway-time.Second))
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}, KindDevice, AudienceQueue},
		{"device token used as an admission token", func(*testing.T) string { return good }, KindAdmission, AudienceQueue},
		{"wrong audience", func(*testing.T) string { return good }, KindDevice, AudienceCore},
		{"unknown kid", func(t *testing.T) string {
			raw, err := newRing(t, "k9").Sign(KindDevice, deviceClaims(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			return raw
		}, KindDevice, AudienceQueue},
		{"tampered payload", func(*testing.T) string {
			parts := strings.Split(good, ".")
			payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
			payload = []byte(strings.Replace(string(payload), "w_device1", "w_device2", 1))
			parts[1] = base64.RawURLEncoding.EncodeToString(payload)
			return strings.Join(parts, ".")
		}, KindDevice, AudienceQueue},
		{"garbage", func(*testing.T) string { return "not.a.jwt" }, KindDevice, AudienceQueue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c DeviceClaims
			err := ring.Verify(tt.kind, tt.raw(t), tt.audience, &c)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestVerifyAllowsSkewWithinLeeway(t *testing.T) {
	ring := newRing(t, "k1")
	raw, err := ring.Sign(KindDevice, deviceClaims(-Leeway/2))
	if err != nil {
		t.Fatal(err)
	}
	var c DeviceClaims
	if err := ring.Verify(KindDevice, raw, AudienceQueue, &c); err != nil {
		t.Fatalf("expired %s ago should pass with %s leeway: %v", Leeway/2, Leeway, err)
	}
}

func TestRotation(t *testing.T) {
	oldKey, _ := GenerateKey("2026-10")
	newKey, _ := GenerateKey("2026-11")

	before, _ := NewKeyring(oldKey)
	raw, err := before.Sign(KindDevice, deviceClaims(time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	during, _ := NewKeyring(newKey, oldKey)
	var c DeviceClaims
	if err := during.Verify(KindDevice, raw, AudienceQueue, &c); err != nil {
		t.Fatalf("old token during rotation: %v", err)
	}
	fresh, _ := during.Sign(KindDevice, deviceClaims(time.Hour))
	if kid := headerKid(t, fresh); kid != "2026-11" {
		t.Fatalf("signing kid = %q, want the new key", kid)
	}
	if set := during.JWKS(); len(set.Keys) != 2 || set.Keys[0].Kid != "2026-11" {
		t.Fatalf("jwks = %+v", set)
	}

	after, _ := NewKeyring(newKey)
	if err := after.Verify(KindDevice, raw, AudienceQueue, &c); !errors.Is(err, ErrInvalid) {
		t.Fatalf("old token after its key is removed: err = %v", err)
	}
}

func TestParseKeys(t *testing.T) {
	seed := base64.StdEncoding.EncodeToString(make([]byte, ed25519.SeedSize))
	keys, err := ParseKeys(" a=" + seed + ", b=" + seed + " ")
	if err != nil || len(keys) != 2 || keys[0].ID != "a" || keys[1].ID != "b" {
		t.Fatalf("keys = %v, err = %v", keys, err)
	}

	for _, bad := range []string{"", "noequals", "a=notbase64!", "a=" + base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, err := ParseKeys(bad); err == nil {
			t.Errorf("ParseKeys(%q) should fail", bad)
		}
	}
	if _, err := NewKeyring(keys[0], keys[0]); err == nil {
		t.Error("duplicate kid should fail")
	}
}

func TestJWKSFormat(t *testing.T) {
	ring := newRing(t, "k1")
	k := ring.JWKS().Keys[0]
	x, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil || len(x) != ed25519.PublicKeySize {
		t.Fatalf("x = %q, err = %v", k.X, err)
	}
	if k.Kty != "OKP" || k.Crv != "Ed25519" || k.Alg != "EdDSA" || k.Use != "sig" {
		t.Fatalf("jwk = %+v", k)
	}
}

func headerKid(t *testing.T, raw string) string {
	t.Helper()
	tok, _, err := jwt.NewParser().ParseUnverified(raw, &DeviceClaims{})
	if err != nil {
		t.Fatal(err)
	}
	kid, _ := tok.Header["kid"].(string)
	return kid
}
