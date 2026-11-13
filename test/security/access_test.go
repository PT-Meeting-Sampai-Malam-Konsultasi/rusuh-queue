package security

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

// OWASP A07: a bad core access token is refused, never downgraded to a guest,
// so it neither joins nor attaches an account nor passes the login check of a claim.
func TestBadAccessTokenIsRefusedNotTreatedAsGuest(t *testing.T) {
	e := newEnv(t)
	k := e.openEvent(t, "evt_acc", 3)
	_, attacker, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now()
	claims := func(iss, aud string, exp *jwt.NumericDate) auth.AccessClaims {
		return auth.AccessClaims{RegisteredClaims: jwt.RegisteredClaims{
			Issuer: iss, Subject: "acc_victim", Audience: jwt.ClaimStrings{aud},
			IssuedAt: jwt.NewNumericDate(now), ExpiresAt: exp,
		}}
	}
	valid := jwt.NewNumericDate(now.Add(15 * time.Minute))

	bad := map[string]string{
		"signed by another key under core's kid": "Bearer " + signAccess(t, attacker, coreKid, jwt.SigningMethodEdDSA, claims(coreIssuer, coreAudience, valid)),
		"alg none":                               "Bearer " + signAccess(t, jwt.UnsafeAllowNoneSignatureType, coreKid, jwt.SigningMethodNone, claims(coreIssuer, coreAudience, valid)),
		"issuer of tokens.md, not core's auth":   "Bearer " + signAccess(t, e.core, coreKid, jwt.SigningMethodEdDSA, claims(auth.CoreIssuer, coreAudience, valid)),
		"audience of another service":            "Bearer " + signAccess(t, e.core, coreKid, jwt.SigningMethodEdDSA, claims(coreIssuer, "rusuh-admin", valid)),
		"expired": "Bearer " + signAccess(t, e.core, coreKid, jwt.SigningMethodEdDSA,
			claims(coreIssuer, coreAudience, jwt.NewNumericDate(now.Add(-time.Minute)))),
		"no exp":                   "Bearer " + signAccess(t, e.core, coreKid, jwt.SigningMethodEdDSA, claims(coreIssuer, coreAudience, nil)),
		"queue's own device token": "Bearer " + e.deviceToken(t, "w_x"),
		"basic scheme":             "Basic YWRtaW46YWRtaW4=",
		"empty bearer":             "Bearer ",
	}

	device := e.deviceToken(t, "w_acc")
	before := seqCount(t, e, k)
	for name, h := range bad {
		rec := e.do("POST", "/v1/queue/evt_acc/join", map[string]string{"X-Device-Token": device, "Authorization": h})
		if rec.Code != http.StatusUnauthorized || problemCode(t, rec) != api.ErrorCodeUNAUTHORIZED {
			t.Errorf("join with %s: %d %s, want 401 UNAUTHORIZED", name, rec.Code, rec.Body)
		}
	}
	if after := seqCount(t, e, k); after != before {
		t.Fatalf("seq %d → %d: a bad access token got a place", before, after)
	}

	guest := caller{device: "w_acc", deviceToken: device}
	rec := e.do("POST", "/v1/queue/evt_acc/join", map[string]string{"X-Device-Token": device})
	var p place
	if rec.Code != http.StatusOK || json.NewDecoder(rec.Body).Decode(&p) != nil {
		t.Fatalf("guest join: %d %s", rec.Code, rec.Body)
	}
	guest.queueToken = p.QueueToken
	testkit.Tick(t, e.client, k, testkit.TickArgs{})

	for name, h := range bad {
		headers := guest.headers()
		headers["Authorization"] = h
		if rec := e.do("POST", "/v1/queue/evt_acc/claim", headers); rec.Code != http.StatusUnauthorized ||
			problemCode(t, rec) != api.ErrorCodeUNAUTHORIZED {
			t.Errorf("claim with %s: %d %s, want 401 UNAUTHORIZED", name, rec.Code, rec.Body)
		}
	}
	if rec := e.claim("evt_acc", guest); rec.Code != http.StatusUnauthorized || problemCode(t, rec) != api.ErrorCodeLOGINREQUIRED {
		t.Fatalf("guest claim: %d %s, want 401 LOGIN_REQUIRED", rec.Code, rec.Body)
	}
	guest.bearer = e.accessToken(t, "acc_victim")
	if a := decodeAdmission(t, e.claim("evt_acc", guest)); a.ReservedUnits < 1 {
		t.Fatalf("claim with core's real token: %+v", a)
	}
}
