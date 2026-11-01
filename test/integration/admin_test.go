package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/delivery"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

func (c *fakeCore) operatorToken(t *testing.T, sub string, amr []string, aud string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, auth.AccessClaims{AMR: amr, RegisteredClaims: jwt.RegisteredClaims{
		Issuer: "rusuh-admin", Subject: sub, Audience: jwt.ClaimStrings{aud},
		IssuedAt: jwt.NewNumericDate(time.Now()), ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
	}})
	tok.Header["kid"] = "core-1"
	raw, err := tok.SignedString(c.priv)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func patchQueue(mux *http.ServeMux, id, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("PATCH", "/admin/events/"+id+"/queue", bytes.NewBufferString(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// P5 / OPS-01: an operator pauses, closes and reopens a queue; every change is audited.
func TestAdminUpdateQueue(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	sso := newFakeCore(t)
	admin := delivery.NewAdmin(db, client)
	mux := http.NewServeMux()
	delivery.RegisterAdmin(mux, admin, auth.NewAdmin(sso.srv.URL, "rusuh-admin"))
	ctrl := delivery.NewController(delivery.ControllerDeps{Config: fastController(), DB: db, Valkey: client, Settings: admin})

	const id = "evt_ops"
	k := qkeys.For(id)
	testkit.CoreSale(t, client, sale(id, -time.Minute, "published"), stock(id, 100))
	if err := ctrl.Sync(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	ok := sso.operatorToken(t, "ops@rusuh", []string{"pwd", "mfa"}, auth.AdminAudience)

	for _, tc := range []struct {
		name, token, body string
		want              int
	}{
		{"no token", "", `{"state":"paused"}`, 401},
		{"no mfa", sso.operatorToken(t, "ops@rusuh", []string{"pwd"}, auth.AdminAudience), `{"state":"paused"}`, 403},
		{"wrong audience", sso.operatorToken(t, "ops@rusuh", []string{"mfa"}, "rusuh-queue"), `{"state":"paused"}`, 401},
		{"unknown field", ok, `{"state":"paused","speed":9}`, 400},
		{"zero rate cap", ok, `{"rateCapPerSecond":0}`, 400},
		{"unknown state", ok, `{"state":"stopped"}`, 400},
	} {
		if rec := patchQueue(mux, id, tc.token, tc.body); rec.Code != tc.want {
			t.Fatalf("%s: %d %s, want %d", tc.name, rec.Code, rec.Body, tc.want)
		}
	}
	if rec := patchQueue(mux, "evt_none", ok, `{"state":"paused"}`); rec.Code != 404 {
		t.Fatalf("unknown event: %d", rec.Code)
	}

	frontier := func(rec *httptest.ResponseRecorder) api.Frontier {
		t.Helper()
		if rec.Code != 200 {
			t.Fatalf("patch %d %s", rec.Code, rec.Body)
		}
		var f api.Frontier
		if err := json.NewDecoder(rec.Body).Decode(&f); err != nil {
			t.Fatal(err)
		}
		return f
	}

	f := frontier(patchQueue(mux, id, ok, `{"state":"paused","rateCapPerSecond":20,"reason":"payment gateway slow"}`))
	if f.State != api.QueueStatePaused || f.Reason == nil || *f.Reason != api.FrontierReasonOperator {
		t.Fatalf("paused frontier %+v", f)
	}
	if r := testkit.Tick(t, client, k, testkit.TickArgs{}); r[3] != "paused" {
		t.Fatalf("tick %v, want paused", r)
	}
	var state, by, reason string
	var rate int
	if err := db.QueryRow(`SELECT state, rate_cap, updated_by, reason FROM queue.event_queue_config WHERE event_id = $1`, id).
		Scan(&state, &rate, &by, &reason); err != nil || state != "paused" || rate != 20 || by != "ops@rusuh" || reason != "payment gateway slow" {
		t.Fatalf("audit row %q %d %q %q %v", state, rate, by, reason, err)
	}

	// The sync re-applies the saved switch if the Valkey write was lost.
	if err := client.Do(context.Background(), client.B().Hdel().Key(k.Cfg).Field("hold").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Sync(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if r := testkit.Tick(t, client, k, testkit.TickArgs{}); r[3] != "paused" {
		t.Fatalf("tick after sync %v, want paused", r)
	}

	f = frontier(patchQueue(mux, id, ok, `{"state":"closed","reason":"venue closed"}`))
	if f.State != api.QueueStateClosed || f.Reason == nil || *f.Reason != api.FrontierReasonOperator {
		t.Fatalf("closed frontier %+v", f)
	}
	f = frontier(patchQueue(mux, id, ok, `{"state":"open"}`))
	if f.State != api.QueueStateOpen || f.Reason != nil {
		t.Fatalf("reopened frontier %+v", f)
	}
	if r := testkit.Tick(t, client, k, testkit.TickArgs{}); r[3] != "open" {
		t.Fatalf("tick %v, want open", r)
	}
}

// An operator cannot reopen a queue that closed for another reason.
func TestAdminOpenKeepsSoldOutClosed(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	sso := newFakeCore(t)
	mux := http.NewServeMux()
	delivery.RegisterAdmin(mux, delivery.NewAdmin(db, client), auth.NewAdmin(sso.srv.URL, "rusuh-admin"))
	k := testkit.Event(t, client, "evt_gone", testkit.EventOpts{Available: 1})
	if err := client.Do(context.Background(), client.B().Set().Key(k.Unreserved).Value("0").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	testkit.Tick(t, client, k, testkit.TickArgs{})

	rec := patchQueue(mux, k.ID, sso.operatorToken(t, "ops", []string{"mfa"}, auth.AdminAudience), `{"state":"open"}`)
	var f api.Frontier
	_ = json.NewDecoder(rec.Body).Decode(&f)
	if rec.Code != 200 || f.State != api.QueueStateClosed || f.Reason == nil || *f.Reason != api.FrontierReasonSoldOut {
		t.Fatalf("%d %+v, want still closed / sold_out", rec.Code, f)
	}
}
