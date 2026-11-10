package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/remotejwks"
)

func request(headers map[string]string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestDevice(t *testing.T) {
	tk := testkit.NewTokens(t)
	v := tk.Verifier()
	if id, err := v.Device(request(map[string]string{"X-Device-Token": tk.Device(t, "w_1")})); err != nil || id != "w_1" {
		t.Fatalf("id = %q, err = %v", id, err)
	}
	if _, err := v.Device(request(nil)); !errors.Is(err, auth.ErrMissing) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := v.Device(request(map[string]string{"X-Device-Token": tk.Queue(t, "w_1", "evt1", 1)})); !errors.Is(err, auth.ErrInvalid) {
		t.Fatalf("queue token as device token: %v", err)
	}
	if _, err := v.Device(request(map[string]string{"X-Device-Token": tk.Device(t, "")})); !errors.Is(err, auth.ErrInvalid) {
		t.Fatalf("no subject: %v", err)
	}
}

func TestPlace(t *testing.T) {
	tk := testkit.NewTokens(t)
	v := tk.Verifier()
	c, err := v.Place(request(map[string]string{"X-Queue-Token": tk.Queue(t, "w_1", "evt1", 7)}), "evt1", "w_1")
	if err != nil || c.Seq != 7 {
		t.Fatalf("claims = %+v, err = %v", c, err)
	}
	if _, err := v.Place(request(nil), "evt1", "w_1"); !errors.Is(err, auth.ErrMissing) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := v.Place(request(map[string]string{"X-Queue-Token": "garbage"}), "evt1", "w_1"); !errors.Is(err, auth.ErrInvalid) {
		t.Fatalf("garbage: %v", err)
	}
	for _, other := range [][2]string{{"evt2", "w_1"}, {"evt1", "w_2"}} {
		if _, err := v.Place(request(map[string]string{"X-Queue-Token": tk.Queue(t, "w_1", "evt1", 7)}), other[0], other[1]); !errors.Is(err, auth.ErrInvalid) {
			t.Fatalf("%v: %v", other, err)
		}
	}
}

func TestAccount(t *testing.T) {
	tk := testkit.NewTokens(t)
	v := tk.Verifier()
	ctx := context.Background()
	bearer := func(raw string) *http.Request { return request(map[string]string{"Authorization": "Bearer " + raw}) }

	if id, err := v.Account(ctx, request(nil)); err != nil || id != "" {
		t.Fatalf("guest: %q, %v", id, err)
	}
	if id, err := v.Account(ctx, bearer(tk.Core.Access(t, "user_1"))); err != nil || id != "user_1" {
		t.Fatalf("user: %q, %v", id, err)
	}
	for name, r := range map[string]*http.Request{
		"basic":        request(map[string]string{"Authorization": "Basic x"}),
		"empty bearer": request(map[string]string{"Authorization": "Bearer "}),
		"no otp":       bearer(tk.Core.Token(t, auth.CoreIssuer, "user_1", []string{"pwd"}, auth.AccessAudience)),
		"no subject":   bearer(tk.Core.Token(t, auth.CoreIssuer, "", []string{"otp"}, auth.AccessAudience)),
		"other issuer": bearer(tk.Core.Token(t, "someone", "user_1", []string{"otp"}, auth.AccessAudience)),
		"other aud":    bearer(tk.Core.Token(t, auth.CoreIssuer, "user_1", []string{"otp"}, "rusuh-core")),
	} {
		if _, err := v.Account(ctx, r); !errors.Is(err, auth.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}

	tk.Core.Down.Store(true)
	cold := auth.New(tk.Keys, remotejwks.New(tk.Core.URL()))
	if _, err := cold.Account(ctx, bearer(tk.Core.Access(t, "user_1"))); !errors.Is(err, auth.ErrUnavailable) {
		t.Fatalf("core down: %v", err)
	}
}

func TestOperator(t *testing.T) {
	sso := testkit.NewIssuer(t)
	v := auth.NewAdmin(sso.URL(), "rusuh-admin")
	v.Warm()
	ctx := context.Background()
	bearer := func(sub string, amr ...string) *http.Request {
		return request(map[string]string{"Authorization": "Bearer " + sso.Token(t, "rusuh-admin", sub, amr, auth.AdminAudience)})
	}

	if !v.Configured() || auth.NewAdmin("", "x").Configured() {
		t.Fatal("Configured is wrong")
	}
	if id, err := v.Operator(ctx, bearer("op_1", "pwd", "mfa")); err != nil || id != "op_1" {
		t.Fatalf("operator: %q, %v", id, err)
	}
	if _, err := v.Operator(ctx, request(nil)); !errors.Is(err, auth.ErrMissing) {
		t.Fatalf("missing: %v", err)
	}
	if _, err := v.Operator(ctx, bearer("op_1", "pwd")); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("no mfa: %v", err)
	}
	if _, err := v.Operator(ctx, bearer("", "mfa")); !errors.Is(err, auth.ErrInvalid) {
		t.Fatalf("no subject: %v", err)
	}
	if _, err := v.Operator(ctx, request(map[string]string{"Authorization": "Bearer x"})); !errors.Is(err, auth.ErrInvalid) {
		t.Fatalf("garbage: %v", err)
	}
}
