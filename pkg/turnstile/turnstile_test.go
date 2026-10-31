package turnstile

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func fakeSiteverify(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil || r.PostForm.Get("secret") != "s3cret" || r.PostForm.Get("remoteip") != "203.0.113.7" {
			t.Errorf("form = %v", r.PostForm)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestVerify(t *testing.T) {
	ip := netip.MustParseAddr("203.0.113.7")
	tests := []struct {
		name      string
		status    int
		body      string
		hostnames []string
		want      error
	}{
		{"success", 200, `{"success":true,"hostname":"rusuh-app.bingbing.my.id"}`, nil, nil},
		{"success on the expected host", 200, `{"success":true,"hostname":"rusuh-app.bingbing.my.id"}`, []string{"rusuh-app.bingbing.my.id"}, nil},
		{"token minted on another host", 200, `{"success":true,"hostname":"evil.example"}`, []string{"rusuh-app.bingbing.my.id"}, ErrRejected},
		{"invalid token", 200, `{"success":false,"error-codes":["invalid-input-response"]}`, nil, ErrRejected},
		{"reused token", 200, `{"success":false,"error-codes":["timeout-or-duplicate"]}`, nil, ErrRejected},
		{"cloudflare internal error", 200, `{"success":false,"error-codes":["internal-error"]}`, nil, ErrUnavailable},
		{"siteverify 5xx", 502, `bad gateway`, nil, ErrUnavailable},
		{"garbage body", 200, `<html>`, nil, ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := fakeSiteverify(t, tt.status, tt.body)
			err := New("s3cret", srv.URL, tt.hostnames).Verify(context.Background(), "tok", ip)
			if (tt.want == nil) != (err == nil) || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestVerifyEmptyTokenSkipsNetwork(t *testing.T) {
	c := New("s3cret", "http://127.0.0.1:1", nil)
	if err := c.Verify(context.Background(), "  ", netip.Addr{}); !errors.Is(err, ErrRejected) {
		t.Fatalf("err = %v", err)
	}
}

func TestVerifyUnreachable(t *testing.T) {
	c := New("s3cret", "http://127.0.0.1:1", nil)
	if err := c.Verify(context.Background(), "tok", netip.Addr{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
}
