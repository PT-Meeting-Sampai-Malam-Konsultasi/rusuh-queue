package delivery

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_inbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/queue/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	queueentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	streamentity "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

func signingKeys(t *testing.T) string {
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	return "k1=" + base64.StdEncoding.EncodeToString(seed)
}

func testConfig(t *testing.T) *config.Config {
	t.Setenv("ENV", "development")
	t.Setenv("VALKEY_TLS_ENABLED", "false")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.DatabaseURL = testkit.PostgresDSN(t)
	cfg.ValkeyAddr = testkit.ValkeyAddr(t)
	cfg.ValkeyPassword = ""
	cfg.SigningKeys = ""
	cfg.TurnstileSecret = ""
	cfg.CoreJWKSURL = ""
	cfg.CoreInternalURL = ""
	cfg.AdminJWKSURL = ""
	cfg.ClientIPHeader = ""
	cfg.TrustedProxyCIDRs = nil
	cfg.ShutdownTimeout = 45 * time.Second
	return cfg
}

func TestNewServesAndStops(t *testing.T) {
	cfg := testConfig(t)
	cfg.CoreInternalURL = "http://core.internal"
	cfg.CoreJWKSURL = testkit.NewIssuer(t).URL()
	cfg.AdminJWKSURL = testkit.NewIssuer(t).URL()
	cfg.RequestLog = false
	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	for path, want := range map[string]int{"/healthz": 200, "/readyz": 200, "/.well-known/jwks.json": 200, "/v1/queue/evt1/frontier": 404} {
		w := httptest.NewRecorder()
		s.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != want {
			t.Errorf("GET %s = %d, want %d", path, w.Code, want)
		}
	}
	w := httptest.NewRecorder()
	s.AdminHandler.ServeHTTP(w, httptest.NewRequest(http.MethodPatch, "/admin/events/evt1/queue", strings.NewReader(`{}`)))
	if w.Code != http.StatusUnauthorized {
		t.Errorf("admin without token = %d", w.Code)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	s.Drain()
	w = httptest.NewRecorder()
	s.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz while draining = %d", w.Code)
	}
	cancel()
	<-done
	s.Close()
}

func TestRunLogsAFailedRoutine(t *testing.T) {
	s := &Server{background: []func(context.Context) error{
		func(context.Context) error { return errors.New("boom") },
		func(context.Context) error { return context.Canceled },
	}}
	s.Run(context.Background())
	(&Server{}).Run(context.Background())
}

func TestNewRefusesBadSettings(t *testing.T) {
	for name, mutate := range map[string]func(c *config.Config){
		"bad signing keys":         func(c *config.Config) { c.SigningKeys = "nope" },
		"no keys outside dev":      func(c *config.Config) { c.Env = "production" },
		"no turnstile outside dev": func(c *config.Config) { c.Env = "production"; c.SigningKeys = signingKeys(t) },
		"unknown attestor":         func(c *config.Config) { c.Attestor = "play" },
		"rate window below miss":   func(c *config.Config) { c.RateWindow = time.Second; c.MissWindow = time.Minute },
		"rate window below login": func(c *config.Config) {
			c.RateWindow = time.Minute
			c.MissWindow = time.Second
			c.LoginWindow = time.Hour
		},
		"stream drain above timeout": func(c *config.Config) { c.StreamDrain = time.Minute; c.ShutdownTimeout = time.Second },
		"bad trusted proxy": func(c *config.Config) {
			c.TrustedProxyCIDRs = []string{"not-a-cidr"}
			c.ClientIPHeader = "X-Forwarded-For"
		},
		"internal url without token": func(c *config.Config) {
			c.Env = "production"
			c.SigningKeys = signingKeys(t)
			c.TurnstileSecret = "s"
			c.CoreInternalURL = "http://core.internal"
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			mutate(cfg)
			if s, err := New(context.Background(), cfg); err == nil {
				s.Close()
				t.Fatal("bad settings accepted")
			}
		})
	}
}

func TestNewRefusesUnreachableStores(t *testing.T) {
	cfg := testConfig(t)
	cfg.ValkeyAddr = "127.0.0.1:1"
	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("unreachable valkey accepted")
	}
	cfg.DatabaseURL = "postgres://nobody@127.0.0.1:1/x?sslmode=disable&connect_timeout=1"
	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("unreachable database accepted")
	}
}

func TestLoadKeyring(t *testing.T) {
	keys, err := loadKeyring(&config.Config{SigningKeys: signingKeys(t), Env: "production"})
	if err != nil || len(keys.JWKS().Keys) != 1 {
		t.Fatalf("keys = %v, err = %v", keys, err)
	}
	if _, err := loadKeyring(&config.Config{Env: "production"}); !errors.Is(err, token.ErrNoKeys) {
		t.Fatalf("err = %v", err)
	}
	if keys, err := loadKeyring(&config.Config{Env: "development"}); err != nil || keys == nil {
		t.Fatalf("development key: %v", err)
	}
}

func TestStreamPlacesTranslatesQueueErrors(t *testing.T) {
	q := mock_inbound.NewMockIQueueUsecase(gomock.NewController(t))
	p := streamPlaces{q: q}
	seq := int64(4)
	turn := time.Now()
	q.EXPECT().MyPlace(gomock.Any(), "evt1", "w_1", int64(4)).Return(&queueentity.Place{Seq: &seq, Served: 2, TurnExpiresAt: &turn, ServerNow: turn}, nil)
	if pl, err := p.MyPlace(context.Background(), "evt1", "w_1", 4); err != nil || *pl.Seq != 4 || pl.Served != 2 || *pl.TurnExpiresAt != turn {
		t.Fatalf("place = %+v, err = %v", pl, err)
	}
	q.EXPECT().MyPlace(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, queueentity.ErrMoved)
	if _, err := p.MyPlace(context.Background(), "evt1", "w_1", 4); !errors.Is(err, streamentity.ErrMoved) {
		t.Fatalf("err = %v", err)
	}
	q.EXPECT().Frontier(gomock.Any(), "evt1").Return([]byte(`{}`), nil)
	if b, err := p.Frontier(context.Background(), "evt1"); err != nil || string(b) != `{}` {
		t.Fatalf("frontier %s, %v", b, err)
	}
	q.EXPECT().Frontier(gomock.Any(), "evt1").Return(nil, queueentity.ErrFrontierUnavailable)
	if _, err := p.Frontier(context.Background(), "evt1"); !errors.Is(err, streamentity.ErrUnknownEvent) {
		t.Fatalf("err = %v", err)
	}

	for queueErr, want := range map[error]error{
		queueentity.ErrUnknownEvent:  streamentity.ErrUnknownEvent,
		queueentity.ErrClosed:        streamentity.ErrClosed,
		queueentity.ErrMissed:        streamentity.ErrMissed,
		queueentity.ErrPlaceExpired:  streamentity.ErrPlaceExpired,
		queueentity.ErrPlaceMismatch: streamentity.ErrPlaceMismatch,
	} {
		if got := streamError(queueErr); !errors.Is(got, want) {
			t.Errorf("%v -> %v, want %v", queueErr, got, want)
		}
	}
	other := errors.New("boom")
	if got := streamError(other); got != other {
		t.Errorf("unrelated error changed: %v", got)
	}
}
