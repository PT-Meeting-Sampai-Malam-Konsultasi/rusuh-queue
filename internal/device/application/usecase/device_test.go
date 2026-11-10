package usecase

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_outbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/device/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/attest"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/turnstile"
)

var testCfg = entity.Config{
	TokenTTL:  30 * 24 * time.Hour,
	PerIP:     ratelimit.Rule{Scope: "dev-reg:ip", Limit: 10, Window: time.Minute},
	PerSubnet: ratelimit.Rule{Scope: "dev-reg:net", Limit: 100, Window: time.Minute},
}

type fixture struct {
	svc     inbound.IDeviceUsecase
	human   *mock_outbound.MockIHumanVerifier
	attest  *mock_outbound.MockIAttestor
	limiter *mock_outbound.MockIRateLimiter
	keys    *token.Keyring
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	k, err := token.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := token.NewKeyring(k)
	if err != nil {
		t.Fatal(err)
	}
	f := fixture{
		human:   mock_outbound.NewMockIHumanVerifier(ctrl),
		attest:  mock_outbound.NewMockIAttestor(ctrl),
		limiter: mock_outbound.NewMockIRateLimiter(ctrl),
		keys:    keys,
	}
	f.svc = NewDevice(testCfg, f.human, f.attest, f.limiter, keys)
	return f
}

func (f fixture) allowRate() {
	f.limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true, Count: 1}, nil).Times(2)
}

var ip = netip.MustParseAddr("203.0.113.7")

func TestRegisterWeb(t *testing.T) {
	f := newFixture(t)
	f.limiter.EXPECT().Hit(gomock.Any(), testCfg.PerIP, "203.0.113.7").Return(ratelimit.Result{Allowed: true}, nil)
	f.limiter.EXPECT().Hit(gomock.Any(), testCfg.PerSubnet, "203.0.113.0/24").Return(ratelimit.Result{Allowed: true}, nil)
	f.human.EXPECT().Verify(gomock.Any(), "cf-token", ip).Return(nil)

	reg, err := f.svc.Register(context.Background(), entity.RegisterInput{Platform: entity.PlatformWeb, TurnstileToken: "cf-token", ClientIP: ip})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(reg.DeviceID, "w_") || len(reg.DeviceID) != 24 {
		t.Fatalf("device id = %q", reg.DeviceID)
	}
	var c token.DeviceClaims
	if err := f.keys.Verify(token.KindDevice, reg.Token, token.AudienceQueue, &c); err != nil {
		t.Fatalf("issued token does not verify: %v", err)
	}
	if c.Subject != reg.DeviceID || c.Platform != "web" {
		t.Fatalf("claims = %+v", c)
	}
	if d := time.Until(reg.ExpiresAt); d < 29*24*time.Hour || d > 30*24*time.Hour {
		t.Fatalf("expiresAt in %s, want ~30 days", d)
	}
}

func TestRegisterMobileKeepsDeviceIDAcrossRestarts(t *testing.T) {
	f := newFixture(t)
	in := entity.RegisterInput{Platform: entity.PlatformAndroid, InstallID: "9b2f6c1e-4a7d-4f0e-a1b2-c3d4e5f60718", Attestation: "play-integrity-token", ClientIP: ip}
	for range 2 {
		f.allowRate()
		f.attest.EXPECT().Verify(gomock.Any(), "android", in.InstallID, in.Attestation).Return(nil)
	}

	first, err := f.svc.Register(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.svc.Register(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if first.DeviceID != second.DeviceID || !strings.HasPrefix(first.DeviceID, "m_") {
		t.Fatalf("ids = %q, %q; want the same m_ id", first.DeviceID, second.DeviceID)
	}
}

func TestRegisterInvalidInputSkipsChecks(t *testing.T) {
	f := newFixture(t) // no EXPECT: neither the limiter nor Cloudflare may be called
	for _, in := range []entity.RegisterInput{
		{Platform: "desktop"},
		{Platform: entity.PlatformIOS, InstallID: "short", Attestation: "x"},
		{Platform: entity.PlatformIOS, InstallID: "9b2f6c1e-4a7d-4f0e-a1b2", Attestation: ""},
		{Platform: entity.PlatformAndroid, InstallID: "has spaces in it!!", Attestation: "x"},
	} {
		if _, err := f.svc.Register(context.Background(), in); !errors.Is(err, entity.ErrInvalidInput) {
			t.Errorf("%+v: err = %v, want ErrInvalidInput", in, err)
		}
	}
}

func TestRegisterRateLimitedBeforeTurnstile(t *testing.T) {
	f := newFixture(t)
	f.limiter.EXPECT().Hit(gomock.Any(), testCfg.PerIP, gomock.Any()).Return(ratelimit.Result{Allowed: false, RetryAfter: 42 * time.Second}, nil)
	// human.Verify must not be called: limits protect Cloudflare's siteverify too

	_, err := f.svc.Register(context.Background(), entity.RegisterInput{Platform: entity.PlatformWeb, TurnstileToken: "t", ClientIP: ip})
	var limited *entity.RateLimitedError
	if !errors.As(err, &limited) || limited.RetryAfter != 42*time.Second || limited.Scope != "dev-reg:ip" {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterSubnetLimit(t *testing.T) {
	f := newFixture(t)
	f.limiter.EXPECT().Hit(gomock.Any(), testCfg.PerIP, gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	f.limiter.EXPECT().Hit(gomock.Any(), testCfg.PerSubnet, gomock.Any()).Return(ratelimit.Result{Allowed: false, RetryAfter: time.Second}, nil)

	_, err := f.svc.Register(context.Background(), entity.RegisterInput{Platform: entity.PlatformWeb, TurnstileToken: "t", ClientIP: ip})
	var limited *entity.RateLimitedError
	if !errors.As(err, &limited) || limited.Scope != "dev-reg:net" {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterLimiterDownStillChecksTurnstile(t *testing.T) {
	f := newFixture(t)
	f.limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{}, errors.New("valkey down")).Times(2)
	f.human.EXPECT().Verify(gomock.Any(), "t", ip).Return(turnstile.ErrRejected)

	_, err := f.svc.Register(context.Background(), entity.RegisterInput{Platform: entity.PlatformWeb, TurnstileToken: "t", ClientIP: ip})
	if !errors.Is(err, entity.ErrHumanCheckFailed) {
		t.Fatalf("err = %v, want ErrHumanCheckFailed", err)
	}
}

func TestRegisterCheckErrors(t *testing.T) {
	tests := []struct {
		name string
		in   entity.RegisterInput
		err  error
		want error
	}{
		{"turnstile rejects", entity.RegisterInput{Platform: entity.PlatformWeb, TurnstileToken: "t", ClientIP: ip}, turnstile.ErrRejected, entity.ErrHumanCheckFailed},
		{"turnstile down", entity.RegisterInput{Platform: entity.PlatformWeb, TurnstileToken: "t", ClientIP: ip}, turnstile.ErrUnavailable, entity.ErrCheckUnavailable},
		{"attestation rejects", entity.RegisterInput{Platform: entity.PlatformIOS, InstallID: "9b2f6c1e4a7d4f0ea1b2", Attestation: "a", ClientIP: ip}, attest.ErrRejected, entity.ErrHumanCheckFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.allowRate()
			if tt.in.Platform == entity.PlatformWeb {
				f.human.EXPECT().Verify(gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.err)
			} else {
				f.attest.EXPECT().Verify(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.err)
			}
			if _, err := f.svc.Register(context.Background(), tt.in); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}
