package usecase

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/device/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/attest"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/turnstile"
)

var installIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)

// Register runs: validate → rate limit (before calling Cloudflare) → human or
// app check → mint a device id → sign the device token.
func (s *DeviceUsecase) Register(ctx context.Context, in entity.RegisterInput) (*entity.Registration, error) {
	if err := validate(in, s.cfg.SkipVerification); err != nil {
		return nil, err
	}
	if err := s.checkRate(ctx, in); err != nil {
		return nil, err
	}
	if err := s.checkCaller(ctx, in); err != nil {
		return nil, err
	}

	id, err := deviceID(in)
	if err != nil {
		return nil, err
	}
	exp := s.now().Add(s.cfg.TokenTTL)
	raw, err := s.signer.Sign(token.KindDevice, &token.DeviceClaims{
		Platform: string(in.Platform),
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   id,
			Audience:  jwt.ClaimStrings{token.AudienceQueue},
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("device: sign: %w", err)
	}
	return &entity.Registration{DeviceID: id, Token: raw, ExpiresAt: exp.UTC().Truncate(time.Second)}, nil
}

func (s *DeviceUsecase) JWKS() token.JWKS { return s.signer.JWKS() }

func validate(in entity.RegisterInput, skipVerification bool) error {
	switch in.Platform {
	case entity.PlatformWeb:
		return nil // the Turnstile token is checked by checkCaller
	case entity.PlatformAndroid, entity.PlatformIOS:
		if !installIDPattern.MatchString(in.InstallID) {
			return fmt.Errorf("%w: installId must be 16-128 characters of [A-Za-z0-9_-]", entity.ErrInvalidInput)
		}
		if in.Attestation == "" && !skipVerification {
			return fmt.Errorf("%w: attestation is required on mobile", entity.ErrInvalidInput)
		}
		return nil
	default:
		return fmt.Errorf("%w: platform must be web, android or ios", entity.ErrInvalidInput)
	}
}

// checkRate limits per IP, then per subnet. If Valkey fails we let the call
// through: Turnstile or attestation still stands, and a Valkey outage pauses
// the queue anyway.
func (s *DeviceUsecase) checkRate(ctx context.Context, in entity.RegisterInput) error {
	ip, subnet := "unknown", "unknown"
	if in.ClientIP.IsValid() {
		ip = in.ClientIP.String()
		subnet = clientip.Subnet(in.ClientIP).String()
	}
	for _, step := range []struct {
		rule ratelimit.Rule
		id   string
	}{{s.cfg.PerIP, ip}, {s.cfg.PerSubnet, subnet}} {
		res, err := s.limiter.Hit(ctx, step.rule, step.id)
		if err != nil {
			slog.WarnContext(ctx, "device rate limit unavailable, allowing", "scope", step.rule.Scope, "error", err)
			continue
		}
		if !res.Allowed {
			return &entity.RateLimitedError{Scope: step.rule.Scope, RetryAfter: res.RetryAfter}
		}
	}
	return nil
}

func (s *DeviceUsecase) checkCaller(ctx context.Context, in entity.RegisterInput) error {
	if s.cfg.SkipVerification {
		return nil
	}
	var err error
	if in.Platform == entity.PlatformWeb {
		err = s.human.Verify(ctx, in.TurnstileToken, in.ClientIP)
	} else {
		err = s.attest.Verify(ctx, string(in.Platform), in.InstallID, in.Attestation)
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, turnstile.ErrUnavailable):
		return fmt.Errorf("%w: %w", entity.ErrCheckUnavailable, err)
	case errors.Is(err, turnstile.ErrRejected), errors.Is(err, attest.ErrRejected):
		return fmt.Errorf("%w: %w", entity.ErrHumanCheckFailed, err)
	default:
		return fmt.Errorf("%w: %w", entity.ErrCheckUnavailable, err)
	}
}

// deviceID: web gets a random id; mobile gets an id derived from its install
// id, so reinstalling the token after an app restart keeps the same place (FR-10).
func deviceID(in entity.RegisterInput) (string, error) {
	if in.Platform != entity.PlatformWeb {
		sum := sha256.Sum256([]byte("rusuh-device:" + in.InstallID))
		return "m_" + base64.RawURLEncoding.EncodeToString(sum[:16]), nil
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("device: random id: %w", err)
	}
	return "w_" + base64.RawURLEncoding.EncodeToString(b), nil
}
