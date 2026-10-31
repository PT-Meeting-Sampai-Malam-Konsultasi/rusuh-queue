package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_outbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/queue/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

var testCfg = entity.Config{
	JoinPerDevice:  ratelimit.Rule{Scope: "join:dev", Limit: 30, Window: time.Minute},
	ClaimPerDevice: ratelimit.Rule{Scope: "claim:dev", Limit: 60, Window: time.Minute},
	OrderWindow:    5 * time.Minute,
	MissWindow:     5 * time.Minute,
}

func TestClaimMapsEveryStatus(t *testing.T) {
	for status, want := range map[dto.ClaimStatus]error{
		dto.ClaimNoEvent: entity.ErrUnknownEvent,
		dto.ClaimClosed:  entity.ErrClosed,
		dto.ClaimExpired: entity.ErrPlaceExpired,
		dto.ClaimNotYet:  entity.ErrNotYet,
		dto.ClaimMissed:  entity.ErrMissed,
		dto.ClaimFull:    entity.ErrFull,
		dto.ClaimNoStock: entity.ErrNoStock,
	} {
		ctrl := gomock.NewController(t)
		store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
		limiter.EXPECT().Hit(gomock.Any(), testCfg.ClaimPerDevice, "w_1").Return(ratelimit.Result{Allowed: true}, nil)
		store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ClaimResult{Status: status}, nil)
		svc := NewQueue(testCfg, store, mock_outbound.NewMockISigner(ctrl), limiter)
		if _, err := svc.Claim(context.Background(), "evt1", "w_1", 7); !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", status, err, want)
		}
	}
}

// OK signs an admission token for core whose exp is the lease end.
func TestClaimSignsAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	k, _ := token.GenerateKey("k1")
	keys, _ := token.NewKeyring(k)
	now := time.Now().Truncate(time.Millisecond)
	ends := now.Add(5 * time.Minute)

	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ any, in dto.ClaimInput) (dto.ClaimResult, error) {
			if in.TokenSeq != 7 || !strings.HasPrefix(in.SlotID, "s_") || in.OrderWindow != testCfg.OrderWindow {
				t.Errorf("claim input = %+v", in)
			}
			return dto.ClaimResult{Status: dto.ClaimOK, SlotID: "s_x", Units: 2, EndsAt: ends, ServerNow: now, Attached: true}, nil
		})
	adm, err := NewQueue(testCfg, store, keys, limiter).Claim(context.Background(), "evt1", "w_1", 7)
	if err != nil || adm.SlotID != "s_x" || adm.ReservedUnits != 2 || adm.LoginRequired || !adm.ExpiresAt.Equal(ends) {
		t.Fatalf("admission = %+v, %v", adm, err)
	}
	var c token.AdmissionClaims
	if err := keys.Verify(token.KindAdmission, adm.Token, token.AudienceCore, &c); err != nil {
		t.Fatal(err)
	}
	if c.Event != "evt1" || c.Seq != 7 || c.Slot != "s_x" || c.Reserved != 2 || c.Subject != "w_1" || c.ExpiresAt.Unix() != ends.Unix() {
		t.Fatalf("claims = %+v", c)
	}
}

func TestClaimRateLimited(t *testing.T) {
	ctrl := gomock.NewController(t)
	limiter := mock_outbound.NewMockIRateLimiter(ctrl)
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: false, RetryAfter: 3 * time.Second}, nil)
	_, err := NewQueue(testCfg, mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockISigner(ctrl), limiter).Claim(context.Background(), "evt1", "w_1", 7)
	var rl *entity.RateLimitedError
	if !errors.As(err, &rl) || rl.RetryAfter != 3*time.Second {
		t.Fatalf("err = %v", err)
	}
}
