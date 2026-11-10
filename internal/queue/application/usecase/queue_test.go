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
	LoginWindow:    10 * time.Minute,
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

		dto.ClaimDeviceOtherAccount: entity.ErrDeviceOtherAccount,
		dto.ClaimAccountElsewhere:   entity.ErrAccountElsewhere,
	} {
		ctrl := gomock.NewController(t)
		store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
		limiter.EXPECT().Hit(gomock.Any(), testCfg.ClaimPerDevice, "w_1").Return(ratelimit.Result{Allowed: true}, nil)
		store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ClaimResult{Status: status}, nil)
		if status == dto.ClaimExpired {
			// EXPIRED reads the place once to tell a moved place apart (D-13).
			store.EXPECT().Place(gomock.Any(), gomock.Any(), "w_1", gomock.Any()).Return(dto.PlaceView{Found: true, Value: "x:7"}, nil)
		}
		svc := NewQueue(testCfg, store, mock_outbound.NewMockISigner(ctrl), limiter, mock_outbound.NewMockIAdmissions(ctrl))
		if _, err := svc.Claim(context.Background(), "evt1", "w_1", "user_1", 7); !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", status, err, want)
		}
	}
}

// D-13: a claim refused as EXPIRED on a place that moved to another device is MOVED.
func TestClaimOnMovedPlaceIsMoved(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ClaimResult{Status: dto.ClaimExpired}, nil)
	store.EXPECT().Place(gomock.Any(), gomock.Any(), "w_1", gomock.Any()).Return(dto.PlaceView{Found: true, Value: "m:7"}, nil)
	if _, err := NewQueue(testCfg, store, mock_outbound.NewMockISigner(ctrl), limiter, mock_outbound.NewMockIAdmissions(ctrl)).Claim(context.Background(), "evt1", "w_1", "user_1", 7); !errors.Is(err, entity.ErrMoved) {
		t.Fatalf("err = %v, want ErrMoved", err)
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
			if in.TokenSeq != 7 || !strings.HasPrefix(in.SlotID, "s_") || in.OrderWindow != testCfg.OrderWindow || in.UserID != "user_1" ||
				in.Windows != (dto.Windows{Miss: testCfg.MissWindow, Login: testCfg.LoginWindow}) {
				t.Errorf("claim input = %+v", in)
			}
			return dto.ClaimResult{Status: dto.ClaimOK, SlotID: "s_x", Units: 2, EndsAt: ends, ServerNow: now, Attached: true}, nil
		})
	var recorded dto.AdmissionRecord
	admissions := mock_outbound.NewMockIAdmissions(ctrl)
	admissions.EXPECT().Register(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, rec dto.AdmissionRecord) error {
		recorded = rec
		return nil
	})
	adm, err := NewQueue(testCfg, store, keys, limiter, admissions).Claim(context.Background(), "evt1", "w_1", "user_1", 7)
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
	want := dto.AdmissionRecord{Token: adm.Token, EventID: "evt1", DeviceID: "w_1", UserID: "user_1", Seq: 7, SlotID: "s_x", ReservedUnits: 2, ExpiresAt: ends}
	if recorded != want {
		t.Fatalf("recorded = %+v, want %+v", recorded, want)
	}
}

func TestClaimWithholdsTokenCoreDidNotRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	k, _ := token.GenerateKey("k1")
	keys, _ := token.NewKeyring(k)
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		dto.ClaimResult{Status: dto.ClaimOK, SlotID: "s_x", Units: 1, EndsAt: time.Now().Add(time.Minute)}, nil)
	admissions := mock_outbound.NewMockIAdmissions(ctrl)
	admissions.EXPECT().Register(gomock.Any(), gomock.Any()).Return(entity.ErrAdmissionUnavailable)

	adm, err := NewQueue(testCfg, store, keys, limiter, admissions).Claim(context.Background(), "evt1", "w_1", "user_1", 7)
	if adm != nil || !errors.Is(err, entity.ErrAdmissionUnavailable) {
		t.Fatalf("admission = %+v, err = %v", adm, err)
	}
}

func TestClaimRateLimited(t *testing.T) {
	ctrl := gomock.NewController(t)
	limiter := mock_outbound.NewMockIRateLimiter(ctrl)
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: false, RetryAfter: 3 * time.Second}, nil)
	_, err := NewQueue(testCfg, mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockISigner(ctrl), limiter, mock_outbound.NewMockIAdmissions(ctrl)).Claim(context.Background(), "evt1", "w_1", "user_1", 7)
	var rl *entity.RateLimitedError
	if !errors.As(err, &rl) || rl.RetryAfter != 3*time.Second {
		t.Fatalf("err = %v", err)
	}
}

// A login moves the account's place here without asking (5 Nov, Backend 2): no takeOver needed.
func TestLoggedInJoinMovesThePlaceWithoutTakeOver(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	k, _ := token.GenerateKey("k1")
	keys, _ := token.NewKeyring(k)
	cfg := testCfg
	cfg.TransfersPerEvent = 1
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	store.EXPECT().Transfer(gomock.Any(), gomock.Any(), "w_b", "user_1", dto.Windows{Miss: cfg.MissWindow, Login: cfg.LoginWindow}, int64(1)).
		Return(dto.JoinResult{Status: dto.JoinOK, Seq: 200, Attached: true}, nil)

	p, err := NewQueue(cfg, store, keys, limiter, mock_outbound.NewMockIAdmissions(ctrl)).
		Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_b", UserID: "user_1"})
	if err != nil || p.Seq == nil || *p.Seq != 200 {
		t.Fatalf("place = %+v, err = %v", p, err)
	}
}

func TestLoggedInJoinDoesNotMoveWhenMovingIsOff(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	cfg := testCfg
	cfg.TransfersPerEvent = 0
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	store.EXPECT().Join(gomock.Any(), gomock.Any(), "w_b", "user_1", gomock.Any()).
		Return(dto.JoinResult{Status: dto.JoinAccountElsewhere}, nil)

	_, err := NewQueue(cfg, store, mock_outbound.NewMockISigner(ctrl), limiter, mock_outbound.NewMockIAdmissions(ctrl)).
		Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_b", UserID: "user_1"})
	if !errors.Is(err, entity.ErrAccountElsewhere) {
		t.Fatalf("err = %v", err)
	}
}

func TestClaimNeedsALogin(t *testing.T) {
	ctrl := gomock.NewController(t)
	cfg := testCfg
	cfg.ClaimRequiresLogin = true
	_, err := NewQueue(cfg, mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockISigner(ctrl),
		mock_outbound.NewMockIRateLimiter(ctrl), mock_outbound.NewMockIAdmissions(ctrl)).
		Claim(context.Background(), "evt1", "w_1", "", 7)
	if !errors.Is(err, entity.ErrLoginRequired) {
		t.Fatalf("err = %v, want ErrLoginRequired", err)
	}
}

// ADR-0021: the account's claimed lease moved here; the app claims at once.
func TestJoinReportsAMovedClaimedLease(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, limiter := mock_outbound.NewMockIQueueStore(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	k, _ := token.GenerateKey("k1")
	keys, _ := token.NewKeyring(k)
	cfg := testCfg
	cfg.TransfersPerEvent = 1
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	store.EXPECT().Transfer(gomock.Any(), gomock.Any(), "w_b", "user_1", dto.Windows{Miss: cfg.MissWindow, Login: cfg.LoginWindow}, int64(1)).
		Return(dto.JoinResult{Status: dto.JoinClaimed, Seq: 7, Attached: true}, nil)

	p, err := NewQueue(cfg, store, keys, limiter, mock_outbound.NewMockIAdmissions(ctrl)).
		Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_b", UserID: "user_1"})
	if err != nil || !p.Claimed || p.Seq == nil || *p.Seq != 7 || p.QueueToken == "" {
		t.Fatalf("place = %+v, err = %v; want claimed seq 7 with a queue token", p, err)
	}
}
