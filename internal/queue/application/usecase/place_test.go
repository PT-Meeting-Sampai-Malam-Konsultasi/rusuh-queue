package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_outbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/queue/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

var errBoom = errors.New("boom")

type deps struct {
	svc     *QueueUsecase
	store   *mock_outbound.MockIQueueStore
	signer  *mock_outbound.MockISigner
	limiter *mock_outbound.MockIRateLimiter
	admits  *mock_outbound.MockIAdmissions
}

func newDeps(t *testing.T, cfg entity.Config) deps {
	ctrl := gomock.NewController(t)
	d := deps{
		store: mock_outbound.NewMockIQueueStore(ctrl), signer: mock_outbound.NewMockISigner(ctrl),
		limiter: mock_outbound.NewMockIRateLimiter(ctrl), admits: mock_outbound.NewMockIAdmissions(ctrl),
	}
	d.svc = NewQueue(cfg, d.store, d.signer, d.limiter, d.admits).(*QueueUsecase)
	return d
}

func (d deps) allow() {
	d.limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
}

func (d deps) signsQueue(seq int64) {
	d.signer.EXPECT().Sign(token.KindQueue, gomock.Any()).DoAndReturn(func(_ token.Kind, c token.Claims) (string, error) {
		qc := c.(*token.QueueClaims)
		if qc.Seq != seq || qc.Subject != "w_1" || qc.Event != "evt1" {
			return "", errors.New("unexpected queue claims")
		}
		return "qt", nil
	})
}

func TestJoinRefusesBadEventsAndLimitedDevices(t *testing.T) {
	d := newDeps(t, testCfg)
	if _, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "bad:id"}); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("bad id: %v", err)
	}

	d.limiter.EXPECT().Hit(gomock.Any(), testCfg.JoinPerDevice, "w_1").Return(ratelimit.Result{RetryAfter: 3 * time.Second}, nil)
	var limited *entity.RateLimitedError
	if _, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1"}); !errors.As(err, &limited) || limited.RetryAfter != 3*time.Second {
		t.Fatalf("limited: %v", err)
	}

	d.limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{}, errBoom)
	d.store.EXPECT().Join(gomock.Any(), qkeys.For("evt1"), "w_1", "", gomock.Any()).Return(dto.JoinResult{}, errBoom)
	if _, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1"}); !errors.Is(err, errBoom) {
		t.Fatalf("store error: %v", err)
	}
}

func TestJoinMapsEveryStatus(t *testing.T) {
	for status, want := range map[dto.JoinStatus]error{
		dto.JoinNoEvent:            entity.ErrUnknownEvent,
		dto.JoinClosed:             entity.ErrClosed,
		dto.JoinAccountElsewhere:   entity.ErrAccountElsewhere,
		dto.JoinDeviceOtherAccount: entity.ErrDeviceOtherAccount,
		dto.JoinTransferLimit:      entity.ErrTransferLimit,
		"WEIRD":                    nil,
	} {
		d := newDeps(t, testCfg)
		d.allow()
		d.store.EXPECT().Join(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.JoinResult{Status: status}, nil)
		_, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1"})
		if err == nil || (want != nil && !errors.Is(err, want)) {
			t.Errorf("%s: err = %v, want %v", status, err, want)
		}
	}
}

func TestJoinGivesAPlaceWithAToken(t *testing.T) {
	now := time.Now()
	turn := now.Add(time.Minute)
	ends := now.Add(2 * time.Hour)

	d := newDeps(t, testCfg)
	d.allow()
	d.store.EXPECT().Join(gomock.Any(), gomock.Any(), "w_1", "", dto.Windows{Miss: testCfg.MissWindow, Login: testCfg.LoginWindow}).Return(dto.JoinResult{
		Status: dto.JoinOK, Seq: 12, EndsAt: ends, Turn: dto.Turn{Served: 10, OpensAt: now, TurnEnds: turn, ServerNow: now},
	}, nil)
	d.signer.EXPECT().Sign(token.KindQueue, gomock.Any()).DoAndReturn(func(_ token.Kind, c token.Claims) (string, error) {
		if exp := c.(*token.QueueClaims).ExpiresAt.Time; !exp.Equal(ends.Add(queueTokenGrace).Truncate(time.Second)) {
			t.Errorf("exp = %v", exp)
		}
		return "qt", nil
	})
	p, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1"})
	if err != nil || p.State != "open" || *p.Seq != 12 || p.Served != 10 || *p.TurnExpiresAt != turn || p.QueueToken != "qt" {
		t.Fatalf("place = %+v, err = %v", p, err)
	}

	d.allow()
	d.store.EXPECT().Join(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.JoinResult{Status: dto.JoinPre}, nil)
	d.signer.EXPECT().Sign(token.KindQueue, gomock.Any()).Return("", errBoom)
	if _, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1"}); !errors.Is(err, errBoom) {
		t.Fatalf("sign error: %v", err)
	}

	d.allow()
	d.store.EXPECT().Join(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.JoinResult{Status: dto.JoinPre}, nil)
	d.signsQueue(0)
	if p, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1"}); err != nil || p.State != "preopen" || p.Seq != nil || p.TurnExpiresAt != nil {
		t.Fatalf("preopen place = %+v, err = %v", p, err)
	}
}

func TestJoinPassesTransferErrorsThrough(t *testing.T) {
	cfg := testCfg
	cfg.TransfersPerEvent = 1
	d := newDeps(t, cfg)
	d.allow()
	d.store.EXPECT().Transfer(gomock.Any(), gomock.Any(), "w_1", "user_1", gomock.Any(), int64(1)).Return(dto.JoinResult{}, errBoom)
	if _, err := d.svc.Join(context.Background(), entity.JoinInput{EventID: "evt1", DeviceID: "w_1", UserID: "user_1"}); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
}

func TestMyPlace(t *testing.T) {
	for name, tc := range map[string]struct {
		view     dto.PlaceView
		err      error
		tokenSeq int64
		want     error
		seq      int64
	}{
		"store error":  {err: errBoom, want: errBoom},
		"no event":     {view: dto.PlaceView{}, want: entity.ErrUnknownEvent},
		"closed":       {view: dto.PlaceView{Found: true, State: "closed", Value: "3"}, want: entity.ErrClosed},
		"moved":        {view: dto.PlaceView{Found: true, State: "open", Value: "m:3"}, want: entity.ErrMoved},
		"no place":     {view: dto.PlaceView{Found: true, State: "open"}, want: entity.ErrPlaceExpired},
		"used":         {view: dto.PlaceView{Found: true, State: "open", Value: "x:3"}, want: entity.ErrPlaceExpired},
		"missed":       {view: dto.PlaceView{Found: true, State: "open", Value: "3", Missed: true}, want: entity.ErrMissed},
		"corrupt":      {view: dto.PlaceView{Found: true, State: "open", Value: "three"}, want: nil},
		"other seq":    {view: dto.PlaceView{Found: true, State: "open", Value: "3"}, tokenSeq: 4, want: entity.ErrPlaceMismatch},
		"numbered":     {view: dto.PlaceView{Found: true, State: "open", Value: "3"}, tokenSeq: 3, seq: 3},
		"after a draw": {view: dto.PlaceView{Found: true, State: "open", Value: "3"}, seq: 3},
		"pre-draw":     {view: dto.PlaceView{Found: true, State: "preopen", Value: "pre"}},
	} {
		t.Run(name, func(t *testing.T) {
			d := newDeps(t, testCfg)
			d.store.EXPECT().Place(gomock.Any(), qkeys.For("evt1"), "w_1", gomock.Any()).Return(tc.view, tc.err)
			ok := tc.want == nil && name != "corrupt"
			if ok {
				d.signsQueue(tc.seq)
			}
			p, err := d.svc.MyPlace(context.Background(), "evt1", "w_1", tc.tokenSeq)
			switch {
			case ok && (err != nil || p.State != tc.view.State || (tc.seq != 0) != (p.Seq != nil)):
				t.Fatalf("place = %+v, err = %v", p, err)
			case !ok && (err == nil || (tc.want != nil && !errors.Is(err, tc.want))):
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	d := newDeps(t, testCfg)
	if _, err := d.svc.MyPlace(context.Background(), "bad:id", "w_1", 0); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("bad id: %v", err)
	}
}

func TestLeave(t *testing.T) {
	d := newDeps(t, testCfg)
	if err := d.svc.Leave(context.Background(), "bad:id", "w_1"); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("bad id: %v", err)
	}
	d.store.EXPECT().Leave(gomock.Any(), qkeys.For("evt1"), "w_1").Return(true, nil)
	if err := d.svc.Leave(context.Background(), "evt1", "w_1"); err != nil {
		t.Fatal(err)
	}
	d.store.EXPECT().Leave(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	if err := d.svc.Leave(context.Background(), "evt1", "w_1"); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("unknown: %v", err)
	}
	d.store.EXPECT().Leave(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, errBoom)
	if err := d.svc.Leave(context.Background(), "evt1", "w_1"); !errors.Is(err, errBoom) {
		t.Fatalf("store error: %v", err)
	}
}

func TestFrontier(t *testing.T) {
	d := newDeps(t, testCfg)
	if _, err := d.svc.Frontier(context.Background(), "bad:id"); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("bad id: %v", err)
	}
	d.store.EXPECT().Frontier(gomock.Any(), qkeys.For("evt1")).Return([]byte(`{}`), nil)
	if b, err := d.svc.Frontier(context.Background(), "evt1"); err != nil || string(b) != `{}` {
		t.Fatalf("frontier %s, %v", b, err)
	}
	d.store.EXPECT().Frontier(gomock.Any(), gomock.Any()).Return(nil, nil)
	if _, err := d.svc.Frontier(context.Background(), "evt1"); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("no frontier: %v", err)
	}
	d.store.EXPECT().Frontier(gomock.Any(), gomock.Any()).Return(nil, errBoom)
	if _, err := d.svc.Frontier(context.Background(), "evt1"); !errors.Is(err, errBoom) {
		t.Fatalf("store error: %v", err)
	}
}

func TestClaimWithAPreDrawTokenUsesTheDrawnSeq(t *testing.T) {
	for value, want := range map[string]int64{"57": 57, "x:57": 57, "pre": 0} {
		d := newDeps(t, testCfg)
		d.allow()
		d.store.EXPECT().Place(gomock.Any(), gomock.Any(), "w_1", gomock.Any()).Return(dto.PlaceView{Found: true, Value: value}, nil)
		d.store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ qkeys.Event, in dto.ClaimInput) (dto.ClaimResult, error) {
			if in.TokenSeq != want {
				t.Errorf("%s: token seq = %d, want %d", value, in.TokenSeq, want)
			}
			return dto.ClaimResult{Status: dto.ClaimNotYet}, nil
		})
		if _, err := d.svc.Claim(context.Background(), "evt1", "w_1", "user_1", 0); !errors.Is(err, entity.ErrNotYet) {
			t.Errorf("%s: err = %v", value, err)
		}
	}
}

func TestClaimFailures(t *testing.T) {
	d := newDeps(t, testCfg)
	if _, err := d.svc.Claim(context.Background(), "bad:id", "w_1", "user_1", 1); !errors.Is(err, entity.ErrUnknownEvent) {
		t.Fatalf("bad id: %v", err)
	}

	d.limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{}, errBoom)
	d.store.EXPECT().Place(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PlaceView{}, errBoom)
	if _, err := d.svc.Claim(context.Background(), "evt1", "w_1", "user_1", 0); !errors.Is(err, errBoom) {
		t.Fatalf("place error: %v", err)
	}

	d.allow()
	d.store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ClaimResult{}, errBoom)
	if _, err := d.svc.Claim(context.Background(), "evt1", "w_1", "user_1", 1); !errors.Is(err, errBoom) {
		t.Fatalf("claim error: %v", err)
	}

	d.allow()
	d.store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ClaimResult{Status: "WEIRD"}, nil)
	if _, err := d.svc.Claim(context.Background(), "evt1", "w_1", "user_1", 1); err == nil {
		t.Fatal("unknown status accepted")
	}

	d.allow()
	d.store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ClaimResult{Status: dto.ClaimOK, SlotID: "s_1", Units: 2, EndsAt: time.Now()}, nil)
	d.signer.EXPECT().Sign(token.KindAdmission, gomock.Any()).Return("", errBoom)
	if _, err := d.svc.Claim(context.Background(), "evt1", "w_1", "user_1", 1); !errors.Is(err, errBoom) {
		t.Fatalf("sign error: %v", err)
	}

	d.allow()
	d.store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ClaimResult{Status: dto.ClaimExpired}, nil)
	d.store.EXPECT().Place(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PlaceView{}, errBoom)
	if _, err := d.svc.Claim(context.Background(), "evt1", "w_1", "user_1", 1); !errors.Is(err, entity.ErrPlaceExpired) {
		t.Fatalf("expired: %v", err)
	}
}

func TestPlaceTokenOutlivesAnEndedSale(t *testing.T) {
	d := newDeps(t, testCfg)
	d.signer.EXPECT().Sign(token.KindQueue, gomock.Any()).DoAndReturn(func(_ token.Kind, c token.Claims) (string, error) {
		if exp := c.(*token.QueueClaims).ExpiresAt.Time; time.Until(exp) < queueTokenGrace-time.Minute {
			t.Errorf("exp = %v, want a day from now", exp)
		}
		return "qt", nil
	})
	if _, err := d.svc.place("evt1", "w_1", "open", nil, false, time.Now().Add(-48*time.Hour), dto.Turn{}); err != nil {
		t.Fatal(err)
	}
}
