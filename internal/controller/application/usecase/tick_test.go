package usecase

import (
	"context"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

var k1 = qkeys.For("evt1")

func leading(h harness, acquired bool) {
	h.store.EXPECT().Lead(gomock.Any(), k1, "task-a", h.svc.cfg.LeaderTTL).Return(dto.Lead{Leading: true, Fence: 7, Acquired: acquired}, nil)
}

func tickOK(h harness, state string) {
	h.store.EXPECT().Tick(gomock.Any(), k1, dto.TickArgs{
		Fence: 7, HistWindow: h.svc.cfg.HistWindow, InviteWindow: h.svc.cfg.InviteWindow, PriorOrderTime: h.svc.cfg.PriorOrderTime,
	}).Return(dto.TickResult{Status: dto.TickOK, State: state, Seq: 40, Served: 30, Frontier: []byte(`{}`)}, nil)
}

func settled(h harness) {
	h.svc.states["evt1"] = "open"
	h.svc.reconciled["evt1"] = t0
}

func TestTickEventLeadsAdmitsAndCheckpoints(t *testing.T) {
	h := newHarness(t)
	settled(h)
	leading(h, true)
	h.store.EXPECT().Purge(gomock.Any(), k1, int64(7), h.svc.cfg.PurgeGrace, purgeLimit).Return(dto.PurgeResult{Purged: 2, Stuck: 1}, nil)
	tickOK(h, "open")
	h.checkpoints.EXPECT().Upsert(gomock.Any(), "evt1", int64(40), int64(30), int64(7)).Return(nil)

	h.svc.tickEvent(context.Background(), "evt1", false)
	if h.svc.leading["evt1"] != 7 || !h.svc.checkpointed["evt1"].Equal(t0) {
		t.Fatalf("leading = %v, checkpointed = %v", h.svc.leading, h.svc.checkpointed)
	}

	leading(h, false)
	h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, errBoom)
	tickOK(h, "open")
	h.checkpoints.EXPECT().Upsert(gomock.Any(), "evt1", int64(40), int64(30), int64(7)).Return(nil)
	h.svc.tickEvent(context.Background(), "evt1", false)
}

func TestTickEventRetriesAFailedCheckpoint(t *testing.T) {
	h := newHarness(t)
	settled(h)
	leading(h, false)
	h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, nil)
	tickOK(h, "open")
	h.checkpoints.EXPECT().Upsert(gomock.Any(), "evt1", int64(40), int64(30), int64(7)).Return(errBoom)
	h.svc.tickEvent(context.Background(), "evt1", false)
	if _, ok := h.svc.checkpointed["evt1"]; ok {
		t.Fatal("a failed checkpoint was recorded")
	}
}

func TestTickEventGivesUpTheEvent(t *testing.T) {
	for name, setup := range map[string]func(h harness){
		"not leading": func(h harness) {
			h.store.EXPECT().Lead(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.Lead{}, nil)
		},
		"stale purge": func(h harness) {
			leading(h, false)
			h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{Stale: true}, nil)
		},
		"stale tick": func(h harness) {
			leading(h, false)
			h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, nil)
			h.store.EXPECT().Tick(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.TickResult{Status: dto.TickStale}, nil)
		},
		"event gone": func(h harness) {
			leading(h, false)
			h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, nil)
			h.store.EXPECT().Tick(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.TickResult{Status: dto.TickNoEvent}, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			settled(h)
			h.svc.leading["evt1"] = 6
			h.svc.checkpointed["evt1"] = t0
			setup(h)
			h.svc.tickEvent(context.Background(), "evt1", false)
			if len(h.svc.leading)+len(h.svc.checkpointed)+len(h.svc.states)+len(h.svc.reconciled) != 0 {
				t.Fatalf("state kept: %v %v %v %v", h.svc.leading, h.svc.checkpointed, h.svc.states, h.svc.reconciled)
			}
		})
	}
}

func TestTickEventKeepsStateOnTransientErrors(t *testing.T) {
	h := newHarness(t)
	settled(h)
	h.store.EXPECT().Lead(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.Lead{}, errBoom)
	h.svc.tickEvent(context.Background(), "evt1", false)

	leading(h, false)
	h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, nil)
	h.store.EXPECT().Tick(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.TickResult{}, errBoom)
	h.svc.tickEvent(context.Background(), "evt1", false)
	if h.svc.states["evt1"] != "open" || h.svc.leading["evt1"] != 7 {
		t.Fatalf("states = %v, leading = %v", h.svc.states, h.svc.leading)
	}
}

func TestReconcileOnNudgeOrSchedule(t *testing.T) {
	for name, tc := range map[string]struct {
		nudged     bool
		reconciled time.Time
		stock      []byte
		stockErr   error
		result     dto.ReconcileResult
		resultErr  error
		reconciles bool
		lost       bool
		state      string
	}{
		"nudged, applied, reopened": {
			nudged: true, reconciled: t0, stock: stockDoc("evt1", 9, t0),
			result: dto.ReconcileResult{Applied: true, Drift: 2, Reopened: true}, reconciles: true, state: "open",
		},
		"scheduled, in step": {
			reconciled: t0.Add(-time.Hour), stock: stockDoc("evt1", 9, t0), reconciles: true, state: "closed",
		},
		"stale fence": {
			nudged: true, stock: stockDoc("evt1", 9, t0), result: dto.ReconcileResult{Stale: true}, reconciles: true, lost: true,
		},
		"store error":    {nudged: true, stock: stockDoc("evt1", 9, t0), resultErr: errBoom, reconciles: true, state: "closed"},
		"no snapshot":    {nudged: true, state: "closed"},
		"snapshot error": {nudged: true, stockErr: errBoom, state: "closed"},
		"bad snapshot":   {nudged: true, stock: []byte(`{"v":1}`), state: "closed"},
		"old snapshot":   {nudged: true, stock: stockDoc("evt1", 9, t0.Add(-time.Hour)), state: "closed"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.svc.states["evt1"] = "closed"
			h.svc.reconciled["evt1"] = tc.reconciled
			leading(h, false)
			h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, nil)
			if tc.nudged || !tc.reconciled.Equal(t0) {
				h.store.EXPECT().CoreStock(gomock.Any(), k1).Return(tc.stock, tc.stockErr)
			}
			if tc.reconciles {
				h.store.EXPECT().Reconcile(gomock.Any(), k1, int64(7), int64(9)).Return(tc.result, tc.resultErr)
			}
			if !tc.lost {
				h.store.EXPECT().Tick(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, qkeys.Event, dto.TickArgs) (dto.TickResult, error) {
					if got := h.svc.states["evt1"]; got != tc.state {
						t.Errorf("state before tick = %q, want %q", got, tc.state)
					}
					return dto.TickResult{}, errBoom
				})
			}
			h.svc.tickEvent(context.Background(), "evt1", tc.nudged)
			if _, ok := h.svc.leading["evt1"]; ok == tc.lost {
				t.Fatalf("leading = %v, lost = %v", h.svc.leading, tc.lost)
			}
		})
	}
}

func TestDrawRunsTheLotteryUntilDone(t *testing.T) {
	for name, tc := range map[string]struct {
		results []dto.LotteryResult
		err     error
		lost    bool
	}{
		"batches until empty": {results: []dto.LotteryResult{{Status: dto.LotteryOK, Drawn: 3, Left: 2}, {Status: dto.LotteryOK, Drawn: 2}}},
		"not yet":             {results: []dto.LotteryResult{{Status: dto.LotteryWait}}},
		"stale":               {results: []dto.LotteryResult{{Status: dto.LotteryStale}}, lost: true},
		"store error":         {err: errBoom},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.svc.reconciled["evt1"] = t0
			h.svc.states["evt1"] = "preopen"
			leading(h, false)
			h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, nil)
			if tc.err != nil {
				h.store.EXPECT().Lottery(gomock.Any(), k1, int64(7), h.svc.cfg.DrawBatch).Return(dto.LotteryResult{}, tc.err)
			}
			for _, r := range tc.results {
				h.store.EXPECT().Lottery(gomock.Any(), k1, int64(7), h.svc.cfg.DrawBatch).Return(r, nil)
			}
			if !tc.lost {
				h.store.EXPECT().Tick(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.TickResult{}, errBoom)
			}
			h.svc.tickEvent(context.Background(), "evt1", false)
			if _, ok := h.svc.leading["evt1"]; ok == tc.lost {
				t.Fatalf("leading = %v, lost = %v", h.svc.leading, tc.lost)
			}
		})
	}
}

func TestTickAllPassesNudges(t *testing.T) {
	h := newHarness(t)
	h.store.EXPECT().LiveEvents(gomock.Any()).Return(nil, errBoom)
	h.svc.tickAll(context.Background())

	h.svc.reconciled["evt1"] = t0
	h.svc.states["evt1"] = "open"
	h.svc.nudged <- "evt1"
	h.svc.nudged <- "evt1"
	h.store.EXPECT().LiveEvents(gomock.Any()).Return([]string{"evt1"}, nil)
	leading(h, false)
	h.store.EXPECT().Purge(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.PurgeResult{}, nil)
	h.store.EXPECT().CoreStock(gomock.Any(), k1).Return(nil, nil)
	h.store.EXPECT().Tick(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.TickResult{}, errBoom)
	h.svc.tickAll(context.Background())
	if len(h.svc.nudged) != 0 {
		t.Fatal("nudges left over")
	}
}

func TestTickLoopTicksSkipsLateTicksAndResigns(t *testing.T) {
	h := newHarness(t)
	h.svc.cfg.TickEvery = 20 * time.Millisecond
	h.svc.leading["evt1"] = 7
	h.svc.leading["evt2"] = 3
	ctx, cancel := context.WithCancel(t.Context())

	ticked := make(chan struct{})
	calls := 0
	h.store.EXPECT().LiveEvents(gomock.Any()).DoAndReturn(func(context.Context) ([]string, error) {
		calls++
		switch calls {
		case 1:
			time.Sleep(50 * time.Millisecond)
		case 2:
			close(ticked)
		}
		return nil, nil
	}).MinTimes(2)
	h.store.EXPECT().Resign(gomock.Any(), qkeys.For("evt1"), "task-a").Return(nil)
	h.store.EXPECT().Resign(gomock.Any(), qkeys.For("evt2"), "task-a").Return(errBoom)

	done := make(chan struct{})
	go func() { defer close(done); h.svc.tickLoop(ctx) }()
	<-ticked
	cancel()
	<-done
	if len(h.svc.leading) != 0 {
		t.Fatalf("still leading %v", h.svc.leading)
	}
}
