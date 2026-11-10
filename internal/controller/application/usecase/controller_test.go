package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_outbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/controller/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	admin "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

var (
	errBoom = errors.New("boom")
	t0      = time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
)

type harness struct {
	svc         *ControllerUsecase
	store       *mock_outbound.MockIControllerStore
	checkpoints *mock_outbound.MockICheckpoints
	settings    *mock_outbound.MockISettings
}

func newHarness(t *testing.T) harness {
	ctrl := gomock.NewController(t)
	h := harness{
		store:       mock_outbound.NewMockIControllerStore(ctrl),
		checkpoints: mock_outbound.NewMockICheckpoints(ctrl),
		settings:    mock_outbound.NewMockISettings(ctrl),
	}
	cfg := entity.DefaultConfig()
	cfg.TaskID = "task-a"
	cfg.DiscoverEvery = time.Hour
	cfg.TickEvery = time.Hour
	h.svc = NewController(cfg, h.store, h.checkpoints, h.settings).(*ControllerUsecase)
	h.svc.now = func() time.Time { return t0 }
	return h
}

func saleDoc(id, status string, opens time.Time) []byte {
	return fmt.Appendf(nil, `{"v":1,"eventId":%q,"status":%q,"saleOpensAt":%q,"endsAt":%q}`,
		id, status, opens.Format(time.RFC3339), opens.Add(2*time.Hour).Format(time.RFC3339))
}

func stockDoc(id string, available int64, ts time.Time) []byte {
	return fmt.Appendf(nil, `{"v":1,"eventId":%q,"available":%d,"version":1,"ts":%q}`, id, available, ts.Format(time.RFC3339))
}

func TestSyncRefusesUnpublishedOrBrokenDocs(t *testing.T) {
	for name, tc := range map[string]struct {
		sale, stock []byte
		err         error
		want        error
	}{
		"read fails":   {err: errBoom, want: errBoom},
		"no sale":      {stock: stockDoc("evt1", 5, t0), want: ErrNotPublished},
		"no stock":     {sale: saleDoc("evt1", "published", t0), want: ErrNotPublished},
		"bad sale":     {sale: []byte(`{}`), stock: stockDoc("evt1", 5, t0)},
		"bad stock":    {sale: saleDoc("evt1", "published", t0), stock: []byte(`{"v":2}`)},
		"other events": {sale: saleDoc("evt2", "published", t0), stock: stockDoc("evt1", 5, t0)},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.store.EXPECT().CoreDocs(gomock.Any(), qkeys.For("evt1")).Return(tc.sale, tc.stock, tc.err)
			err := h.svc.Sync(context.Background(), "evt1")
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSyncStopsOnDependencyErrors(t *testing.T) {
	for name, setup := range map[string]func(h harness){
		"settings": func(h harness) {
			h.settings.EXPECT().QueueSettings(gomock.Any(), "evt1").Return(admin.Settings{}, errBoom)
		},
		"bootstrap": func(h harness) {
			h.settings.EXPECT().QueueSettings(gomock.Any(), "evt1").Return(admin.DefaultSettings("evt1"), nil)
			h.store.EXPECT().Bootstrap(gomock.Any(), gomock.Any(), gomock.Any()).Return("", "", errBoom)
		},
		"add live": func(h harness) {
			h.settings.EXPECT().QueueSettings(gomock.Any(), "evt1").Return(admin.DefaultSettings("evt1"), nil)
			h.store.EXPECT().Bootstrap(gomock.Any(), gomock.Any(), gomock.Any()).Return("UPDATED", "open", nil)
			h.store.EXPECT().AddLive(gomock.Any(), "evt1").Return(errBoom)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.store.EXPECT().CoreDocs(gomock.Any(), gomock.Any()).Return(saleDoc("evt1", "published", t0), stockDoc("evt1", 5, t0), nil)
			setup(h)
			if err := h.svc.Sync(context.Background(), "evt1"); !errors.Is(err, errBoom) {
				t.Fatalf("err = %v, want boom", err)
			}
		})
	}
}

func TestSyncBootstrapsWithTheInitialFrontier(t *testing.T) {
	for name, tc := range map[string]struct {
		status    string
		opens     time.Time
		available int64
		state     api.QueueState
		reason    api.FrontierReason
	}{
		"open":      {status: "published", opens: t0.Add(-time.Minute), available: 5, state: api.QueueStateOpen},
		"preopen":   {status: "published", opens: t0.Add(time.Hour), available: 5, state: api.QueueStatePreopen},
		"sold out":  {status: "published", opens: t0, available: 0, state: api.QueueStateClosed, reason: api.FrontierReasonSoldOut},
		"cancelled": {status: "cancelled", opens: t0, available: 5, state: api.QueueStateClosed, reason: api.FrontierReasonCancelled},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			settings := admin.DefaultSettings("evt1")
			h.store.EXPECT().CoreDocs(gomock.Any(), gomock.Any()).Return(saleDoc("evt1", tc.status, tc.opens), stockDoc("evt1", tc.available, t0), nil)
			h.settings.EXPECT().QueueSettings(gomock.Any(), "evt1").Return(settings, nil)
			h.store.EXPECT().Bootstrap(gomock.Any(), qkeys.For("evt1"), gomock.Any()).DoAndReturn(
				func(_ context.Context, _ qkeys.Event, a dto.BootstrapArgs) (string, string, error) {
					if !a.Now.Equal(t0) || !a.OpensAt.Equal(tc.opens) || !a.ExpireAt.Equal(a.EndsAt.Add(keyGrace)) ||
						a.Cancelled != (tc.status == "cancelled") || a.Available != tc.available || a.Settings != settings {
						t.Errorf("bootstrap args = %+v", a)
					}
					var f api.Frontier
					if err := json.Unmarshal(a.InitialFrontier, &f); err != nil {
						t.Fatal(err)
					}
					if f.Event != "evt1" || f.State != tc.state || *f.Sellable != int(tc.available) || *f.StreamFrom != int(settings.StreamFrom) ||
						*f.OpensAt != int(tc.opens.Unix()) || f.Ts != int(t0.Unix()) {
						t.Errorf("frontier = %+v", f)
					}
					if (f.Reason == nil) != (tc.reason == "") || (f.Reason != nil && *f.Reason != tc.reason) {
						t.Errorf("reason = %v, want %q", f.Reason, tc.reason)
					}
					return "CREATED", string(tc.state), nil
				})
			h.store.EXPECT().AddLive(gomock.Any(), "evt1").Return(nil)
			if err := h.svc.Sync(context.Background(), "evt1"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func expectSync(h harness, id string) {
	h.store.EXPECT().CoreDocs(gomock.Any(), qkeys.For(id)).Return(saleDoc(id, "published", t0), stockDoc(id, 5, t0), nil)
	h.settings.EXPECT().QueueSettings(gomock.Any(), id).Return(admin.DefaultSettings(id), nil)
	h.store.EXPECT().Bootstrap(gomock.Any(), gomock.Any(), gomock.Any()).Return("UPDATED", "open", nil)
	h.store.EXPECT().AddLive(gomock.Any(), id).Return(nil)
}

func TestDiscoverSyncsFollowsAndForgets(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	syncs := make(chan string, 4)

	subscribed := make(chan func(string), 1)
	h.store.EXPECT().CoreEvents(gomock.Any()).Return([]string{"evt1", "bad:id", "evt2"}, nil)
	expectSync(h, "evt1")
	h.store.EXPECT().CoreDocs(gomock.Any(), qkeys.For("evt2")).Return(nil, nil, nil)
	h.store.EXPECT().Subscribe(gomock.Any(), qkeys.For("evt1").CoreChanged, gomock.Any()).
		Do(func(_ context.Context, _ string, on func(string)) { subscribed <- on })
	evt2 := make(chan struct{})
	h.store.EXPECT().Subscribe(gomock.Any(), qkeys.For("evt2").CoreChanged, gomock.Any()).
		Do(func(context.Context, string, func(string)) { close(evt2) })
	h.store.EXPECT().LiveEvents(gomock.Any()).Return([]string{"evt1", "gone"}, nil)
	h.store.EXPECT().Counters(gomock.Any(), qkeys.For("evt1")).Return(int64(3), int64(1), true, nil)
	h.store.EXPECT().Counters(gomock.Any(), qkeys.For("gone")).Return(int64(0), int64(0), false, nil)
	h.store.EXPECT().RemoveLive(gomock.Any(), "gone").Return(nil)

	goneCancelled := false
	h.svc.subs["gone"] = func() { goneCancelled = true }
	h.svc.discover(ctx, syncs)

	if !goneCancelled || h.svc.subs["gone"] != nil {
		t.Fatal("expired event still followed")
	}
	if _, ok := h.svc.subs["evt1"]; !ok {
		t.Fatal("evt1 not followed")
	}
	<-evt2
	on := <-subscribed
	on("changed")
	on("changed again")
	if got := <-syncs; got != "evt1" {
		t.Fatalf("sync = %q", got)
	}
	if got := <-h.svc.nudged; got != "evt1" {
		t.Fatalf("nudge = %q", got)
	}

	h.svc.follow(ctx, "evt1", syncs)
	if len(h.svc.subs) != 2 {
		t.Fatalf("subs = %d, want 2", len(h.svc.subs))
	}
}

func TestDiscoverToleratesStoreErrors(t *testing.T) {
	h := newHarness(t)
	h.store.EXPECT().CoreEvents(gomock.Any()).Return(nil, errBoom)
	h.svc.discover(context.Background(), nil)

	h.store.EXPECT().CoreEvents(gomock.Any()).Return([]string{"evt1"}, nil)
	h.store.EXPECT().CoreDocs(gomock.Any(), gomock.Any()).Return(nil, nil, errBoom)
	h.store.EXPECT().LiveEvents(gomock.Any()).Return(nil, errBoom)
	h.svc.discover(context.Background(), nil)
	if len(h.svc.subs) != 0 {
		t.Fatal("a failing event was followed")
	}
}

func TestRunDiscoversSyncsAndStops(t *testing.T) {
	h := newHarness(t)
	h.svc.cfg.DiscoverEvery = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())

	nudgeEvents := make(chan func(string), 1)
	nudgeEvent := make(chan func(string), 1)
	synced := make(chan struct{}, 8)
	h.store.EXPECT().Subscribe(gomock.Any(), qkeys.CoreEventsChanged, gomock.Any()).
		Do(func(_ context.Context, _ string, on func(string)) { nudgeEvents <- on })
	h.store.EXPECT().Subscribe(gomock.Any(), qkeys.For("evt1").CoreChanged, gomock.Any()).
		Do(func(ctx context.Context, _ string, on func(string)) { nudgeEvent <- on; <-ctx.Done() })
	h.store.EXPECT().CoreEvents(gomock.Any()).Return([]string{"evt1"}, nil).MinTimes(1)
	h.store.EXPECT().CoreDocs(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, qkeys.Event) ([]byte, []byte, error) {
		select {
		case synced <- struct{}{}:
		default:
		}
		return saleDoc("evt1", "published", t0), stockDoc("evt1", 5, t0), nil
	}).MinTimes(1)
	h.settings.EXPECT().QueueSettings(gomock.Any(), "evt1").Return(admin.DefaultSettings("evt1"), nil).AnyTimes()
	h.store.EXPECT().Bootstrap(gomock.Any(), gomock.Any(), gomock.Any()).Return("UPDATED", "open", nil).AnyTimes()
	h.store.EXPECT().AddLive(gomock.Any(), "evt1").Return(nil).AnyTimes()
	h.store.EXPECT().LiveEvents(gomock.Any()).Return(nil, nil).AnyTimes()

	done := make(chan error, 1)
	go func() { done <- h.svc.Run(ctx) }()
	(<-nudgeEvents)("added")
	(<-nudgeEvent)("changed")
	for range 3 {
		<-synced
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunLogsFailedSyncs(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(t.Context())
	nudgeEvent := make(chan func(string), 1)
	failed := make(chan struct{}, 4)
	h.store.EXPECT().Subscribe(gomock.Any(), qkeys.CoreEventsChanged, gomock.Any()).
		Do(func(_ context.Context, _ string, on func(string)) { on("added") })
	h.store.EXPECT().Subscribe(gomock.Any(), qkeys.For("evt1").CoreChanged, gomock.Any()).
		Do(func(ctx context.Context, _ string, on func(string)) { nudgeEvent <- on; <-ctx.Done() })
	h.store.EXPECT().CoreEvents(gomock.Any()).Return([]string{"evt1"}, nil)
	expectSync(h, "evt1")
	h.store.EXPECT().CoreDocs(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, qkeys.Event) ([]byte, []byte, error) {
		failed <- struct{}{}
		return nil, nil, errBoom
	})
	h.store.EXPECT().LiveEvents(gomock.Any()).Return(nil, nil)

	done := make(chan error, 1)
	go func() { done <- h.svc.Run(ctx) }()
	(<-nudgeEvent)("changed")
	<-failed
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
