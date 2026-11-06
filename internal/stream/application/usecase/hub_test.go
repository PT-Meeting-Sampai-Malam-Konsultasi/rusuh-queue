package usecase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_outbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/stream/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
)

// fakeSub plays the tick channel: publish reaches every live subscription of the event.
type fakeSub struct {
	mu     sync.Mutex
	subs   map[string]func(string)
	starts map[string]int
	ended  map[string]int
}

func newFakeSub() *fakeSub {
	return &fakeSub{subs: map[string]func(string){}, starts: map[string]int{}, ended: map[string]int{}}
}

func (f *fakeSub) Subscribe(ctx context.Context, eventID string, on func(string)) {
	f.mu.Lock()
	f.subs[eventID] = on
	f.starts[eventID]++
	f.mu.Unlock()
	on("")
	<-ctx.Done()
	f.mu.Lock()
	delete(f.subs, eventID)
	f.ended[eventID]++
	f.mu.Unlock()
}

func (f *fakeSub) publish(eventID, msg string) {
	f.mu.Lock()
	on := f.subs[eventID]
	f.mu.Unlock()
	if on != nil {
		on(msg)
	}
}

func (f *fakeSub) live(eventID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subs[eventID] != nil
}

func frontier(state string, served, streamFrom, ts int64) string {
	return fmt.Sprintf(`{"event":"evt1","state":%q,"served":%d,"streamFrom":%d,"ts":%d}`, state, served, streamFrom, ts)
}

func seq(n int64) *int64 { return &n }

type env struct {
	hub    inbound.IStreamUsecase
	places *mock_outbound.MockIPlaces
	sub    *fakeSub
}

func newEnv(t *testing.T, cfg entity.Config) env {
	ctrl := gomock.NewController(t)
	places, limiter := mock_outbound.NewMockIPlaces(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil).AnyTimes()
	sub := newFakeSub()
	hub := NewStream(cfg, places, sub, limiter)
	return env{hub: hub, places: places, sub: sub}
}

// place makes the device's place seq and the current frontier at served, streamFrom 100.
func (e env) place(device string, n, served int64) {
	e.places.EXPECT().MyPlace(gomock.Any(), "evt1", device, n).Return(dto.Place{Seq: seq(n), Served: served}, nil).AnyTimes()
	e.places.EXPECT().Frontier(gomock.Any(), "evt1").Return([]byte(frontier("open", served, 100, 1)), nil).AnyTimes()
}

func (e env) open(t *testing.T, device string, n int64) inbound.IStream {
	t.Helper()
	s, err := e.hub.Open(context.Background(), dto.OpenInput{EventID: "evt1", DeviceID: device, TokenSeq: n})
	if err != nil {
		t.Fatalf("open %s: %v", device, err)
	}
	return s
}

func recv(t *testing.T, s inbound.IStream) dto.Frame {
	t.Helper()
	select {
	case f := <-s.Frames():
		return f
	case <-time.After(time.Second):
		t.Fatal("no frame within 1 s")
		return dto.Frame{}
	}
}

func TestOpenSendsTheCurrentFrontierThenEveryTick(t *testing.T) {
	e := newEnv(t, entity.DefaultConfig())
	e.place("w_1", 150, 100)
	s := e.open(t, "w_1", 150)
	if f := s.Initial(); f.Served != 100 || f.State != "open" || f.TS != 1 || s.Seq() != 150 {
		t.Fatalf("initial = %+v, seq %d", f, s.Seq())
	}
	eventually(t, func() bool { return e.sub.live("evt1") })
	e.sub.publish("evt1", frontier("open", 120, 100, 2))
	if f := recv(t, s); f.Served != 120 || f.TS != 2 || string(f.Data) != frontier("open", 120, 100, 2) {
		t.Fatalf("frame = %+v / %s", f, f.Data)
	}
}

func TestOpenRefusesADeviceFarFromTheFront(t *testing.T) {
	ctrl := gomock.NewController(t)
	for name, tc := range map[string]struct {
		place    dto.Place
		frontier string
		want     error
	}{
		"beyond streamFrom": {dto.Place{Seq: seq(300)}, frontier("open", 100, 100, 1), entity.ErrTooFar},
		"streaming off":     {dto.Place{Seq: seq(101)}, frontier("open", 100, 0, 1), entity.ErrTooFar},
		"before the draw":   {dto.Place{}, frontier("preopen", 0, 100, 1), entity.ErrTooFar},
		"closed":            {dto.Place{Seq: seq(101)}, frontier("closed", 100, 100, 1), entity.ErrClosed},
	} {
		places, limiter := mock_outbound.NewMockIPlaces(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
		limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
		places.EXPECT().MyPlace(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tc.place, nil)
		places.EXPECT().Frontier(gomock.Any(), gomock.Any()).Return([]byte(tc.frontier), nil).MaxTimes(1)
		_, err := NewStream(entity.DefaultConfig(), places, newFakeSub(), limiter).
			Open(context.Background(), dto.OpenInput{EventID: "evt1", DeviceID: "w_1", TokenSeq: 1})
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
	}
}

func TestOpenPassesPlaceErrorsThrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	places, limiter := mock_outbound.NewMockIPlaces(ctrl), mock_outbound.NewMockIRateLimiter(ctrl)
	limiter.EXPECT().Hit(gomock.Any(), gomock.Any(), gomock.Any()).Return(ratelimit.Result{Allowed: true}, nil)
	places.EXPECT().MyPlace(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.Place{}, entity.ErrMoved)
	_, err := NewStream(entity.DefaultConfig(), places, newFakeSub(), limiter).
		Open(context.Background(), dto.OpenInput{EventID: "evt1", DeviceID: "w_1", TokenSeq: 1})
	if !errors.Is(err, entity.ErrMoved) {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenIsRateLimited(t *testing.T) {
	ctrl := gomock.NewController(t)
	limiter := mock_outbound.NewMockIRateLimiter(ctrl)
	limiter.EXPECT().Hit(gomock.Any(), entity.DefaultConfig().ConnectPerDevice, "w_1").
		Return(ratelimit.Result{Allowed: false, RetryAfter: 4 * time.Second}, nil)
	_, err := NewStream(entity.DefaultConfig(), mock_outbound.NewMockIPlaces(ctrl), newFakeSub(), limiter).
		Open(context.Background(), dto.OpenInput{EventID: "evt1", DeviceID: "w_1", TokenSeq: 1})
	var rl *entity.RateLimitedError
	if !errors.As(err, &rl) || rl.RetryAfter != 4*time.Second {
		t.Fatalf("err = %v", err)
	}
}

func TestOpenRefusesBeyondTheTaskLimit(t *testing.T) {
	cfg := entity.DefaultConfig()
	cfg.MaxPerTask = 2
	e := newEnv(t, cfg)
	e.place("w_1", 150, 100)
	e.place("w_2", 150, 100)
	e.place("w_3", 150, 100)
	a := e.open(t, "w_1", 150)
	e.open(t, "w_2", 150)
	if _, err := e.hub.Open(context.Background(), dto.OpenInput{EventID: "evt1", DeviceID: "w_3", TokenSeq: 150}); !errors.Is(err, entity.ErrFull) {
		t.Fatalf("third stream: err = %v, want ErrFull", err)
	}
	e.hub.Close(a)
	e.open(t, "w_3", 150)
}

// One subscription per event per task: it starts with the first stream and ends with the last.
func TestOneSubscriptionPerEvent(t *testing.T) {
	e := newEnv(t, entity.DefaultConfig())
	e.place("w_1", 150, 100)
	e.place("w_2", 150, 100)
	a, b := e.open(t, "w_1", 150), e.open(t, "w_2", 150)
	eventually(t, func() bool { return e.sub.live("evt1") })
	e.sub.mu.Lock()
	starts := e.sub.starts["evt1"]
	e.sub.mu.Unlock()
	if starts != 1 {
		t.Fatalf("subscriptions = %d, want 1", starts)
	}
	e.hub.Close(a)
	if !e.sub.live("evt1") {
		t.Fatal("subscription ended while a stream was still open")
	}
	e.hub.Close(b)
	eventually(t, func() bool { return !e.sub.live("evt1") })
}

func TestASlowStreamIsDroppedNotWaitedFor(t *testing.T) {
	cfg := entity.DefaultConfig()
	cfg.Buffer = 2
	e := newEnv(t, cfg)
	e.place("w_slow", 150, 100)
	e.place("w_fast", 150, 100)
	slow, fast := e.open(t, "w_slow", 150), e.open(t, "w_fast", 150)
	eventually(t, func() bool { return e.sub.live("evt1") })
	for i := int64(2); i <= 5; i++ {
		e.sub.publish("evt1", frontier("open", 100+i, 100, i))
		recv(t, fast)
	}
	select {
	case <-slow.Done():
	case <-time.After(time.Second):
		t.Fatal("slow stream not dropped")
	}
}

func TestDrainEndsEveryStreamAndRefusesNewOnes(t *testing.T) {
	cfg := entity.DefaultConfig()
	cfg.DrainOver = 100 * time.Millisecond
	e := newEnv(t, cfg)
	e.place("w_1", 150, 100)
	e.place("w_2", 150, 100)
	a, b := e.open(t, "w_1", 150), e.open(t, "w_2", 150)
	e.hub.Drain()
	for _, s := range []inbound.IStream{a, b} {
		select {
		case <-s.Done():
		case <-time.After(time.Second):
			t.Fatal("stream not ended by the drain")
		}
	}
	if _, err := e.hub.Open(context.Background(), dto.OpenInput{EventID: "evt1", DeviceID: "w_1", TokenSeq: 150}); !errors.Is(err, entity.ErrDraining) {
		t.Fatalf("open while draining: err = %v", err)
	}
}

func TestTurnReadsTheDevicesWindow(t *testing.T) {
	e := newEnv(t, entity.DefaultConfig())
	e.places.EXPECT().Frontier(gomock.Any(), "evt1").Return([]byte(frontier("open", 100, 100, 1)), nil).AnyTimes()
	ends, now := time.Now().Add(3*time.Minute), time.Now()
	gomock.InOrder(
		e.places.EXPECT().MyPlace(gomock.Any(), "evt1", "w_1", int64(120)).Return(dto.Place{Seq: seq(120), Served: 100}, nil),
		e.places.EXPECT().MyPlace(gomock.Any(), "evt1", "w_1", int64(120)).
			Return(dto.Place{Seq: seq(120), Served: 121, TurnExpiresAt: &ends, ServerNow: now}, nil),
	)
	s := e.open(t, "w_1", 120)
	turn, err := e.hub.Turn(context.Background(), s)
	if err != nil || turn.Seq != 120 || turn.Served != 121 || !turn.TurnExpiresAt.Equal(ends) || turn.EventID != "evt1" {
		t.Fatalf("turn = %+v, %v", turn, err)
	}
}

func eventually(t *testing.T, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatal("condition not reached within 1 s")
}
