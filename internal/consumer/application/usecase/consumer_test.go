package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	mock_outbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/consumer/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

var errBoom = errors.New("boom")

const device = "w_abcdefghijklmnop"

type rig struct {
	svc   *ConsumerUsecase
	store *mock_outbound.MockIConsumerStore
	dlq   *mock_outbound.MockIDeadLetterLog
}

func newRig(t *testing.T) rig {
	ctrl := gomock.NewController(t)
	r := rig{store: mock_outbound.NewMockIConsumerStore(ctrl), dlq: mock_outbound.NewMockIDeadLetterLog(ctrl)}
	cfg := entity.DefaultConfig()
	cfg.RetryEvery = time.Hour
	r.svc = NewConsumer(cfg, r.store, r.dlq).(*ConsumerUsecase)
	return r
}

func payload(typ string, mutate ...func(string) string) string {
	p := fmt.Sprintf(`{"v":1,"id":"b1","type":%q,"eventId":"evt1","slotId":"s_1","deviceId":%q,"bookingId":"bk_1","units":2,"paymentDeadline":"2026-11-13T10:10:00Z","occurredAt":"2026-11-13T10:00:00Z"}`, typ, device)
	for _, m := range mutate {
		p = m(p)
	}
	return p
}

func replace(old, new string) func(string) string {
	return func(s string) string { return strings.Replace(s, old, new, 1) }
}

func TestParseEventRejectsBadPayloads(t *testing.T) {
	if _, err := parseEvent(payload("booking.held")); err != nil {
		t.Fatalf("valid payload refused: %v", err)
	}
	if _, err := parseEvent(payload("booking.paid", replace(`,"paymentDeadline":"2026-11-13T10:10:00Z"`, ""))); err != nil {
		t.Fatalf("paid without deadline refused: %v", err)
	}
	for name, p := range map[string]string{
		"not json":         "{",
		"unknown field":    payload("booking.held", replace(`"v":1`, `"v":1,"x":1`)),
		"version":          payload("booking.held", replace(`"v":1`, `"v":2`)),
		"no id":            payload("booking.held", replace(`"id":"b1"`, `"id":""`)),
		"type":             payload("booking.cancelled"),
		"event id":         payload("booking.held", replace(`"eventId":"evt1"`, `"eventId":"a:b"`)),
		"no slot":          payload("booking.held", replace(`"slotId":"s_1"`, `"slotId":""`)),
		"device":           payload("booking.held", replace(device, "x_1")),
		"units":            payload("booking.held", replace(`"units":2`, `"units":4`)),
		"held, no payment": payload("booking.held", replace(`,"paymentDeadline":"2026-11-13T10:10:00Z"`, "")),
		"no occurredAt":    payload("booking.held", replace(`,"occurredAt":"2026-11-13T10:00:00Z"`, "")),
	} {
		if _, err := parseEvent(p); !errors.Is(err, entity.ErrInvalidEvent) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestHandleAcksAppliedAndDeadLettersInvalid(t *testing.T) {
	r := newRig(t)
	msgs := []dto.Message{
		{ID: "1-0", Payload: payload("booking.held")},
		{ID: "2-0", Payload: payload("booking.paid")},
		{ID: "3-0", Payload: payload("booking.expired")},
		{ID: "4-0", Payload: `{"eventId":"evt1","type":"booking.held"}`},
	}
	r.store.EXPECT().Release(gomock.Any(), qkeys.For("evt1"), gomock.Any()).DoAndReturn(func(_ context.Context, _ qkeys.Event, a dto.ReleaseArgs) (dto.ReleaseStatus, error) {
		if a.Type != "held" || a.Units != 2 || a.SlotID != "s_1" || a.HoldGrace != r.svc.cfg.HoldGrace || a.PaymentDeadline.IsZero() {
			t.Errorf("release args = %+v", a)
		}
		return dto.ReleaseApplied, nil
	})
	r.store.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ReleaseUnknownSlot, nil)
	r.store.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ReleaseStatus(""), errBoom)
	r.dlq.EXPECT().Log(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, d dto.DeadLetter) error {
		if d.StreamID != "4-0" || d.EventID != "evt1" || d.EventType != "booking.held" || d.Deliveries != 1 || d.Payload != msgs[3].Payload {
			t.Errorf("dead letter = %+v", d)
		}
		return nil
	})
	r.store.EXPECT().DeadLetter(gomock.Any(), msgs[3], gomock.Any()).Return(nil)
	r.store.EXPECT().Ack(gomock.Any(), []string{"1-0", "2-0"}).Return(errBoom)

	r.svc.handle(context.Background(), msgs, nil)
	r.svc.handle(context.Background(), nil, nil)
}

func TestDeadLetterKeepsTheEntryWhenLoggingFails(t *testing.T) {
	r := newRig(t)
	m := dto.Message{ID: "1-0", Payload: "{"}
	r.dlq.EXPECT().Log(gomock.Any(), gomock.Any()).Return(errBoom)
	r.svc.deadLetter(context.Background(), m, 2, errBoom)

	r.dlq.EXPECT().Log(gomock.Any(), gomock.Any()).Return(nil)
	r.store.EXPECT().DeadLetter(gomock.Any(), m, "boom").Return(errBoom)
	r.svc.deadLetter(context.Background(), m, 2, errBoom)
}

func TestRetryPending(t *testing.T) {
	r := newRig(t)
	cfg := r.svc.cfg
	r.store.EXPECT().Pending(gomock.Any(), cfg.RetryIdle, cfg.Batch).Return(nil, errBoom)
	r.svc.retryPending(context.Background())

	r.store.EXPECT().Pending(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	r.svc.retryPending(context.Background())

	pending := []dto.Pending{{ID: "1-0", Deliveries: 2}, {ID: "2-0", Deliveries: cfg.MaxDeliveries}, {ID: "3-0", Deliveries: 1}}
	r.store.EXPECT().Pending(gomock.Any(), gomock.Any(), gomock.Any()).Return(pending, nil)
	r.store.EXPECT().Claim(gomock.Any(), cfg.RetryIdle, []string{"1-0", "2-0", "3-0"}).Return(nil, errBoom)
	r.svc.retryPending(context.Background())

	r.store.EXPECT().Pending(gomock.Any(), gomock.Any(), gomock.Any()).Return(pending, nil)
	r.store.EXPECT().Claim(gomock.Any(), gomock.Any(), gomock.Any()).Return([]dto.Message{
		{ID: "1-0", Payload: payload("booking.paid")},
		{ID: "2-0", Payload: payload("booking.paid")},
		{ID: "3-0", Payload: "{"},
	}, nil)
	r.store.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ReleaseDuplicate, nil)
	r.dlq.EXPECT().Log(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, d dto.DeadLetter) error {
		if d.StreamID != "2-0" || d.Deliveries != cfg.MaxDeliveries || d.LastError != "too many deliveries" {
			t.Errorf("dead letter = %+v", d)
		}
		return nil
	})
	r.dlq.EXPECT().Log(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, d dto.DeadLetter) error {
		if d.StreamID != "3-0" || d.Deliveries != 1 {
			t.Errorf("dead letter = %+v", d)
		}
		return nil
	})
	r.store.EXPECT().DeadLetter(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
	r.store.EXPECT().Ack(gomock.Any(), []string{"1-0"}).Return(nil)
	r.svc.retryPending(context.Background())
}

func TestRunWaitsForTheGroupThenReadsAndRetries(t *testing.T) {
	r := newRig(t)
	r.svc.cfg.RetryEvery = time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	r.store.EXPECT().EnsureGroup(gomock.Any()).Return(errBoom)
	r.store.EXPECT().EnsureGroup(gomock.Any()).Return(nil)
	reads := 0
	r.store.EXPECT().Read(gomock.Any()).DoAndReturn(func(context.Context) ([]dto.Message, error) {
		reads++
		switch reads {
		case 1:
			return nil, errBoom
		case 2:
			return []dto.Message{{ID: "1-0", Payload: payload("booking.paid")}}, nil
		}
		time.Sleep(5 * time.Millisecond)
		if reads == 4 {
			cancel()
			return nil, context.Canceled
		}
		return nil, nil
	}).MinTimes(4)
	r.store.EXPECT().Release(gomock.Any(), gomock.Any(), gomock.Any()).Return(dto.ReleaseApplied, nil)
	r.store.EXPECT().Ack(gomock.Any(), []string{"1-0"}).Return(nil)
	r.store.EXPECT().Pending(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).MinTimes(1)

	if err := r.svc.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunStopsWhileWaiting(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r.store.EXPECT().EnsureGroup(gomock.Any()).DoAndReturn(func(context.Context) error { cancel(); return errBoom })
	if err := r.svc.Run(ctx); err != nil {
		t.Fatal(err)
	}

	r2 := newRig(t)
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	r2.store.EXPECT().EnsureGroup(gomock.Any()).Return(nil)
	r2.store.EXPECT().Read(gomock.Any()).DoAndReturn(func(context.Context) ([]dto.Message, error) {
		go func() { time.Sleep(10 * time.Millisecond); cancel2() }()
		return nil, errBoom
	})
	if err := r2.svc.Run(ctx2); err != nil {
		t.Fatal(err)
	}

	r3 := newRig(t)
	ctx3, cancel3 := context.WithCancel(t.Context())
	cancel3()
	r3.store.EXPECT().EnsureGroup(gomock.Any()).Return(nil)
	if err := r3.svc.Run(ctx3); err != nil {
		t.Fatal(err)
	}
}
