package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

var deviceIDPattern = regexp.MustCompile(`^[wm]_[A-Za-z0-9_-]{16,32}$`)

func (u *ConsumerUsecase) Run(ctx context.Context) error {
	for {
		err := u.store.EnsureGroup(ctx)
		if err == nil {
			break
		}
		slog.WarnContext(ctx, "consumer group not ready, retrying", "error", err)
		if !sleep(ctx, time.Second) {
			return nil
		}
	}

	retry := time.NewTicker(u.cfg.RetryEvery)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-retry.C:
			u.retryPending(ctx)
		default:
		}
		msgs, err := u.store.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.WarnContext(ctx, "read stream:booking", "error", err)
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
		u.handle(ctx, msgs, nil)
	}
}

// handle applies each message and acknowledges the ones that are done. A
// message that fails for a transient reason stays pending and is retried.
func (u *ConsumerUsecase) handle(ctx context.Context, msgs []dto.Message, deliveries map[string]int64) {
	if len(msgs) == 0 {
		return
	}
	acks := make([]string, 0, len(msgs))
	for _, m := range msgs {
		err := u.apply(ctx, m)
		switch {
		case err == nil:
			acks = append(acks, m.ID)
		case errors.Is(err, entity.ErrInvalidEvent):
			u.deadLetter(ctx, m, max(1, deliveries[m.ID]), err)
		default:
			slog.WarnContext(ctx, "booking event failed, will retry", "id", m.ID, "error", err)
		}
	}
	if len(acks) == 0 {
		return
	}
	if err := u.store.Ack(ctx, acks); err != nil {
		slog.WarnContext(ctx, "ack stream:booking", "count", len(acks), "error", err)
	}
}

func (u *ConsumerUsecase) apply(ctx context.Context, m dto.Message) error {
	ev, err := parseEvent(m.Payload)
	if err != nil {
		return err
	}
	st, err := u.store.Release(ctx, qkeys.For(ev.EventID), dto.ReleaseArgs{
		ID:              ev.ID,
		EventID:         ev.EventID,
		Type:            strings.TrimPrefix(string(ev.Type), "booking."),
		SlotID:          ev.SlotID,
		Units:           ev.Units,
		PaymentDeadline: ev.PaymentDeadline,
		HoldGrace:       u.cfg.HoldGrace,
	})
	if err != nil {
		return err
	}
	u.metrics.processed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("type", string(ev.Type)), attribute.String("result", string(st))))
	if st == dto.ReleaseUnknownSlot || st == dto.ReleaseNoEvent {
		slog.WarnContext(ctx, "booking event for no live lease", "event", ev.EventID, "type", ev.Type, "result", st, "id", ev.ID)
	}
	return nil
}

// retryPending claims entries nobody acknowledged within RetryIdle (a task
// died, or Valkey failed mid-batch) and retries them; after MaxDeliveries an
// entry goes to the dead-letter stream.
func (u *ConsumerUsecase) retryPending(ctx context.Context) {
	pending, err := u.store.Pending(ctx, u.cfg.RetryIdle, u.cfg.Batch)
	if err != nil {
		slog.WarnContext(ctx, "pending stream:booking", "error", err)
		return
	}
	if len(pending) == 0 {
		return
	}
	ids := make([]string, len(pending))
	deliveries := make(map[string]int64, len(pending))
	for i, p := range pending {
		ids[i] = p.ID
		deliveries[p.ID] = p.Deliveries
	}
	msgs, err := u.store.Claim(ctx, u.cfg.RetryIdle, ids)
	if err != nil {
		slog.WarnContext(ctx, "claim stream:booking", "error", err)
		return
	}

	retry := msgs[:0]
	for _, m := range msgs {
		if deliveries[m.ID] >= u.cfg.MaxDeliveries {
			u.deadLetter(ctx, m, deliveries[m.ID], errors.New("too many deliveries"))
			continue
		}
		retry = append(retry, m)
	}
	u.handle(ctx, retry, deliveries)
}

func (u *ConsumerUsecase) deadLetter(ctx context.Context, m dto.Message, deliveries int64, cause error) {
	var head struct {
		EventID string `json:"eventId"`
		Type    string `json:"type"`
	}
	_ = json.Unmarshal([]byte(m.Payload), &head)
	d := dto.DeadLetter{
		StreamID: m.ID, EventID: head.EventID, EventType: head.Type,
		Deliveries: deliveries, LastError: cause.Error(), Payload: m.Payload,
	}
	if err := u.dlq.Log(ctx, d); err != nil {
		slog.WarnContext(ctx, "dead-letter log failed, will retry", "id", m.ID, "error", err)
		return
	}
	if err := u.store.DeadLetter(ctx, m, cause.Error()); err != nil {
		slog.WarnContext(ctx, "dead-letter move failed, will retry", "id", m.ID, "error", err)
		return
	}
	u.metrics.dlq.Add(ctx, 1)
	slog.ErrorContext(ctx, "booking event dead-lettered", "id", m.ID, "event", head.EventID, "type", head.Type,
		"deliveries", deliveries, "error", cause)
}

func parseEvent(payload string) (entity.BookingEvent, error) {
	var ev entity.BookingEvent
	dec := json.NewDecoder(strings.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ev); err != nil {
		return ev, fmt.Errorf("%w: %w", entity.ErrInvalidEvent, err)
	}
	if err := validate(ev); err != nil {
		return ev, fmt.Errorf("%w: %s", entity.ErrInvalidEvent, err)
	}
	return ev, nil
}

func validate(ev entity.BookingEvent) error {
	switch {
	case ev.V != 1:
		return fmt.Errorf("v=%d", ev.V)
	case ev.ID == "" || len(ev.ID) > 64:
		return errors.New("id must be 1-64 characters")
	case ev.Type != entity.BookingHeld && ev.Type != entity.BookingPaid && ev.Type != entity.BookingExpired:
		return fmt.Errorf("type %q", ev.Type)
	case !qkeys.ValidEvent(ev.EventID):
		return fmt.Errorf("eventId %q", ev.EventID)
	case ev.SlotID == "" || ev.BookingID == "":
		return errors.New("slotId and bookingId are required")
	case !deviceIDPattern.MatchString(ev.DeviceID):
		return fmt.Errorf("deviceId %q", ev.DeviceID)
	case ev.Units < 1 || ev.Units > 3:
		return fmt.Errorf("units %d", ev.Units)
	case ev.Type == entity.BookingHeld && ev.PaymentDeadline.IsZero():
		return errors.New("paymentDeadline is required on booking.held")
	case ev.OccurredAt.IsZero():
		return errors.New("occurredAt is required")
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
