package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/coredocs"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

// keyGrace keeps an event's queue keys one day after the sale ends (queue
// tokens live as long; Solution Architecture "Sale end + 1 day").
const keyGrace = 24 * time.Hour

var ErrNotPublished = errors.New("controller: core has not published this event")

// Run serialises every sync on one goroutine, so nudges, timers and
// subscriptions never race on the same event. Ticks run on a second
// goroutine, so a slow sync (PostgreSQL) never delays admission.
func (s *ControllerUsecase) Run(ctx context.Context) error {
	ticking := make(chan struct{})
	go func() { defer close(ticking); s.tickLoop(ctx) }()
	defer func() { <-ticking }() // leaderships are given up before Run returns

	discover := make(chan struct{}, 1)
	syncs := make(chan string, 64)
	nudge := func(ch chan struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	go s.store.Subscribe(ctx, qkeys.CoreEventsChanged, func(string) { nudge(discover) })

	discoverTick := time.NewTicker(s.cfg.DiscoverEvery)
	defer discoverTick.Stop()

	for {
		select {
		case <-ctx.Done():
			for _, cancel := range s.subs {
				cancel()
			}
			return nil
		case <-discover:
			s.discover(ctx, syncs)
		case <-discoverTick.C:
			s.discover(ctx, syncs)
		case id := <-syncs:
			if err := s.Sync(ctx, id); err != nil && !errors.Is(err, ErrNotPublished) {
				slog.WarnContext(ctx, "event sync failed", "event", id, "error", err)
			}
		}
	}
}

// discover syncs every event core publishes, and forgets live events whose keys expired.
func (s *ControllerUsecase) discover(ctx context.Context, syncs chan<- string) {
	ids, err := s.store.CoreEvents(ctx)
	if err != nil {
		slog.WarnContext(ctx, "discover: read core events", "error", err)
		return
	}
	for _, id := range ids {
		if !qkeys.ValidEvent(id) {
			slog.WarnContext(ctx, "discover: skipping invalid event id", "event", id)
			continue
		}
		if err := s.Sync(ctx, id); err != nil && !errors.Is(err, ErrNotPublished) {
			slog.WarnContext(ctx, "event sync failed", "event", id, "error", err)
			continue
		}
		s.follow(ctx, id, syncs)
	}

	live, err := s.store.LiveEvents(ctx)
	if err != nil {
		return
	}
	for _, id := range live {
		if _, _, ok, err := s.store.Counters(ctx, qkeys.For(id)); err == nil && !ok {
			_ = s.store.RemoveLive(ctx, id)
			if cancel, found := s.subs[id]; found {
				cancel()
				delete(s.subs, id)
			}
			slog.InfoContext(ctx, "event keys expired; forgotten", "event", id)
		}
	}
}

// follow subscribes to core:{evt}:changed once per event.
func (s *ControllerUsecase) follow(ctx context.Context, id string, syncs chan<- string) {
	if _, ok := s.subs[id]; ok {
		return
	}
	subCtx, cancel := context.WithCancel(ctx)
	s.subs[id] = cancel
	go s.store.Subscribe(subCtx, qkeys.For(id).CoreChanged, func(string) {
		select {
		case syncs <- id:
		default: // a sync is already queued; it will read the latest documents
		}
	})
}

// Sync creates or refreshes one event's queue from core's sale and stock documents.
func (s *ControllerUsecase) Sync(ctx context.Context, eventID string) error {
	k := qkeys.For(eventID)
	saleRaw, stockRaw, err := s.store.CoreDocs(ctx, k)
	if err != nil {
		return err
	}
	if saleRaw == nil || stockRaw == nil {
		return fmt.Errorf("%w: %s (sale %t, stock %t)", ErrNotPublished, eventID, saleRaw != nil, stockRaw != nil)
	}
	sale, err := coredocs.ParseSale(saleRaw, eventID)
	if err != nil {
		return err
	}
	stock, err := coredocs.ParseStock(stockRaw, eventID)
	if err != nil {
		return err
	}
	settings, err := s.settings.QueueSettings(ctx, eventID)
	if err != nil {
		return err
	}

	now := s.now()
	initial, err := initialFrontier(eventID, now, sale, stock, settings.StreamFrom)
	if err != nil {
		return err
	}
	status, state, err := s.store.Bootstrap(ctx, k, dto.BootstrapArgs{
		Now:             now,
		OpensAt:         sale.SaleOpensAt,
		EndsAt:          sale.EndsAt,
		ExpireAt:        sale.EndsAt.Add(keyGrace),
		Cancelled:       sale.Status == "cancelled",
		Available:       stock.Available,
		Settings:        settings,
		InitialFrontier: initial,
	})
	if err != nil {
		return err
	}
	if err := s.store.AddLive(ctx, eventID); err != nil {
		return fmt.Errorf("add live %s: %w", eventID, err)
	}
	if status == "CREATED" {
		slog.InfoContext(ctx, "event queue created", "event", eventID, "state", state, "available", stock.Available)
	}
	return nil
}

// initialFrontier is the frontier before the first tick (the leader rewrites it every second).
func initialFrontier(eventID string, now time.Time, sale coredocs.Sale, stock coredocs.Stock, streamFrom int64) ([]byte, error) {
	state := api.QueueStateOpen
	var reason *api.FrontierReason
	switch {
	case sale.Status == "cancelled":
		state, reason = api.QueueStateClosed, ptr(api.FrontierReasonCancelled)
	case stock.Available == 0:
		state, reason = api.QueueStateClosed, ptr(api.FrontierReasonSoldOut)
	case now.Before(sale.SaleOpensAt):
		state = api.QueueStatePreopen
	}
	opensAt := int(sale.SaleOpensAt.Unix())
	zero := 0
	sellable := int(stock.Available)
	stream := int(streamFrom)
	return json.Marshal(api.Frontier{
		Event:      eventID,
		State:      state,
		Served:     0,
		Sellable:   &sellable,
		Reserved:   &zero,
		Held:       &zero,
		RatePerMin: &zero,
		RateLow:    &zero,
		RateHigh:   &zero,
		StreamFrom: &stream,
		Reason:     reason,
		OpensAt:    &opensAt,
		Ts:         int(now.Unix()),
	})
}

func ptr[T any](v T) *T { return &v }
