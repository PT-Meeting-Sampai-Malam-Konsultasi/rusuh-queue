package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/coredocs"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

// purgeLimit bounds one purge call, so a tick stays short after a mass expiry;
// the rest is purged on the next ticks.
const purgeLimit = 500

// tickLoop runs the admission tick (Admission controller steps 1, 3, 5, 6).
// A tick that starts late is skipped, never queued: the next one reads the
// current state anyway.
func (s *ControllerUsecase) tickLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.TickEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.resignAll()
			return
		case at := <-t.C:
			lag := time.Since(at)
			s.metrics.lag.Record(ctx, lag.Seconds())
			if lag > s.cfg.TickEvery/2 {
				s.metrics.skipped.Add(ctx, 1)
				slog.WarnContext(ctx, "tick skipped: running late", "lag", lag)
				continue
			}
			s.tickAll(ctx)
		}
	}
}

func (s *ControllerUsecase) tickAll(ctx context.Context) {
	ids, err := s.store.LiveEvents(ctx)
	if err != nil {
		slog.WarnContext(ctx, "tick: read live events", "error", err)
		return
	}
	due := s.drainNudges()
	for _, id := range ids {
		s.tickEvent(ctx, id, due[id])
	}
}

func (s *ControllerUsecase) drainNudges() map[string]bool {
	var due map[string]bool
	for {
		select {
		case id := <-s.nudged:
			if due == nil {
				due = make(map[string]bool)
			}
			due[id] = true
		default:
			return due
		}
	}
}

// tickEvent renews or takes the event's leadership and, when leading, purges
// ended leases, admits, writes the frontier and checkpoints. Every script is
// fenced: after a pause longer than the leader TTL another task holds a newer
// fence and this task's writes are refused.
func (s *ControllerUsecase) tickEvent(ctx context.Context, id string, nudged bool) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.TickEvery)
	defer cancel()
	k := qkeys.For(id)

	lead, err := s.store.Lead(ctx, k, s.cfg.TaskID, s.cfg.LeaderTTL)
	if err != nil {
		slog.WarnContext(ctx, "tick: leadership", "event", id, "error", err)
		return
	}
	if !lead.Leading {
		s.lose(ctx, id)
		return
	}
	if lead.Acquired {
		s.metrics.leaderChanges.Add(ctx, 1)
		slog.InfoContext(ctx, "leading event", "event", id, "task", s.cfg.TaskID, "fence", lead.Fence)
	}
	s.leading[id] = lead.Fence

	start := time.Now()
	p, err := s.store.Purge(ctx, k, lead.Fence, s.cfg.PurgeGrace, purgeLimit)
	switch {
	case err != nil:
		slog.WarnContext(ctx, "purge failed", "event", id, "error", err)
	case p.Stale:
		s.lose(ctx, id)
		return
	default:
		if p.Purged > 0 {
			s.metrics.purged.Add(ctx, p.Purged)
			slog.InfoContext(ctx, "leases purged", "event", id, "count", p.Purged)
		}
		if p.Stuck > 0 {
			slog.WarnContext(ctx, "held leases past their end", "event", id, "count", p.Stuck)
		}
	}

	if nudged || s.now().Sub(s.reconciled[id]) >= s.cfg.ReconcileEvery {
		if !s.reconcile(ctx, k, lead.Fence) {
			s.lose(ctx, id)
			return
		}
	}

	if st := s.states[id]; st == "" || st == "preopen" {
		if !s.draw(ctx, k, lead.Fence) {
			s.lose(ctx, id)
			return
		}
	}

	r, err := s.store.Tick(ctx, k, dto.TickArgs{
		Fence: lead.Fence, HistWindow: s.cfg.HistWindow, InviteWindow: s.cfg.InviteWindow, PriorOrderTime: s.cfg.PriorOrderTime,
	})
	if err != nil {
		slog.WarnContext(ctx, "tick failed", "event", id, "error", err)
		return
	}
	switch r.Status {
	case dto.TickStale, dto.TickNoEvent:
		s.lose(ctx, id)
		return
	}
	s.states[id] = r.State
	s.metrics.duration.Record(ctx, time.Since(start).Seconds())

	// Half a tick of slack, so a 1 s checkpoint is not pushed to every other tick by jitter.
	if now := s.now(); now.Add(s.cfg.TickEvery/2).Sub(s.checkpointed[id]) >= s.cfg.CheckpointEvery {
		if err := s.checkpoints.Upsert(ctx, id, r.Seq, r.Served, lead.Fence); err != nil {
			slog.WarnContext(ctx, "checkpoint failed", "event", id, "error", err)
		} else {
			s.checkpointed[id] = now
		}
	}
}

func (s *ControllerUsecase) lose(ctx context.Context, id string) {
	if _, ok := s.leading[id]; ok {
		slog.InfoContext(ctx, "no longer leading event", "event", id, "task", s.cfg.TaskID)
		delete(s.leading, id)
		delete(s.checkpointed, id)
	}
	delete(s.states, id)
	delete(s.reconciled, id)
}

// reconcile corrects unreserved from core's stock snapshot. It returns false on a stale fence.
func (s *ControllerUsecase) reconcile(ctx context.Context, k qkeys.Event, fence int64) bool {
	now := s.now()
	s.reconciled[k.ID] = now
	raw, err := s.store.CoreStock(ctx, k)
	if err != nil || raw == nil {
		slog.WarnContext(ctx, "reconcile: no stock snapshot", "event", k.ID, "error", err)
		return true
	}
	stock, err := coredocs.ParseStock(raw, k.ID)
	if err != nil {
		slog.WarnContext(ctx, "reconcile: bad stock snapshot", "event", k.ID, "error", err)
		return true
	}
	age := now.Sub(stock.TS)
	s.metrics.snapshotAge.Record(ctx, age.Seconds())
	if age > s.cfg.SnapshotMaxAge {
		slog.WarnContext(ctx, "reconcile skipped: stock snapshot too old", "event", k.ID, "age", age)
		return true
	}
	r, err := s.store.Reconcile(ctx, k, fence, stock.Available)
	switch {
	case err != nil:
		slog.WarnContext(ctx, "reconcile failed", "event", k.ID, "error", err)
	case r.Stale:
		return false
	case r.Applied:
		s.metrics.drift.Add(ctx, r.Drift)
		slog.WarnContext(ctx, "unreserved corrected from core's stock", "event", k.ID, "drift", r.Drift, "reopened", r.Reopened)
		if r.Reopened {
			s.states[k.ID] = "open"
		}
	}
	return true
}

// draw runs the pre-open lottery in batches until the draw list is empty or
// the budget is spent, so 300,000 early devices are numbered within seconds
// without one long script blocking the shard. It returns false on a stale fence.
func (s *ControllerUsecase) draw(ctx context.Context, k qkeys.Event, fence int64) bool {
	deadline := time.Now().Add(s.cfg.DrawBudget)
	var drawn int64
	defer func() {
		if drawn > 0 {
			s.metrics.drawn.Add(ctx, drawn)
			slog.InfoContext(ctx, "pre-open draw", "event", k.ID, "drawn", drawn)
		}
	}()
	for {
		r, err := s.store.Lottery(ctx, k, fence, s.cfg.DrawBatch)
		if err != nil {
			slog.WarnContext(ctx, "lottery failed", "event", k.ID, "error", err)
			return true
		}
		if r.Status == dto.LotteryStale {
			return false
		}
		drawn += r.Drawn
		if r.Status != dto.LotteryOK || r.Left == 0 || time.Now().After(deadline) {
			return true
		}
	}
}

// resignAll gives up every leadership on shutdown (step 6), so another task
// takes over within a tick instead of after the leader TTL.
func (s *ControllerUsecase) resignAll() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for id := range s.leading {
		if err := s.store.Resign(ctx, qkeys.For(id), s.cfg.TaskID); err != nil {
			slog.WarnContext(ctx, "resign failed", "event", id, "error", err)
		}
		delete(s.leading, id)
	}
}
