// Package valkey implements the controller's Store on Valkey.
package valkey

import (
	"context"
	"fmt"
	"strconv"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

type store struct {
	client vk.Client
}

func New(client vk.Client) outbound.IControllerStore {
	return &store{client: client}
}

func (s *store) CoreEvents(ctx context.Context) ([]string, error) {
	return s.members(ctx, qkeys.CoreEvents)
}

func (s *store) LiveEvents(ctx context.Context) ([]string, error) {
	return s.members(ctx, qkeys.LiveEvents)
}

func (s *store) members(ctx context.Context, key string) ([]string, error) {
	ids, err := s.client.Do(ctx, s.client.B().Smembers().Key(key).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("smembers %s: %w", key, err)
	}
	return ids, nil
}

func (s *store) CoreDocs(ctx context.Context, k qkeys.Event) ([]byte, []byte, error) {
	vals, err := s.client.Do(ctx, s.client.B().Mget().Key(k.CoreSale, k.CoreStock).Build()).ToArray()
	if err != nil {
		return nil, nil, fmt.Errorf("core docs %s: %w", k.ID, err)
	}
	get := func(v vk.ValkeyMessage) []byte {
		b, err := v.AsBytes()
		if err != nil {
			return nil
		}
		return b
	}
	return get(vals[0]), get(vals[1]), nil
}

func (s *store) Bootstrap(ctx context.Context, k qkeys.Event, a dto.BootstrapArgs) (string, string, error) {
	cancelled := "0"
	if a.Cancelled {
		cancelled = "1"
	}
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_bootstrap_v1",
		[]string{k.Cfg, k.Seq, k.Served, k.Unreserved, k.Resv, k.Frontier},
		ms(a.Now.UnixMilli()), ms(a.OpensAt.UnixMilli()), ms(a.EndsAt.UnixMilli()), ms(a.ExpireAt.UnixMilli()),
		cancelled, ms(a.Available),
		ms(a.Settings.MaxSlots), ms(a.Settings.RateCap), ms(a.Settings.StreamFrom),
		string(a.InitialFrontier),
	).AsStrSlice()
	if err != nil {
		return "", "", fmt.Errorf("bootstrap %s: %w", k.ID, err)
	}
	if len(vals) != 2 {
		return "", "", fmt.Errorf("bootstrap %s: unexpected reply %v", k.ID, vals)
	}
	return vals[0], vals[1], nil
}

func (s *store) AddLive(ctx context.Context, eventID string) error {
	return s.client.Do(ctx, s.client.B().Sadd().Key(qkeys.LiveEvents).Member(eventID).Build()).Error()
}

func (s *store) RemoveLive(ctx context.Context, eventID string) error {
	return s.client.Do(ctx, s.client.B().Srem().Key(qkeys.LiveEvents).Member(eventID).Build()).Error()
}

func (s *store) Counters(ctx context.Context, k qkeys.Event) (int64, int64, bool, error) {
	vals, err := s.client.Do(ctx, s.client.B().Mget().Key(k.Seq, k.Served).Build()).ToArray()
	if err != nil {
		return 0, 0, false, fmt.Errorf("counters %s: %w", k.ID, err)
	}
	seq, err1 := vals[0].AsInt64()
	served, err2 := vals[1].AsInt64()
	if vk.IsValkeyNil(err1) || vk.IsValkeyNil(err2) {
		return 0, 0, false, nil
	}
	if err1 != nil || err2 != nil {
		return 0, 0, false, fmt.Errorf("counters %s: %v %v", k.ID, err1, err2)
	}
	return seq, served, true, nil
}

func (s *store) Lead(ctx context.Context, k qkeys.Event, task string, ttl time.Duration) (dto.Lead, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_lead_v1", []string{k.Leader, k.Fence, k.Cfg},
		task, ms(ttl.Milliseconds())).AsIntSlice()
	if err != nil || len(vals) != 3 {
		return dto.Lead{}, fmt.Errorf("lead %s: %v %v", k.ID, vals, err)
	}
	return dto.Lead{Leading: vals[0] == 1, Fence: vals[1], Acquired: vals[2] == 1}, nil
}

func (s *store) Resign(ctx context.Context, k qkeys.Event, task string) error {
	if err := valkeyx.FCall(ctx, s.client, "rusuh_resign_v1", []string{k.Leader}, task).Error(); err != nil {
		return fmt.Errorf("resign %s: %w", k.ID, err)
	}
	return nil
}

func (s *store) Purge(ctx context.Context, k qkeys.Event, fence int64, grace time.Duration, limit int) (dto.PurgeResult, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_purge_v1",
		[]string{k.Cfg, k.Unreserved, k.Resv, k.Active, k.Leases, k.Fence},
		ms(fence), ms(grace.Milliseconds()), strconv.Itoa(limit)).ToArray()
	if err != nil || len(vals) != 3 {
		return dto.PurgeResult{}, fmt.Errorf("purge %s: %v %v", k.ID, vals, err)
	}
	status, _ := vals[0].ToString()
	purged, _ := vals[1].AsInt64()
	stuck, _ := vals[2].AsInt64()
	return dto.PurgeResult{Stale: status == "STALE", Purged: purged, Stuck: stuck}, nil
}

func (s *store) Tick(ctx context.Context, k qkeys.Event, a dto.TickArgs) (dto.TickResult, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_tick_v1",
		[]string{k.Cfg, k.Seq, k.Served, k.Unreserved, k.Resv, k.Held, k.Active, k.Hist, k.Inv, k.Claimed, k.Pre, k.Frontier, k.Fence},
		ms(a.Fence), k.ID, k.Tick, ms(a.HistWindow.Milliseconds()), ms(a.InviteWindow.Milliseconds()),
		ms(a.PriorOrderTime.Milliseconds())).ToArray()
	if err != nil || len(vals) == 0 {
		return dto.TickResult{}, fmt.Errorf("tick %s: %v", k.ID, err)
	}
	status, _ := vals[0].ToString()
	r := dto.TickResult{Status: dto.TickStatus(status)}
	if r.Status != dto.TickOK {
		return r, nil
	}
	if len(vals) != 7 {
		return dto.TickResult{}, fmt.Errorf("tick %s: unexpected reply length %d", k.ID, len(vals))
	}
	r.Frontier, _ = vals[1].AsBytes()
	r.Advance, _ = vals[2].AsInt64()
	r.State, _ = vals[3].ToString()
	r.Seq, _ = vals[4].AsInt64()
	r.Served, _ = vals[5].AsInt64()
	r.Pending, _ = vals[6].AsInt64()
	return r, nil
}

func (s *store) Lottery(ctx context.Context, k qkeys.Event, fence int64, batch int) (dto.LotteryResult, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_lottery_v1",
		[]string{k.Cfg, k.Seq, k.Devs, k.Pre, k.Fence}, ms(fence), strconv.Itoa(batch)).ToArray()
	if err != nil || len(vals) != 3 {
		return dto.LotteryResult{}, fmt.Errorf("lottery %s: %v %v", k.ID, vals, err)
	}
	status, _ := vals[0].ToString()
	drawn, _ := vals[1].AsInt64()
	left, _ := vals[2].AsInt64()
	return dto.LotteryResult{Status: dto.LotteryStatus(status), Drawn: drawn, Left: left}, nil
}

func (s *store) CoreStock(ctx context.Context, k qkeys.Event) ([]byte, error) {
	b, err := s.client.Do(ctx, s.client.B().Get().Key(k.CoreStock).Build()).AsBytes()
	if vk.IsValkeyNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("core stock %s: %w", k.ID, err)
	}
	return b, nil
}

func (s *store) Reconcile(ctx context.Context, k qkeys.Event, fence, available int64) (dto.ReconcileResult, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_reconcile_v1",
		[]string{k.Cfg, k.Unreserved, k.Resv, k.Fence}, ms(fence), ms(available)).ToArray()
	if err != nil || len(vals) != 4 {
		return dto.ReconcileResult{}, fmt.Errorf("reconcile %s: %v %v", k.ID, vals, err)
	}
	status, _ := vals[0].ToString()
	drift, _ := vals[1].AsInt64()
	applied, _ := vals[2].AsInt64()
	reopened, _ := vals[3].AsInt64()
	return dto.ReconcileResult{Stale: status == "STALE", Drift: drift, Applied: applied == 1, Reopened: reopened == 1}, nil
}

func ms(v int64) string { return strconv.FormatInt(v, 10) }

func (s *store) Subscribe(ctx context.Context, channel string, onEvent func(string)) {
	valkeyx.Subscribe(ctx, s.client, channel, onEvent)
}
