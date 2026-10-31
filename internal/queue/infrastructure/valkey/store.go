// Package valkey implements the queue module's Store on the rusuh_q functions.
package valkey

import (
	"context"
	"fmt"
	"strconv"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

type store struct {
	client vk.Client
}

func New(client vk.Client) outbound.IQueueStore {
	return &store{client: client}
}

func (s *store) Join(ctx context.Context, k qkeys.Event, deviceID, userID string, missWindow time.Duration) (dto.JoinResult, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_join_v1",
		[]string{k.Cfg, k.Seq, k.Devs, k.Pre, k.Accts, k.Served, k.Hist},
		deviceID, userID, strconv.FormatInt(missWindow.Milliseconds(), 10)).ToArray()
	if err != nil {
		return dto.JoinResult{}, fmt.Errorf("join %s: %w", k.ID, err)
	}
	if len(vals) != 8 {
		return dto.JoinResult{}, fmt.Errorf("join %s: unexpected reply length %d", k.ID, len(vals))
	}
	status, err := vals[0].ToString()
	if err != nil {
		return dto.JoinResult{}, fmt.Errorf("join %s: status: %w", k.ID, err)
	}
	n := ints(vals[1:])
	return dto.JoinResult{
		Status:   dto.JoinStatus(status),
		Seq:      n[0],
		Attached: n[1] == 1,
		EndsAt:   time.UnixMilli(n[2]),
		Turn:     turn(n[3], n[4], n[5], n[6]),
	}, nil
}

func (s *store) Place(ctx context.Context, k qkeys.Event, deviceID string, missWindow time.Duration) (dto.PlaceView, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_place_v1",
		[]string{k.Cfg, k.Devs, k.Accts, k.Served, k.Hist},
		deviceID, strconv.FormatInt(missWindow.Milliseconds(), 10)).ToArray()
	if err != nil {
		return dto.PlaceView{}, fmt.Errorf("place %s: %w", k.ID, err)
	}
	if len(vals) != 9 {
		return dto.PlaceView{}, fmt.Errorf("place %s: unexpected reply length %d", k.ID, len(vals))
	}
	state, _ := vals[0].ToString()
	if state == "" {
		return dto.PlaceView{}, nil
	}
	value, _ := vals[1].ToString()
	n := ints(vals[2:])
	return dto.PlaceView{
		Found:    true,
		State:    state,
		Value:    value,
		Attached: n[0] == 1,
		EndsAt:   time.UnixMilli(n[1]),
		Missed:   n[5] == 1,
		Turn:     turn(n[2], n[3], n[4], n[6]),
	}, nil
}

func (s *store) Leave(ctx context.Context, k qkeys.Event, deviceID string) (bool, error) {
	status, err := valkeyx.FCall(ctx, s.client, "rusuh_leave_v1", []string{k.Cfg, k.Devs, k.Pre}, deviceID).ToString()
	if err != nil {
		return false, fmt.Errorf("leave %s: %w", k.ID, err)
	}
	return status == "OK", nil
}

func ints(vals []vk.ValkeyMessage) []int64 {
	out := make([]int64, len(vals))
	for i, v := range vals {
		out[i], _ = v.AsInt64()
	}
	return out
}

// turn builds dto.Turn from Valkey milliseconds (turnEnds 0 = not reached).
func turn(served, opensMs, turnEndsMs, nowMs int64) dto.Turn {
	t := dto.Turn{Served: served, OpensAt: time.UnixMilli(opensMs), ServerNow: time.UnixMilli(nowMs)}
	if turnEndsMs > 0 {
		t.TurnEnds = time.UnixMilli(turnEndsMs)
	}
	return t
}

func (s *store) Frontier(ctx context.Context, k qkeys.Event) ([]byte, error) {
	b, err := s.client.Do(ctx, s.client.B().Get().Key(k.Frontier).Build()).AsBytes()
	if vk.IsValkeyNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("frontier %s: %w", k.ID, err)
	}
	return b, nil
}

func (s *store) Claim(ctx context.Context, k qkeys.Event, in dto.ClaimInput) (dto.ClaimResult, error) {
	vals, err := valkeyx.FCall(ctx, s.client, "rusuh_claim_v1",
		[]string{k.Cfg, k.Served, k.Devs, k.Unreserved, k.Resv, k.Active, k.Leases, k.Hist, k.Claimed, k.Accts},
		in.DeviceID, strconv.FormatInt(in.TokenSeq, 10), in.SlotID,
		strconv.FormatInt(in.OrderWindow.Milliseconds(), 10), strconv.FormatInt(in.MissWindow.Milliseconds(), 10),
	).ToArray()
	if err != nil {
		return dto.ClaimResult{}, fmt.Errorf("claim %s: %w", k.ID, err)
	}
	if len(vals) != 6 {
		return dto.ClaimResult{}, fmt.Errorf("claim %s: unexpected reply length %d", k.ID, len(vals))
	}
	status, err := vals[0].ToString()
	if err != nil {
		return dto.ClaimResult{}, fmt.Errorf("claim %s: status: %w", k.ID, err)
	}
	slot, _ := vals[1].ToString()
	units, _ := vals[2].AsInt64()
	ends, _ := vals[3].AsInt64()
	attached, _ := vals[4].AsInt64()
	now, _ := vals[5].AsInt64()
	return dto.ClaimResult{
		Status:    dto.ClaimStatus(status),
		SlotID:    slot,
		Units:     int(units),
		EndsAt:    time.UnixMilli(ends),
		Attached:  attached == 1,
		ServerNow: time.UnixMilli(now),
	}, nil
}
