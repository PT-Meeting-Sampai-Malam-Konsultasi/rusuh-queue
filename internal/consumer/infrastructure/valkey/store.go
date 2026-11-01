package valkey

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

type store struct {
	client vk.Client
	cfg    entity.Config
}

func New(client vk.Client, cfg entity.Config) outbound.IConsumerStore {
	return &store{client: client, cfg: cfg}
}

func (s *store) EnsureGroup(ctx context.Context) error {
	err := s.client.Do(ctx, s.client.B().XgroupCreate().Key(s.cfg.Stream).Group(s.cfg.Group).Id("$").Mkstream().Build()).Error()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("xgroup create %s: %w", s.cfg.Stream, err)
	}
	return nil
}

func (s *store) Read(ctx context.Context) ([]dto.Message, error) {
	res, err := s.client.Do(ctx, s.client.B().Xreadgroup().Group(s.cfg.Group, s.cfg.Consumer).
		Count(s.cfg.Batch).Block(s.cfg.Block.Milliseconds()).Streams().Key(s.cfg.Stream).Id(">").Build()).AsXRead()
	if vk.IsValkeyNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup %s: %w", s.cfg.Stream, err)
	}
	return messages(res[s.cfg.Stream]), nil
}

func (s *store) Pending(ctx context.Context, idle time.Duration, count int64) ([]dto.Pending, error) {
	rows, err := s.client.Do(ctx, s.client.B().Xpending().Key(s.cfg.Stream).Group(s.cfg.Group).
		Idle(idle.Milliseconds()).Start("-").End("+").Count(count).Build()).ToArray()
	if err != nil {
		return nil, fmt.Errorf("xpending %s: %w", s.cfg.Stream, err)
	}
	out := make([]dto.Pending, 0, len(rows))
	for _, r := range rows {
		f, err := r.ToArray()
		if err != nil || len(f) != 4 {
			return nil, fmt.Errorf("xpending %s: bad entry: %v", s.cfg.Stream, err)
		}
		id, _ := f[0].ToString()
		n, _ := f[3].AsInt64()
		out = append(out, dto.Pending{ID: id, Deliveries: n})
	}
	return out, nil
}

func (s *store) Claim(ctx context.Context, idle time.Duration, ids []string) ([]dto.Message, error) {
	entries, err := s.client.Do(ctx, s.client.B().Xclaim().Key(s.cfg.Stream).Group(s.cfg.Group).Consumer(s.cfg.Consumer).
		MinIdleTime(strconv.FormatInt(idle.Milliseconds(), 10)).Id(ids...).Build()).AsXRange()
	if err != nil {
		return nil, fmt.Errorf("xclaim %s: %w", s.cfg.Stream, err)
	}
	return messages(entries), nil
}

func (s *store) Ack(ctx context.Context, ids []string) error {
	if err := s.client.Do(ctx, s.client.B().Xack().Key(s.cfg.Stream).Group(s.cfg.Group).Id(ids...).Build()).Error(); err != nil {
		return fmt.Errorf("xack %s: %w", s.cfg.Stream, err)
	}
	return nil
}

func (s *store) DeadLetter(ctx context.Context, m dto.Message, reason string) error {
	b := s.client.B()
	for _, r := range s.client.DoMulti(ctx,
		b.Xadd().Key(s.cfg.DLQStream).Id("*").FieldValue().
			FieldValue("source", m.ID).FieldValue("payload", m.Payload).FieldValue("error", reason).Build(),
		b.Xack().Key(s.cfg.Stream).Group(s.cfg.Group).Id(m.ID).Build(),
	) {
		if err := r.Error(); err != nil {
			return fmt.Errorf("dead-letter %s: %w", m.ID, err)
		}
	}
	return nil
}

func (s *store) Release(ctx context.Context, k qkeys.Event, a dto.ReleaseArgs) (dto.ReleaseStatus, error) {
	var deadline int64
	if !a.PaymentDeadline.IsZero() {
		deadline = a.PaymentDeadline.UnixMilli()
	}
	st, err := valkeyx.FCall(ctx, s.client, "rusuh_release_v1",
		[]string{k.Cfg, k.Unreserved, k.Resv, k.Held, k.Active, k.Leases, k.Done},
		a.ID, a.Type, a.SlotID, strconv.Itoa(a.Units), strconv.FormatInt(deadline, 10),
		strconv.FormatInt(a.HoldGrace.Milliseconds(), 10)).ToString()
	if err != nil {
		return "", fmt.Errorf("release %s %s: %w", k.ID, a.ID, err)
	}
	return dto.ReleaseStatus(st), nil
}

func messages(entries []vk.XRangeEntry) []dto.Message {
	out := make([]dto.Message, len(entries))
	for i, e := range entries {
		out[i] = dto.Message{ID: e.ID, Payload: e.FieldValues["payload"]}
	}
	return out
}
