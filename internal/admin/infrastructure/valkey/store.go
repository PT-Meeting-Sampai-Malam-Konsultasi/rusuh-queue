package valkey

import (
	"context"
	"fmt"
	"strconv"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

type queueConfig struct {
	client vk.Client
}

func New(client vk.Client) outbound.IQueueConfig {
	return &queueConfig{client: client}
}

func (q *queueConfig) Live(ctx context.Context, eventID string) (bool, error) {
	n, err := q.client.Do(ctx, q.client.B().Exists().Key(qkeys.For(eventID).Cfg).Build()).AsInt64()
	if err != nil {
		return false, fmt.Errorf("live %s: %w", eventID, err)
	}
	return n == 1, nil
}

func (q *queueConfig) Apply(ctx context.Context, s entity.Settings) ([]byte, error) {
	k := qkeys.For(s.EventID)
	b, err := valkeyx.FCall(ctx, q.client, "rusuh_configure_v1", []string{k.Cfg, k.Frontier, k.Pre},
		s.State, strconv.FormatInt(s.MaxSlots, 10), strconv.FormatInt(s.RateCap, 10), strconv.FormatInt(s.StreamFrom, 10)).AsBytes()
	if vk.IsValkeyNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("configure %s: %w", s.EventID, err)
	}
	return b, nil
}
