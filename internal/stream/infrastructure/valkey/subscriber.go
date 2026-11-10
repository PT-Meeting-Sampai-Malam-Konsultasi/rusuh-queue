// Package valkey listens to the controller's tick channel for the stream hub.
package valkey

import (
	"context"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/port/outbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

type subscriber struct{ client vk.Client }

func New(client vk.Client) outbound.ISubscriber { return subscriber{client: client} }

// Subscribe uses SSUBSCRIBE on q:{evt}:tick, which rusuh_tick_v1 publishes every tick.
func (s subscriber) Subscribe(ctx context.Context, eventID string, onMessage func(string)) {
	valkeyx.Subscribe(ctx, s.client, qkeys.For(eventID).Tick, onMessage)
}
