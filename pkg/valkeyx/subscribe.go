package valkeyx

import (
	"context"
	"log/slog"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Subscribe listens to one sharded pub/sub channel (SSUBSCRIBE) until ctx ends,
// resubscribing with backoff after any disconnect. onEvent runs for every
// message and once each time the subscription (re)starts, because messages
// sent while disconnected are lost: callers re-read the keys the channel
// announces (the "key + nudge" rule of ADR-0016).
func Subscribe(ctx context.Context, client valkey.Client, channel string, onEvent func(message string)) {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		onEvent("")
		start := time.Now()
		err := client.Receive(ctx, client.B().Ssubscribe().Channel(channel).Build(), func(m valkey.PubSubMessage) {
			onEvent(m.Message)
		})
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > 10*time.Second {
			backoff = 100 * time.Millisecond
		}
		slog.WarnContext(ctx, "pub/sub disconnected, resubscribing", "channel", channel, "error", err, "in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}
