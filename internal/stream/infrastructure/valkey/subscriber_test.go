package valkey_test

import (
	"context"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
)

func TestSubscribeReceivesTicks(t *testing.T) {
	client := testkit.Valkey(t)
	ctx, cancel := context.WithCancel(t.Context())
	got := make(chan string, 8)
	done := make(chan struct{})
	go func() { defer close(done); valkey.New(client).Subscribe(ctx, "evt_tick", func(m string) { got <- m }) }()
	<-got
	deadline := time.After(5 * time.Second)
	for {
		testkit.Publish(t, client, qkeys.For("evt_tick").Tick)
		select {
		case <-got:
			cancel()
			<-done
			return
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no tick received")
		}
	}
}
