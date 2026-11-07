package valkeyx_test

import (
	"context"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

func TestConnectLoadsTheLibrary(t *testing.T) {
	ctx := context.Background()
	client, err := valkeyx.Connect(ctx, testkit.ValkeyAddr(t), "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := valkeyx.Ping(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := valkeyx.FCall(ctx, client, "rusuh_resign_v1", []string{"q:{evt_x}:leader"}, "task").Error(); err != nil {
		t.Fatalf("library function missing: %v", err)
	}
}

func TestConnectRefusesBadServers(t *testing.T) {
	ctx := context.Background()
	if _, err := valkeyx.Connect(ctx, "127.0.0.1:1", ""); err == nil {
		t.Fatal("unreachable address accepted")
	}

	client, err := valkeyx.Connect(ctx, testkit.ValkeyAddr(t), "")
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	if err := valkeyx.Ping(ctx, client); err == nil {
		t.Fatal("ping on a closed client")
	}
	if err := valkeyx.LoadFunctions(ctx, client); err == nil {
		t.Fatal("load on a closed client")
	}
}

func TestSubscribeResubscribesAfterADisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client, err := valkeyx.Connect(ctx, testkit.ValkeyAddr(t), "")
	if err != nil {
		t.Fatal(err)
	}
	starts := make(chan struct{}, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		valkeyx.Subscribe(ctx, client, "x:{evt}:tick", func(m string) {
			if m == "" {
				starts <- struct{}{}
			}
		})
	}()
	<-starts
	client.Close()
	select {
	case <-starts:
	case <-time.After(5 * time.Second):
		t.Fatal("no resubscribe after the connection closed")
	}
	cancel()
	<-done
}
