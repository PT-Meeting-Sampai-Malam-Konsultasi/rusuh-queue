package valkey_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

func TestLiveAndApply(t *testing.T) {
	client := testkit.Valkey(t)
	q := valkey.New(client)
	ctx := context.Background()

	if live, err := q.Live(ctx, "evt_cfg"); err != nil || live {
		t.Fatalf("live before bootstrap = %v, %v", live, err)
	}
	if f, err := q.Apply(ctx, entity.DefaultSettings("evt_cfg")); err != nil || f != nil {
		t.Fatalf("apply on a missing event = %s, %v", f, err)
	}

	testkit.Event(t, client, "evt_cfg", testkit.EventOpts{})
	if live, err := q.Live(ctx, "evt_cfg"); err != nil || !live {
		t.Fatalf("live = %v, %v", live, err)
	}
	s := entity.DefaultSettings("evt_cfg")
	s.State = "paused"
	f, err := q.Apply(ctx, s)
	var frontier struct{ State string }
	if err != nil || json.Unmarshal(f, &frontier) != nil || frontier.State != "paused" {
		t.Fatalf("frontier = %s, %v", f, err)
	}

	closed, err := valkeyx.Connect(ctx, testkit.ValkeyAddr(t), "")
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	if _, err := valkey.New(closed).Live(ctx, "evt_cfg"); err == nil {
		t.Error("Live on a closed client")
	}
	if _, err := valkey.New(closed).Apply(ctx, s); err == nil {
		t.Error("Apply on a closed client")
	}
}
