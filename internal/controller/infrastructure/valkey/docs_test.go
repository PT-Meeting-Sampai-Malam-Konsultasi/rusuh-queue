package valkey_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	admin "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/coredocs"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

func TestCoreDocsBootstrapAndLiveSet(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := qkeys.For("evt_docs")

	if sale, stock, err := s.CoreDocs(ctx, k); err != nil || sale != nil || stock != nil {
		t.Fatalf("missing docs: %s %s %v", sale, stock, err)
	}
	if b, err := s.CoreStock(ctx, k); err != nil || b != nil {
		t.Fatalf("missing stock: %s %v", b, err)
	}
	if _, _, ok, err := s.Counters(ctx, k); err != nil || ok {
		t.Fatalf("counters of a missing event: ok=%v err=%v", ok, err)
	}

	now := time.Now().Truncate(time.Second)
	testkit.CoreSale(t, client,
		coredocs.Sale{V: 1, EventID: k.ID, Status: "published", SaleOpensAt: now, EndsAt: now.Add(time.Hour)},
		coredocs.Stock{V: 1, EventID: k.ID, Available: 9, Version: 1, TS: now})
	if ids, err := s.CoreEvents(ctx); err != nil || len(ids) != 1 || ids[0] != k.ID {
		t.Fatalf("core events = %v, %v", ids, err)
	}
	sale, stock, err := s.CoreDocs(ctx, k)
	if err != nil || sale == nil || stock == nil {
		t.Fatalf("docs: %s %s %v", sale, stock, err)
	}
	if b, err := s.CoreStock(ctx, k); err != nil || string(b) != string(stock) {
		t.Fatalf("stock: %s %v", b, err)
	}

	frontier, _ := json.Marshal(map[string]any{"event": k.ID, "state": "open"})
	status, state, err := s.Bootstrap(ctx, k, dto.BootstrapArgs{
		Now: now, OpensAt: now.Add(-time.Minute), EndsAt: now.Add(time.Hour), ExpireAt: now.Add(25 * time.Hour),
		Cancelled: true, Available: 9, Settings: admin.DefaultSettings(k.ID), InitialFrontier: frontier,
	})
	if err != nil || status != "CREATED" || state == "" {
		t.Fatalf("bootstrap = %q %q %v", status, state, err)
	}
	if seq, served, ok, err := s.Counters(ctx, k); err != nil || !ok || seq != 0 || served != 0 {
		t.Fatalf("counters = %d %d %v %v", seq, served, ok, err)
	}

	if err := s.AddLive(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if ids, err := s.LiveEvents(ctx); err != nil || len(ids) != 1 {
		t.Fatalf("live = %v, %v", ids, err)
	}
	if err := s.RemoveLive(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if ids, _ := s.LiveEvents(ctx); len(ids) != 0 {
		t.Fatalf("live after remove = %v", ids)
	}
}

func TestSubscribeDeliversNudges(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx, cancel := context.WithCancel(t.Context())
	got := make(chan string, 4)
	done := make(chan struct{})
	go func() { defer close(done); s.Subscribe(ctx, "core:{evt_sub}:changed", func(m string) { got <- m }) }()
	if m := <-got; m != "" {
		t.Fatalf("first call = %q, want the resubscribe signal", m)
	}
	deadline := time.After(5 * time.Second)
	for {
		testkit.Publish(t, client, "core:{evt_sub}:changed")
		select {
		case m := <-got:
			if m != "1" {
				t.Fatalf("message = %q", m)
			}
			cancel()
			<-done
			return
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no message")
		}
	}
}

func TestStoreReportsValkeyErrors(t *testing.T) {
	testkit.Valkey(t)
	client, err := valkeyx.Connect(context.Background(), testkit.ValkeyAddr(t), "")
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	s := valkey.New(client)
	ctx := context.Background()
	k := qkeys.For("evt_err")

	if _, err := s.CoreEvents(ctx); err == nil {
		t.Error("CoreEvents")
	}
	if _, _, err := s.CoreDocs(ctx, k); err == nil {
		t.Error("CoreDocs")
	}
	if _, _, err := s.Bootstrap(ctx, k, dto.BootstrapArgs{}); err == nil {
		t.Error("Bootstrap")
	}
	if _, _, _, err := s.Counters(ctx, k); err == nil {
		t.Error("Counters")
	}
	if _, err := s.Lead(ctx, k, "t", time.Second); err == nil {
		t.Error("Lead")
	}
	if err := s.Resign(ctx, k, "t"); err == nil {
		t.Error("Resign")
	}
	if _, err := s.Purge(ctx, k, 1, time.Second, 1); err == nil {
		t.Error("Purge")
	}
	if _, err := s.Tick(ctx, k, dto.TickArgs{}); err == nil {
		t.Error("Tick")
	}
	if _, err := s.Lottery(ctx, k, 1, 1); err == nil {
		t.Error("Lottery")
	}
	if _, err := s.CoreStock(ctx, k); err == nil {
		t.Error("CoreStock")
	}
	if _, err := s.Reconcile(ctx, k, 1, 1); err == nil {
		t.Error("Reconcile")
	}
}
