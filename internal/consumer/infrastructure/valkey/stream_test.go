package valkey_test

import (
	"context"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

func streamConfig(consumer string) entity.Config {
	cfg := entity.DefaultConfig()
	cfg.Consumer = consumer
	cfg.Block = 10 * time.Millisecond
	return cfg
}

func TestStreamReadPendingClaimAckAndDeadLetter(t *testing.T) {
	client := testkit.Valkey(t)
	ctx := context.Background()
	a := valkey.New(client, streamConfig("task-a"))
	b := valkey.New(client, streamConfig("task-b"))

	if err := a.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.EnsureGroup(ctx); err != nil {
		t.Fatalf("second create must tolerate BUSYGROUP: %v", err)
	}
	if msgs, err := a.Read(ctx); err != nil || len(msgs) != 0 {
		t.Fatalf("empty read = %v, %v", msgs, err)
	}

	first := testkit.PublishRaw(t, client, "booking.paid", `{"n":1}`)
	second := testkit.PublishRaw(t, client, "booking.paid", `{"n":2}`)
	msgs, err := a.Read(ctx)
	if err != nil || len(msgs) != 2 || msgs[0].ID != first || msgs[0].Payload != `{"n":1}` {
		t.Fatalf("read = %+v, %v", msgs, err)
	}

	pending, err := b.Pending(ctx, 0, 10)
	if err != nil || len(pending) != 2 || pending[0].ID != first || pending[0].Deliveries != 1 {
		t.Fatalf("pending = %+v, %v", pending, err)
	}
	claimed, err := b.Claim(ctx, 0, []string{first})
	if err != nil || len(claimed) != 1 || claimed[0].Payload != `{"n":1}` {
		t.Fatalf("claimed = %+v, %v", claimed, err)
	}
	if err := b.Ack(ctx, []string{first}); err != nil {
		t.Fatal(err)
	}
	if err := a.DeadLetter(ctx, dto.Message{ID: second, Payload: `{"n":2}`}, "poison"); err != nil {
		t.Fatal(err)
	}
	if pending, _ := a.Pending(ctx, 0, 10); len(pending) != 0 {
		t.Fatalf("still pending: %+v", pending)
	}
	n, err := client.Do(ctx, client.B().Xlen().Key("stream:booking:dlq").Build()).AsInt64()
	if err != nil || n != 1 {
		t.Fatalf("dlq length = %d, %v", n, err)
	}
}

func TestStreamStoreReportsValkeyErrors(t *testing.T) {
	client, err := valkeyx.Connect(context.Background(), testkit.ValkeyAddr(t), "")
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	s := valkey.New(client, streamConfig("task-a"))
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err == nil {
		t.Error("EnsureGroup")
	}
	if _, err := s.Read(ctx); err == nil {
		t.Error("Read")
	}
	if _, err := s.Pending(ctx, 0, 1); err == nil {
		t.Error("Pending")
	}
	if _, err := s.Claim(ctx, 0, []string{"1-0"}); err == nil {
		t.Error("Claim")
	}
	if err := s.Ack(ctx, []string{"1-0"}); err == nil {
		t.Error("Ack")
	}
	if err := s.DeadLetter(ctx, dto.Message{ID: "1-0"}, "x"); err == nil {
		t.Error("DeadLetter")
	}
	if _, err := s.Release(ctx, qkeys.For("evt1"), dto.ReleaseArgs{}); err == nil {
		t.Error("Release")
	}
}
