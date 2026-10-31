package ratelimit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/ratelimit"
)

func TestHitAgainstValkey(t *testing.T) {
	client := testkit.Valkey(t)
	l := ratelimit.New(client)
	ctx := context.Background()
	rule := ratelimit.Rule{Scope: "test", Limit: 10, Window: time.Minute}

	for i := 1; i <= 10; i++ {
		res, err := l.Hit(ctx, rule, "203.0.113.7")
		if err != nil || !res.Allowed || res.Count != int64(i) {
			t.Fatalf("hit %d = %+v, err %v", i, res, err)
		}
	}
	res, err := l.Hit(ctx, rule, "203.0.113.7")
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed || res.RetryAfter <= 0 || res.RetryAfter > time.Minute {
		t.Fatalf("11th hit = %+v, want blocked with 0 < RetryAfter <= 1m", res)
	}

	other, err := l.Hit(ctx, rule, "203.0.113.8")
	if err != nil || !other.Allowed {
		t.Fatalf("another id must have its own window: %+v, %v", other, err)
	}
}

func TestWindowResets(t *testing.T) {
	client := testkit.Valkey(t)
	l := ratelimit.New(client)
	ctx := context.Background()
	rule := ratelimit.Rule{Scope: "short", Limit: 1, Window: 200 * time.Millisecond}

	if res, _ := l.Hit(ctx, rule, "x"); !res.Allowed {
		t.Fatal("first hit blocked")
	}
	if res, _ := l.Hit(ctx, rule, "x"); res.Allowed {
		t.Fatal("second hit allowed")
	}
	time.Sleep(300 * time.Millisecond)
	if res, _ := l.Hit(ctx, rule, "x"); !res.Allowed {
		t.Fatal("hit after the window was blocked")
	}
}

func TestRepairsKeyWithoutExpiry(t *testing.T) {
	client := testkit.Valkey(t)
	ctx := context.Background()
	// A counter left without TTL (e.g. written by hand) must not block forever.
	if err := client.Do(ctx, client.B().Set().Key("rl:stuck:x").Value("5").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	res, err := ratelimit.New(client).Hit(ctx, ratelimit.Rule{Scope: "stuck", Limit: 100, Window: time.Second}, "x")
	if err != nil || !res.Allowed {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
	ttl, err := client.Do(ctx, client.B().Pttl().Key("rl:stuck:x").Build()).AsInt64()
	if err != nil || ttl <= 0 {
		t.Fatalf("ttl = %d, err = %v; want the expiry repaired", ttl, err)
	}
}

func TestConcurrentHitsCountExactly(t *testing.T) {
	client := testkit.Valkey(t)
	l := ratelimit.New(client)
	rule := ratelimit.Rule{Scope: "race", Limit: 50, Window: time.Minute}

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for range 200 {
		wg.Go(func() {
			res, err := l.Hit(context.Background(), rule, "same")
			if err != nil {
				t.Error(err)
				return
			}
			if res.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if allowed != 50 {
		t.Fatalf("allowed = %d of 200, want exactly 50", allowed)
	}
}
