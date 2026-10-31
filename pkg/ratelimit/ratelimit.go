// Package ratelimit is a fixed-window limiter on Valkey (rusuh_ratelimit_v1).
// Keys follow the Solution Architecture: rl:{scope}:{id}.
package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

// Rule is one limit: at most Limit hits per Window.
type Rule struct {
	Scope  string
	Limit  int64
	Window time.Duration
}

// Result of one hit.
type Result struct {
	Allowed    bool
	Count      int64
	RetryAfter time.Duration
}

type Limiter struct {
	client valkey.Client
}

func New(client valkey.Client) *Limiter {
	return &Limiter{client: client}
}

// Hit counts one request for id under rule.
func (l *Limiter) Hit(ctx context.Context, rule Rule, id string) (Result, error) {
	key := "rl:" + rule.Scope + ":" + id
	vals, err := valkeyx.FCall(ctx, l.client, "rusuh_ratelimit_v1", []string{key},
		strconv.FormatInt(rule.Window.Milliseconds(), 10)).AsIntSlice()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit %s: %w", rule.Scope, err)
	}
	if len(vals) != 2 {
		return Result{}, fmt.Errorf("ratelimit %s: unexpected reply %v", rule.Scope, vals)
	}
	res := Result{Count: vals[0], Allowed: vals[0] <= rule.Limit}
	if !res.Allowed {
		res.RetryAfter = time.Duration(vals[1]) * time.Millisecond
	}
	return res, nil
}
