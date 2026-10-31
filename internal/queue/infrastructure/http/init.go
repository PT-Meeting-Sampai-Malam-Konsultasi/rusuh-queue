package http

import (
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
)

type QueueHandler struct {
	service inbound.IQueueUsecase
	auth    *auth.Verifier

	group    singleflight.Group
	mu       sync.RWMutex
	frontier map[string]cachedFrontier
	now      func() time.Time
}

func NewQueue(s inbound.IQueueUsecase, a *auth.Verifier) *QueueHandler {
	return &QueueHandler{service: s, auth: a, frontier: map[string]cachedFrontier{}, now: time.Now}
}
