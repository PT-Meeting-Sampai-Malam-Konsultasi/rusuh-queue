package usecase

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/port/outbound"
)

type QueueUsecase struct {
	cfg     entity.Config
	store   outbound.IQueueStore
	signer  outbound.ISigner
	limiter outbound.IRateLimiter
}

// New builds the concrete Service implementation for queue.
func NewQueue(cfg entity.Config, store outbound.IQueueStore, signer outbound.ISigner, limiter outbound.IRateLimiter) inbound.IQueueUsecase {
	return &QueueUsecase{cfg: cfg, store: store, signer: signer, limiter: limiter}
}
