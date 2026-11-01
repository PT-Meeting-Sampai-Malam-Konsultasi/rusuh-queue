package usecase

import (
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/consumer/port/outbound"
)

type ConsumerUsecase struct {
	cfg     entity.Config
	store   outbound.IConsumerStore
	dlq     outbound.IDeadLetterLog
	metrics metrics
}

func NewConsumer(cfg entity.Config, store outbound.IConsumerStore, dlq outbound.IDeadLetterLog) inbound.IConsumerUsecase {
	return &ConsumerUsecase{cfg: cfg, store: store, dlq: dlq, metrics: newMetrics()}
}
