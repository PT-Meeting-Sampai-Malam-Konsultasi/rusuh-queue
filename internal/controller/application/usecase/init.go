package usecase

import (
	"context"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/port/outbound"
)

type ControllerUsecase struct {
	cfg         entity.Config
	store       outbound.IControllerStore
	checkpoints outbound.ICheckpoints
	settings    outbound.ISettings
	now         func() time.Time

	// subs holds one cancel per event whose core:{evt}:changed channel we
	// follow. Owned by Run's discovery goroutine.
	subs map[string]context.CancelFunc

	// Owned by the tick goroutine: the fence of every event this task leads,
	// and when each was last checkpointed.
	leading      map[string]int64
	checkpointed map[string]time.Time
	states       map[string]string
	reconciled   map[string]time.Time
	// nudged carries core:{evt}:changed nudges from the subscriptions to the tick goroutine.
	nudged  chan string
	metrics metrics
}

// New builds the concrete Service implementation for controller.
func NewController(cfg entity.Config, store outbound.IControllerStore, checkpoints outbound.ICheckpoints, settings outbound.ISettings) inbound.IControllerUsecase {
	return &ControllerUsecase{
		cfg: cfg, store: store, checkpoints: checkpoints, settings: settings, now: time.Now,
		subs:         map[string]context.CancelFunc{},
		leading:      map[string]int64{},
		checkpointed: map[string]time.Time{},
		states:       map[string]string{},
		reconciled:   map[string]time.Time{},
		nudged:       make(chan string, 256),
		metrics:      newMetrics(),
	}
}
