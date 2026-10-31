package inbound

import (
	"context"
)

//go:generate mockgen -source=controller.go -destination=../../../../_mock/controller/port/inbound/controller_mock.go -package=mock_inbound

// IControllerUsecase is the inbound (driving) port of the controller domain:
// what it offers to its HTTP handlers and to the composition root.
type IControllerUsecase interface {
	// Run discovers and sets up events, keeps them in sync with core and,
	// for every event this task leads, ticks each second (purge, admission,
	// frontier) and writes checkpoints until ctx ends. On return it gives up
	// its leaderships so another task takes over within a tick.
	Run(ctx context.Context) error
	// Sync sets up or refreshes one event from core's documents now.
	Sync(ctx context.Context, eventID string) error
}
