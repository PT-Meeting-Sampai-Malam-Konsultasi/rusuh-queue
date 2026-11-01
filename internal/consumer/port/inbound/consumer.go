package inbound

import "context"

//go:generate mockgen -source=consumer.go -destination=../../../../_mock/consumer/port/inbound/consumer_mock.go -package=mock_inbound

// IConsumerUsecase reads stream:booking and applies each event to the queue's counters.
type IConsumerUsecase interface {
	// Run blocks until ctx ends.
	Run(ctx context.Context) error
}
