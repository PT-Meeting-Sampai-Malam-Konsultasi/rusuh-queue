package usecase

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type metrics struct {
	processed metric.Int64Counter
	dlq       metric.Int64Counter
}

func newMetrics() metrics {
	m := otel.Meter("github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/consumer")
	processed, _ := m.Int64Counter("consumer_events_total")
	dlq, _ := m.Int64Counter("dlq_entries_total")
	return metrics{processed: processed, dlq: dlq}
}
