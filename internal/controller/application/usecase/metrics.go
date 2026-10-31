package usecase

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// metrics are the controller's NFR-09 instruments. They record into the global
// meter provider, a no-op until metrics export is wired (M6).
type metrics struct {
	duration      metric.Float64Histogram
	lag           metric.Float64Histogram
	skipped       metric.Int64Counter
	leaderChanges metric.Int64Counter
	purged        metric.Int64Counter
}

func newMetrics() metrics {
	m := otel.Meter("github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/controller")
	// The API returns a usable no-op instrument alongside any error.
	duration, _ := m.Float64Histogram("tick_duration_seconds", metric.WithUnit("s"))
	lag, _ := m.Float64Histogram("tick_lag_seconds", metric.WithUnit("s"))
	skipped, _ := m.Int64Counter("ticks_skipped_total")
	leaderChanges, _ := m.Int64Counter("leader_changes_total")
	purged, _ := m.Int64Counter("purged_total")
	return metrics{duration: duration, lag: lag, skipped: skipped, leaderChanges: leaderChanges, purged: purged}
}
