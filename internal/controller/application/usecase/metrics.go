package usecase

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
)

// metrics are the controller's NFR-09 instruments on the global meter provider
// (cmd/main.go sets it up; a no-op when metrics are off).
type metrics struct {
	duration      metric.Float64Histogram
	lag           metric.Float64Histogram
	skipped       metric.Int64Counter
	leaderChanges metric.Int64Counter
	purged        metric.Int64Counter
	drawn         metric.Int64Counter
	drift         metric.Int64UpDownCounter
	snapshotAge   metric.Float64Gauge

	length     metric.Int64Gauge
	served     metric.Int64Gauge
	pending    metric.Int64Gauge
	rate       metric.Int64Gauge
	unreserved metric.Int64Gauge
	reserved   metric.Int64Gauge
	held       metric.Int64Gauge
	state      metric.Int64Gauge
	ts         metric.Int64Gauge

	// one attribute set per event, so recording allocates nothing per tick
	attrs map[string]metric.MeasurementOption
}

func newMetrics() metrics {
	m := otel.Meter("github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/controller")
	// The API returns a usable no-op instrument alongside any error.
	duration, _ := m.Float64Histogram("tick_duration_seconds", metric.WithUnit("s"))
	lag, _ := m.Float64Histogram("tick_lag_seconds", metric.WithUnit("s"))
	skipped, _ := m.Int64Counter("ticks_skipped_total")
	leaderChanges, _ := m.Int64Counter("leader_changes_total")
	purged, _ := m.Int64Counter("purged_total")
	drawn, _ := m.Int64Counter("lottery_drawn_total")
	drift, _ := m.Int64UpDownCounter("reconcile_drift_units")
	snapshotAge, _ := m.Float64Gauge("stock_snapshot_age_seconds", metric.WithUnit("s"))
	gauge := func(name, desc string) metric.Int64Gauge {
		g, _ := m.Int64Gauge(name, metric.WithDescription(desc))
		return g
	}
	return metrics{
		duration: duration, lag: lag, skipped: skipped, leaderChanges: leaderChanges, purged: purged,
		drawn: drawn, drift: drift, snapshotAge: snapshotAge,
		length:     gauge("queue_length", "seq - served: places still waiting"),
		served:     gauge("queue_served", "highest admitted seq"),
		pending:    gauge("queue_invited_pending", "invited in the miss window, not claimed yet (D-7)"),
		rate:       gauge("admit_rate_per_min", "the frontier's ratePerMin"),
		unreserved: gauge("units_unreserved", "units free to reserve"),
		reserved:   gauge("units_reserved", "units reserved by unheld leases"),
		held:       gauge("units_held", "units held by core"),
		state:      gauge("queue_state", "0 preopen, 1 open, 2 paused, 3 closed"),
		ts:         gauge("frontier_ts_seconds", "Valkey time of the last frontier; alert when it stops moving"),
		attrs:      map[string]metric.MeasurementOption{},
	}
}

var stateCodes = map[string]int64{"preopen": 0, "open": 1, "paused": 2, "closed": 3}

// recordQueue publishes one event's gauges from the tick's frontier. Only the
// leader records, so each event has one series writer.
func (m *metrics) recordQueue(ctx context.Context, id string, r dto.TickResult) {
	var f struct {
		Sellable   int64 `json:"sellable"`
		Reserved   int64 `json:"reserved"`
		Held       int64 `json:"held"`
		RatePerMin int64 `json:"ratePerMin"`
		Ts         int64 `json:"ts"`
	}
	if err := json.Unmarshal(r.Frontier, &f); err != nil {
		return
	}
	o, ok := m.attrs[id]
	if !ok {
		o = metric.WithAttributeSet(attribute.NewSet(attribute.String("event", id)))
		m.attrs[id] = o
	}
	m.length.Record(ctx, r.Seq-r.Served, o)
	m.served.Record(ctx, r.Served, o)
	m.pending.Record(ctx, r.Pending, o)
	m.rate.Record(ctx, f.RatePerMin, o)
	m.unreserved.Record(ctx, f.Sellable, o)
	m.reserved.Record(ctx, f.Reserved, o)
	m.held.Record(ctx, f.Held, o)
	m.state.Record(ctx, stateCodes[r.State], o)
	m.ts.Record(ctx, f.Ts, o)
}
