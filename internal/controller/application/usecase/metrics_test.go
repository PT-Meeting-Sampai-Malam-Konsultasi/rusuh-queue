package usecase

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/controller/domain/dto"
)

func TestRecordQueueGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m := newMetrics()
	ctx := context.Background()
	m.recordQueue(ctx, "evt_m", dto.TickResult{
		Status: dto.TickOK, State: "paused", Seq: 120, Served: 100, Pending: 4,
		Frontier: []byte(`{"sellable":9,"reserved":6,"held":3,"ratePerMin":50,"ts":1794652800}`),
	})

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if g, ok := mt.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) == 1 {
				if v, _ := g.DataPoints[0].Attributes.Value("event"); v.AsString() == "evt_m" {
					got[mt.Name] = g.DataPoints[0].Value
				}
			}
		}
	}
	want := map[string]int64{
		"queue_length": 20, "queue_served": 100, "queue_invited_pending": 4, "admit_rate_per_min": 50,
		"units_unreserved": 9, "units_reserved": 6, "units_held": 3, "queue_state": 2, "frontier_ts_seconds": 1794652800,
	}
	for name, v := range want {
		if got[name] != v {
			t.Errorf("%s = %d, want %d", name, got[name], v)
		}
	}
}

func TestTickHistogramsUseSecondScaleBuckets(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m := newMetrics()
	ctx := context.Background()
	m.duration.Record(ctx, 0.003)
	m.lag.Record(ctx, 0.6)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"tick_duration_seconds": 0.005, "tick_lag_seconds": 1}
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			upper, ok := want[mt.Name]
			if !ok {
				continue
			}
			delete(want, mt.Name)
			h, ok := mt.Data.(metricdata.Histogram[float64])
			if !ok || len(h.DataPoints) != 1 {
				t.Fatalf("%s: want one histogram point, got %T", mt.Name, mt.Data)
			}
			dp := h.DataPoints[0]
			got := -1.0
			for i, c := range dp.BucketCounts {
				if c == 1 && i < len(dp.Bounds) {
					got = dp.Bounds[i]
				}
			}
			if got != upper {
				t.Errorf("%s: observation landed in bucket with upper bound %v, want %v (bounds %v)", mt.Name, got, upper, dp.Bounds)
			}
		}
	}
	for name := range want {
		t.Errorf("%s not collected", name)
	}
}
