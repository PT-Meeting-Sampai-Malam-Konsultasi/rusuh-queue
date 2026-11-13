package usecase

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestVerificationEnforcedGauge(t *testing.T) {
	for _, tc := range []struct {
		skip bool
		want int64
	}{{false, 1}, {true, 0}} {
		reader := sdkmetric.NewManualReader()
		prev := otel.GetMeterProvider()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
		t.Cleanup(func() { otel.SetMeterProvider(prev) })

		cfg := testCfg
		cfg.SkipVerification = tc.skip
		NewDevice(cfg, nil, nil, nil, nil)

		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatal(err)
		}
		got := int64(-1)
		for _, sm := range rm.ScopeMetrics {
			for _, mt := range sm.Metrics {
				if g, ok := mt.Data.(metricdata.Gauge[int64]); ok && mt.Name == "device_verification_enforced" && len(g.DataPoints) == 1 {
					got = g.DataPoints[0].Value
				}
			}
		}
		if got != tc.want {
			t.Errorf("skip=%v: device_verification_enforced = %d, want %d", tc.skip, got, tc.want)
		}
	}
}
