package usecase

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

func registerVerificationGauge(skip bool) {
	enforced := int64(1)
	if skip {
		enforced = 0
	}
	m := otel.Meter("github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/device")
	_, _ = m.Int64ObservableGauge("device_verification_enforced",
		metric.WithDescription("1 when device registration checks Turnstile and attestation, 0 when DEVICE_VERIFICATION=off"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(enforced)
			return nil
		}))
}
