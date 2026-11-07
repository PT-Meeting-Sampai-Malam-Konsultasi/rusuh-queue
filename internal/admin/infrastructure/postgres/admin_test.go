package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/admin/infrastructure/postgres"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

func TestSettingsRoundTrip(t *testing.T) {
	db := testkit.Postgres(t)
	repo := postgres.New(db)
	ctx := context.Background()

	if s, err := repo.GetSettings(ctx, "evt_adm"); err != nil || s != nil {
		t.Fatalf("unsaved settings = %+v, %v", s, err)
	}
	want := entity.Settings{EventID: "evt_adm", MaxSlots: 10, RateCap: 3, StreamFrom: 0, State: "paused"}
	if err := repo.SaveSettings(ctx, want, "op_1", "incident"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveSettings(ctx, want, "op_1", ""); err != nil {
		t.Fatal(err)
	}
	if s, err := repo.GetSettings(ctx, "evt_adm"); err != nil || *s != want {
		t.Fatalf("settings = %+v, %v", s, err)
	}

	tooBig := want
	tooBig.MaxSlots = 1 << 31
	if err := repo.SaveSettings(ctx, tooBig, "op_1", ""); !errors.Is(err, entity.ErrInvalidSettings) {
		t.Fatalf("out-of-range settings: %v", err)
	}

	_ = db.Close()
	if _, err := repo.GetSettings(ctx, "evt_adm"); err == nil {
		t.Fatal("closed database read")
	}
	if err := repo.SaveSettings(ctx, want, "op_1", ""); err == nil {
		t.Fatal("closed database write")
	}
}
