package database_test

import (
	"testing"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/database"
)

func TestConnect(t *testing.T) {
	db, err := database.Connect("pgx", testkit.PostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := database.Connect("nope", "x"); err == nil {
		t.Fatal("unknown driver accepted")
	}
	if _, err := database.Connect("pgx", "postgres://nobody@127.0.0.1:1/x?sslmode=disable&connect_timeout=1"); err == nil {
		t.Fatal("unreachable database accepted")
	}
}
