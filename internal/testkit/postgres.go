package testkit

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	pgOnce sync.Once
	pgDSN  string
	pgErr  error
)

// Postgres returns a pool to the shared PostgreSQL 16 with the Up part of every
// goose migration in db/migrations applied and the queue tables emptied.
func Postgres(t *testing.T) *sql.DB {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgOnce.Do(func() {
		var endpoint string
		endpoint, pgErr = startReachable(ctx, testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			Env:          map[string]string{"POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "rusuh"},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(90 * time.Second),
		}, "5432/tcp", func(addr string) error {
			db, err := sql.Open("pgx", "postgres://postgres:postgres@"+addr+"/rusuh?sslmode=disable")
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()
			return db.PingContext(ctx)
		})
		if pgErr != nil {
			return
		}
		pgDSN = "postgres://postgres:postgres@" + endpoint + "/rusuh?sslmode=disable"
		pgErr = migrate(ctx, pgDSN)
	})
	if pgErr != nil {
		t.Fatalf("start postgres: %v", pgErr)
	}

	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `TRUNCATE queue.event_queue_config, queue.checkpoint, queue.consumer_dlq_log`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return db
}

func migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	files, err := migrationFiles()
	if err != nil {
		return err
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		up, _, _ := strings.Cut(string(b), "-- +goose Down")
		if _, err := db.ExecContext(ctx, up); err != nil {
			return fmt.Errorf("migration %s: %w", filepath.Base(f), err)
		}
	}
	return nil
}

func migrationFiles() ([]string, error) {
	_, here, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(here), "..", "..", "db", "migrations")
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no migrations in %s: %v", dir, err)
	}
	sort.Strings(files)
	return files, nil
}

// PostgresDSN is the shared PostgreSQL's URL, migrated and emptied like Postgres.
func PostgresDSN(t *testing.T) string {
	t.Helper()
	Postgres(t)
	return pgDSN
}
