// Package delivery is the composition root: it builds the shared clients from
// config and wires every domain's adapters → usecase → handler.
package delivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	vk "github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/clientip"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/database"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/health"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/httpmw"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

// Server holds the wired HTTP handler, the background routines and the
// clients to close on shutdown.
type Server struct {
	Handler http.Handler
	// AdminHandler serves the operator API; it must only listen on a private network.
	AdminHandler http.Handler
	Health       *health.Handler

	db         *sql.DB
	valkey     vk.Client
	background []func(ctx context.Context) error
}

// New connects PostgreSQL and Valkey, checks the settings that must be right
// before taking traffic, and wires every domain.
func New(ctx context.Context, cfg *config.Config) (*Server, error) {
	db, err := database.Connect("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	valkey, err := valkeyx.Connect(ctx, cfg.ValkeyAddr, cfg.ValkeyPassword)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect valkey: %w", err)
	}
	s := &Server{db: db, valkey: valkey}
	if err := s.wire(cfg); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Server) wire(cfg *config.Config) error {
	ipResolver, err := clientip.New(cfg.ClientIPHeader, cfg.TrustedProxyCIDRs)
	if err != nil {
		return fmt.Errorf("client IP settings: %w", err)
	}

	mux := http.NewServeMux()
	s.Health = health.New(map[string]health.Check{
		"postgres": s.db.PingContext,
		"valkey":   func(ctx context.Context) error { return valkeyx.Ping(ctx, s.valkey) },
	})
	s.Health.RegisterRoutes(mux)

	adminMux := http.NewServeMux()
	if err := s.wireDomains(cfg, mux, adminMux, ipResolver); err != nil {
		return err
	}
	s.Handler = httpmw.CORS(cfg.CORSOrigins, mux)
	s.AdminHandler = adminMux
	return nil
}

// Run starts the background routines (the admission controller) and blocks
// until ctx ends and every routine has returned.
func (s *Server) Run(ctx context.Context) {
	done := make(chan struct{}, len(s.background))
	for _, run := range s.background {
		go func() {
			if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("Background routine stopped", "error", err)
			}
			done <- struct{}{}
		}()
	}
	for range s.background {
		<-done
	}
}

// Close closes the Valkey and database clients; call it after Run returned.
func (s *Server) Close() {
	s.valkey.Close()
	if err := s.db.Close(); err != nil {
		slog.Error("Database connection pool close failed", "error", err)
	}
}
