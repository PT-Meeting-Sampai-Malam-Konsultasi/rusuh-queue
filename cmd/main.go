package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	obsconfig "github.com/goleggo/observer/config"
	obslog "github.com/goleggo/observer/log"
	obstrace "github.com/goleggo/observer/trace"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/config"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/delivery"
)

func main() {
	cfg := config.Load()

	slog.SetDefault(obslog.SetupLogger(obsconfig.LogConfig{Level: cfg.LogLevel, Format: cfg.LogFormat}))

	ctx := context.Background()
	tp, err := obstrace.SetupTrace(ctx, obsconfig.OTELConfig{
		ServiceName:  "rusuh-queue",
		ExporterType: cfg.OtelExporterType,
		Endpoint:     cfg.OtelEndpoint,
		Insecure:     true,
	})
	if err != nil {
		slog.Error("Failed to set up tracing", "error", err)
	} else if tp != nil { // OTEL_EXPORTER_TYPE=none gives no provider
		defer func() {
			if err := tp.Shutdown(ctx); err != nil {
				slog.Error("Failed to shut down tracer provider", "error", err)
			}
		}()
	}
	slog.Info("Starting rusuh-queue in " + cfg.Env + " mode...")

	app, err := delivery.New(ctx, cfg)
	if err != nil {
		slog.Error("Failed to start", "error", err)
		os.Exit(1)
	}

	srv := newServer(":"+cfg.Port, app.Handler)
	adminSrv := newServer(":"+cfg.AdminPort, app.AdminHandler)

	bgCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	bgDone := make(chan struct{})
	go func() { app.Run(bgCtx); close(bgDone) }()

	go serve(srv, "Server")
	go serve(adminSrv, "Admin server")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("Shutting down server...")

	// 1. Fail readiness first so the load balancer stops routing new requests here.
	app.Health.Drain()

	// 2. Stop accepting connections and wait for in-flight requests.
	ctxShutdown, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctxShutdown); err != nil {
		slog.Error("Server HTTP shutdown failed", "error", err)
	}
	if err := adminSrv.Shutdown(ctxShutdown); err != nil {
		slog.Error("Admin server HTTP shutdown failed", "error", err)
	}

	// 3. Stop the background routines (the controller resigns its leaderships).
	stopBackground()
	select {
	case <-bgDone:
		slog.Info("All background routines finished cleanly.")
	case <-time.After(3 * time.Second):
		slog.Warn("Shutdown timeout: forcing exit of background routines.")
	}

	// 4. Close Valkey and the database pool after the workers finished.
	app.Close()
	slog.Info("Server exiting")
}

// newServer has no WriteTimeout: the SSE route (ADR-0015) sets per-write deadlines itself.
func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

func serve(srv *http.Server, name string) {
	slog.Info(name + " listening on " + srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error(name+" failed to run", "error", err)
		os.Exit(1)
	}
}
