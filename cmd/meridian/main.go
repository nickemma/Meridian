package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	config "github.com/nickemma/meridian/internal/config/adapters"
	metricsadapters "github.com/nickemma/meridian/internal/metrics/adapters"
	metrics "github.com/nickemma/meridian/internal/metrics/application"
	observability "github.com/nickemma/meridian/internal/observability/adapters"
	server "github.com/nickemma/meridian/internal/server/adapters"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	// Load config from environment variables. A missing or malformed
	// required variable stops the node with a clear error.
	cfg, err := config.LoadFromEnvironment()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	log.Printf("[main] starting Meridian node %s (raft=%d client=%d peers=%d)",
		cfg.NodeID, cfg.RaftPort, cfg.ClientPort, len(cfg.Peers))

	// Create a context that cancels on SIGTERM or SIGINT.
	// This is how the node shuts down cleanly when Docker stops it.
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Metrics are served on ClientPort+1000, e.g. client 8080 → metrics 9080.
	m := metricsadapters.New(cfg.NodeID)
	metricsSrv := metricsadapters.NewServer(cfg.MetricsPort())
	go func() {
		if err := metricsSrv.Start(ctx); err != nil {
			log.Printf("[main] metrics server error: %v", err)
		}
	}()

	// Start the gRPC server. This blocks until ctx is cancelled.
	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}
	srv.RaftNode().SetMetrics(metrics.NewRaftRecorder(m))
	srv.SetObserver(observability.New(cfg.NodeID, prometheus.DefaultRegisterer, srv.PendingDependencies, srv.ApplyGap))

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}

	log.Printf("[main] node %s stopped cleanly", cfg.NodeID)
}
