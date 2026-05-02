package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nickemma/meridian/internal/config"
	"github.com/nickemma/meridian/internal/server"
)

func main() {
	// Load config from environment variables.
	// If any required variable is missing, this exits immediately with a clear error.

	cfg, err := config.Load()
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

	// Start the gRPC server. This blocks until ctx is cancelled.
	srv := server.New(cfg)

	// Temporary — submit a test command after startup to verify commit
	go func() {
		log.Println("********** SUBMIT GOROUTINE STARTED ***********")
		time.Sleep(5 * time.Second) // wait for leader election
		log.Println("ABOUT TO SUBMIT")
		idx, err := srv.RaftNode().Submit([]byte("set x=hello"))
		if err != nil {
			log.Printf("[smoke] submit failed: %v", err)
			return
		}
		log.Printf("[smoke] submitted command at index %d", idx)
	}()

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}

	log.Printf("[main] node %s stopped cleanly", cfg.NodeID)

}
