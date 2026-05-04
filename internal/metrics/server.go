package metrics

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

// Server exposes Prometheus metrics over HTTP.
// Runs on a separate port from the gRPC server so metrics
// collection does not interfere with Raft RPCs.
type Server struct {
	port int
}

// NewServer creates a metrics HTTP server on the given port.
func NewServer(port int) *Server {
	return &Server{port: port}
}

// Start begins serving metrics. Blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("[metrics] serving on :%d", s.port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("[metrics] shutting down metrics server")
		return srv.Shutdown(context.Background())
	case err := <-errCh:
		return fmt.Errorf("metrics server error: %w", err)
	}
}
