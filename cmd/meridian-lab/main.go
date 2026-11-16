package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	experiment "github.com/nickemma/meridian/internal/experiment/adapters"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	out := flag.String("out", "", "new artifact directory (required)")
	binary := flag.String("node", "bin/meridian", "Rust-backed node executable")
	timeout := flag.Duration("timeout", 2*time.Minute, "whole trial deadline")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "-out is required")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, deadlineCancel := context.WithTimeout(ctx, *timeout)
	defer deadlineCancel()
	summary, err := experiment.Run(ctx, *out, *binary)
	_ = json.NewEncoder(os.Stdout).Encode(summary)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
