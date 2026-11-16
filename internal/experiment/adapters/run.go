package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/nickemma/meridian/internal/experiment/application"
	"github.com/nickemma/meridian/internal/experiment/domain"
)

// Run executes storage-fault-trial-v1 in a new directory dir using the node
// executable binary. Artifacts (timeline.jsonl, environment.json,
// summary.json, node logs and metrics captures) are retained; node processes
// are always cleaned up.
func Run(ctx context.Context, dir, binary string) (summary domain.Summary, err error) {
	absolute, err := filepath.Abs(binary)
	if err != nil {
		return summary, err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return summary, err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return summary, fmt.Errorf("experiment needs a new output directory: %w", err)
	}
	timelineFile, err := os.OpenFile(filepath.Join(dir, "timeline.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return summary, err
	}
	defer timelineFile.Close()

	started := time.Now()
	var stats domain.Stats
	defer func() {
		summary = domain.Summarize(stats, time.Since(started), domain.Platform{Go: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH}, err)
		if writeErr := writeJSON(filepath.Join(dir, "summary.json"), summary); err == nil && writeErr != nil {
			err = writeErr
		}
	}()

	cluster, err := NewLocalCluster(dir, absolute)
	if err != nil {
		return summary, err
	}
	defer cluster.Close()
	if err = writeEnvironment(dir, absolute, cluster); err != nil {
		return summary, err
	}
	for i := 0; i < cluster.Size(); i++ {
		if err = cluster.Start(i); err != nil {
			return summary, err
		}
	}
	stats, err = application.Run(ctx, cluster, NewJSONLTimeline(timelineFile))
	return summary, err
}

func writeEnvironment(dir, binary string, cluster *LocalCluster) error {
	binaryData, err := os.ReadFile(binary)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(binaryData)
	environment := cluster.Environment()
	environment["node_sha256"] = fmt.Sprintf("%x", digest)
	environment["scenario"] = domain.ScenarioName
	environment["binary"] = binary
	environment["transport"] = "mTLS"
	data, err := json.MarshalIndent(environment, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "environment.json"), data, 0600)
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0600)
}
