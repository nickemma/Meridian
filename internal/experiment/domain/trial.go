// Package domain defines the storage fault trial's record format: timeline
// events, the latency/error statistics and the run summary.
package domain

import (
	"sort"
	"time"
)

// ScenarioName identifies the fixed fault sequence and artifact format.
const ScenarioName = "storage-fault-trial-v1"

// SummaryNote states how the numbers in a Summary may be interpreted.
const SummaryNote = "Sequential functional trial. Percentiles include expected denied/timeout requests, exclude setup/polling, and are not benchmark results. Timeout mutations have uncertain outcomes; no automatic mutation retry."

// Event kinds.
const (
	KindRPC       = "rpc"
	KindAssertion = "assertion"
	KindFault     = "fault"
)

// Event is one line of timeline.jsonl.
type Event struct {
	Sequence     int       `json:"sequence"`
	Name         string    `json:"name"`
	Kind         string    `json:"kind"`
	At           time.Time `json:"at"`
	Milliseconds float64   `json:"milliseconds,omitempty"`
	Code         string    `json:"code,omitempty"`
	Error        string    `json:"error,omitempty"`
}

// Stats accumulates recorded RPC outcomes.
type Stats struct {
	Latencies []float64      // milliseconds, one per recorded RPC
	Errors    map[string]int // gRPC code → count, failed RPCs only
}

// Quantile returns the q-th sample of the sorted latencies (nearest rank,
// rounding down), or 0 when nothing was recorded.
func (s Stats) Quantile(q float64) float64 {
	if len(s.Latencies) == 0 {
		return 0
	}
	sorted := append([]float64(nil), s.Latencies...)
	sort.Float64s(sorted)
	return sorted[int(q*float64(len(sorted)-1))]
}

// Summary is summary.json.
type Summary struct {
	Passed         bool           `json:"passed"`
	Error          string         `json:"error,omitempty"`
	Scenario       string         `json:"scenario"`
	Go             string         `json:"go"`
	OS             string         `json:"os"`
	Architecture   string         `json:"architecture"`
	ElapsedSeconds float64        `json:"elapsed_seconds"`
	Operations     int            `json:"operations"`
	Errors         map[string]int `json:"rpc_errors"`
	P50            float64        `json:"p50_ms"`
	P95            float64        `json:"p95_ms"`
	P99            float64        `json:"p99_ms"`
	Note           string         `json:"note"`
}

// Platform describes where the trial ran.
type Platform struct{ Go, OS, Architecture string }

// Summarize builds the run summary. A nil runErr means every assertion passed.
func Summarize(stats Stats, elapsed time.Duration, platform Platform, runErr error) Summary {
	summary := Summary{
		Passed: runErr == nil, Scenario: ScenarioName,
		Go: platform.Go, OS: platform.OS, Architecture: platform.Architecture,
		ElapsedSeconds: elapsed.Seconds(), Operations: len(stats.Latencies), Errors: stats.Errors,
		P50: stats.Quantile(.50), P95: stats.Quantile(.95), P99: stats.Quantile(.99), Note: SummaryNote,
	}
	if runErr != nil {
		summary.Error = runErr.Error()
	}
	return summary
}
