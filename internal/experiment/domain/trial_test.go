package domain

import (
	"errors"
	"testing"
	"time"
)

func TestSummarizeReportsQuantilesAndFailure(t *testing.T) {
	stats := Stats{Latencies: []float64{5, 1, 3, 2, 4}, Errors: map[string]int{"PermissionDenied": 2}}
	summary := Summarize(stats, 2*time.Second, Platform{Go: "go", OS: "linux", Architecture: "amd64"}, errors.New("boom"))
	if summary.Passed || summary.Error != "boom" || summary.Operations != 5 || summary.P50 != 3 || summary.P99 != 4 || summary.Scenario != ScenarioName {
		t.Fatalf("summary = %+v", summary)
	}
	if stats.Latencies[0] != 5 {
		t.Fatal("Quantile mutated the recorded latencies")
	}
	if (Stats{}).Quantile(0.5) != 0 {
		t.Fatal("empty stats quantile is not zero")
	}
}
