// Package domain holds the anomaly model: access events, per-identity
// baselines, detection thresholds and the alert rules. It is pure and
// deterministic given its inputs.
package domain

import (
	"math"
	"time"
)

// Severity indicates how strongly the access deviates from baseline.
type Severity string

const (
	SeverityLow    Severity = "low"
	SeverityMedium Severity = "medium"
	SeverityHigh   Severity = "high"
)

// AccessEvent is a single secret access observation fed to the detector.
type AccessEvent struct {
	Identity  string
	Path      string
	SourceIP  string
	Timestamp time.Time
	Outcome   string // "success", "denied", "error"
}

// Alert is raised when an access pattern deviates from baseline.
type Alert struct {
	Identity  string
	Reason    string
	Severity  Severity
	Timestamp time.Time
	Request   AccessEvent
}

// RateSeverity maps how far above threshold a rate is to a severity level.
func RateSeverity(rate, threshold float64) Severity {
	ratio := rate / threshold
	switch {
	case ratio >= 5:
		return SeverityHigh
	case ratio >= 2:
		return SeverityMedium
	default:
		return SeverityLow
	}
}

// StdDev computes the population standard deviation of values. It is kept
// for statistical baseline comparison.
func StdDev(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(values))
	return math.Sqrt(variance)
}
