package domain

import (
	"fmt"
	"time"
)

// Thresholds configure detection.
type Thresholds struct {
	Window          time.Duration // how far back to look for rate calculation
	Rate            float64       // requests/min above this is anomalous
	NewIPAfter      int           // known IPs needed before a new IP alerts
	NewPathAfter    int           // known paths needed before a new path alerts
	MinObservations int           // don't alert until we have this many data points
	DenialRateLimit float64       // fraction of denied requests above this is anomalous
}

// DefaultThresholds are the production detection settings.
func DefaultThresholds() Thresholds {
	return Thresholds{
		Window:          5 * time.Minute,
		Rate:            100, // >100 req/min is anomalous
		NewIPAfter:      3,   // seen fewer than 3 IPs = still learning
		NewPathAfter:    5,   // seen fewer than 5 paths = still learning
		MinObservations: 10,  // need at least 10 data points before alerting
		DenialRateLimit: 0.5, // >50% denials is anomalous
	}
}

// Baseline is the learned normal behaviour for a single identity.
type Baseline struct {
	AccessTimes   []time.Time    // sliding window of access timestamps
	KnownPaths    map[string]int // path → access count
	KnownIPs      map[string]int // ip → access count
	TotalRequests int
	DeniedCount   int
	FirstSeen     time.Time
	LastSeen      time.Time
}

func NewBaseline(firstSeen time.Time) *Baseline {
	return &Baseline{KnownPaths: make(map[string]int), KnownIPs: make(map[string]int), FirstSeen: firstSeen}
}

// Record adds event to the baseline.
func (b *Baseline) Record(event AccessEvent) {
	b.AccessTimes = append(b.AccessTimes, event.Timestamp)
	b.KnownPaths[event.Path]++
	b.KnownIPs[event.SourceIP]++
	b.TotalRequests++
	if event.Outcome == "denied" {
		b.DeniedCount++
	}
	b.LastSeen = event.Timestamp
}

// RequestRate returns accesses per minute in the window ending at now.
func (b *Baseline) RequestRate(window time.Duration, now time.Time) float64 {
	cutoff := now.Add(-window)
	count := 0
	for _, t := range b.AccessTimes {
		if t.After(cutoff) {
			count++
		}
	}
	return float64(count) / window.Minutes()
}

// Trim drops access times older than cutoff to bound memory.
func (b *Baseline) Trim(cutoff time.Time) {
	trimmed := b.AccessTimes[:0]
	for _, t := range b.AccessTimes {
		if t.After(cutoff) {
			trimmed = append(trimmed, t)
		}
	}
	b.AccessTimes = trimmed
}

// Evaluate applies the detection rules in priority order and returns the
// first anomaly, or nil. No alert is raised while the baseline is learning.
func (b *Baseline) Evaluate(event AccessEvent, t Thresholds, now time.Time) *Alert {
	if b.TotalRequests < t.MinObservations {
		return nil
	}
	alert := func(severity Severity, format string, args ...any) *Alert {
		return &Alert{Identity: event.Identity, Reason: fmt.Sprintf(format, args...), Severity: severity, Timestamp: event.Timestamp, Request: event}
	}

	// Rule 1 — request rate spike.
	if rate := b.RequestRate(t.Window, now); rate > t.Rate {
		return alert(RateSeverity(rate, t.Rate), "request rate %.1f req/min exceeds threshold %.1f", rate, t.Rate)
	}
	// Rule 2 — first access from a new IP once the IP baseline is stable.
	if len(b.KnownIPs) > t.NewIPAfter && b.KnownIPs[event.SourceIP] == 1 {
		return alert(SeverityMedium, "access from new IP %s not seen in baseline", event.SourceIP)
	}
	// Rule 3 — first access to a new secret path.
	if len(b.KnownPaths) > t.NewPathAfter && b.KnownPaths[event.Path] == 1 {
		return alert(SeverityLow, "access to new path %q not seen in baseline", event.Path)
	}
	// Rule 4 — high denial rate.
	if b.TotalRequests > 0 {
		if denialRate := float64(b.DeniedCount) / float64(b.TotalRequests); denialRate > t.DenialRateLimit {
			return alert(SeverityHigh, "denial rate %.0f%% exceeds threshold %.0f%%", denialRate*100, t.DenialRateLimit*100)
		}
	}
	return nil
}

// Clone copies the baseline for analysis outside a lock.
func (b *Baseline) Clone() *Baseline {
	clone := *b
	clone.AccessTimes = append([]time.Time(nil), b.AccessTimes...)
	clone.KnownPaths = make(map[string]int, len(b.KnownPaths))
	for k, v := range b.KnownPaths {
		clone.KnownPaths[k] = v
	}
	clone.KnownIPs = make(map[string]int, len(b.KnownIPs))
	for k, v := range b.KnownIPs {
		clone.KnownIPs[k] = v
	}
	return &clone
}
