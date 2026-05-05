package anomaly

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Alert is raised when an access pattern deviates from baseline.
type Alert struct {
	Identity  string
	Reason    string
	Severity  Severity
	Timestamp time.Time
	Request   AccessEvent
}

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

// baseline holds the learned normal behaviour for a single identity.
type baseline struct {
	// Request rate tracking
	// We keep a sliding window of access timestamps.
	accessTimes []time.Time

	// Path tracking — which paths has this identity accessed
	knownPaths map[string]int // path → access count

	// IP tracking — which IPs has this identity come from
	knownIPs map[string]int // ip → access count

	// Denial tracking — what fraction of requests are denied
	totalRequests int
	deniedCount   int

	firstSeen time.Time
	lastSeen  time.Time
}

// requestRate returns accesses per minute in the last windowDuration.
func (b *baseline) requestRate(window time.Duration) float64 {
	cutoff := time.Now().Add(-window)
	count := 0
	for _, t := range b.accessTimes {
		if t.After(cutoff) {
			count++
		}
	}
	return float64(count) / window.Minutes()
}

// Detector maintains per-identity baselines and detects anomalies.
type Detector struct {
	mu        sync.RWMutex
	baselines map[string]*baseline

	// Configuration
	windowDuration   time.Duration // how far back to look for rate calculation
	rateThreshold    float64       // requests/min above this is anomalous
	newIPThreshold   int           // seen fewer IPs than this = still learning
	newPathThreshold int           // seen fewer paths than this = still learning
	minObservations  int           // don't alert until we have this many data points
	denialRateLimit  float64       // fraction of denied requests above this is anomalous

	// Alert channel — consumers read from this
	alerts chan *Alert
}

// NewDetector creates an anomaly detector with sensible defaults.
func NewDetector() *Detector {
	d := &Detector{
		baselines:        make(map[string]*baseline),
		windowDuration:   5 * time.Minute,
		rateThreshold:    100, // >100 req/min is anomalous
		newIPThreshold:   3,   // seen fewer than 3 IPs = still learning
		newPathThreshold: 5,   // seen fewer than 5 paths = still learning
		minObservations:  10,  // need at least 10 data points before alerting
		denialRateLimit:  0.5, // >50% denials is anomalous
		alerts:           make(chan *Alert, 256),
	}
	go d.cleanupLoop()
	return d
}

// Observe records a secret access event and checks for anomalies.
// Returns an Alert if the access is anomalous, nil if normal.
func (d *Detector) Observe(event AccessEvent) *Alert {
	d.mu.Lock()
	b, exists := d.baselines[event.Identity]
	if !exists {
		b = &baseline{
			knownPaths: make(map[string]int),
			knownIPs:   make(map[string]int),
			firstSeen:  event.Timestamp,
		}
		d.baselines[event.Identity] = b
	}

	// Update baseline with this observation.
	b.accessTimes = append(b.accessTimes, event.Timestamp)
	b.knownPaths[event.Path]++
	b.knownIPs[event.SourceIP]++
	b.totalRequests++
	if event.Outcome == "denied" {
		b.deniedCount++
	}
	b.lastSeen = event.Timestamp

	// Take a snapshot for analysis — release lock before checking.
	snapshot := *b
	d.mu.Unlock()

	// Do not alert until we have enough data to establish a baseline.
	if snapshot.totalRequests < d.minObservations {
		return nil
	}

	return d.analyse(event, &snapshot)
}

// analyse checks a snapshot against anomaly rules.
// Returns the first anomaly found, or nil if everything looks normal.
func (d *Detector) analyse(event AccessEvent, b *baseline) *Alert {
	// Rule 1 — request rate spike.
	rate := b.requestRate(d.windowDuration)
	if rate > d.rateThreshold {
		alert := &Alert{
			Identity:  event.Identity,
			Reason:    fmt.Sprintf("request rate %.1f req/min exceeds threshold %.1f", rate, d.rateThreshold),
			Severity:  rateSeverity(rate, d.rateThreshold),
			Timestamp: event.Timestamp,
			Request:   event,
		}
		d.emit(alert)
		return alert
	}

	// Rule 2 — access from a new IP address.
	// Only alert if we have seen enough IPs to have a stable baseline.
	if len(b.knownIPs) > d.newIPThreshold {
		if b.knownIPs[event.SourceIP] == 1 {
			// This is the first time we have seen this IP.
			alert := &Alert{
				Identity:  event.Identity,
				Reason:    fmt.Sprintf("access from new IP %s not seen in baseline", event.SourceIP),
				Severity:  SeverityMedium,
				Timestamp: event.Timestamp,
				Request:   event,
			}
			d.emit(alert)
			return alert
		}
	}

	// Rule 3 — access to a new secret path.
	if len(b.knownPaths) > d.newPathThreshold {
		if b.knownPaths[event.Path] == 1 {
			alert := &Alert{
				Identity:  event.Identity,
				Reason:    fmt.Sprintf("access to new path %q not seen in baseline", event.Path),
				Severity:  SeverityLow,
				Timestamp: event.Timestamp,
				Request:   event,
			}
			d.emit(alert)
			return alert
		}
	}

	// Rule 4 — high denial rate.
	if b.totalRequests > 0 {
		denialRate := float64(b.deniedCount) / float64(b.totalRequests)
		if denialRate > d.denialRateLimit {
			alert := &Alert{
				Identity: event.Identity,
				Reason: fmt.Sprintf(
					"denial rate %.0f%% exceeds threshold %.0f%%",
					denialRate*100, d.denialRateLimit*100,
				),
				Severity:  SeverityHigh,
				Timestamp: event.Timestamp,
				Request:   event,
			}
			d.emit(alert)
			return alert
		}
	}

	return nil
}

// Alerts returns the channel on which anomaly alerts are published.
// Consumers should read from this channel continuously.
func (d *Detector) Alerts() <-chan *Alert {
	return d.alerts
}

// BaselineFor returns a summary of the baseline for a given identity.
// Used for debugging and observability.
func (d *Detector) BaselineFor(identity string) (map[string]interface{}, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	b, exists := d.baselines[identity]
	if !exists {
		return nil, false
	}

	return map[string]interface{}{
		"total_requests": b.totalRequests,
		"denied_count":   b.deniedCount,
		"known_paths":    len(b.knownPaths),
		"known_ips":      len(b.knownIPs),
		"first_seen":     b.firstSeen,
		"last_seen":      b.lastSeen,
	}, true
}

// IdentityCount returns the number of identities being tracked.
func (d *Detector) IdentityCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.baselines)
}

// emit sends an alert to the alerts channel non-blocking.
func (d *Detector) emit(alert *Alert) {
	select {
	case d.alerts <- alert:
	default:
		// Channel full — drop the alert rather than blocking the read path.
		// A full channel means the consumer is not keeping up.
	}
}

// cleanupLoop periodically trims old access timestamps from baselines
// to prevent unbounded memory growth.
func (d *Detector) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-d.windowDuration)
		d.mu.Lock()
		for _, b := range d.baselines {
			trimmed := b.accessTimes[:0]
			for _, t := range b.accessTimes {
				if t.After(cutoff) {
					trimmed = append(trimmed, t)
				}
			}
			b.accessTimes = trimmed
		}
		d.mu.Unlock()
	}
}

// rateSeverity maps how far above threshold a rate is to a severity level.
func rateSeverity(rate, threshold float64) Severity {
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

// stdDev computes standard deviation of a float64 slice.
// Used for statistical baseline comparison.
func stdDev(values []float64) float64 {
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
