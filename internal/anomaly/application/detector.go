// Package application maintains per-identity baselines and runs the domain
// detection rules on every observed secret access.
package application

import (
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/anomaly/domain"
	"github.com/nickemma/meridian/internal/anomaly/ports"
)

type Detector struct {
	mu         sync.RWMutex
	baselines  map[string]*domain.Baseline
	thresholds domain.Thresholds
	sink       ports.AlertSink
	now        func() time.Time
	stop       chan struct{}
	stopOnce   sync.Once
}

// NewDetector starts a detector whose background loop trims stale access
// times every minute until Close. A nil sink discards alerts; Observe still
// returns them.
func NewDetector(thresholds domain.Thresholds, sink ports.AlertSink) *Detector {
	d := &Detector{baselines: make(map[string]*domain.Baseline), thresholds: thresholds, sink: sink, now: time.Now, stop: make(chan struct{})}
	go d.cleanupLoop(time.Minute)
	return d
}

// Close stops the background cleanup loop.
func (d *Detector) Close() { d.stopOnce.Do(func() { close(d.stop) }) }

// Observe records an access and returns an Alert if it is anomalous.
func (d *Detector) Observe(event domain.AccessEvent) *domain.Alert {
	d.mu.Lock()
	b, exists := d.baselines[event.Identity]
	if !exists {
		b = domain.NewBaseline(event.Timestamp)
		d.baselines[event.Identity] = b
	}
	b.Record(event)
	// Analyse a copy so the lock is not held while rules run.
	snapshot := b.Clone()
	d.mu.Unlock()

	alert := snapshot.Evaluate(event, d.thresholds, d.now())
	if alert != nil && d.sink != nil {
		d.sink.Emit(alert)
	}
	return alert
}

// BaselineFor returns a summary of the baseline for a given identity.
func (d *Detector) BaselineFor(identity string) (map[string]interface{}, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	b, exists := d.baselines[identity]
	if !exists {
		return nil, false
	}
	return map[string]interface{}{
		"total_requests": b.TotalRequests,
		"denied_count":   b.DeniedCount,
		"known_paths":    len(b.KnownPaths),
		"known_ips":      len(b.KnownIPs),
		"first_seen":     b.FirstSeen,
		"last_seen":      b.LastSeen,
	}, true
}

// IdentityCount returns the number of identities being tracked.
func (d *Detector) IdentityCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.baselines)
}

// cleanupLoop periodically trims old access timestamps from baselines to
// prevent unbounded memory growth.
func (d *Detector) cleanupLoop(every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			cutoff := d.now().Add(-d.thresholds.Window)
			d.mu.Lock()
			for _, b := range d.baselines {
				b.Trim(cutoff)
			}
			d.mu.Unlock()
		}
	}
}
