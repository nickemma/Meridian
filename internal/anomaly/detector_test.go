package anomaly

import (
	"fmt"
	"testing"
	"time"
)

// makeDetector creates a detector with low thresholds for testing.
func makeDetector() *Detector {
	d := NewDetector()
	d.rateThreshold = 10    // 10 req/min threshold for tests
	d.minObservations = 5   // only need 5 observations
	d.newIPThreshold = 2    // alert after 2 known IPs
	d.newPathThreshold = 2  // alert after 2 known paths
	d.denialRateLimit = 0.5 // 50% denial rate
	return d
}

func normalEvent(identity string) AccessEvent {
	return AccessEvent{
		Identity:  identity,
		Path:      "services/payments/db-password",
		SourceIP:  "10.0.0.1",
		Timestamp: time.Now(),
		Outcome:   "success",
	}
}

func TestDetector_NoAlertDuringLearning(t *testing.T) {
	d := makeDetector()

	// Feed fewer than minObservations — should never alert
	for i := 0; i < 4; i++ {
		alert := d.Observe(normalEvent("payments-service"))
		if alert != nil {
			t.Errorf("expected no alert during learning phase, got: %s", alert.Reason)
		}
	}
}

func TestDetector_NoAlertForNormalBehaviour(t *testing.T) {
	d := makeDetector()

	// Establish baseline with normal behaviour
	for i := 0; i < 20; i++ {
		alert := d.Observe(AccessEvent{
			Identity:  "payments-service",
			Path:      "services/payments/db-password",
			SourceIP:  "10.0.0.1",
			Timestamp: time.Now(),
			Outcome:   "success",
		})
		// After learning phase, normal behaviour should not alert
		if alert != nil && i >= d.minObservations {
			t.Errorf("unexpected alert for normal behaviour: %s", alert.Reason)
		}
	}
}

func TestDetector_AlertsOnRateSpike(t *testing.T) {
	d := makeDetector()
	d.windowDuration = 1 * time.Minute // wide window so all requests count

	// Establish baseline
	for i := 0; i < d.minObservations; i++ {
		d.Observe(normalEvent("payments-service"))
	}

	// Flood with requests to spike the rate above threshold
	var lastAlert *Alert
	for i := 0; i < 100; i++ {
		alert := d.Observe(normalEvent("payments-service"))
		if alert != nil {
			lastAlert = alert
		}
	}

	if lastAlert == nil {
		t.Fatal("expected rate spike alert, got none")
	}
	if lastAlert.Severity == "" {
		t.Error("expected non-empty severity on alert")
	}
}

func TestDetector_AlertsOnNewIP(t *testing.T) {
	d := makeDetector()

	// Establish baseline from two known IPs
	for i := 0; i < d.minObservations; i++ {
		ip := "10.0.0.1"
		if i%2 == 0 {
			ip = "10.0.0.2"
		}
		d.Observe(AccessEvent{
			Identity:  "payments-service",
			Path:      "services/payments/db-password",
			SourceIP:  ip,
			Timestamp: time.Now(),
			Outcome:   "success",
		})
	}

	// Access from a brand new IP never seen before
	alert := d.Observe(AccessEvent{
		Identity:  "payments-service",
		Path:      "services/payments/db-password",
		SourceIP:  "203.0.113.99", // public IP — never seen
		Timestamp: time.Now(),
		Outcome:   "success",
	})

	if alert == nil {
		t.Error("expected alert for access from new IP")
	}
}

func TestDetector_AlertsOnHighDenialRate(t *testing.T) {
	d := makeDetector()

	// Feed mostly denied requests
	for i := 0; i < d.minObservations+5; i++ {
		outcome := "denied"
		if i == 0 {
			outcome = "success" // one success to avoid division edge case
		}
		d.Observe(AccessEvent{
			Identity:  "suspicious-service",
			Path:      "services/payments/db-password",
			SourceIP:  "10.0.0.1",
			Timestamp: time.Now(),
			Outcome:   outcome,
		})
	}

	// One more denied to trigger the check
	alert := d.Observe(AccessEvent{
		Identity:  "suspicious-service",
		Path:      "services/payments/db-password",
		SourceIP:  "10.0.0.1",
		Timestamp: time.Now(),
		Outcome:   "denied",
	})

	if alert == nil {
		t.Error("expected alert for high denial rate")
	}
	if alert.Severity != SeverityHigh {
		t.Errorf("expected high severity for denial rate alert, got %s", alert.Severity)
	}
}

func TestDetector_TracksMultipleIdentities(t *testing.T) {
	d := makeDetector()

	identities := []string{"payments-service", "billing-service", "auth-service"}
	for _, id := range identities {
		for i := 0; i < 3; i++ {
			d.Observe(AccessEvent{
				Identity:  id,
				Path:      fmt.Sprintf("services/%s/key", id),
				SourceIP:  "10.0.0.1",
				Timestamp: time.Now(),
				Outcome:   "success",
			})
		}
	}

	if d.IdentityCount() != 3 {
		t.Errorf("expected 3 identities tracked, got %d", d.IdentityCount())
	}
}

func TestDetector_BaselineFor_ReturnsData(t *testing.T) {
	d := makeDetector()

	for i := 0; i < 5; i++ {
		d.Observe(normalEvent("payments-service"))
	}

	info, exists := d.BaselineFor("payments-service")
	if !exists {
		t.Fatal("expected baseline to exist")
	}
	if info["total_requests"].(int) != 5 {
		t.Errorf("expected 5 total requests, got %v", info["total_requests"])
	}
}

func TestDetector_BaselineFor_UnknownIdentity(t *testing.T) {
	d := makeDetector()
	_, exists := d.BaselineFor("unknown-service")
	if exists {
		t.Error("expected no baseline for unknown identity")
	}
}

func TestRateSeverity(t *testing.T) {
	if rateSeverity(50, 10) != SeverityHigh {
		t.Error("expected high severity for 5x threshold")
	}
	if rateSeverity(25, 10) != SeverityMedium {
		t.Error("expected medium severity for 2.5x threshold")
	}
	if rateSeverity(12, 10) != SeverityLow {
		t.Error("expected low severity for 1.2x threshold")
	}
}
