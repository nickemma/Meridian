// Package ports declares where the detector delivers alerts.
package ports

import "github.com/nickemma/meridian/internal/anomaly/domain"

// AlertSink receives alerts. Emit must not block the read path; a sink that
// cannot keep up drops alerts rather than delaying secret access.
type AlertSink interface {
	Emit(*domain.Alert)
}
