// Package adapters delivers anomaly alerts to consumers.
package adapters

import (
	"github.com/nickemma/meridian/internal/anomaly/domain"
	"github.com/nickemma/meridian/internal/anomaly/ports"
)

// ChannelSink publishes alerts on a bounded channel and drops them when the
// consumer is not keeping up.
type ChannelSink struct {
	alerts chan *domain.Alert
}

var _ ports.AlertSink = (*ChannelSink)(nil)

func NewChannelSink(capacity int) *ChannelSink {
	return &ChannelSink{alerts: make(chan *domain.Alert, capacity)}
}

func (s *ChannelSink) Emit(alert *domain.Alert) {
	select {
	case s.alerts <- alert:
	default:
	}
}

// Alerts returns the channel consumers should read continuously.
func (s *ChannelSink) Alerts() <-chan *domain.Alert { return s.alerts }
