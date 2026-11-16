// Package application translates protocol events into metric updates.
package application

import (
	"github.com/nickemma/meridian/internal/metrics/domain"
	"github.com/nickemma/meridian/internal/metrics/ports"
	raftports "github.com/nickemma/meridian/internal/raft/ports"
)

// RaftRecorder implements the Raft core's metrics port over a RaftSink.
type RaftRecorder struct {
	sink ports.RaftSink
}

var _ raftports.Metrics = (*RaftRecorder)(nil)

func NewRaftRecorder(sink ports.RaftSink) *RaftRecorder { return &RaftRecorder{sink: sink} }

func (r *RaftRecorder) HeartbeatSent()   { r.sink.IncHeartbeats() }
func (r *RaftRecorder) ElectionStarted() { r.sink.IncElections() }

// Observe publishes one protocol snapshot. Peer lag is leader log index minus
// confirmed match index and is zero on non-leaders; it is not time staleness.
func (r *RaftRecorder) Observe(sample raftports.Sample) {
	for _, lag := range sample.PeerLag {
		r.sink.SetPeerLag(lag.Peer, float64(lag.Entries))
	}
	r.sink.SetTerm(float64(sample.Term))
	r.sink.SetCommitIndex(float64(sample.CommitIndex))
	r.sink.SetLogEntries(float64(sample.LogEntries))
	if code := domain.RoleCode(sample.Role); code >= 0 {
		r.sink.SetRole(code)
	}
}
