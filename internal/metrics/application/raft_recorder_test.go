package application

import (
	"testing"

	raft "github.com/nickemma/meridian/internal/raft/domain"
	raftports "github.com/nickemma/meridian/internal/raft/ports"
)

type fakeSink struct {
	term, role, commit, entries float64
	lag                         map[string]float64
	heartbeats, elections       int
}

func (f *fakeSink) SetTerm(v float64)                 { f.term = v }
func (f *fakeSink) SetRole(v float64)                 { f.role = v }
func (f *fakeSink) SetCommitIndex(v float64)          { f.commit = v }
func (f *fakeSink) SetLogEntries(v float64)           { f.entries = v }
func (f *fakeSink) SetPeerLag(peer string, v float64) { f.lag[peer] = v }
func (f *fakeSink) IncHeartbeats()                    { f.heartbeats++ }
func (f *fakeSink) IncElections()                     { f.elections++ }

func TestRaftRecorderPublishesSample(t *testing.T) {
	sink := &fakeSink{lag: map[string]float64{}}
	recorder := NewRaftRecorder(sink)
	recorder.Observe(raftports.Sample{Role: raft.Leader, Term: 4, CommitIndex: 7, LogEntries: 9, PeerLag: []raftports.PeerLag{{Peer: "node-2", Entries: 2}}})
	recorder.HeartbeatSent()
	recorder.ElectionStarted()
	if sink.role != 2 || sink.term != 4 || sink.commit != 7 || sink.entries != 9 || sink.lag["node-2"] != 2 || sink.heartbeats != 1 || sink.elections != 1 {
		t.Fatalf("sink = %+v", sink)
	}
}
