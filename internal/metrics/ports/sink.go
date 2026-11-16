// Package ports declares the metric sink the recorders write to, so protocol
// reporting does not depend on Prometheus.
package ports

// RaftSink receives Raft gauge and counter updates.
type RaftSink interface {
	SetTerm(float64)
	SetRole(float64)
	SetCommitIndex(float64)
	SetLogEntries(float64)
	SetPeerLag(peer string, entries float64)
	IncHeartbeats()
	IncElections()
}
