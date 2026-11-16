package domain

import (
	"sort"
	"time"
)

// Options are the protocol settings a node needs; they are derived from the
// node configuration by the composition root.
type Options struct {
	NodeID             string
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	QuorumSize         int
}

// CandidateLogUpToDate is the election restriction shared by RequestVote and
// PreVote: a voter grants only to a candidate whose log is at least as
// up-to-date as its own. This prevents a stale node from becoming leader and
// overwriting entries already committed on a majority.
func CandidateLogUpToDate(candidateLastTerm, candidateLastIndex, ourLastTerm, ourLastIndex uint64) bool {
	return candidateLastTerm > ourLastTerm ||
		(candidateLastTerm == ourLastTerm && candidateLastIndex >= ourLastIndex)
}

// QuorumIndex returns the highest log index present on at least quorumSize
// nodes, given every node's replicated index. ok is false when there are
// fewer known indexes than the quorum.
func QuorumIndex(indexes []uint64, quorumSize int) (index uint64, ok bool) {
	if quorumSize <= 0 || len(indexes) < quorumSize {
		return 0, false
	}
	sorted := append([]uint64(nil), indexes...)
	// Sort descending; position quorumSize-1 is held by a majority. For a
	// three-node cluster quorumSize=2, so this is the median.
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
	return sorted[quorumSize-1], true
}
