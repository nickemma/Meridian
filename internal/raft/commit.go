package raft

import (
	"log"
	"sort"
)

// advanceCommitIndex recalculates the commit index after a successful
// replication round. Called by the leader after replicateToPeer succeeds.
//
// The algorithm:
//  1. Collect the last log index for this node + matchIndex for all peers
//  2. Sort descending
//  3. The value at position quorumSize-1 is the highest index
//     that exists on at least quorumSize nodes (a majority)
//  4. Advance commitIndex if that index is from the current term
func (n *Node) advanceCommitIndex() {
	if n.state.GetRole() != Leader {
		return
	}

	// Collect all known replicated indexes.
	// Leader always has its own last log index.
	indexes := []uint64{n.state.LastLogIndex()}

	for _, matchIdx := range n.state.MatchIndexes() {
		indexes = append(indexes, matchIdx)
	}

	// Sort descending — highest indexes first.
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] > indexes[j]
	})

	// The index at position quorumSize-1 is the highest index
	// present on at least quorumSize nodes.
	// For a 3-node cluster quorumSize=2, so index[1] is the median.
	if len(indexes) < n.quorumSize {
		return
	}

	quorumIndex := indexes[n.quorumSize-1]
	currentCommit := n.state.CommitIndex()

	if quorumIndex <= currentCommit {
		return // nothing new to commit
	}

	// Safety rule — only commit entries from the current term.
	// Entries from previous terms are committed as a side effect
	// when a current-term entry is committed after them.
	entryTerm := n.state.TermAt(quorumIndex)
	if entryTerm != n.state.CurrentTerm() {
		log.Printf("[raft] %s quorum index %d is from term %d not %d — skipping direct commit",
			n.state.NodeID(), quorumIndex, entryTerm, n.state.CurrentTerm())
		return
	}

	// Advance the commit index.
	n.state.SetCommitIndex(quorumIndex)

	log.Printf("[raft] %s committed entries up to index %d (term %d)",
		n.state.NodeID(), quorumIndex, entryTerm)

	// Notify the apply goroutine that new entries are ready.
	select {
	case n.commitCh <- quorumIndex:
	default:
		// Channel full — apply loop will catch up on its own.
	}
}

// runApplyLoop watches for newly committed entries and applies them
// to the state machine. Runs in a dedicated goroutine for the lifetime
// of the node.
//
// "Applying" an entry means handing it to the application layer.
// In Meridian this means writing to the storage engine.
// For now we log it — the storage engine integration comes in Phase 4.
func (n *Node) runApplyLoop() {
	for range n.commitCh {
		n.applyCommitted()
	}
}

// applyCommitted applies all committed but not yet applied entries
// to the state machine in order.
func (n *Node) applyCommitted() {
	commitIndex := n.state.CommitIndex()
	lastApplied := n.state.LastApplied()

	for lastApplied < commitIndex {
		lastApplied++

		entry, ok := n.state.EntryAt(lastApplied)
		if !ok {
			log.Printf("[raft] %s missing entry at index %d during apply — skipping",
				n.state.NodeID(), lastApplied)
			break
		}

		// Apply the entry to the state machine.
		// Phase 4 wires this to the storage engine.
		log.Printf("[raft] %s applying entry index=%d term=%d command=%q",
			n.state.NodeID(), entry.Index, entry.Term, entry.Command)

		n.state.SetLastApplied(lastApplied)
	}
}
