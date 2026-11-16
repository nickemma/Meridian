package application

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/nickemma/meridian/internal/raft/domain"
	"github.com/nickemma/meridian/internal/raft/ports"
)

// State holds all the durable and volatile state for a single Raft node.
// Fields marked "durable" must survive crashes — in a full implementation
// they are written to the WAL before any response is sent.
// Fields marked "volatile" are rebuilt on restart.
type State struct {
	mu sync.RWMutex

	// --- Identity ---
	nodeID string // this node's unique ID e.g. "node-1"

	// --- Durable state (must survive crashes) ---
	currentTerm uint64
	votedFor    string
	log         []domain.LogEntry

	// --- Volatile state (rebuilt on restart) ---
	role          domain.Role
	leaderID      string
	lastHeartbeat time.Time
	commitIndex   uint64
	lastApplied   uint64

	// --- Leader-only volatile state ---
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	store         ports.StateStore
	durabilityErr error
}

// NewState creates the initial state for a Raft node.
// Every node starts as a Follower in term 0 with an empty log.
func NewState(nodeID string) *State {
	return newState(nodeID, domain.PersistentState{}, nil)
}

// NewStateFromStore restores durable Raft state before the node joins the
// cluster. A corrupt or incomplete state file prevents startup rather than
// allowing a node to participate with an invented history.
func NewStateFromStore(nodeID string, store ports.StateStore) (*State, error) {
	if store == nil {
		return nil, fmt.Errorf("raft state store is required")
	}
	persisted, err := store.Load()
	if err != nil {
		return nil, err
	}
	return newState(nodeID, persisted, store), nil
}

func newState(nodeID string, persisted domain.PersistentState, store ports.StateStore) *State {
	return &State{
		nodeID:      nodeID,
		currentTerm: persisted.CurrentTerm,
		votedFor:    persisted.VotedFor,
		log:         persisted.Clone().Log,
		role:        domain.Follower,
		leaderID:    "",
		commitIndex: persisted.CommitIndex,
		lastApplied: persisted.LastApplied,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		store:       store,
	}
}

// --- Term and role transitions ---

// CurrentTerm returns the node's current term.
func (s *State) CurrentTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

// Role returns the node's current role.
func (s *State) GetRole() domain.Role {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role
}

// BecomeFollower transitions this node to Follower for the given term.
// Called when:
//   - A higher term is seen in any incoming message
//   - An election is lost
func (s *State) BecomeFollower(term uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if term > s.currentTerm {
		s.currentTerm = term
		s.votedFor = ""
	}
	s.role = domain.Follower
	s.leaderID = ""
	s.nextIndex = make(map[string]uint64)
	s.matchIndex = make(map[string]uint64)
	s.persistLocked()
}

// BecomeCandidate transitions this node to Candidate.
// Increments the term and votes for itself.
// Called when the election timer fires.
func (s *State) BecomeCandidate() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm++
	s.role = domain.Candidate
	s.votedFor = s.nodeID // vote for ourselves
	s.leaderID = ""
	s.persistLocked()
	return s.currentTerm
}

// BecomeLeader transitions this node to Leader.
// Initialises nextIndex and matchIndex for all peers.
// Called after winning an election.
func (s *State) BecomeLeader(peers []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = domain.Leader
	s.leaderID = s.nodeID

	// nextIndex starts at our last log index + 1 for all peers.
	// We optimistically assume peers are up to date — we'll correct
	// this downward if AppendEntries is rejected.
	lastIndex := s.lastLogIndex()
	for _, peer := range peers {
		s.nextIndex[peer] = lastIndex + 1
		s.matchIndex[peer] = 0
	}
}

// --- Log operations ---

// LastLogIndex returns the index of the last entry in the log.
// Returns 0 if the log is empty.
func (s *State) LastLogIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastLogIndex()
}

// lastLogIndex is the internal version — caller must hold the lock.
func (s *State) lastLogIndex() uint64 {
	if len(s.log) == 0 {
		return 0
	}
	return s.log[len(s.log)-1].Index
}

// LastLogTerm returns the term of the last entry in the log.
// Returns 0 if the log is empty.
func (s *State) LastLogTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.log) == 0 {
		return 0
	}
	return s.log[len(s.log)-1].Term
}

// LastApplied returns the highest log index applied to the state machine.
func (s *State) LastApplied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastApplied
}

// SetLastApplied advances the last applied index.
// Never moves backward.
func (s *State) SetLastApplied(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index > s.lastApplied {
		s.lastApplied = index
		s.persistLocked()
	}
}

// DurabilityError reports the latest failed state flush. Callers must not
// acknowledge a vote, replicated entry, or client command while it is non-nil.
func (s *State) DurabilityError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.durabilityErr
}

func (s *State) persistLocked() {
	if s.store == nil {
		return
	}
	s.durabilityErr = s.store.Save(domain.PersistentState{
		CurrentTerm: s.currentTerm,
		VotedFor:    s.votedFor,
		Log:         s.log,
		CommitIndex: s.commitIndex,
		LastApplied: s.lastApplied,
	})
}

// AppendEntry adds a new entry to the log.
// Called by the leader when it receives a client command.
func (s *State) AppendEntry(term uint64, command []byte) domain.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := domain.LogEntry{
		Index:   s.lastLogIndex() + 1,
		Term:    term,
		Command: command,
	}
	s.log = append(s.log, entry)
	s.persistLocked()

	return entry
}

// EntryAt returns the log entry at the given index.
// Returns false if the index is out of range.
func (s *State) EntryAt(index uint64) (domain.LogEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 || index > uint64(len(s.log)) {
		return domain.LogEntry{}, false
	}
	return s.log[index-1], true // log is 1-indexed
}

// TermAt returns the term of the entry at the given index.
// Returns 0 if the index is 0 (before the log begins).
func (s *State) TermAt(index uint64) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 || index > uint64(len(s.log)) {
		return 0
	}
	return s.log[index-1].Term
}

// TruncateFrom removes all entries from the given index onwards.
// Called when a follower receives entries that conflict with the leader's log.
func (s *State) TruncateFrom(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 || index > uint64(len(s.log)) {
		return
	}
	s.log = s.log[:index-1]
	s.persistLocked()
}

// AppendEntries appends a slice of entries to the log.
// Called by followers when replicating from the leader.
func (s *State) AppendEntries(entries []domain.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	s.persistLocked()
}

// MergeEntriesFromLeader incorporates a validated AppendEntries suffix in one
// durable state transition. Existing entries with the same index and term are
// retained; the first conflicting uncommitted entry and everything after it is
// replaced. It never creates the transient state "commit index beyond log"
// that separate truncate and append operations could expose to another RPC.
func (s *State) MergeEntriesFromLeader(prevLogIndex uint64, entries []domain.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if prevLogIndex > uint64(len(s.log)) {
		return fmt.Errorf("previous log index %d exceeds log length %d", prevLogIndex, len(s.log))
	}
	for offset, entry := range entries {
		expected := prevLogIndex + uint64(offset) + 1
		if entry.Index != expected || entry.Term == 0 {
			return fmt.Errorf("invalid replicated entry at offset %d: index=%d term=%d, want index %d and non-zero term", offset, entry.Index, entry.Term, expected)
		}
	}

	updated := domain.CloneEntries(s.log)
	changed := false
	for offset, entry := range entries {
		index := prevLogIndex + uint64(offset) + 1
		if index <= uint64(len(updated)) {
			existing := updated[index-1]
			if existing.Term == entry.Term && bytes.Equal(existing.Command, entry.Command) {
				continue
			}
			if index <= s.commitIndex {
				return fmt.Errorf("refusing to replace committed entry at index %d", index)
			}
			updated = updated[:index-1]
		}
		changed = true
		updated = append(updated, domain.LogEntry{Index: entry.Index, Term: entry.Term, Command: bytes.Clone(entry.Command)})
	}

	if !changed {
		return nil
	}
	previous := s.log
	s.log = updated
	s.persistLocked()
	if s.durabilityErr != nil {
		s.log = previous
		return s.durabilityErr
	}
	return nil
}

// EntriesFrom returns all log entries starting at the given index.
// Used by the leader to build AppendEntries RPCs.
func (s *State) EntriesFrom(index uint64) []domain.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 || index > uint64(len(s.log)) {
		return []domain.LogEntry{}
	}
	return s.log[index-1:]
}

// --- Commit tracking ---

// CommitIndex returns the highest log index known to be committed.
func (s *State) CommitIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.commitIndex
}

// SetCommitIndex advances the commit index.
// Never moves backward.
func (s *State) SetCommitIndex(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index > s.commitIndex {
		s.commitIndex = index
		s.persistLocked()
	}
}

// --- Vote tracking ---

// VotedFor returns who this node voted for in the current term.
func (s *State) VotedFor() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.votedFor
}

// GrantVote records that this node voted for the given candidate.
func (s *State) GrantVote(candidateID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.votedFor = candidateID
	s.persistLocked()
}

// --- Leader tracking ---

// SetLeader records the current leader ID.
// Called when a follower receives a valid AppendEntries from a leader.
func (s *State) SetLeader(leaderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaderID = leaderID
}

// LeaderID returns the current known leader.
func (s *State) LeaderID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.leaderID
}

// --- Leader replication tracking ---

// SetNextIndex sets the next log index to send to a peer.
func (s *State) SetNextIndex(peer string, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIndex[peer] = index
}

// GetNextIndex returns the next log index to send to a peer.
func (s *State) GetNextIndex(peer string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextIndex[peer]
}

// SetMatchIndex sets the highest log index known replicated on a peer.
func (s *State) SetMatchIndex(peer string, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matchIndex[peer] = index
}

// GetMatchIndex returns the highest log index known replicated on a peer.
func (s *State) GetMatchIndex(peer string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.matchIndex[peer]
}

// MatchIndexes returns a snapshot of all peer match indexes.
// Used by commit logic to find the highest index replicated on a majority.
func (s *State) MatchIndexes() map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make(map[string]uint64, len(s.matchIndex))
	for k, v := range s.matchIndex {
		snapshot[k] = v
	}
	return snapshot
}

// --- Node identity ---

// NodeID returns this node's ID.
func (s *State) NodeID() string {
	return s.nodeID
}

// RecordHeartbeat records the time we last heard from a valid leader.
// Called in handleAppendEntries when we accept a message.
func (s *State) RecordHeartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastHeartbeat = time.Now()
}

// HeardFromLeaderRecently returns true if we have received a valid heartbeat within the last election timeout window.
// Used by pre-vote to decide whether to grant a pre-vote.
func (s *State) HeardFromLeaderRecently(within time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastHeartbeat.IsZero() {
		return false
	}
	return time.Since(s.lastHeartbeat) < within
}
