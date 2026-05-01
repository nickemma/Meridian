package raft

import "sync"

// Role represents the current state of a Raft node.
// A node is always in exactly one of these three states.
type Role int

const (
	Follower  Role = iota // default state - listens, votes, never initiates
	Candidate             // running for leader
	Leader                // in-charge - replicates, log, sends heartbeats
)

func (r Role) string() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single entry in the Raft log.
// Every state change in Meridian is a LogEntry committed through consensus.
type LogEntry struct {
	Index   uint64 // position in the log — never changes once written
	Term    uint64 // which leader term this entry was created in
	Command []byte // the operation — opaque to Raft, meaningful to the application
}

// State holds all the durable and volatile state for a single Raft node.
// Fields marked "durable" must survive crashes — in a full implementation
// they are written to the WAL before any response is sent.
// Fields marked "volatile" are rebuilt on restart.
type State struct {
	mu sync.RWMutex

	// --- Identity ---
	nodeID string // this node's unique ID e.g. "node-1"

	// --- Durable state (must survive crashes) ---
	currentTerm uint64 // latest term this node has seen — monotonically increasing
	votedFor    string // candidateID we voted for in currentTerm ("" if none)
	log         []LogEntry

	// --- Volatile state (rebuilt on restart) ---
	role        Role
	leaderID    string // who the current leader is ("" if unknown)
	commitIndex uint64 // highest log index known to be committed
	lastApplied uint64 // highest log index applied to the state machine

	// --- Leader-only volatile state ---
	// Rebuilt after every election win.
	// nextIndex[peer]  — next log index to send to this peer
	// matchIndex[peer] — highest index known to be replicated on this peer
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
}

// NewState creates the initial state for a Raft node.
// Every node starts as a Follower in term 0 with an empty log.
func NewState(nodeID string) *State {
	return &State{
		nodeID:      nodeID,
		currentTerm: 0,
		votedFor:    "",
		log:         []LogEntry{},
		role:        Follower,
		leaderID:    "",
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
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
func (s *State) GetRole() Role {
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
	s.currentTerm = term
	s.role = Follower
	s.votedFor = ""
	s.leaderID = ""
	s.nextIndex = make(map[string]uint64)
	s.matchIndex = make(map[string]uint64)
}

// BecomeCandidate transitions this node to Candidate.
// Increments the term and votes for itself.
// Called when the election timer fires.
func (s *State) BecomeCandidate() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm++
	s.role = Candidate
	s.votedFor = s.nodeID // vote for ourselves
	s.leaderID = ""
	return s.currentTerm
}

// BecomeLeader transitions this node to Leader.
// Initialises nextIndex and matchIndex for all peers.
// Called after winning an election.
func (s *State) BecomeLeader(peers []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.role = Leader
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

// AppendEntry adds a new entry to the log.
// Called by the leader when it receives a client command.
func (s *State) AppendEntry(term uint64, command []byte) LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := LogEntry{
		Index:   s.lastLogIndex() + 1,
		Term:    term,
		Command: command,
	}
	s.log = append(s.log, entry)
	return entry
}

// EntryAt returns the log entry at the given index.
// Returns false if the index is out of range.
func (s *State) EntryAt(index uint64) (LogEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 || index > uint64(len(s.log)) {
		return LogEntry{}, false
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
}

// AppendEntries appends a slice of entries to the log.
// Called by followers when replicating from the leader.
func (s *State) AppendEntries(entries []LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
}

// EntriesFrom returns all log entries starting at the given index.
// Used by the leader to build AppendEntries RPCs.
func (s *State) EntriesFrom(index uint64) []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 || index > uint64(len(s.log)) {
		return []LogEntry{}
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
