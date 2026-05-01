package raft

import (
	"math/rand"
	"sync"
	"time"
)

// ElectionTimer drives Raft's follower → candidate transition.
// When the timer fires, the node has not heard from a leader
// within the election timeout window and must start an election.
//
// The timer is randomised on every reset to prevent split votes
// from becoming permanent — each candidate gets a different
// timeout so one almost always fires before the others.
type ElectionTimer struct {
	mu      sync.Mutex
	timer   *time.Timer
	minTime time.Duration
	maxTime time.Duration
	firedCh chan struct{}
	stopped bool
}

// NewElectionTimer creates a new timer with the given min/max range.
// The timer does not start until Reset() is called.
func NewElectionTimer(min, max time.Duration) *ElectionTimer {
	t := &ElectionTimer{
		minTime: min,
		maxTime: max,
		firedCh: make(chan struct{}, 1),
	}
	return t
}

// Reset restarts the countdown from a new random duration.
// Must be called:
//   - On startup (to begin the countdown)
//   - On every valid heartbeat received from the leader
//   - After granting a vote to a candidate
//   - After starting a new election (so we don't re-fire immediately)
func (t *ElectionTimer) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return
	}

	duration := t.randomDuration()

	if t.timer == nil {
		// First call — create the timer
		t.timer = time.AfterFunc(duration, t.onFired)
	} else {
		// Subsequent calls — reset the existing timer.
		// Stop first to drain any pending fire, then reset.
		t.timer.Stop()
		t.timer.Reset(duration)
	}
}

// Stop permanently disables the timer.
// Called when the node shuts down or becomes leader
// (leaders use a heartbeat ticker, not an election timer).
func (t *ElectionTimer) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
	if t.timer != nil {
		t.timer.Stop()
	}
}

// Fired returns a channel that receives a signal when the timer expires.
// The Raft node's main loop selects on this channel.
func (t *ElectionTimer) Fired() <-chan struct{} {
	return t.firedCh
}

// onFired is called by the Go runtime when the timer expires.
// It sends a non-blocking signal on firedCh.
// Non-blocking because the Raft loop may be busy — we do not
// want the timer goroutine to block if the channel is full.
func (t *ElectionTimer) onFired() {
	select {
	case t.firedCh <- struct{}{}:
	default:
		// Channel already has a signal pending — the Raft loop
		// hasn't processed the last one yet. This is fine —
		// the existing signal is enough to trigger an election.
	}
}

// randomDuration returns a random duration between min and max.
func (t *ElectionTimer) randomDuration() time.Duration {
	if t.maxTime <= t.minTime {
		// Fallback: deterministic duration
		return t.minTime
	}
	diff := int64(t.maxTime - t.minTime)
	return t.minTime + time.Duration(rand.Int63n(diff))
}
