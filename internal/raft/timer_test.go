package raft

import (
	"testing"
	"time"
)

func TestElectionTimer_FiresAfterTimeout(t *testing.T) {
	timer := NewElectionTimer(50*time.Millisecond, 100*time.Millisecond)
	timer.Reset()

	select {
	case <-timer.Fired():
		// correct — timer fired within the expected window
	case <-time.After(200 * time.Millisecond):
		t.Fatal("election timer did not fire within expected window")
	}
}

func TestElectionTimer_ResetPreventsEarlyFire(t *testing.T) {
	// Timer with 100–150ms timeout
	timer := NewElectionTimer(100*time.Millisecond, 150*time.Millisecond)
	timer.Reset()

	// Reset repeatedly for 200ms — timer should never fire
	// because we keep restarting it before it can expire
	deadline := time.After(200 * time.Millisecond)
	resetTicker := time.NewTicker(40 * time.Millisecond)
	defer resetTicker.Stop()

	for {
		select {
		case <-timer.Fired():
			t.Fatal("timer fired despite being reset before expiry")
		case <-resetTicker.C:
			timer.Reset()
		case <-deadline:
			// Correct — timer never fired because we kept resetting it
			return
		}
	}
}

func TestElectionTimer_StopPreventsfire(t *testing.T) {
	timer := NewElectionTimer(50*time.Millisecond, 80*time.Millisecond)
	timer.Reset()
	timer.Stop()

	select {
	case <-timer.Fired():
		t.Fatal("timer fired after being stopped")
	case <-time.After(200 * time.Millisecond):
		// Correct — stopped timer never fires
	}
}

func TestElectionTimer_FiresAgainAfterReset(t *testing.T) {
	// Verify that after firing, a reset causes it to fire again.
	// This covers the split vote re-election scenario.
	timer := NewElectionTimer(50*time.Millisecond, 80*time.Millisecond)
	timer.Reset()

	// Wait for first fire
	select {
	case <-timer.Fired():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire first time")
	}

	// Reset — simulates starting a new election
	timer.Reset()

	// Should fire again
	select {
	case <-timer.Fired():
		// correct
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer did not fire second time after reset")
	}
}
