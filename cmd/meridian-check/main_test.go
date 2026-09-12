package main

import "testing"

func TestCheckerAcceptsOverlappingWriteAndRead(t *testing.T) {
	checker := checker{operations: []operation{
		{Kind: "put", Key: "k", Value: "v", Invoked: 10, Completed: 30},
		{Kind: "get", Key: "k", Found: false, Invoked: 20, Completed: 25},
	}, stateLimit: 100}
	if !checker.search(3, map[string]value{}) {
		t.Fatal("checker rejected legal overlapping history")
	}
}

func TestCheckerRejectsStaleNonOverlappingRead(t *testing.T) {
	checker := checker{operations: []operation{
		{Kind: "put", Key: "k", Value: "v", Invoked: 10, Completed: 20},
		{Kind: "get", Key: "k", Found: false, Invoked: 30, Completed: 40},
	}, stateLimit: 100}
	if checker.search(3, map[string]value{}) {
		t.Fatal("checker accepted stale non-overlapping read")
	}
}

func TestCheckerValidatesCompareAndSetOutcome(t *testing.T) {
	valid := checker{operations: []operation{
		{Kind: "compare_and_set", Key: "k", ExpectedExists: false, Swapped: true, Value: "v", Invoked: 10, Completed: 20},
		{Kind: "compare_and_set", Key: "k", ExpectedExists: false, Swapped: false, CurrentExists: true, Current: "v", Invoked: 30, Completed: 40},
	}, stateLimit: 100}
	if !valid.search(3, map[string]value{}) {
		t.Fatal("checker rejected a legal compare-and-set history")
	}
	invalid := checker{operations: []operation{
		{Kind: "put", Key: "k", Value: "v", Invoked: 10, Completed: 20},
		{Kind: "compare_and_set", Key: "k", ExpectedExists: false, Swapped: true, Value: "x", Invoked: 30, Completed: 40},
	}, stateLimit: 100}
	if invalid.search(3, map[string]value{}) {
		t.Fatal("checker accepted an impossible compare-and-set success")
	}
}
