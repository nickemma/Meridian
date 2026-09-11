// meridian-check performs an exhaustive, bounded linearizability check for the
// single-register strong histories emitted by meridian-load. It is a useful
// fault-smoke checker, not a replacement for Porcupine on large histories.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type operation struct {
	Sequence  uint64 `json:"sequence"`
	Kind      string `json:"kind"`
	Class     string `json:"class"`
	Key       string `json:"key"`
	Invoked   int64  `json:"invoked_unix_nano"`
	Completed int64  `json:"completed_unix_nano"`
	Outcome   string `json:"outcome"`
	Found     bool   `json:"found,omitempty"`
	Value     string `json:"value_base64,omitempty"`
	Returned  string `json:"returned_value_base64,omitempty"`
}

type result struct {
	Linearizable bool   `json:"linearizable"`
	Checked      int    `json:"checked_operations"`
	Explored     uint64 `json:"explored_states"`
	Reason       string `json:"reason,omitempty"`
}

type value struct {
	found bool
	data  string
}

func main() {
	historyPath := flag.String("history", "", "JSONL history emitted by meridian-load")
	limit := flag.Int("limit", 20, "maximum successful strong operations to check exhaustively")
	flag.Parse()
	if *historyPath == "" || *limit <= 0 {
		fmt.Fprintln(os.Stderr, "history and positive limit are required")
		os.Exit(2)
	}
	operations, err := load(*historyPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if len(operations) > *limit {
		write(result{Checked: len(operations), Reason: fmt.Sprintf("history exceeds exhaustive limit %d", *limit)})
		os.Exit(2)
	}
	checker := checker{operations: operations, stateLimit: 1_000_000}
	ok := checker.search((uint64(1)<<len(operations))-1, map[string]value{})
	response := result{Linearizable: ok, Checked: len(operations), Explored: checker.explored}
	if checker.exhausted {
		response.Linearizable = false
		response.Reason = "search-state limit exhausted"
	} else if !ok {
		response.Reason = "no legal sequential history respects real-time order and returned reads"
	}
	write(response)
	if !response.Linearizable {
		os.Exit(1)
	}
}

func load(path string) ([]operation, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var operations []operation
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var operation operation
		if err := json.Unmarshal(scanner.Bytes(), &operation); err != nil {
			return nil, fmt.Errorf("decode history: %w", err)
		}
		if operation.Class == "strong" && operation.Outcome == "ok" && (operation.Kind == "get" || operation.Kind == "put") {
			operations = append(operations, operation)
		}
	}
	return operations, scanner.Err()
}

type checker struct {
	operations []operation
	explored   uint64
	stateLimit uint64
	exhausted  bool
}

func (c *checker) search(remaining uint64, state map[string]value) bool {
	if remaining == 0 {
		return true
	}
	if c.explored >= c.stateLimit {
		c.exhausted = true
		return false
	}
	c.explored++
	for index, operation := range c.operations {
		bit := uint64(1) << index
		if remaining&bit == 0 || c.hasPredecessor(index, remaining) || !legal(operation, state) {
			continue
		}
		next := copyState(state)
		apply(operation, next)
		if c.search(remaining&^bit, next) {
			return true
		}
	}
	return false
}

func (c *checker) hasPredecessor(index int, remaining uint64) bool {
	operation := c.operations[index]
	for otherIndex, other := range c.operations {
		if otherIndex != index && remaining&(uint64(1)<<otherIndex) != 0 && other.Completed < operation.Invoked {
			return true
		}
	}
	return false
}

func legal(operation operation, state map[string]value) bool {
	if operation.Kind != "get" {
		return true
	}
	current := state[operation.Key]
	return current.found == operation.Found && (!current.found || current.data == operation.Returned)
}

func apply(operation operation, state map[string]value) {
	if operation.Kind == "put" {
		state[operation.Key] = value{found: true, data: operation.Value}
	}
}

func copyState(source map[string]value) map[string]value {
	target := make(map[string]value, len(source))
	for key, value := range source {
		target[key] = value
	}
	return target
}

func write(value result) {
	_ = json.NewEncoder(os.Stdout).Encode(value)
}
