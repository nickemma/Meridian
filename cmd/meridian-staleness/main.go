// meridian-staleness derives conservative, client-observable staleness data
// from an operation history emitted by meridian-openload. It never invents a
// total order for concurrent weak writes: such reads are reported separately.
package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/nickemma/meridian/internal/consistency"
)

type record struct {
	Kind      string        `json:"kind"`
	Class     string        `json:"class"`
	Key       string        `json:"key"`
	Invoked   int64         `json:"invoked_unix_nano"`
	Completed int64         `json:"completed_unix_nano"`
	Outcome   string        `json:"outcome"`
	Value     string        `json:"value_base64,omitempty"`
	Returned  string        `json:"returned_value_base64,omitempty"`
	Found     bool          `json:"found,omitempty"`
	Swapped   bool          `json:"swapped,omitempty"`
	RaftIndex uint64        `json:"raft_index,omitempty"`
	Context   contextRecord `json:"response_context,omitempty"`
	Versions  []version     `json:"versions,omitempty"`
}

type contextRecord struct {
	Vector    string `json:"vector_clock_base64,omitempty"`
	RaftIndex uint64 `json:"raft_index,omitempty"`
}

type version struct {
	Value     string        `json:"value_base64,omitempty"`
	Tombstone bool          `json:"tombstone,omitempty"`
	Context   contextRecord `json:"context"`
}

type write struct {
	completed int64
	raftIndex uint64
	vector    consistency.VersionVector
}

type classSummary struct {
	EligibleReads        int     `json:"eligible_reads"`
	KnownStaleReads      int     `json:"known_stale_reads"`
	ConcurrentExclusions int     `json:"concurrent_write_exclusions"`
	UnknownMetadata      int     `json:"unknown_metadata"`
	StaleFraction        float64 `json:"known_stale_fraction"`
	MedianLagMS          float64 `json:"median_staleness_ms,omitempty"`
	P95LagMS             float64 `json:"p95_staleness_ms,omitempty"`
	MaxLagMS             float64 `json:"max_staleness_ms,omitempty"`
}

type output struct {
	History string                  `json:"history"`
	Classes map[string]classSummary `json:"classes"`
	Notes   []string                `json:"notes"`
}

func main() {
	history := flag.String("history", "", "JSONL history emitted by meridian-openload")
	flag.Parse()
	if *history == "" {
		fmt.Fprintln(os.Stderr, "history is required")
		os.Exit(2)
	}
	records, err := load(*history)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	result := analyze(records)
	result.History = *history
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func load(path string) ([]record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var records []record
	scanner := bufio.NewScanner(file)
	buffer := make([]byte, 0, 64*1024)
	scanner.Buffer(buffer, 8*1024*1024)
	for scanner.Scan() {
		var record record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode history: %w", err)
		}
		records = append(records, record)
	}
	return records, scanner.Err()
}

func analyze(records []record) output {
	result := output{Classes: map[string]classSummary{}, Notes: []string{
		"A read is eligible only when this history contains a completed successful write to the same key before the read began.",
		"A known-stale read misses a prior, non-concurrent write. Concurrent weak writes are excluded rather than assigned a fabricated order.",
		"The reported lag is from the missed write's completion to the read response. It is a lower bound on client-observable staleness, not a network propagation measurement.",
	}}
	writes := make(map[string][]write)
	lags := make(map[string][]float64)
	for _, record := range records {
		if record.Outcome != "ok" || (record.Kind != "put" && !(record.Kind == "compare_and_set" && record.Swapped)) {
			continue
		}
		item, ok := toWrite(record)
		if !ok {
			continue
		}
		writes[record.Class+"\x00"+record.Key] = append(writes[record.Class+"\x00"+record.Key], item)
	}
	for _, record := range records {
		if record.Outcome != "ok" || record.Kind != "get" {
			continue
		}
		key := record.Class + "\x00" + record.Key
		candidates := writes[key]
		latest, eligible := latestBefore(candidates, record.Invoked)
		if !eligible {
			continue
		}
		summary := result.Classes[record.Class]
		summary.EligibleReads++
		visible, metadata := readObserves(record, latest)
		if !metadata {
			summary.UnknownMetadata++
		} else if !visible {
			summary.KnownStaleReads++
			lag := float64(record.Completed-latest.completed) / 1e6
			lags[record.Class] = append(lags[record.Class], lag)
		}
		result.Classes[record.Class] = summary
	}
	for class, summary := range result.Classes {
		if summary.EligibleReads > 0 {
			summary.StaleFraction = float64(summary.KnownStaleReads) / float64(summary.EligibleReads)
		}
		if samples := lags[class]; len(samples) > 0 {
			sort.Float64s(samples)
			summary.MedianLagMS = percentile(samples, .50)
			summary.P95LagMS = percentile(samples, .95)
			summary.MaxLagMS = samples[len(samples)-1]
		}
		result.Classes[class] = summary
	}
	return result
}

func toWrite(record record) (write, bool) {
	item := write{completed: record.Completed, raftIndex: record.RaftIndex}
	if record.Class == "strong" {
		return item, item.raftIndex > 0
	}
	vector, ok := decodeVector(record.Context.Vector)
	return write{completed: record.Completed, vector: vector}, ok
}

func latestBefore(writes []write, invoked int64) (write, bool) {
	var latest write
	for _, item := range writes {
		if item.completed <= invoked && (latest.completed == 0 || item.completed > latest.completed) {
			latest = item
		}
	}
	return latest, latest.completed != 0
}

func readObserves(record record, latest write) (visible, metadata bool) {
	if record.Class == "strong" {
		return record.RaftIndex >= latest.raftIndex, record.RaftIndex > 0
	}
	for _, item := range record.Versions {
		vector, ok := decodeVector(item.Context.Vector)
		if !ok {
			continue
		}
		metadata = true
		if vector.Dominates(latest.vector) {
			return true, true
		}
	}
	return false, metadata
}

func decodeVector(encoded string) (consistency.VersionVector, bool) {
	if encoded == "" {
		return nil, false
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, false
	}
	vector, err := consistency.UnmarshalVersionVector(data)
	return vector, err == nil
}

func percentile(samples []float64, fraction float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	index := int(fraction*float64(len(samples)-1) + .5)
	return samples[index]
}
