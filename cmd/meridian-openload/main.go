// meridian-openload schedules requests at a fixed offered rate. Unlike the
// closed-loop development driver, a slow request does not delay the next
// scheduled request, so its JSONL history can expose queueing and tail latency.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nickemma/meridian/client"
	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc/status"
)

type workload struct {
	Name       string       `json:"name"`
	Seed       int64        `json:"seed"`
	Operations []workloadOp `json:"operations"`
}

type workloadOp struct {
	Name        string  `json:"name"`
	Share       float64 `json:"share"`
	Consistency string  `json:"consistency"`
	Operation   string  `json:"operation"`
	Namespace   string  `json:"namespace"`
}

type operationRecord struct {
	Sequence       uint64            `json:"sequence"`
	Worker         int               `json:"worker"`
	Workload       string            `json:"workload"`
	Kind           string            `json:"kind"`
	Class          string            `json:"class"`
	Key            string            `json:"key"`
	Scheduled      int64             `json:"scheduled_unix_nano"`
	Invoked        int64             `json:"invoked_unix_nano"`
	Completed      int64             `json:"completed_unix_nano"`
	LatencyNS      int64             `json:"latency_ns"`
	Outcome        string            `json:"outcome"`
	Error          string            `json:"error,omitempty"`
	Found          bool              `json:"found,omitempty"`
	Value          string            `json:"value_base64,omitempty"`
	Returned       string            `json:"returned_value_base64,omitempty"`
	Expected       string            `json:"expected_value_base64,omitempty"`
	ExpectedExists bool              `json:"expected_exists"`
	Swapped        bool              `json:"swapped,omitempty"`
	Current        string            `json:"current_value_base64,omitempty"`
	CurrentExists  bool              `json:"current_exists"`
	RaftIndex      uint64            `json:"raft_index,omitempty"`
	Context        responseContext   `json:"response_context,omitempty"`
	Versions       []observedVersion `json:"versions,omitempty"`
}

type responseContext struct {
	Vector        string `json:"vector_clock_base64,omitempty"`
	RaftIndex     uint64 `json:"raft_index,omitempty"`
	PolicyVersion uint64 `json:"policy_version,omitempty"`
}

type observedVersion struct {
	Value     string          `json:"value_base64,omitempty"`
	Tombstone bool            `json:"tombstone,omitempty"`
	Context   responseContext `json:"context"`
}

type summary struct {
	Mode       string  `json:"mode"`
	Workload   string  `json:"workload"`
	Scheduled  uint64  `json:"scheduled_operations"`
	Completed  uint64  `json:"completed_operations"`
	Successes  uint64  `json:"successes"`
	Errors     uint64  `json:"errors"`
	DurationMS float64 `json:"duration_ms"`
	Offered    float64 `json:"offered_ops_per_second"`
	Goodput    float64 `json:"goodput_ops_per_second"`
}

func main() {
	targetsFlag := flag.String("targets", "127.0.0.1:8080", "comma-separated Meridian client gRPC addresses")
	workloadPath := flag.String("workload", "bench/workloads/mixed.json", "workload manifest")
	duration := flag.Duration("duration", 30*time.Second, "offered-load measurement duration")
	rate := flag.Float64("rate", 100, "offered operations per second")
	maxInflight := flag.Int("max-inflight", 128, "maximum simultaneously executing requests")
	valueSize := flag.Int("value-size", 128, "value size in bytes")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request deadline")
	rawPath := flag.String("raw", "", "JSONL output path; stdout when empty")
	flag.Parse()
	if *duration <= 0 || *rate <= 0 || *maxInflight <= 0 || *valueSize < 0 || *timeout <= 0 {
		fatal(fmt.Errorf("duration, rate, max-inflight, value-size, and timeout must be positive"))
	}

	spec, err := loadWorkload(*workloadPath)
	if err != nil {
		fatal(err)
	}
	writer, closeWriter, err := rawWriter(*rawPath)
	if err != nil {
		fatal(err)
	}
	defer closeWriter()
	encoder := json.NewEncoder(writer)
	meridian, err := client.New(splitTargets(*targetsFlag))
	if err != nil {
		fatal(err)
	}
	defer meridian.Close()

	value := make([]byte, *valueSize)
	for index := range value {
		value[index] = byte('a' + index%26)
	}
	count := uint64(*rate * duration.Seconds())
	if count == 0 {
		fatal(fmt.Errorf("duration and rate schedule zero operations"))
	}
	choices := chooseOperations(spec, count)
	start := time.Now()
	semaphore := make(chan struct{}, *maxInflight)
	var completed, successes, failures uint64
	var outputMu sync.Mutex
	var wait sync.WaitGroup

	for sequence := uint64(0); sequence < count; sequence++ {
		scheduled := start.Add(time.Duration(float64(sequence) / *rate * float64(time.Second)))
		if delay := time.Until(scheduled); delay > 0 {
			time.Sleep(delay)
		}
		semaphore <- struct{}{}
		wait.Add(1)
		choice := choices[sequence]
		go func(sequence uint64, scheduled time.Time, choice workloadOp) {
			defer wait.Done()
			defer func() { <-semaphore }()
			invoked := time.Now()
			record := runOperation(meridian, spec.Name, int(sequence%uint64(*maxInflight)), sequence, scheduled, invoked, choice, value, *timeout)
			if record.Outcome == "ok" {
				atomic.AddUint64(&successes, 1)
			} else {
				atomic.AddUint64(&failures, 1)
			}
			atomic.AddUint64(&completed, 1)
			outputMu.Lock()
			err := encoder.Encode(record)
			outputMu.Unlock()
			if err != nil {
				fatal(fmt.Errorf("write raw result: %w", err))
			}
		}(sequence, scheduled, choice)
	}
	wait.Wait()
	elapsed := time.Since(start)
	result := summary{Mode: "open-loop", Workload: spec.Name, Scheduled: count, Completed: completed, Successes: successes, Errors: failures, DurationMS: float64(elapsed) / float64(time.Millisecond), Offered: float64(count) / elapsed.Seconds(), Goodput: float64(successes) / elapsed.Seconds()}
	if err := json.NewEncoder(os.Stderr).Encode(result); err != nil {
		fatal(err)
	}
}

func runOperation(meridian *client.Client, workloadName string, worker int, sequence uint64, scheduled, invoked time.Time, choice workloadOp, value []byte, timeout time.Duration) operationRecord {
	class, policyVersion, err := consistency(choice.Consistency)
	record := operationRecord{Sequence: sequence, Worker: worker, Workload: workloadName, Kind: choice.Operation, Class: choice.Consistency, Key: fmt.Sprintf("%skey-%08d", choice.Namespace, sequence%1024), Scheduled: scheduled.UnixNano(), Invoked: invoked.UnixNano()}
	if err != nil {
		record.Outcome, record.Error = "InvalidWorkload", err.Error()
		finish(&record, invoked)
		return record
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	requestID := fmt.Sprintf("open-%d", sequence)
	requestContext := &kv.CausalContext{PolicyVersion: policyVersion}
	switch choice.Operation {
	case "get":
		response, callErr := meridian.Get(ctx, &kv.GetRequest{RequestId: requestID, Key: []byte(record.Key), Consistency: class, Context: requestContext, DeadlineUnixNano: time.Now().Add(timeout).UnixNano()})
		if callErr == nil {
			record.Found = response.GetFound()
			record.Returned = base64.StdEncoding.EncodeToString(response.GetValue())
			record.RaftIndex = response.GetRaftIndex()
			record.Context = contextRecord(response.GetContext())
			record.Versions = observedVersions(response.GetVersions())
		}
		record.Outcome, record.Error = outcome(callErr)
	case "put":
		response, callErr := meridian.Put(ctx, &kv.PutRequest{RequestId: requestID, Key: []byte(record.Key), Value: value, Consistency: class, Context: requestContext, DeadlineUnixNano: time.Now().Add(timeout).UnixNano()})
		record.Value = base64.StdEncoding.EncodeToString(value)
		if callErr == nil {
			record.RaftIndex = response.GetRaftIndex()
			record.Context = contextRecord(response.GetContext())
		}
		record.Outcome, record.Error = outcome(callErr)
	case "compare_and_set":
		expectedExists := false
		response, callErr := meridian.CompareAndSet(ctx, &kv.CompareAndSetRequest{RequestId: requestID, Key: []byte(record.Key), ExpectedExists: expectedExists, Value: value, Consistency: class, Context: requestContext, DeadlineUnixNano: time.Now().Add(timeout).UnixNano()})
		record.ExpectedExists = expectedExists
		if callErr == nil {
			record.Swapped = response.GetSwapped()
			record.CurrentExists = response.GetCurrentExists()
			record.Current = base64.StdEncoding.EncodeToString(response.GetCurrentValue())
			record.RaftIndex = response.GetRaftIndex()
			record.Context = contextRecord(response.GetContext())
		}
		record.Outcome, record.Error = outcome(callErr)
		record.Value = base64.StdEncoding.EncodeToString(value)
	default:
		record.Outcome, record.Error = "InvalidWorkload", fmt.Sprintf("unsupported operation %q", choice.Operation)
	}
	finish(&record, invoked)
	return record
}

func finish(record *operationRecord, invoked time.Time) {
	completed := time.Now()
	record.Completed = completed.UnixNano()
	record.LatencyNS = completed.Sub(invoked).Nanoseconds()
}

func consistency(name string) (kv.Consistency, uint64, error) {
	switch name {
	case "strong":
		return kv.Consistency_CONSISTENCY_STRONG, 1, nil
	case "causal":
		return kv.Consistency_CONSISTENCY_CAUSAL, 2, nil
	case "eventual":
		return kv.Consistency_CONSISTENCY_EVENTUAL, 3, nil
	default:
		return kv.Consistency_CONSISTENCY_UNSPECIFIED, 0, fmt.Errorf("unsupported consistency %q", name)
	}
}

func contextRecord(source *kv.CausalContext) responseContext {
	if source == nil {
		return responseContext{}
	}
	return responseContext{Vector: base64.StdEncoding.EncodeToString(source.GetVectorClock()), RaftIndex: source.GetRaftIndex(), PolicyVersion: source.GetPolicyVersion()}
}

func observedVersions(source []*kv.VersionedValue) []observedVersion {
	if len(source) == 0 {
		return nil
	}
	result := make([]observedVersion, 0, len(source))
	for _, version := range source {
		result = append(result, observedVersion{Value: base64.StdEncoding.EncodeToString(version.GetValue()), Tombstone: version.GetTombstone(), Context: contextRecord(version.GetContext())})
	}
	return result
}

func loadWorkload(path string) (workload, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return workload{}, fmt.Errorf("read workload: %w", err)
	}
	var spec workload
	if err := json.Unmarshal(data, &spec); err != nil {
		return workload{}, fmt.Errorf("decode workload: %w", err)
	}
	if spec.Name == "" || len(spec.Operations) == 0 {
		return workload{}, fmt.Errorf("workload name and operations are required")
	}
	var share float64
	for _, operation := range spec.Operations {
		if operation.Name == "" || operation.Share <= 0 || operation.Namespace == "" {
			return workload{}, fmt.Errorf("invalid workload operation %#v", operation)
		}
		if _, _, err := consistency(operation.Consistency); err != nil {
			return workload{}, err
		}
		share += operation.Share
	}
	if share < .999999 || share > 1.000001 {
		return workload{}, fmt.Errorf("workload shares sum to %g, want 1", share)
	}
	return spec, nil
}

func chooseOperations(spec workload, count uint64) []workloadOp {
	random := rand.New(rand.NewSource(spec.Seed))
	choices := make([]workloadOp, count)
	for index := range choices {
		selection := random.Float64()
		var total float64
		for _, operation := range spec.Operations {
			total += operation.Share
			if selection < total {
				choices[index] = operation
				break
			}
		}
		if choices[index].Name == "" {
			choices[index] = spec.Operations[len(spec.Operations)-1]
		}
	}
	return choices
}

func rawWriter(path string) (io.Writer, func() error, error) {
	if path == "" {
		return os.Stdout, func() error { return nil }, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return file, file.Close, nil
}

func splitTargets(raw string) []string {
	var targets []string
	for _, target := range strings.Split(raw, ",") {
		if target = strings.TrimSpace(target); target != "" {
			targets = append(targets, target)
		}
	}
	return targets
}

func outcome(err error) (string, string) {
	if err == nil {
		return "ok", ""
	}
	return status.Code(err).String(), err.Error()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
