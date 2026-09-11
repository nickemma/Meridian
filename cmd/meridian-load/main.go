// meridian-load is a deliberately small, reproducible client driver. It emits
// one JSON record per operation plus a percentile summary; analysis code should
// consume the raw records rather than attempting to reconstruct latency from
// server logs.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type operation struct {
	Sequence  uint64 `json:"sequence"`
	Worker    int    `json:"worker"`
	Kind      string `json:"kind"`
	Class     string `json:"class"`
	Key       string `json:"key"`
	Scheduled int64  `json:"scheduled_unix_nano"`
	Invoked   int64  `json:"invoked_unix_nano"`
	Completed int64  `json:"completed_unix_nano"`
	LatencyNS int64  `json:"latency_ns"`
	Outcome   string `json:"outcome"`
	Error     string `json:"error,omitempty"`
	Found     bool   `json:"found,omitempty"`
	Value     string `json:"value_base64,omitempty"`
	Returned  string `json:"returned_value_base64,omitempty"`
}

type summary struct {
	Mode       string  `json:"mode"`
	Operations uint64  `json:"operations"`
	Successes  uint64  `json:"successes"`
	Errors     uint64  `json:"errors"`
	DurationMS float64 `json:"duration_ms"`
	Throughput float64 `json:"throughput_ops_per_second"`
	P50MS      float64 `json:"p50_ms"`
	P95MS      float64 `json:"p95_ms"`
	P99MS      float64 `json:"p99_ms"`
	P999MS     float64 `json:"p99_9_ms"`
}

func main() {
	target := flag.String("target", "127.0.0.1:8080", "Meridian client gRPC address")
	classFlag := flag.String("consistency", "strong", "strong, causal, or eventual")
	keyPrefix := flag.String("key-prefix", "", "absolute namespace prefix; default follows consistency")
	policyVersion := flag.Uint64("policy-version", 1, "immutable namespace policy version")
	operations := flag.Uint64("operations", 10_000, "total operations to issue")
	concurrency := flag.Int("concurrency", 16, "closed-loop worker count")
	readPercent := flag.Int("read-percent", 50, "percentage of Get operations")
	valueSize := flag.Int("value-size", 128, "value size in bytes")
	timeout := flag.Duration("timeout", 2*time.Second, "per-request deadline")
	rawPath := flag.String("raw", "", "JSONL output path; stdout when empty")
	flag.Parse()

	class, defaultPrefix, err := parseClass(*classFlag)
	if err != nil {
		fatal(err)
	}
	if *keyPrefix == "" {
		*keyPrefix = defaultPrefix
	}
	if *operations == 0 || *concurrency <= 0 || *readPercent < 0 || *readPercent > 100 || *valueSize < 0 || *policyVersion == 0 {
		fatal(fmt.Errorf("operations, concurrency, policy-version, value-size, and read-percent are invalid"))
	}

	writer, closeWriter, err := rawWriter(*rawPath)
	if err != nil {
		fatal(err)
	}
	defer closeWriter()
	encoder := json.NewEncoder(writer)

	connection, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(fmt.Errorf("connect: %w", err))
	}
	defer connection.Close()
	client := kv.NewKVServiceClient(connection)

	value := make([]byte, *valueSize)
	for index := range value {
		value[index] = byte('a' + index%26)
	}
	started := time.Now()
	var sequence, successes, failures uint64
	latencies := make([]time.Duration, 0, *operations)
	var latencyMu, outputMu sync.Mutex
	var workers sync.WaitGroup
	for worker := 0; worker < *concurrency; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			var causalContext *kv.CausalContext
			for {
				index := atomic.AddUint64(&sequence, 1) - 1
				if index >= *operations {
					return
				}
				now := time.Now()
				record := operation{Sequence: index, Worker: worker, Class: *classFlag, Key: fmt.Sprintf("%sload-%08d", *keyPrefix, index%1024), Scheduled: now.UnixNano(), Invoked: now.UnixNano()}
				requestContext, cancel := context.WithTimeout(context.Background(), *timeout)
				requestID := fmt.Sprintf("load-%d-%d", worker, index)
				contextValue := causalContext
				if contextValue == nil {
					contextValue = &kv.CausalContext{PolicyVersion: *policyVersion}
				}
				if int(index%100) < *readPercent {
					record.Kind = "get"
					response, callErr := client.Get(requestContext, &kv.GetRequest{RequestId: requestID, Key: []byte(record.Key), Consistency: class, Context: contextValue})
					if callErr == nil && class == kv.Consistency_CONSISTENCY_CAUSAL {
						causalContext = response.GetContext()
					}
					if callErr == nil {
						record.Found = response.GetFound()
						record.Returned = base64.StdEncoding.EncodeToString(response.GetValue())
					}
					record.Outcome, record.Error = outcome(callErr)
				} else {
					record.Kind = "put"
					response, callErr := client.Put(requestContext, &kv.PutRequest{RequestId: requestID, Key: []byte(record.Key), Value: value, Consistency: class, Context: contextValue})
					if callErr == nil && class == kv.Consistency_CONSISTENCY_CAUSAL {
						causalContext = response.GetContext()
					}
					record.Value = base64.StdEncoding.EncodeToString(value)
					record.Outcome, record.Error = outcome(callErr)
				}
				cancel()
				completed := time.Now()
				record.Completed, record.LatencyNS = completed.UnixNano(), completed.Sub(now).Nanoseconds()
				latencyMu.Lock()
				latencies = append(latencies, time.Duration(record.LatencyNS))
				latencyMu.Unlock()
				if record.Outcome == "ok" {
					atomic.AddUint64(&successes, 1)
				} else {
					atomic.AddUint64(&failures, 1)
				}
				outputMu.Lock()
				err := encoder.Encode(record)
				outputMu.Unlock()
				if err != nil {
					fatal(fmt.Errorf("write raw result: %w", err))
				}
			}
		}(worker)
	}
	workers.Wait()
	duration := time.Since(started)
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	result := summary{Mode: "closed-loop", Operations: *operations, Successes: successes, Errors: failures, DurationMS: float64(duration) / float64(time.Millisecond)}
	if duration > 0 {
		result.Throughput = float64(successes) / duration.Seconds()
	}
	result.P50MS = percentile(latencies, 0.50)
	result.P95MS = percentile(latencies, 0.95)
	result.P99MS = percentile(latencies, 0.99)
	result.P999MS = percentile(latencies, 0.999)
	if err := json.NewEncoder(os.Stderr).Encode(result); err != nil {
		fatal(err)
	}
}

func parseClass(value string) (kv.Consistency, string, error) {
	switch value {
	case "strong":
		return kv.Consistency_CONSISTENCY_STRONG, "/strong/", nil
	case "causal":
		return kv.Consistency_CONSISTENCY_CAUSAL, "/causal/", nil
	case "eventual":
		return kv.Consistency_CONSISTENCY_EVENTUAL, "/eventual/", nil
	default:
		return kv.Consistency_CONSISTENCY_UNSPECIFIED, "", fmt.Errorf("unknown consistency %q", value)
	}
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

func outcome(err error) (string, string) {
	if err == nil {
		return "ok", ""
	}
	return status.Code(err).String(), err.Error()
}

func percentile(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * percentile)
	return float64(values[index]) / float64(time.Millisecond)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
