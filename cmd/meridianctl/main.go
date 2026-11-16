package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nickemma/meridian/client"
	security "github.com/nickemma/meridian/internal/security/adapters"
	kv "github.com/nickemma/meridian/proto/kv"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	targets := flag.String("targets", "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083", "comma-separated client endpoints; weak operations use first")
	operation := flag.String("op", "status", "status|get|put|delete|cas")
	key := flag.String("key", "", "absolute namespace key")
	value := flag.String("value", "", "UTF-8 value")
	expected := flag.String("expected", "", "expected CAS value")
	expectedExists := flag.Bool("expected-exists", true, "CAS expected existence")
	class := flag.String("consistency", "strong", "strong|causal|eventual")
	version := flag.Uint64("policy-version", 1, "namespace policy version on writes")
	contextFile := flag.String("context-in", "", "JSON causal context from prior --context-out")
	contextOut := flag.String("context-out", "", "save returned causal context to this file")
	ca := flag.String("ca", "", "TLS CA file; all three TLS files required together")
	cert := flag.String("cert", "", "client certificate file")
	privateKey := flag.String("tls-key", "", "client private key file")
	serverName := flag.String("server-name", "", "optional TLS destination DNS override")
	timeout := flag.Duration("timeout", 5*time.Second, "whole-operation timeout, including leader discovery")
	flag.Parse()
	consistency := kv.Consistency_CONSISTENCY_STRONG
	switch *class {
	case "strong":
	case "causal":
		consistency = kv.Consistency_CONSISTENCY_CAUSAL
	case "eventual":
		consistency = kv.Consistency_CONSISTENCY_EVENTUAL
	default:
		return fmt.Errorf("invalid consistency")
	}
	var transport credentials.TransportCredentials = insecure.NewCredentials()
	if *ca != "" || *cert != "" || *privateKey != "" {
		var err error
		transport, err = security.ClientCredentials(*ca, *cert, *privateKey, *serverName)
		if err != nil {
			return err
		}
	}
	c, err := client.NewWithCredentials(strings.Split(*targets, ","), transport)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	causal := &kv.CausalContext{PolicyVersion: *version}
	if *contextFile != "" {
		data, err := os.ReadFile(*contextFile)
		if err != nil {
			return err
		}
		if err := protojson.Unmarshal(data, causal); err != nil {
			return err
		}
	}
	id := fmt.Sprintf("ctl-%d", time.Now().UnixNano())
	var response proto.Message
	switch *operation {
	case "status":
		response, err = c.Status(ctx)
	case "get":
		response, err = c.Get(ctx, &kv.GetRequest{RequestId: id, Key: []byte(*key), Consistency: consistency, Context: causal})
	case "put":
		response, err = c.Put(ctx, &kv.PutRequest{RequestId: id, Key: []byte(*key), Value: []byte(*value), Consistency: consistency, Context: causal})
	case "delete":
		response, err = c.Delete(ctx, &kv.DeleteRequest{RequestId: id, Key: []byte(*key), Consistency: consistency, Context: causal})
	case "cas":
		response, err = c.CompareAndSet(ctx, &kv.CompareAndSetRequest{RequestId: id, Key: []byte(*key), Value: []byte(*value), ExpectedValue: []byte(*expected), ExpectedExists: *expectedExists, Consistency: consistency, Context: causal})
	default:
		return fmt.Errorf("invalid operation")
	}
	if err != nil {
		return err
	}
	if *contextOut != "" {
		if r, ok := response.(interface{ GetContext() *kv.CausalContext }); ok && r.GetContext() != nil {
			data, err := protojson.Marshal(r.GetContext())
			if err != nil {
				return err
			}
			if err := os.WriteFile(*contextOut, data, 0600); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("operation returned no causal context")
		}
	}
	// Standard protobuf JSON encodes bytes as base64; add a text rendering for demos.
	data, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(response)
	if err != nil {
		return err
	}
	output := map[string]any{}
	if err := json.Unmarshal(data, &output); err != nil {
		return err
	}
	if get, ok := response.(*kv.GetResponse); ok {
		output["value_text"] = string(get.Value)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}
