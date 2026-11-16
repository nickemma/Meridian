package domain

import (
	"bytes"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	input := Command{
		Type:           CommandCompareAndSet,
		Key:            []byte("key"),
		Value:          []byte("new"),
		Expected:       []byte("old"),
		ExpectedExists: true,
	}
	encoded, err := input.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	got, err := UnmarshalCommand(encoded)
	if err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	if got.Type != input.Type || !bytes.Equal(got.Key, input.Key) || !bytes.Equal(got.Value, input.Value) || !bytes.Equal(got.Expected, input.Expected) || got.ExpectedExists != input.ExpectedExists {
		t.Fatalf("round trip = %+v, want %+v", got, input)
	}
}

func TestCommandRejectsMalformedPayload(t *testing.T) {
	for _, encoded := range [][]byte{
		nil,
		{CommandVersion},
		{CommandVersion, byte(CommandPut), 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{2, byte(CommandPut), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	} {
		if _, err := UnmarshalCommand(encoded); err == nil {
			t.Fatalf("UnmarshalCommand(%v) succeeded", encoded)
		}
	}
}
