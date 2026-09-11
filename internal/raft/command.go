package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// CommandVersion is included in every replicated command so future changes can
// be rejected deterministically instead of being misinterpreted by an older
// replica.
const CommandVersion byte = 1

type CommandType byte

const (
	CommandPut CommandType = iota + 1
	CommandDelete
	CommandCompareAndSet
	CommandNoop
)

const commandHeaderSize = 1 + 1 + 1 + 4 + 4 + 4

// Command is the application-level payload replicated by Raft. ExpectedExists
// is meaningful only for compare-and-set and distinguishes an absent key from a
// key holding an empty value.
type Command struct {
	Type           CommandType
	Key            []byte
	Value          []byte
	Expected       []byte
	ExpectedExists bool
}

func (c Command) MarshalBinary() ([]byte, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if uint64(len(c.Key)) > uint64(^uint32(0)) || uint64(len(c.Value)) > uint64(^uint32(0)) || uint64(len(c.Expected)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("raft command field exceeds 4 GiB")
	}

	encoded := make([]byte, commandHeaderSize+len(c.Key)+len(c.Value)+len(c.Expected))
	encoded[0] = CommandVersion
	encoded[1] = byte(c.Type)
	if c.ExpectedExists {
		encoded[2] = 1
	}
	binary.LittleEndian.PutUint32(encoded[3:7], uint32(len(c.Key)))
	binary.LittleEndian.PutUint32(encoded[7:11], uint32(len(c.Value)))
	binary.LittleEndian.PutUint32(encoded[11:15], uint32(len(c.Expected)))
	offset := commandHeaderSize
	offset += copy(encoded[offset:], c.Key)
	offset += copy(encoded[offset:], c.Value)
	copy(encoded[offset:], c.Expected)
	return encoded, nil
}

func UnmarshalCommand(encoded []byte) (Command, error) {
	if len(encoded) < commandHeaderSize {
		return Command{}, fmt.Errorf("raft command is shorter than its header")
	}
	if encoded[0] != CommandVersion {
		return Command{}, fmt.Errorf("unsupported raft command version %d", encoded[0])
	}
	if encoded[2] > 1 {
		return Command{}, fmt.Errorf("invalid expected-exists flag %d", encoded[2])
	}
	keyLength := uint64(binary.LittleEndian.Uint32(encoded[3:7]))
	valueLength := uint64(binary.LittleEndian.Uint32(encoded[7:11]))
	expectedLength := uint64(binary.LittleEndian.Uint32(encoded[11:15]))
	totalLength := uint64(commandHeaderSize) + keyLength + valueLength + expectedLength
	if totalLength != uint64(len(encoded)) {
		return Command{}, fmt.Errorf("raft command length does not match its header")
	}

	offset := commandHeaderSize
	keyEnd := offset + int(keyLength)
	valueEnd := keyEnd + int(valueLength)
	command := Command{
		Type:           CommandType(encoded[1]),
		Key:            bytes.Clone(encoded[offset:keyEnd]),
		Value:          bytes.Clone(encoded[keyEnd:valueEnd]),
		Expected:       bytes.Clone(encoded[valueEnd:]),
		ExpectedExists: encoded[2] == 1,
	}
	if err := command.validate(); err != nil {
		return Command{}, err
	}
	return command, nil
}

func (c Command) validate() error {
	if c.Type != CommandNoop && len(c.Key) == 0 {
		return fmt.Errorf("raft command key is required")
	}
	switch c.Type {
	case CommandPut:
		if c.ExpectedExists || len(c.Expected) != 0 {
			return fmt.Errorf("put command cannot have an expected value")
		}
	case CommandDelete:
		if len(c.Value) != 0 || c.ExpectedExists || len(c.Expected) != 0 {
			return fmt.Errorf("delete command cannot have a value or expected value")
		}
	case CommandCompareAndSet:
		// All fields are valid. ExpectedExists distinguishes missing and empty.
	case CommandNoop:
		if len(c.Key) != 0 || len(c.Value) != 0 || c.ExpectedExists || len(c.Expected) != 0 {
			return fmt.Errorf("noop command cannot carry key or value data")
		}
	default:
		return fmt.Errorf("unknown raft command type %d", c.Type)
	}
	return nil
}
