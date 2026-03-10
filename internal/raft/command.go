package raft

import (
	"encoding/json"
	"fmt"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// CommandType identifies the type of RAFT command.
type CommandType uint8

const (
	// CmdSet writes a key-value pair.
	CmdSet CommandType = iota + 1
	// CmdDelete marks a key as deleted (tombstone).
	CmdDelete
	// CmdCreateNode creates a graph node with metadata, properties, and adjacency lists.
	CmdCreateNode
	// CmdCreateEdge creates a graph edge with metadata, properties, and adjacency updates.
	CmdCreateEdge
	// CmdBatchSet writes multiple key-value pairs atomically.
	CmdBatchSet
)

// String returns the command type name for logging.
func (ct CommandType) String() string {
	switch ct {
	case CmdSet:
		return "CmdSet"
	case CmdDelete:
		return "CmdDelete"
	case CmdCreateNode:
		return "CmdCreateNode"
	case CmdCreateEdge:
		return "CmdCreateEdge"
	case CmdBatchSet:
		return "CmdBatchSet"
	default:
		return fmt.Sprintf("CommandType(%d)", ct)
	}
}

// KVOp represents a single key-value operation within a RAFT command.
// All HLC timestamps are pre-assigned by the leader before RAFT submission.
type KVOp struct {
	Key       string  `json:"key"`
	Value     []byte  `json:"value,omitempty"`
	HLC       hlc.HLC `json:"hlc"`
	Tombstone bool    `json:"tombstone,omitempty"`
	BlobRef   bool    `json:"blob_ref,omitempty"`
}

// RaftCommand is the payload submitted through RAFT consensus.
// It contains a command type and a list of pre-computed KV operations.
type RaftCommand struct {
	Type CommandType `json:"type"`
	Ops  []KVOp      `json:"ops"`
}

// MarshalCommand serializes a RaftCommand to JSON bytes for RAFT log entry.
func MarshalCommand(cmd RaftCommand) ([]byte, error) {
	return json.Marshal(cmd)
}

// UnmarshalCommand deserializes a RaftCommand from JSON bytes.
func UnmarshalCommand(data []byte) (RaftCommand, error) {
	var cmd RaftCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return RaftCommand{}, fmt.Errorf("raft: unmarshal command: %w", err)
	}
	return cmd, nil
}
