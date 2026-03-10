package raft

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"

	hraft "github.com/hashicorp/raft"

	"github.com/thatscalaguy/naladb/internal/store"
)

// FSM implements the hashicorp/raft.FSM interface. It applies committed RAFT
// log entries to the temporal KV store, which also captures all graph state
// (nodes, edges, adjacency lists are stored as regular KV entries).
type FSM struct {
	store *store.Store
}

// NewFSM creates a new FSM backed by the given store.
// The store should be created with NewWithoutWAL since the RAFT log serves
// as the WAL (no double-write).
func NewFSM(s *store.Store) *FSM {
	return &FSM{store: s}
}

// ApplyResponse is returned by FSM.Apply to communicate results back
// to the caller through the raft.ApplyFuture.
type ApplyResponse struct {
	Error error
}

// Apply processes a committed RAFT log entry by deserializing the command
// and applying each KV operation to the store.
func (f *FSM) Apply(l *hraft.Log) interface{} {
	cmd, err := UnmarshalCommand(l.Data)
	if err != nil {
		log.Printf("raft/fsm: failed to unmarshal command: %v", err)
		return &ApplyResponse{Error: fmt.Errorf("raft: fsm apply: %w", err)}
	}

	for _, op := range cmd.Ops {
		f.store.SetWithHLCAndFlags(op.Key, op.HLC, op.Value, op.Tombstone, op.BlobRef)
	}

	return &ApplyResponse{}
}

// Snapshot creates a point-in-time snapshot of the entire store state.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	versions := f.store.ExportVersions()
	return &fsmSnapshot{versions: versions}, nil
}

// Restore replaces the store state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string][]store.VersionExport
	if err := gob.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("raft: fsm restore: %w", err)
	}

	f.store.RestoreVersions(data)
	return nil
}

// Store returns the underlying store for direct reads.
func (f *FSM) Store() *store.Store {
	return f.store
}
