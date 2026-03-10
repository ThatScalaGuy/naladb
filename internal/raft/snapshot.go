package raft

import (
	"encoding/gob"
	"fmt"

	hraft "github.com/hashicorp/raft"

	"github.com/thatscalaguy/naladb/internal/store"
)

// fsmSnapshot implements the hashicorp/raft.FSMSnapshot interface.
// It holds a point-in-time copy of the store's version log.
type fsmSnapshot struct {
	versions map[string][]store.VersionExport
}

// Persist writes the snapshot data to the given sink using gob encoding.
// The snapshot contains the complete version log which includes:
//   - Memory-Index state (latest version per key)
//   - All version history (for GetAt, History queries)
//   - Graph-Index (nodes, edges, adjacency lists as KV entries)
func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	if err := gob.NewEncoder(sink).Encode(s.versions); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("raft: snapshot persist: %w", err)
	}
	return sink.Close()
}

// Release is called when the snapshot is no longer needed.
func (s *fsmSnapshot) Release() {}
