package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

func newTestFSM(t *testing.T) *FSM {
	t.Helper()
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	return NewFSM(s)
}

func applyCmd(t *testing.T, fsm *FSM, cmd RaftCommand) *ApplyResponse {
	t.Helper()
	data, err := MarshalCommand(cmd)
	require.NoError(t, err)
	resp := fsm.Apply(&hraft.Log{Data: data})
	return resp.(*ApplyResponse)
}

func TestFSM_Apply_CmdSet(t *testing.T) {
	fsm := newTestFSM(t)
	ts := hlc.NewHLC(1000, 0, 0)

	resp := applyCmd(t, fsm, RaftCommand{
		Type: CmdSet,
		Ops:  []KVOp{{Key: "k1", Value: []byte("v1"), HLC: ts}},
	})
	require.NoError(t, resp.Error)

	r := fsm.Store().Get("k1")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v1"), r.Value)
	assert.Equal(t, ts, r.HLC)
}

func TestFSM_Apply_CmdDelete(t *testing.T) {
	fsm := newTestFSM(t)

	// Set a value first.
	applyCmd(t, fsm, RaftCommand{
		Type: CmdSet,
		Ops:  []KVOp{{Key: "k1", Value: []byte("v1"), HLC: hlc.NewHLC(100, 0, 0)}},
	})

	// Delete it.
	resp := applyCmd(t, fsm, RaftCommand{
		Type: CmdDelete,
		Ops:  []KVOp{{Key: "k1", HLC: hlc.NewHLC(200, 0, 0), Tombstone: true}},
	})
	require.NoError(t, resp.Error)

	r := fsm.Store().Get("k1")
	assert.False(t, r.Found)
}

func TestFSM_Apply_CmdBatchSet(t *testing.T) {
	fsm := newTestFSM(t)

	resp := applyCmd(t, fsm, RaftCommand{
		Type: CmdBatchSet,
		Ops: []KVOp{
			{Key: "k1", Value: []byte("v1"), HLC: hlc.NewHLC(100, 0, 0)},
			{Key: "k2", Value: []byte("v2"), HLC: hlc.NewHLC(200, 0, 0)},
			{Key: "k3", Value: []byte("v3"), HLC: hlc.NewHLC(300, 0, 0)},
		},
	})
	require.NoError(t, resp.Error)

	for _, key := range []string{"k1", "k2", "k3"} {
		r := fsm.Store().Get(key)
		assert.True(t, r.Found, "key %s should be found", key)
	}
}

func TestFSM_Apply_CmdCreateNode(t *testing.T) {
	fsm := newTestFSM(t)

	resp := applyCmd(t, fsm, RaftCommand{
		Type: CmdCreateNode,
		Ops: []KVOp{
			{Key: "node:abc:meta", Value: []byte(`{"id":"abc","type":"sensor"}`), HLC: hlc.NewHLC(100, 0, 0)},
			{Key: "node:abc:prop:temp", Value: []byte("42"), HLC: hlc.NewHLC(101, 0, 0)},
			{Key: "graph:adj:abc:out", Value: []byte(`{"edge_ids":[]}`), HLC: hlc.NewHLC(102, 0, 0)},
			{Key: "graph:adj:abc:in", Value: []byte(`{"edge_ids":[]}`), HLC: hlc.NewHLC(103, 0, 0)},
		},
	})
	require.NoError(t, resp.Error)

	r := fsm.Store().Get("node:abc:meta")
	assert.True(t, r.Found)
	assert.Contains(t, string(r.Value), "sensor")

	r = fsm.Store().Get("node:abc:prop:temp")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("42"), r.Value)
}

func TestFSM_Apply_CmdCreateEdge(t *testing.T) {
	fsm := newTestFSM(t)

	resp := applyCmd(t, fsm, RaftCommand{
		Type: CmdCreateEdge,
		Ops: []KVOp{
			{Key: "edge:e1:meta", Value: []byte(`{"id":"e1","from":"a","to":"b","relation":"connects"}`), HLC: hlc.NewHLC(100, 0, 0)},
			{Key: "graph:adj:a:out", Value: []byte(`{"edge_ids":["e1"]}`), HLC: hlc.NewHLC(101, 0, 0)},
			{Key: "graph:adj:b:in", Value: []byte(`{"edge_ids":["e1"]}`), HLC: hlc.NewHLC(102, 0, 0)},
		},
	})
	require.NoError(t, resp.Error)

	r := fsm.Store().Get("edge:e1:meta")
	assert.True(t, r.Found)
}

func TestFSM_Apply_InvalidCommand(t *testing.T) {
	fsm := newTestFSM(t)

	resp := fsm.Apply(&hraft.Log{Data: []byte("not json")})
	ar := resp.(*ApplyResponse)
	assert.Error(t, ar.Error)
}

func TestFSM_SnapshotAndRestore(t *testing.T) {
	fsm := newTestFSM(t)

	// Write some data.
	applyCmd(t, fsm, RaftCommand{
		Type: CmdSet,
		Ops:  []KVOp{{Key: "k1", Value: []byte("v1"), HLC: hlc.NewHLC(100, 0, 0)}},
	})
	applyCmd(t, fsm, RaftCommand{
		Type: CmdSet,
		Ops:  []KVOp{{Key: "k2", Value: []byte("v2"), HLC: hlc.NewHLC(200, 0, 0)}},
	})
	applyCmd(t, fsm, RaftCommand{
		Type: CmdSet,
		Ops:  []KVOp{{Key: "k1", Value: []byte("v1-updated"), HLC: hlc.NewHLC(300, 0, 0)}},
	})

	// Take a snapshot.
	snap, err := fsm.Snapshot()
	require.NoError(t, err)

	// Persist to buffer.
	var buf bytes.Buffer
	sink := &bufSnapshotSink{buf: &buf}
	err = snap.Persist(sink)
	require.NoError(t, err)

	// Create a new FSM and restore from snapshot.
	fsm2 := newTestFSM(t)
	err = fsm2.Restore(nopCloser{&buf})
	require.NoError(t, err)

	// Verify all data is present.
	r := fsm2.Store().Get("k1")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v1-updated"), r.Value)

	r = fsm2.Store().Get("k2")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v2"), r.Value)

	// Verify history is preserved.
	history := fsm2.Store().History("k1", store.HistoryOptions{})
	assert.Len(t, history, 2)
}

// bufSnapshotSink implements hraft.SnapshotSink for testing.
type bufSnapshotSink struct {
	buf *bytes.Buffer
}

func (s *bufSnapshotSink) Write(p []byte) (n int, err error) { return s.buf.Write(p) }
func (s *bufSnapshotSink) Close() error                      { return nil }
func (s *bufSnapshotSink) ID() string                        { return "test" }
func (s *bufSnapshotSink) Cancel() error                     { return nil }

// nopCloser wraps an io.Reader to add a no-op Close method.
type nopCloser struct {
	*bytes.Buffer
}

func (nopCloser) Close() error { return nil }

func TestFSM_SnapshotRoundtrip_Large(t *testing.T) {
	fsm := newTestFSM(t)

	// Write 1000 keys to verify larger snapshot roundtrips.
	for i := range 1000 {
		ts := hlc.NewHLC(int64(i+1)*100, 0, 0)
		key := "key-" + string(rune('A'+i%26)) + "-" + fmt.Sprintf("%04d", i)
		applyCmd(t, fsm, RaftCommand{
			Type: CmdSet,
			Ops:  []KVOp{{Key: key, Value: []byte("value"), HLC: ts}},
		})
	}

	// Snapshot and restore.
	snap, err := fsm.Snapshot()
	require.NoError(t, err)

	var buf bytes.Buffer
	sink := &bufSnapshotSink{buf: &buf}
	err = snap.Persist(sink)
	require.NoError(t, err)

	fsm2 := newTestFSM(t)
	err = fsm2.Restore(nopCloser{&buf})
	require.NoError(t, err)

	// Verify all keys are present.
	for i := range 1000 {
		key := "key-" + string(rune('A'+i%26)) + "-" + fmt.Sprintf("%04d", i)
		r := fsm2.Store().Get(key)
		assert.True(t, r.Found, "key %s should be found", key)
	}
}

func init() {
	// Register types for gob encoding in tests.
	gob.Register(map[string][]store.VersionExport{})
}
