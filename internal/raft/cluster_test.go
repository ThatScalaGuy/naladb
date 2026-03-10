package raft

import (
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// newTestCluster creates a single-node in-memory RAFT cluster for testing.
func newTestCluster(t *testing.T) *Cluster {
	t.Helper()

	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)

	_, transport := hraft.NewInmemTransport("")
	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapshotStore := hraft.NewInmemSnapshotStore()

	cfg := ClusterConfig{
		NodeID:            "node-1",
		DataDir:           t.TempDir(),
		Bootstrap:         true,
		ApplyTimeout:      5 * time.Second,
		SnapshotRetain:    2,
		SnapshotThreshold: 100,
	}

	c, err := NewClusterWithStores(cfg, s, g, clock, transport, logStore, stableStore, snapshotStore)
	require.NoError(t, err)

	t.Cleanup(func() { c.Shutdown() })

	// Wait for leader election.
	waitForLeader(t, c, 5*time.Second)

	return c
}

// waitForLeader blocks until the cluster has a leader or timeout.
func waitForLeader(t *testing.T, c *Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader election")
}

func TestCluster_SingleNode_BecomesLeader(t *testing.T) {
	c := newTestCluster(t)
	assert.True(t, c.IsLeader())

	status := c.Status()
	assert.Equal(t, 1, len(status.Nodes))
	assert.Equal(t, "Leader", status.Nodes[0].Role)
}

func TestCluster_Set_And_Get(t *testing.T) {
	c := newTestCluster(t)

	ts, err := c.Set("greeting", []byte("hello"))
	require.NoError(t, err)
	assert.NotZero(t, ts)

	r, err := c.Get("greeting", Eventual)
	require.NoError(t, err)
	assert.True(t, r.Found)
	assert.Equal(t, []byte("hello"), r.Value)
}

func TestCluster_Delete(t *testing.T) {
	c := newTestCluster(t)

	_, err := c.Set("k1", []byte("v1"))
	require.NoError(t, err)

	_, err = c.Delete("k1")
	require.NoError(t, err)

	r, err := c.Get("k1", Eventual)
	require.NoError(t, err)
	assert.False(t, r.Found)
}

func TestCluster_BatchSet(t *testing.T) {
	c := newTestCluster(t)

	entries := map[string][]byte{
		"a": []byte("1"),
		"b": []byte("2"),
		"c": []byte("3"),
	}
	hlcs, err := c.BatchSet(entries)
	require.NoError(t, err)
	assert.Len(t, hlcs, 3)

	for key, expectedVal := range entries {
		r, err := c.Get(key, Eventual)
		require.NoError(t, err)
		assert.True(t, r.Found, "key %s not found", key)
		assert.Equal(t, expectedVal, r.Value)
	}
}

func TestCluster_ReadAfterWrite_Linearizable(t *testing.T) {
	c := newTestCluster(t)

	_, err := c.Set("k1", []byte("v1"))
	require.NoError(t, err)

	// Linearizable read on leader should return the value immediately.
	r, err := c.Get("k1", Linearizable)
	require.NoError(t, err)
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v1"), r.Value)
}

func TestCluster_CreateNode(t *testing.T) {
	c := newTestCluster(t)

	meta, err := c.CreateNode("sensor", map[string][]byte{
		"temperature": []byte("42"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, meta.ID)
	assert.Equal(t, "sensor", meta.Type)

	// Verify node data is readable through the store.
	r, err := c.Get("node:"+meta.ID+":meta", Eventual)
	require.NoError(t, err)
	assert.True(t, r.Found)
	assert.Contains(t, string(r.Value), "sensor")

	r, err = c.Get("node:"+meta.ID+":prop:temperature", Eventual)
	require.NoError(t, err)
	assert.True(t, r.Found)
	assert.Equal(t, []byte("42"), r.Value)
}

func TestCluster_CreateEdge(t *testing.T) {
	c := newTestCluster(t)

	n1, err := c.CreateNode("pump", nil)
	require.NoError(t, err)
	n2, err := c.CreateNode("valve", nil)
	require.NoError(t, err)

	edge, err := c.CreateEdge(n1.ID, n2.ID, "feeds", n1.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, edge.ID)
	assert.Equal(t, n1.ID, edge.From)
	assert.Equal(t, n2.ID, edge.To)
	assert.Equal(t, "feeds", edge.Relation)

	// Verify edge meta is readable.
	r, err := c.Get("edge:"+edge.ID+":meta", Eventual)
	require.NoError(t, err)
	assert.True(t, r.Found)
}

func TestCluster_Snapshot(t *testing.T) {
	c := newTestCluster(t)

	// Write data.
	for i := range 50 {
		_, err := c.Set("snap-key-"+string(rune('A'+i%26)), []byte("value"))
		require.NoError(t, err)
	}

	// Trigger snapshot.
	err := c.Snapshot()
	require.NoError(t, err)
}
