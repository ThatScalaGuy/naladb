//go:build integration

package integration

import (
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	nraft "github.com/thatscalaguy/naladb/internal/raft"
	"github.com/thatscalaguy/naladb/internal/store"
)

// testNode bundles all components of a single RAFT node.
type testNode struct {
	cluster   *nraft.Cluster
	store     *store.Store
	graph     *graph.Graph
	clock     *hlc.Clock
	transport hraft.LoopbackTransport
}

// create3NodeCluster creates a 3-node in-memory RAFT cluster for integration testing.
func create3NodeCluster(t *testing.T) []*testNode {
	t.Helper()

	nodes := make([]*testNode, 3)
	transports := make([]hraft.LoopbackTransport, 3)
	addresses := make([]hraft.ServerAddress, 3)

	// Create transports and connect them.
	for i := range 3 {
		_, transport := hraft.NewInmemTransport("")
		transports[i] = transport
		addresses[i] = transport.LocalAddr()
	}

	// Connect all transports to each other.
	for i := range 3 {
		for j := range 3 {
			if i != j {
				transports[i].Connect(addresses[j], transports[j])
			}
		}
	}

	// Create nodes. Only the first node bootstraps.
	for i := range 3 {
		clock := hlc.NewClock(uint8(i))
		s := store.NewWithoutWAL(clock)
		g := graph.New(s, clock)

		cfg := nraft.ClusterConfig{
			NodeID:            nodeID(i),
			DataDir:           t.TempDir(),
			Bootstrap:         false,
			ApplyTimeout:      5 * time.Second,
			SnapshotRetain:    2,
			SnapshotThreshold: 100,
		}

		logStore := hraft.NewInmemStore()
		stableStore := hraft.NewInmemStore()
		snapshotStore := hraft.NewInmemSnapshotStore()

		c, err := nraft.NewClusterWithStores(cfg, s, g, clock, transports[i], logStore, stableStore, snapshotStore)
		require.NoError(t, err)

		nodes[i] = &testNode{
			cluster:   c,
			store:     s,
			graph:     g,
			clock:     clock,
			transport: transports[i],
		}
	}

	// Bootstrap: node-0 creates the initial cluster configuration with all 3 nodes.
	configuration := hraft.Configuration{
		Servers: []hraft.Server{
			{ID: "node-0", Address: addresses[0]},
			{ID: "node-1", Address: addresses[1]},
			{ID: "node-2", Address: addresses[2]},
		},
	}
	f := nodes[0].cluster.Raft().BootstrapCluster(configuration)
	require.NoError(t, f.Error())

	// Wait for leader election.
	leader := waitForClusterLeader(t, nodes, 10*time.Second)
	require.NotNil(t, leader, "no leader elected")

	t.Cleanup(func() {
		for _, n := range nodes {
			n.cluster.Shutdown()
		}
	})

	return nodes
}

func nodeID(i int) string {
	return "node-" + string(rune('0'+i))
}

// waitForClusterLeader waits until one node becomes leader.
func waitForClusterLeader(t *testing.T, nodes []*testNode, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.cluster.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// findLeader returns the current leader node.
func findLeader(nodes []*testNode) *testNode {
	for _, n := range nodes {
		if n.cluster.IsLeader() {
			return n
		}
	}
	return nil
}

// findFollower returns a non-leader node.
func findFollower(nodes []*testNode) *testNode {
	for _, n := range nodes {
		if !n.cluster.IsLeader() {
			return n
		}
	}
	return nil
}

func TestIntegration_3NodeCluster_Consensus(t *testing.T) {
	nodes := create3NodeCluster(t)

	// Exactly one leader.
	leaderCount := 0
	for _, n := range nodes {
		if n.cluster.IsLeader() {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "should have exactly 1 leader")

	// ClusterStatus shows 3 nodes.
	leader := findLeader(nodes)
	require.NotNil(t, leader)
	status := leader.cluster.Status()
	assert.Len(t, status.Nodes, 3)
}

func TestIntegration_Write_Replicated(t *testing.T) {
	nodes := create3NodeCluster(t)
	leader := findLeader(nodes)
	require.NotNil(t, leader)

	// Write on leader.
	_, err := leader.cluster.Set("replicated-key", []byte("replicated-value"))
	require.NoError(t, err)

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	// Value should be readable on all nodes.
	for i, n := range nodes {
		r := n.store.Get("replicated-key")
		assert.True(t, r.Found, "node %d should have the value", i)
		assert.Equal(t, []byte("replicated-value"), r.Value, "node %d value mismatch", i)
	}
}

func TestIntegration_NoSeparateWAL(t *testing.T) {
	nodes := create3NodeCluster(t)
	leader := findLeader(nodes)
	require.NotNil(t, leader)

	// Write through RAFT (which IS the WAL).
	_, err := leader.cluster.Set("raft-wal-key", []byte("raft-wal-value"))
	require.NoError(t, err)

	// The store was created with NewWithoutWAL, so there's no separate WAL.
	// The RAFT log serves as the WAL. Verify value is present via store.
	time.Sleep(100 * time.Millisecond)

	for _, n := range nodes {
		r := n.store.Get("raft-wal-key")
		assert.True(t, r.Found, "value should be present via RAFT-as-WAL")
	}
}

func TestIntegration_FSMApply_AllCommandTypes(t *testing.T) {
	nodes := create3NodeCluster(t)
	leader := findLeader(nodes)
	require.NotNil(t, leader)

	// CmdSet
	_, err := leader.cluster.Set("cmd-set-key", []byte("value"))
	require.NoError(t, err)

	// CmdDelete
	_, err = leader.cluster.Set("to-delete", []byte("temp"))
	require.NoError(t, err)
	_, err = leader.cluster.Delete("to-delete")
	require.NoError(t, err)

	// CmdCreateNode
	nodeMeta, err := leader.cluster.CreateNode("sensor", map[string][]byte{
		"temp": []byte("42"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, nodeMeta.ID)

	// CmdBatchSet
	_, err = leader.cluster.BatchSet(map[string][]byte{
		"batch-1": []byte("b1"),
		"batch-2": []byte("b2"),
	})
	require.NoError(t, err)

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	// Verify on all nodes.
	for i, n := range nodes {
		r := n.store.Get("cmd-set-key")
		assert.True(t, r.Found, "node %d: CmdSet", i)

		r = n.store.Get("to-delete")
		assert.False(t, r.Found, "node %d: CmdDelete", i)

		r = n.store.Get("node:" + nodeMeta.ID + ":meta")
		assert.True(t, r.Found, "node %d: CmdCreateNode meta", i)

		r = n.store.Get("node:" + nodeMeta.ID + ":prop:temp")
		assert.True(t, r.Found, "node %d: CmdCreateNode prop", i)
	}
}

func TestIntegration_ReadAfterWrite_Leader(t *testing.T) {
	nodes := create3NodeCluster(t)
	leader := findLeader(nodes)
	require.NotNil(t, leader)

	_, err := leader.cluster.Set("raw-key", []byte("raw-value"))
	require.NoError(t, err)

	// Immediate read on leader should return the value.
	r, err := leader.cluster.Get("raw-key", nraft.Linearizable)
	require.NoError(t, err)
	assert.True(t, r.Found)
	assert.Equal(t, []byte("raw-value"), r.Value)
}

func TestIntegration_EventualConsistency_FollowerReads(t *testing.T) {
	nodes := create3NodeCluster(t)
	leader := findLeader(nodes)
	require.NotNil(t, leader)

	_, err := leader.cluster.Set("eventual-key", []byte("eventual-value"))
	require.NoError(t, err)

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	follower := findFollower(nodes)
	require.NotNil(t, follower)

	// Eventual read on follower should work without leader routing.
	r, err := follower.cluster.Get("eventual-key", nraft.Eventual)
	require.NoError(t, err)
	assert.True(t, r.Found)
	assert.Equal(t, []byte("eventual-value"), r.Value)
}

func TestIntegration_LeaderFailover(t *testing.T) {
	nodes := create3NodeCluster(t)

	leader := findLeader(nodes)
	require.NotNil(t, leader)

	// Write data before failover.
	_, err := leader.cluster.Set("pre-failover", []byte("value"))
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	// Shut down the leader.
	err = leader.cluster.Shutdown()
	require.NoError(t, err)

	// Wait for new leader election from remaining nodes.
	var remaining []*testNode
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}
	newLeader := waitForClusterLeader(t, remaining, 10*time.Second)
	require.NotNil(t, newLeader, "new leader should be elected after failover")

	// New leader should have the pre-failover data.
	r := newLeader.store.Get("pre-failover")
	assert.True(t, r.Found, "committed data should survive leader failover")
	assert.Equal(t, []byte("value"), r.Value)

	// New writes should work on the new leader.
	_, err = newLeader.cluster.Set("post-failover", []byte("new-value"))
	require.NoError(t, err)

	r, err = newLeader.cluster.Get("post-failover", nraft.Linearizable)
	require.NoError(t, err)
	assert.True(t, r.Found)
	assert.Equal(t, []byte("new-value"), r.Value)
}

func TestIntegration_Snapshot_And_Restore(t *testing.T) {
	nodes := create3NodeCluster(t)
	leader := findLeader(nodes)
	require.NotNil(t, leader)

	// Write enough data to trigger snapshots.
	for i := range 200 {
		key := "snap-" + string(rune('A'+i%26)) + string(rune('0'+i%10))
		_, err := leader.cluster.Set(key, []byte("value"))
		require.NoError(t, err)
	}

	// Trigger snapshot manually.
	err := leader.cluster.Snapshot()
	require.NoError(t, err)

	// Verify data is still consistent.
	time.Sleep(200 * time.Millisecond)
	for _, n := range nodes {
		r := n.store.Get("snap-A0")
		assert.True(t, r.Found, "data should survive snapshot")
	}
}
