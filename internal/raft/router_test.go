package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRouter(t *testing.T) (*Router, *Cluster) {
	t.Helper()
	c := newTestCluster(t)
	peers := map[string]string{
		c.config.NodeID: "localhost:7301",
	}
	r := NewRouter(c, peers)
	t.Cleanup(func() { r.Close() })
	return r, c
}

func TestRouter_RouteWrite_LeaderHandlesLocally(t *testing.T) {
	r, _ := newTestRouter(t)

	routing, addr := r.RouteWrite()
	assert.Equal(t, HandleLocally, routing)
	assert.Empty(t, addr)
}

func TestRouter_RouteRead_Eventual_AlwaysLocal(t *testing.T) {
	r, _ := newTestRouter(t)

	routing, addr := r.RouteRead(Eventual, 5*time.Second)
	assert.Equal(t, HandleLocally, routing)
	assert.Empty(t, addr)
}

func TestRouter_RouteRead_Linearizable_Leader(t *testing.T) {
	r, _ := newTestRouter(t)

	routing, addr := r.RouteRead(Linearizable, 0)
	assert.Equal(t, HandleLocally, routing)
	assert.Empty(t, addr)
}

func TestRouter_RouteRead_BoundedStale_Leader(t *testing.T) {
	r, _ := newTestRouter(t)

	routing, addr := r.RouteRead(BoundedStale, 5*time.Second)
	assert.Equal(t, HandleLocally, routing)
	assert.Empty(t, addr)
}

func TestRouter_LeaderEndpoint(t *testing.T) {
	r, _ := newTestRouter(t)

	endpoint := r.LeaderEndpoint()
	// Single-node cluster: the leader is this node, so the endpoint should be found.
	assert.NotEmpty(t, endpoint)
	assert.Equal(t, "localhost:7301", endpoint)
}

func TestRouter_UpdatePeers(t *testing.T) {
	r, _ := newTestRouter(t)

	newPeers := map[string]string{
		"node-1": "new-host:7301",
	}
	r.UpdatePeers(newPeers)

	endpoint := r.LeaderEndpoint()
	assert.Equal(t, "new-host:7301", endpoint)
}

func TestRouter_GetConn_CachesConnections(t *testing.T) {
	r, _ := newTestRouter(t)

	conn1, err := r.GetConn("localhost:9999")
	require.NoError(t, err)

	conn2, err := r.GetConn("localhost:9999")
	require.NoError(t, err)

	assert.Same(t, conn1, conn2, "should return cached connection")
}

func TestRouter_Close_ClearsPool(t *testing.T) {
	r, _ := newTestRouter(t)

	_, err := r.GetConn("localhost:9999")
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)

	// After close, getting a conn should create a new one.
	_, err = r.GetConn("localhost:9999")
	require.NoError(t, err)
}
