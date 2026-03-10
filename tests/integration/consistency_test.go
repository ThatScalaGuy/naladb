//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	ngrpc "github.com/thatscalaguy/naladb/internal/grpc"
	nraft "github.com/thatscalaguy/naladb/internal/raft"
)

// testClusterNode bundles a RAFT node with its gRPC server and address.
type testClusterNode struct {
	*testNode
	grpcServer *ngrpc.Server
	grpcAddr   string
	router     *nraft.Router
}

// create3NodeGRPCCluster creates a 3-node RAFT cluster where each node
// also runs a gRPC server with leader routing enabled.
func create3NodeGRPCCluster(t *testing.T) []*testClusterNode {
	t.Helper()

	// Create the base RAFT cluster.
	nodes := create3NodeCluster(t)

	// Start gRPC servers for each node.
	clusterNodes := make([]*testClusterNode, 3)
	grpcAddrs := make([]string, 3)

	// First, allocate listeners to get addresses.
	listeners := make([]net.Listener, 3)
	for i := range 3 {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = lis
		grpcAddrs[i] = lis.Addr().String()
	}

	// Build peer address maps (RAFT node ID -> gRPC address).
	peerAddrs := map[string]string{
		"node-0": grpcAddrs[0],
		"node-1": grpcAddrs[1],
		"node-2": grpcAddrs[2],
	}

	for i := range 3 {
		router := nraft.NewRouter(nodes[i].cluster, peerAddrs)
		srv := ngrpc.NewClusterServer(router)

		clusterNodes[i] = &testClusterNode{
			testNode:   nodes[i],
			grpcServer: srv,
			grpcAddr:   grpcAddrs[i],
			router:     router,
		}

		go func(s *ngrpc.Server, lis net.Listener) {
			_ = s.ServeListener(lis)
		}(srv, listeners[i])
	}

	t.Cleanup(func() {
		for _, n := range clusterNodes {
			n.grpcServer.Stop()
			n.router.Close()
		}
	})

	return clusterNodes
}

// dialNode creates a gRPC client connection to the given node.
func dialNode(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// findClusterLeader returns the leader node.
func findClusterLeader(nodes []*testClusterNode) *testClusterNode {
	for _, n := range nodes {
		if n.cluster.IsLeader() {
			return n
		}
	}
	return nil
}

// findClusterFollower returns a non-leader node.
func findClusterFollower(nodes []*testClusterNode) *testClusterNode {
	for _, n := range nodes {
		if !n.cluster.IsLeader() {
			return n
		}
	}
	return nil
}

// TestIntegration_WriteForwardedFromFollower verifies that writes sent to
// a follower are transparently forwarded to the leader.
func TestIntegration_WriteForwardedFromFollower(t *testing.T) {
	nodes := create3NodeGRPCCluster(t)

	follower := findClusterFollower(nodes)
	require.NotNil(t, follower)

	conn := dialNode(t, follower.grpcAddr)
	client := pb.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write to follower — should be forwarded to leader.
	resp, err := client.Set(ctx, &pb.SetRequest{
		Key:   "forwarded-key",
		Value: []byte("forwarded-value"),
	})
	require.NoError(t, err)
	assert.NotZero(t, resp.Timestamp)

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	// Value should be readable from all nodes.
	for i, n := range nodes {
		r := n.store.Get("forwarded-key")
		assert.True(t, r.Found, "node %d should have the value", i)
		assert.Equal(t, []byte("forwarded-value"), r.Value, "node %d value mismatch", i)
	}
}

// TestIntegration_LinearizableRead_ReadIndex verifies that LINEARIZABLE reads
// go through the ReadIndex protocol (VerifyLeader + Barrier).
func TestIntegration_LinearizableRead_ReadIndex(t *testing.T) {
	nodes := create3NodeGRPCCluster(t)

	leader := findClusterLeader(nodes)
	require.NotNil(t, leader)

	// Write on leader.
	conn := dialNode(t, leader.grpcAddr)
	client := pb.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Set(ctx, &pb.SetRequest{
		Key:   "linear-key",
		Value: []byte("linear-value"),
	})
	require.NoError(t, err)

	// LINEARIZABLE read on leader.
	getResp, err := client.Get(ctx, &pb.GetRequest{
		Key:         "linear-key",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
	})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("linear-value"), getResp.Value)

	// LINEARIZABLE read from follower should be forwarded to leader.
	follower := findClusterFollower(nodes)
	require.NotNil(t, follower)

	fConn := dialNode(t, follower.grpcAddr)
	fClient := pb.NewKVServiceClient(fConn)

	getResp, err = fClient.Get(ctx, &pb.GetRequest{
		Key:         "linear-key",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
	})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("linear-value"), getResp.Value)
}

// TestIntegration_BoundedStale_LocalRead verifies that a BOUNDED_STALE read
// on a follower within the staleness window is served locally.
func TestIntegration_BoundedStale_LocalRead(t *testing.T) {
	nodes := create3NodeGRPCCluster(t)

	leader := findClusterLeader(nodes)
	require.NotNil(t, leader)

	// Write on leader.
	lConn := dialNode(t, leader.grpcAddr)
	lClient := pb.NewKVServiceClient(lConn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := lClient.Set(ctx, &pb.SetRequest{
		Key:   "bounded-key",
		Value: []byte("bounded-value"),
	})
	require.NoError(t, err)

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	// BOUNDED_STALE read on follower (within window).
	follower := findClusterFollower(nodes)
	require.NotNil(t, follower)

	fConn := dialNode(t, follower.grpcAddr)
	fClient := pb.NewKVServiceClient(fConn)

	var header metadata.MD
	getResp, err := fClient.Get(ctx, &pb.GetRequest{
		Key:         "bounded-key",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED_STALE,
		MaxStaleMs:  5000,
	}, grpc.Header(&header))
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("bounded-value"), getResp.Value)

	// Should NOT have been forwarded (served locally).
	forwarded := header.Get("x-naladb-forwarded")
	assert.Empty(t, forwarded, "bounded stale read within window should be local")
}

// TestIntegration_BoundedStale_ForwardWhenStale verifies that a BOUNDED_STALE
// read is forwarded when the follower is too stale.
func TestIntegration_BoundedStale_ForwardWhenStale(t *testing.T) {
	nodes := create3NodeGRPCCluster(t)

	leader := findClusterLeader(nodes)
	require.NotNil(t, leader)

	// Write on leader.
	lConn := dialNode(t, leader.grpcAddr)
	lClient := pb.NewKVServiceClient(lConn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := lClient.Set(ctx, &pb.SetRequest{
		Key:   "stale-key",
		Value: []byte("stale-value"),
	})
	require.NoError(t, err)

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	// BOUNDED_STALE with max_stale_ms=1 on follower — 1ms window effectively
	// forces forwarding because the follower's last contact is always > 1ms ago.
	follower := findClusterFollower(nodes)
	require.NotNil(t, follower)

	fConn := dialNode(t, follower.grpcAddr)
	fClient := pb.NewKVServiceClient(fConn)

	// Small sleep to ensure follower's last contact > 1ms.
	time.Sleep(10 * time.Millisecond)

	var header metadata.MD
	getResp, err := fClient.Get(ctx, &pb.GetRequest{
		Key:         "stale-key",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED_STALE,
		MaxStaleMs:  1, // 1ms window = effectively always stale
	}, grpc.Header(&header))
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("stale-value"), getResp.Value)

	// Should have been forwarded.
	forwarded := header.Get("x-naladb-forwarded")
	assert.NotEmpty(t, forwarded, "bounded stale read with 1ms window should be forwarded")
}

// TestIntegration_EventualRead_AlwaysLocal verifies that EVENTUAL reads are
// served locally without leader routing.
func TestIntegration_EventualRead_AlwaysLocal(t *testing.T) {
	nodes := create3NodeGRPCCluster(t)

	leader := findClusterLeader(nodes)
	require.NotNil(t, leader)

	// Write on leader.
	lConn := dialNode(t, leader.grpcAddr)
	lClient := pb.NewKVServiceClient(lConn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := lClient.Set(ctx, &pb.SetRequest{
		Key:   "eventual-key",
		Value: []byte("eventual-value"),
	})
	require.NoError(t, err)

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	// EVENTUAL read on follower.
	follower := findClusterFollower(nodes)
	require.NotNil(t, follower)

	fConn := dialNode(t, follower.grpcAddr)
	fClient := pb.NewKVServiceClient(fConn)

	getResp, err := fClient.Get(ctx, &pb.GetRequest{
		Key:         "eventual-key",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
	})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("eventual-value"), getResp.Value)
}

// TestIntegration_ResponseMetadata_LeaderInfo verifies that every gRPC response
// includes the x-naladb-leader metadata header.
func TestIntegration_ResponseMetadata_LeaderInfo(t *testing.T) {
	nodes := create3NodeGRPCCluster(t)

	follower := findClusterFollower(nodes)
	require.NotNil(t, follower)

	conn := dialNode(t, follower.grpcAddr)
	client := pb.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send a Get request to follower.
	var header metadata.MD
	_, err := client.Get(ctx, &pb.GetRequest{
		Key:         "nonexistent",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
	}, grpc.Header(&header))
	require.NoError(t, err)

	// Response metadata should contain leader endpoint.
	leaderAddrs := header.Get("x-naladb-leader")
	assert.NotEmpty(t, leaderAddrs, "response should contain x-naladb-leader metadata")

	// The leader address should match one of the known gRPC addresses.
	leader := findClusterLeader(nodes)
	require.NotNil(t, leader)
	assert.Contains(t, leaderAddrs, leader.grpcAddr)
}
