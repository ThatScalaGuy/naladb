package grpc_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/graph"
	ngrpc "github.com/thatscalaguy/naladb/internal/grpc"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/store"
)

type statsTestEnv struct {
	store    *store.Store
	graphL   *graph.Graph
	metaReg  *meta.Registry
	stats    pb.StatsServiceClient
	kv       pb.KVServiceClient
	graphCli pb.GraphServiceClient
}

func newStatsTestEnv(t *testing.T) *statsTestEnv {
	t.Helper()

	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)
	mr := meta.NewRegistry()
	s.SetMeta(mr)

	srv := ngrpc.NewServer(s, g,
		ngrpc.WithMetaRegistry(mr),
	)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = srv.ServeListener(lis)
	}()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	return &statsTestEnv{
		store:    s,
		graphL:   g,
		metaReg:  mr,
		stats:    pb.NewStatsServiceClient(conn),
		kv:       pb.NewKVServiceClient(conn),
		graphCli: pb.NewGraphServiceClient(conn),
	}
}

func TestStatsService_GetStats_Empty(t *testing.T) {
	env := newStatsTestEnv(t)
	ctx := context.Background()

	resp, err := env.stats.GetStats(ctx, &pb.GetStatsRequest{})
	require.NoError(t, err)

	assert.Equal(t, int64(0), resp.TotalKeys)
	assert.Equal(t, int64(0), resp.NodesTotal)
	assert.Equal(t, int64(0), resp.EdgesTotal)
}

func TestStatsService_GetStats_WithData(t *testing.T) {
	env := newStatsTestEnv(t)
	ctx := context.Background()

	// Create nodes.
	n1, err := env.graphCli.CreateNode(ctx, &pb.CreateNodeRequest{Type: "sensor"})
	require.NoError(t, err)
	n2, err := env.graphCli.CreateNode(ctx, &pb.CreateNodeRequest{Type: "sensor"})
	require.NoError(t, err)
	_, err = env.graphCli.CreateNode(ctx, &pb.CreateNodeRequest{Type: "pump"})
	require.NoError(t, err)

	// Create an edge.
	_, err = env.graphCli.CreateEdge(ctx, &pb.CreateEdgeRequest{
		From:      n1.Id,
		To:        n2.Id,
		Relation:  "connected",
		ValidFrom: n2.ValidFrom,
		ValidTo:   n1.ValidTo,
	})
	require.NoError(t, err)

	// Write some KV data.
	_, err = env.kv.Set(ctx, &pb.SetRequest{Key: "temp", Value: []byte("25.0")})
	require.NoError(t, err)
	_, err = env.kv.Set(ctx, &pb.SetRequest{Key: "temp", Value: []byte("26.0")})
	require.NoError(t, err)

	resp, err := env.stats.GetStats(ctx, &pb.GetStatsRequest{})
	require.NoError(t, err)

	assert.Greater(t, resp.TotalKeys, int64(0))
	assert.Equal(t, int64(3), resp.NodesActive)
	assert.Equal(t, int64(1), resp.EdgesActive)
	assert.Greater(t, resp.TotalVersions, int64(0))

	// Check nodes_by_type.
	assert.Equal(t, int64(2), resp.NodesByType["sensor"])
	assert.Equal(t, int64(1), resp.NodesByType["pump"])

	// Check edges_by_relation.
	assert.Equal(t, int64(1), resp.EdgesByRelation["connected"])
}

func TestStatsService_GetStats_WithDeletedNodes(t *testing.T) {
	env := newStatsTestEnv(t)
	ctx := context.Background()

	n, err := env.graphCli.CreateNode(ctx, &pb.CreateNodeRequest{Type: "temp"})
	require.NoError(t, err)

	_, err = env.graphCli.DeleteNode(ctx, &pb.DeleteNodeRequest{Id: n.Id})
	require.NoError(t, err)

	resp, err := env.stats.GetStats(ctx, &pb.GetStatsRequest{})
	require.NoError(t, err)

	assert.Equal(t, int64(1), resp.NodesTotal)
	assert.Equal(t, int64(0), resp.NodesActive)
	assert.Equal(t, int64(1), resp.NodesDeleted)
}

func TestStatsService_GetKeyStats(t *testing.T) {
	env := newStatsTestEnv(t)
	ctx := context.Background()

	// Write some data.
	_, err := env.kv.Set(ctx, &pb.SetRequest{Key: "sensor:val", Value: []byte("10.0")})
	require.NoError(t, err)
	_, err = env.kv.Set(ctx, &pb.SetRequest{Key: "sensor:val", Value: []byte("20.0")})
	require.NoError(t, err)
	_, err = env.kv.Set(ctx, &pb.SetRequest{Key: "sensor:val", Value: []byte("30.0")})
	require.NoError(t, err)

	resp, err := env.stats.GetKeyStats(ctx, &pb.GetKeyStatsRequest{Key: "sensor:val"})
	require.NoError(t, err)

	assert.True(t, resp.Found)
	assert.Equal(t, "sensor:val", resp.Key)
	assert.Equal(t, uint64(3), resp.TotalWrites)
	assert.Greater(t, resp.SizeBytes, uint64(0))
	assert.InDelta(t, 20.0, resp.AvgValue, 0.01)
	assert.InDelta(t, 10.0, resp.MinValue, 0.01)
	assert.InDelta(t, 30.0, resp.MaxValue, 0.01)
}

func TestStatsService_GetKeyStats_NotFound(t *testing.T) {
	env := newStatsTestEnv(t)
	ctx := context.Background()

	resp, err := env.stats.GetKeyStats(ctx, &pb.GetKeyStatsRequest{Key: "nonexistent"})
	require.NoError(t, err)

	assert.False(t, resp.Found)
	assert.Equal(t, "nonexistent", resp.Key)
}
