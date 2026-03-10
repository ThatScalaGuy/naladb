package grpc_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "github.com/thatscalaguy/naladb/api/gen/naladb/v1"
	"github.com/thatscalaguy/naladb/internal/graph"
	ngrpc "github.com/thatscalaguy/naladb/internal/grpc"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// testEnv holds a running gRPC server and clients for testing.
type testEnv struct {
	srv    *ngrpc.Server
	conn   *grpc.ClientConn
	kv     pb.KVServiceClient
	graph  pb.GraphServiceClient
	watch  pb.WatchServiceClient
	health healthpb.HealthClient
	store  *store.Store
	graphL *graph.Graph
	clock  *hlc.Clock
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)

	srv := ngrpc.NewServer(s, g)

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

	return &testEnv{
		srv:    srv,
		conn:   conn,
		kv:     pb.NewKVServiceClient(conn),
		graph:  pb.NewGraphServiceClient(conn),
		watch:  pb.NewWatchServiceClient(conn),
		health: healthpb.NewHealthClient(conn),
		store:  s,
		graphL: g,
		clock:  clock,
	}
}

// ---------------------------------------------------------------------------
// Scenario: gRPC Server startet und akzeptiert Verbindungen
// ---------------------------------------------------------------------------

func TestServer_HealthCheck(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	resp, err := env.health.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
}

// ---------------------------------------------------------------------------
// Scenario: Set und Get über gRPC
// ---------------------------------------------------------------------------

func TestKVService_SetAndGet(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Set
	setResp, err := env.kv.Set(ctx, &pb.SetRequest{
		Key:   "k",
		Value: []byte("v"),
	})
	require.NoError(t, err)
	assert.Greater(t, setResp.Timestamp, uint64(0))

	// Get
	getResp, err := env.kv.Get(ctx, &pb.GetRequest{Key: "k"})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, "k", getResp.Key)
	assert.Equal(t, []byte("v"), getResp.Value)
}

func TestKVService_GetAt(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	setResp1, err := env.kv.Set(ctx, &pb.SetRequest{Key: "k", Value: []byte("v1")})
	require.NoError(t, err)

	_, err = env.kv.Set(ctx, &pb.SetRequest{Key: "k", Value: []byte("v2")})
	require.NoError(t, err)

	// GetAt should return v1 at the first timestamp.
	getAtResp, err := env.kv.GetAt(ctx, &pb.GetAtRequest{Key: "k", At: setResp1.Timestamp})
	require.NoError(t, err)
	assert.True(t, getAtResp.Found)
	assert.Equal(t, []byte("v1"), getAtResp.Value)
}

func TestKVService_Delete(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	_, err := env.kv.Set(ctx, &pb.SetRequest{Key: "k", Value: []byte("v")})
	require.NoError(t, err)

	delResp, err := env.kv.Delete(ctx, &pb.DeleteRequest{Key: "k"})
	require.NoError(t, err)
	assert.Greater(t, delResp.Timestamp, uint64(0))

	getResp, err := env.kv.Get(ctx, &pb.GetRequest{Key: "k"})
	require.NoError(t, err)
	assert.False(t, getResp.Found)
}

// ---------------------------------------------------------------------------
// Scenario: History über Server-Side Streaming
// ---------------------------------------------------------------------------

func TestKVService_History_Streaming(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Write 50 entries.
	for i := 0; i < 50; i++ {
		_, err := env.kv.Set(ctx, &pb.SetRequest{
			Key:   "k",
			Value: []byte{byte(i)},
		})
		require.NoError(t, err)
	}

	// Request history with limit=10.
	stream, err := env.kv.History(ctx, &pb.HistoryRequest{Key: "k", Limit: 10})
	require.NoError(t, err)

	var entries []*pb.HistoryEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		entries = append(entries, entry)
	}

	assert.Len(t, entries, 10)
}

// ---------------------------------------------------------------------------
// Scenario: Consistency Levels funktionieren
// ---------------------------------------------------------------------------

func TestKVService_ConsistencyLevels(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	_, err := env.kv.Set(ctx, &pb.SetRequest{Key: "k", Value: []byte("v")})
	require.NoError(t, err)

	// LINEARIZABLE (accepted but not enforced until RAFT).
	resp, err := env.kv.Get(ctx, &pb.GetRequest{
		Key:         "k",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_LINEARIZABLE,
	})
	require.NoError(t, err)
	assert.True(t, resp.Found)

	// EVENTUAL.
	resp, err = env.kv.Get(ctx, &pb.GetRequest{
		Key:         "k",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
	})
	require.NoError(t, err)
	assert.True(t, resp.Found)
}

// ---------------------------------------------------------------------------
// Scenario: CreateNode und TraverseAt über gRPC
// ---------------------------------------------------------------------------

func TestGraphService_CreateNodeAndTraverse(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Create 3 nodes.
	n1, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "sensor"})
	require.NoError(t, err)
	n2, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "sensor"})
	require.NoError(t, err)
	n3, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "sensor"})
	require.NoError(t, err)

	// Create 2 edges: n1->n2, n2->n3.
	// Edge validity must be within both nodes' ranges, so use the later ValidFrom.
	_, err = env.graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
		From:      n1.Id,
		To:        n2.Id,
		Relation:  "connected",
		ValidFrom: n2.ValidFrom, // later of the two
		ValidTo:   n1.ValidTo,
	})
	require.NoError(t, err)

	_, err = env.graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
		From:      n2.Id,
		To:        n3.Id,
		Relation:  "connected",
		ValidFrom: n3.ValidFrom, // later of the two
		ValidTo:   n2.ValidTo,
	})
	require.NoError(t, err)

	// Traverse from n1 at now with max_depth=2.
	at := uint64(env.clock.Now())
	stream, err := env.graph.Traverse(ctx, &pb.TraverseRequest{
		Start:    n1.Id,
		At:       at,
		MaxDepth: 2,
	})
	require.NoError(t, err)

	var results []*pb.TraverseResult
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		results = append(results, r)
	}

	assert.Len(t, results, 2, "should reach n2 and n3")
}

func TestGraphService_GetNode(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{
		Type:       "device",
		Properties: map[string][]byte{"name": []byte("sensor-1")},
	})
	require.NoError(t, err)

	resp, err := env.graph.GetNode(ctx, &pb.GetNodeRequest{Id: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "device", resp.Type)
	assert.False(t, resp.Deleted)
}

func TestGraphService_DeleteNode(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	created, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "temp"})
	require.NoError(t, err)

	_, err = env.graph.DeleteNode(ctx, &pb.DeleteNodeRequest{Id: created.Id})
	require.NoError(t, err)

	resp, err := env.graph.GetNode(ctx, &pb.GetNodeRequest{Id: created.Id})
	require.NoError(t, err)
	assert.True(t, resp.Deleted)
}

func TestGraphService_CreateAndGetEdge(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	n1, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "a"})
	require.NoError(t, err)
	n2, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: "b"})
	require.NoError(t, err)

	e, err := env.graph.CreateEdge(ctx, &pb.CreateEdgeRequest{
		From:      n1.Id,
		To:        n2.Id,
		Relation:  "linked",
		ValidFrom: n2.ValidFrom, // later of the two
		ValidTo:   n1.ValidTo,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, e.Id)

	resp, err := env.graph.GetEdge(ctx, &pb.GetEdgeRequest{Id: e.Id})
	require.NoError(t, err)
	assert.Equal(t, "linked", resp.Relation)
	assert.Equal(t, n1.Id, resp.From)
	assert.Equal(t, n2.Id, resp.To)
}

// ---------------------------------------------------------------------------
// Scenario: CausalQuery über gRPC Streaming
// ---------------------------------------------------------------------------

func TestGraphService_CausalTraverse(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Build a causal chain: pump -> valve -> sensor.
	pump, err := env.graphL.CreateNodeWithID("pump_3", "pump", nil)
	require.NoError(t, err)
	valve, err := env.graphL.CreateNodeWithID("valve_1", "valve", nil)
	require.NoError(t, err)
	sensor, err := env.graphL.CreateNodeWithID("sensor_1", "sensor", nil)
	require.NoError(t, err)

	// Edge validity must be within both nodes' ranges.
	_, err = env.graphL.CreateEdge("pump_3", "valve_1", "feeds",
		valve.ValidFrom, pump.ValidTo, nil)
	require.NoError(t, err)
	_, err = env.graphL.CreateEdge("valve_1", "sensor_1", "feeds",
		sensor.ValidFrom, valve.ValidTo, nil)
	require.NoError(t, err)

	// Simulate property changes with temporal ordering.
	triggerTime := env.clock.Now()
	env.store.Set("node:pump_3:prop:pressure", []byte("high"))

	// Valve changes 5 seconds later.
	valveChangeTime := hlc.NewHLC(triggerTime.WallMicros()+5_000_000, 0, 0)
	env.store.SetWithHLC("node:valve_1:prop:flow", valveChangeTime, []byte("low"), false)

	// Sensor changes 10 seconds later.
	sensorChangeTime := hlc.NewHLC(triggerTime.WallMicros()+10_000_000, 0, 0)
	env.store.SetWithHLC("node:sensor_1:prop:temp", sensorChangeTime, []byte("rising"), false)

	// Causal traverse via gRPC.
	stream, err := env.graph.Causal(ctx, &pb.CausalRequest{
		Trigger:      "pump_3",
		At:           uint64(triggerTime),
		MaxDepth:     3,
		WindowMicros: 30 * 60 * 1_000_000, // 30 minutes
	})
	require.NoError(t, err)

	var results []*pb.CausalResult
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		results = append(results, r)
	}

	assert.GreaterOrEqual(t, len(results), 1, "should find at least one causal result")
	for _, r := range results {
		assert.NotEmpty(t, r.NodeId)
		assert.Greater(t, r.Confidence, 0.0)
		assert.NotEmpty(t, r.CausalPath)
	}

	// Verify the causal result for sensor has the expected fields.
	if len(results) >= 2 {
		found := false
		for _, r := range results {
			if r.NodeId == "sensor_1" {
				found = true
				assert.Equal(t, int32(2), r.Depth)
				break
			}
		}
		assert.True(t, found, "should find sensor_1 in causal results")
	}
}

// ---------------------------------------------------------------------------
// Scenario: Watch-Subscription empfängt Live-Updates
// ---------------------------------------------------------------------------

func TestWatchService_LiveUpdates(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start watching.
	stream, err := env.watch.Watch(ctx, &pb.WatchRequest{
		Keys: []string{"sensor:temp_1:prop:value"},
	})
	require.NoError(t, err)

	// Give the watch subscription time to register.
	time.Sleep(50 * time.Millisecond)

	// Write via another call.
	_, err = env.kv.Set(ctx, &pb.SetRequest{
		Key:   "sensor:temp_1:prop:value",
		Value: []byte("25.0"),
	})
	require.NoError(t, err)

	// Receive the watch event.
	event, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "sensor:temp_1:prop:value", event.Key)
	assert.Equal(t, []byte("25.0"), event.Value)
	assert.Greater(t, event.Timestamp, uint64(0))
	assert.False(t, event.Deleted)
}

func TestWatchService_DeleteNotification(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set initial value.
	_, err := env.kv.Set(ctx, &pb.SetRequest{Key: "watch_del", Value: []byte("v")})
	require.NoError(t, err)

	// Start watching.
	stream, err := env.watch.Watch(ctx, &pb.WatchRequest{Keys: []string{"watch_del"}})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Delete.
	_, err = env.kv.Delete(ctx, &pb.DeleteRequest{Key: "watch_del"})
	require.NoError(t, err)

	// Receive watch event.
	event, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, "watch_del", event.Key)
	assert.True(t, event.Deleted)
}

// ---------------------------------------------------------------------------
// Scenario: gRPC Interceptors loggen und messen
// ---------------------------------------------------------------------------

func TestServer_InterceptorsLogAndCount(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	// Send 10 requests.
	for i := 0; i < 10; i++ {
		_, err := env.kv.Set(ctx, &pb.SetRequest{
			Key:   "counter",
			Value: []byte{byte(i)},
		})
		require.NoError(t, err)
	}

	assert.GreaterOrEqual(t, env.srv.RequestCount(), int64(10))
	assert.GreaterOrEqual(t, len(env.srv.LogEntries()), 10)
}

// ---------------------------------------------------------------------------
// Scenario: Protobuf-Definitionen kompilieren fehlerfrei
// ---------------------------------------------------------------------------
// This scenario is verified by the compilation of this test file itself,
// which imports the generated protobuf types from api/gen/naladb/v1.

// ---------------------------------------------------------------------------
// Edge cases and validation
// ---------------------------------------------------------------------------

func TestKVService_EmptyKey(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	_, err := env.kv.Set(ctx, &pb.SetRequest{Key: "", Value: []byte("v")})
	assert.Error(t, err)

	_, err = env.kv.Get(ctx, &pb.GetRequest{Key: ""})
	assert.Error(t, err)

	_, err = env.kv.Delete(ctx, &pb.DeleteRequest{Key: ""})
	assert.Error(t, err)
}

func TestGraphService_EmptyNodeType(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	_, err := env.graph.CreateNode(ctx, &pb.CreateNodeRequest{Type: ""})
	assert.Error(t, err)
}

func TestGraphService_NodeNotFound(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()

	_, err := env.graph.GetNode(ctx, &pb.GetNodeRequest{Id: "nonexistent"})
	assert.Error(t, err)
}
