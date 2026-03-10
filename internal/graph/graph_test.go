package graph

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
	"github.com/thatscalaguy/naladb/internal/wal"
)

// newTestGraph creates a Graph backed by a temporary WAL file.
func newTestGraph(t *testing.T) (*Graph, *store.Store) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "wal-*.bin")
	require.NoError(t, err)
	w := wal.NewWriter(f, wal.WriterOptions{})
	clock := hlc.NewClock(0)
	t.Cleanup(func() { w.Close() })
	s := store.New(clock, w)
	g := New(s, clock)
	return g, s
}

// newTestGraphWithClock creates a Graph with a controllable physical clock.
func newTestGraphWithClock(t *testing.T, phys hlc.PhysicalClock) (*Graph, *store.Store) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "wal-*.bin")
	require.NoError(t, err)
	w := wal.NewWriter(f, wal.WriterOptions{})
	clock := hlc.NewClockWithPhysical(0, phys)
	t.Cleanup(func() { w.Close() })
	s := store.New(clock, w)
	g := New(s, clock)
	return g, s
}

// setupNodeWithHLC creates a node directly in the store with specific HLC values.
func setupNodeWithHLC(t *testing.T, s *store.Store, id, nodeType string, validFrom, validTo hlc.HLC) {
	t.Helper()
	meta := NodeMeta{
		ID:        id,
		Type:      nodeType,
		ValidFrom: validFrom,
		ValidTo:   validTo,
	}
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	s.SetWithHLC("node:"+id+":meta", validFrom, data, false)

	emptyAdj, _ := json.Marshal(AdjacencyList{EdgeIDs: []string{}})
	s.SetWithHLC("graph:adj:"+id+":out", validFrom, emptyAdj, false)
	s.SetWithHLC("graph:adj:"+id+":in", validFrom, emptyAdj, false)
}

// --- Scenario: Node erstellen ---

func TestGraph_CreateNode(t *testing.T) {
	g, s := newTestGraph(t)

	meta, err := g.CreateNode("sensor", map[string][]byte{
		"unit": []byte("celsius"),
	})
	require.NoError(t, err)

	// UUID v7 ID should be non-empty.
	assert.NotEmpty(t, meta.ID)
	assert.Equal(t, "sensor", meta.Type)
	assert.False(t, meta.ValidFrom.IsZero())
	assert.Equal(t, hlc.MaxHLC, meta.ValidTo)
	assert.False(t, meta.Deleted)

	// Verify meta is stored under "node:{id}:meta".
	r := s.Get("node:" + meta.ID + ":meta")
	assert.True(t, r.Found)
	var stored NodeMeta
	require.NoError(t, json.Unmarshal(r.Value, &stored))
	assert.Equal(t, meta.ID, stored.ID)
	assert.Equal(t, "sensor", stored.Type)
	assert.Equal(t, hlc.MaxHLC, stored.ValidTo)

	// Verify property is stored under "node:{id}:prop:unit".
	r = s.Get("node:" + meta.ID + ":prop:unit")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("celsius"), r.Value)
}

// --- Scenario: Node Properties sind temporal versioniert ---

func TestGraph_NodePropertiesTemporallyVersioned(t *testing.T) {
	g, s := newTestGraph(t)

	meta, err := g.CreateNodeWithID("n1", "sensor", map[string][]byte{
		"temperature": []byte("20.0"),
	})
	require.NoError(t, err)
	_ = meta

	err = g.UpdateNode("n1", map[string][]byte{
		"temperature": []byte("25.0"),
	})
	require.NoError(t, err)

	// Current value should be 25.0.
	r := s.Get("node:n1:prop:temperature")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("25.0"), r.Value)

	// History should contain both values.
	history := s.History("node:n1:prop:temperature", store.HistoryOptions{})
	require.Len(t, history, 2)
	assert.Equal(t, []byte("20.0"), history[0].Value)
	assert.Equal(t, []byte("25.0"), history[1].Value)
}

// --- Scenario: Node Soft-Delete ---

func TestGraph_NodeSoftDelete(t *testing.T) {
	g, s := newTestGraph(t)

	// Create node with 3 properties.
	_, err := g.CreateNodeWithID("n1", "machine", map[string][]byte{
		"prop1": []byte("v1"),
		"prop2": []byte("v2"),
		"prop3": []byte("v3"),
	})
	require.NoError(t, err)

	// Create a second node for edges.
	node2, err := g.CreateNodeWithID("n2", "sensor", nil)
	require.NoError(t, err)

	// Create 2 edges. Use node2.ValidFrom since it's the later of the two.
	e1, err := g.CreateEdge("n1", "n2", "connects", node2.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)
	e2, err := g.CreateEdge("n2", "n1", "feeds", node2.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)

	// Delete the node.
	err = g.DeleteNode("n1")
	require.NoError(t, err)

	// Meta should show deleted=true.
	meta, err := g.GetNode("n1")
	require.NoError(t, err)
	assert.True(t, meta.Deleted)

	// History of node meta should be preserved.
	history := s.History("node:n1:meta", store.HistoryOptions{})
	assert.GreaterOrEqual(t, len(history), 2, "history should contain at least creation and deletion")

	// Connected edges should be soft-deleted.
	e1Meta, err := g.GetEdge(e1.ID)
	require.NoError(t, err)
	assert.True(t, e1Meta.Deleted)

	e2Meta, err := g.GetEdge(e2.ID)
	require.NoError(t, err)
	assert.True(t, e2Meta.Deleted)
}

// --- Scenario: Edge erstellen mit temporaler Gültigkeit ---

func TestGraph_CreateEdgeWithTemporalValidity(t *testing.T) {
	g, s := newTestGraph(t)

	// Set up nodes with known valid ranges using SetWithHLC.
	setupNodeWithHLC(t, s, "pump_3", "pump", hlc.NewHLC(1000, 0, 0), hlc.MaxHLC)
	setupNodeWithHLC(t, s, "press_7", "press", hlc.NewHLC(1000, 0, 0), hlc.MaxHLC)

	edge, err := g.CreateEdge("pump_3", "press_7", "supplies",
		hlc.NewHLC(2000, 0, 0), hlc.MaxHLC, nil)
	require.NoError(t, err)

	// Edge has a UUID v7 ID.
	assert.NotEmpty(t, edge.ID)
	assert.Equal(t, "pump_3", edge.From)
	assert.Equal(t, "press_7", edge.To)
	assert.Equal(t, "supplies", edge.Relation)
	assert.Equal(t, hlc.NewHLC(2000, 0, 0), edge.ValidFrom)
	assert.Equal(t, hlc.MaxHLC, edge.ValidTo)

	// Verify edge meta stored under "edge:{id}:meta".
	r := s.Get("edge:" + edge.ID + ":meta")
	assert.True(t, r.Found)
	var stored EdgeMeta
	require.NoError(t, json.Unmarshal(r.Value, &stored))
	assert.Equal(t, edge.From, stored.From)
	assert.Equal(t, edge.To, stored.To)
	assert.Equal(t, edge.Relation, stored.Relation)

	// Verify adjacency lists.
	outEdges, err := g.GetOutgoingEdges("pump_3")
	require.NoError(t, err)
	assert.Contains(t, outEdges, edge.ID)

	inEdges, err := g.GetIncomingEdges("press_7")
	require.NoError(t, err)
	assert.Contains(t, inEdges, edge.ID)
}

// --- Scenario: Edge-Validierung – valid muss innerhalb Node-valid liegen ---

func TestGraph_EdgeValidation_OutsideNodeValidity(t *testing.T) {
	g, s := newTestGraph(t)

	// Node "n1" with valid=[1000, 5000).
	setupNodeWithHLC(t, s, "n1", "test", hlc.NewHLC(1000, 0, 0), hlc.NewHLC(5000, 0, 0))
	// Node "n2" with valid=[2000, 6000).
	setupNodeWithHLC(t, s, "n2", "test", hlc.NewHLC(2000, 0, 0), hlc.NewHLC(6000, 0, 0))

	// Edge valid=[500, 3000) should fail because 500 < 1000.
	_, err := g.CreateEdge("n1", "n2", "test",
		hlc.NewHLC(500, 0, 0), hlc.NewHLC(3000, 0, 0), nil)
	assert.ErrorIs(t, err, ErrEdgeOutsideNodeValidity)
}

func TestGraph_EdgeValidation_ExceedsNodeEnd(t *testing.T) {
	g, s := newTestGraph(t)

	setupNodeWithHLC(t, s, "n1", "test", hlc.NewHLC(1000, 0, 0), hlc.NewHLC(5000, 0, 0))
	setupNodeWithHLC(t, s, "n2", "test", hlc.NewHLC(2000, 0, 0), hlc.NewHLC(6000, 0, 0))

	// Edge valid=[2000, 7000) should fail because 7000 > 6000.
	_, err := g.CreateEdge("n1", "n2", "test",
		hlc.NewHLC(2000, 0, 0), hlc.NewHLC(7000, 0, 0), nil)
	assert.ErrorIs(t, err, ErrEdgeOutsideNodeValidity)
}

func TestGraph_EdgeValidation_ValidWithinBothNodes(t *testing.T) {
	g, s := newTestGraph(t)

	setupNodeWithHLC(t, s, "n1", "test", hlc.NewHLC(1000, 0, 0), hlc.NewHLC(5000, 0, 0))
	setupNodeWithHLC(t, s, "n2", "test", hlc.NewHLC(2000, 0, 0), hlc.NewHLC(6000, 0, 0))

	// Edge valid=[2000, 5000) is within intersection [2000, 5000) — should succeed.
	edge, err := g.CreateEdge("n1", "n2", "test",
		hlc.NewHLC(2000, 0, 0), hlc.NewHLC(5000, 0, 0), nil)
	require.NoError(t, err)
	assert.NotEmpty(t, edge.ID)
}

// --- Scenario: Multi-Graph – mehrere Edges gleichen Typs erlaubt ---

func TestGraph_MultiGraph_MultipleEdgesSameType(t *testing.T) {
	g, _ := newTestGraph(t)

	_, err := g.CreateNodeWithID("a", "account", nil)
	require.NoError(t, err)
	b, err := g.CreateNodeWithID("b", "account", nil)
	require.NoError(t, err)

	// Create 3 edges of the same type between "a" and "b".
	// Use b.ValidFrom since b was created after a.
	var edges [3]EdgeMeta
	for i := 0; i < 3; i++ {
		e, err := g.CreateEdge("a", "b", "transfers", b.ValidFrom, hlc.MaxHLC, nil)
		require.NoError(t, err)
		edges[i] = e
	}

	// All 3 should have different IDs.
	ids := map[string]bool{}
	for _, e := range edges {
		ids[e.ID] = true
	}
	assert.Len(t, ids, 3)

	// All 3 should be in the outgoing adjacency list of "a".
	outEdges, err := g.GetOutgoingEdges("a")
	require.NoError(t, err)
	assert.Len(t, outEdges, 3)
	for _, e := range edges {
		assert.Contains(t, outEdges, e.ID)
	}

	// All 3 should be in the incoming adjacency list of "b".
	inEdges, err := g.GetIncomingEdges("b")
	require.NoError(t, err)
	assert.Len(t, inEdges, 3)
	for _, e := range edges {
		assert.Contains(t, inEdges, e.ID)
	}
}

// --- Scenario: Adjacency List ist versioniert ---

func TestGraph_AdjacencyListVersioned(t *testing.T) {
	var physTime int64 = 500
	phys := func() int64 { return atomic.LoadInt64(&physTime) }
	g, _ := newTestGraphWithClock(t, phys)

	// Create two nodes at time 500.
	_, err := g.CreateNodeWithID("n1", "test", nil)
	require.NoError(t, err)
	node2, err := g.CreateNodeWithID("n2", "test", nil)
	require.NoError(t, err)

	// Create edge e1 at ~1000. Use node2.ValidFrom since it's the later one.
	atomic.StoreInt64(&physTime, 1000)
	e1, err := g.CreateEdge("n1", "n2", "connects", node2.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)

	// Create edge e2 at ~2000.
	atomic.StoreInt64(&physTime, 2000)
	e2, err := g.CreateEdge("n1", "n2", "depends", node2.ValidFrom, hlc.MaxHLC, nil)
	require.NoError(t, err)

	// Delete edge e1 at ~3000.
	atomic.StoreInt64(&physTime, 3000)
	err = g.DeleteEdge(e1.ID)
	require.NoError(t, err)

	// Query at HLC 1500 — should contain only e1.
	edges, err := g.GetOutgoingEdgesAt("n1", hlc.NewHLC(1500, 0, 0))
	require.NoError(t, err)
	assert.Equal(t, []string{e1.ID}, edges)

	// Query at HLC 2500 — should contain e1 and e2.
	edges, err = g.GetOutgoingEdgesAt("n1", hlc.NewHLC(2500, 0, 0))
	require.NoError(t, err)
	assert.Len(t, edges, 2)
	assert.Contains(t, edges, e1.ID)
	assert.Contains(t, edges, e2.ID)

	// Query at HLC 3500 — should contain only e2.
	edges, err = g.GetOutgoingEdgesAt("n1", hlc.NewHLC(3500, 0, 0))
	require.NoError(t, err)
	assert.Equal(t, []string{e2.ID}, edges)
}

// --- Scenario: Namespace-Konvention wird konsistent verwendet ---

func TestGraph_NamespaceConvention(t *testing.T) {
	g, s := newTestGraph(t)

	_, err := g.CreateNodeWithID("n1", "machine", map[string][]byte{
		"status": []byte("active"),
	})
	require.NoError(t, err)

	// Verify all expected keys exist.
	expectedKeys := []string{
		"node:n1:meta",
		"node:n1:prop:status",
		"graph:adj:n1:out",
		"graph:adj:n1:in",
	}
	for _, key := range expectedKeys {
		r := s.Get(key)
		assert.True(t, r.Found, "key %q should exist", key)
	}
}

// --- Additional tests ---

func TestGraph_GetNode_NotFound(t *testing.T) {
	g, _ := newTestGraph(t)

	_, err := g.GetNode("nonexistent")
	assert.ErrorIs(t, err, ErrNodeNotFound)
}

func TestGraph_UpdateNode_Deleted(t *testing.T) {
	g, _ := newTestGraph(t)

	_, err := g.CreateNodeWithID("n1", "test", nil)
	require.NoError(t, err)

	err = g.DeleteNode("n1")
	require.NoError(t, err)

	err = g.UpdateNode("n1", map[string][]byte{"k": []byte("v")})
	assert.ErrorIs(t, err, ErrNodeDeleted)
}

func TestGraph_GetEdge_NotFound(t *testing.T) {
	g, _ := newTestGraph(t)

	_, err := g.GetEdge("nonexistent")
	assert.ErrorIs(t, err, ErrEdgeNotFound)
}

func TestGraph_ConcurrentNodeCreation(t *testing.T) {
	g, _ := newTestGraph(t)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := g.CreateNode("type"+strconv.Itoa(i), nil)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

func TestResolveNodeID_DirectID(t *testing.T) {
	g, _ := newTestGraph(t)
	node, err := g.CreateNode("sensor", map[string][]byte{
		"name": []byte("temp-1"),
	})
	require.NoError(t, err)

	resolved, err := g.ResolveNodeID(node.ID)
	require.NoError(t, err)
	assert.Equal(t, node.ID, resolved)
}

func TestResolveNodeID_DefaultScansAllProperties(t *testing.T) {
	g, _ := newTestGraph(t)
	node, err := g.CreateNode("sensor", map[string][]byte{
		"customProp": []byte("my-sensor"),
	})
	require.NoError(t, err)

	// With no resolveProps configured, all properties should be scanned.
	resolved, err := g.ResolveNodeID("my-sensor")
	require.NoError(t, err)
	assert.Equal(t, node.ID, resolved)
}

func TestResolveNodeID_ConfiguredProperties(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-*.bin")
	require.NoError(t, err)
	w := wal.NewWriter(f, wal.WriterOptions{})
	clock := hlc.NewClock(0)
	t.Cleanup(func() { w.Close() })
	s := store.New(clock, w)
	g := New(s, clock, WithResolveProperties("name", "serial"))

	node, err := g.CreateNode("device", map[string][]byte{
		"name":   []byte("device-1"),
		"serial": []byte("SN-42"),
		"other":  []byte("ignored"),
	})
	require.NoError(t, err)

	// Should find by configured property "name".
	resolved, err := g.ResolveNodeID("device-1")
	require.NoError(t, err)
	assert.Equal(t, node.ID, resolved)

	// Should find by configured property "serial".
	resolved, err = g.ResolveNodeID("SN-42")
	require.NoError(t, err)
	assert.Equal(t, node.ID, resolved)

	// Should NOT find by unconfigured property "other".
	_, err = g.ResolveNodeID("ignored")
	assert.Error(t, err)
}

func TestResolveNodeID_NotFound(t *testing.T) {
	g, _ := newTestGraph(t)
	_, err := g.ResolveNodeID("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestResolveNodeID_SkipsDeletedNodes(t *testing.T) {
	g, _ := newTestGraph(t)
	node, err := g.CreateNode("sensor", map[string][]byte{
		"name": []byte("temp-1"),
	})
	require.NoError(t, err)

	require.NoError(t, g.DeleteNode(node.ID))

	_, err = g.ResolveNodeID("temp-1")
	assert.Error(t, err)
}
