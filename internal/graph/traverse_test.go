package graph

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// setupEdgeWithHLC creates an edge directly in the store with specific HLC values,
// including adjacency list updates.
func setupEdgeWithHLC(t *testing.T, s *store.Store, id, from, to, relation string, validFrom, validTo hlc.HLC) {
	t.Helper()
	meta := EdgeMeta{
		ID:        id,
		From:      from,
		To:        to,
		Relation:  relation,
		ValidFrom: validFrom,
		ValidTo:   validTo,
	}
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	s.SetWithHLC("edge:"+id+":meta", validFrom, data, false)

	// Update outgoing adjacency list for 'from' node.
	updateAdjListWithHLC(t, s, "graph:adj:"+from+":out", id, validFrom)
	// Update incoming adjacency list for 'to' node.
	updateAdjListWithHLC(t, s, "graph:adj:"+to+":in", id, validFrom)
}

// updateAdjListWithHLC adds an edge ID to an adjacency list at a given HLC.
func updateAdjListWithHLC(t *testing.T, s *store.Store, key, edgeID string, at hlc.HLC) {
	t.Helper()
	var adj AdjacencyList
	r := s.Get(key)
	if r.Found {
		require.NoError(t, json.Unmarshal(r.Value, &adj))
	}
	adj.EdgeIDs = append(adj.EdgeIDs, edgeID)
	data, err := json.Marshal(adj)
	require.NoError(t, err)
	s.SetWithHLC(key, at, data, false)
}

// --- Scenario: TraverseAt findet nur zum Zeitpunkt gültige Edges ---

func TestTraverseAt_TemporalValidity(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	// Setup nodes valid from 0 to MaxHLC.
	for _, id := range []string{"a", "b", "c", "d"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// e1: a→b, valid [1000, 3000)
	setupEdgeWithHLC(t, s, "e1", "a", "b", "calls",
		hlc.NewHLC(1000, 0, 0), hlc.NewHLC(3000, 0, 0))
	// e2: a→c, valid [2000, 5000)
	setupEdgeWithHLC(t, s, "e2", "a", "c", "calls",
		hlc.NewHLC(2000, 0, 0), hlc.NewHLC(5000, 0, 0))
	// e3: b→d, valid [1000, 4000)
	setupEdgeWithHLC(t, s, "e3", "b", "d", "calls",
		hlc.NewHLC(1000, 0, 0), hlc.NewHLC(4000, 0, 0))

	results, err := g.TraverseAt("a", TraversalOption{
		At:        hlc.NewHLC(2500, 0, 0),
		MaxDepth:  2,
		Direction: Outgoing,
	})
	require.NoError(t, err)

	// Should find b (depth=1, via e1), c (depth=1, via e2), d (depth=2, via e3).
	assert.Len(t, results, 3)

	byNode := map[string]TraversalResult{}
	for _, r := range results {
		byNode[r.NodeID] = r
	}

	assert.Equal(t, 1, byNode["b"].Depth)
	assert.Equal(t, "e1", byNode["b"].ViaEdge)
	assert.Equal(t, "calls", byNode["b"].ViaRelation)

	assert.Equal(t, 1, byNode["c"].Depth)
	assert.Equal(t, "e2", byNode["c"].ViaEdge)

	assert.Equal(t, 2, byNode["d"].Depth)
	assert.Equal(t, "e3", byNode["d"].ViaEdge)
}

// --- Scenario: TraverseAt respektiert max_depth ---

func TestTraverseAt_MaxDepth(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	// Chain: a → b → c → d → e, all valid at t=1000.
	nodes := []string{"a", "b", "c", "d", "e"}
	for _, id := range nodes {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}
	for i := 0; i < len(nodes)-1; i++ {
		setupEdgeWithHLC(t, s, fmt.Sprintf("e%d", i+1), nodes[i], nodes[i+1], "calls",
			hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	}

	results, err := g.TraverseAt("a", TraversalOption{
		At:        hlc.NewHLC(1000, 0, 0),
		MaxDepth:  2,
		Direction: Outgoing,
	})
	require.NoError(t, err)

	nodeIDs := map[string]bool{}
	for _, r := range results {
		nodeIDs[r.NodeID] = true
	}

	assert.True(t, nodeIDs["b"], "b should be at depth 1")
	assert.True(t, nodeIDs["c"], "c should be at depth 2")
	assert.False(t, nodeIDs["d"], "d should be excluded (depth 3)")
	assert.False(t, nodeIDs["e"], "e should be excluded (depth 4)")
	assert.Len(t, results, 2)
}

// --- Scenario: TraverseAt mit Relationsfilter ---

func TestTraverseAt_RelationFilter(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	for _, id := range []string{"a", "b", "c", "d"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	setupEdgeWithHLC(t, s, "e1", "a", "b", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "a", "c", "reads",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e3", "a", "d", "writes",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)

	results, err := g.TraverseAt("a", TraversalOption{
		At:             hlc.NewHLC(1000, 0, 0),
		Direction:      Outgoing,
		RelationFilter: []string{"calls", "reads"},
	})
	require.NoError(t, err)

	nodeIDs := map[string]bool{}
	for _, r := range results {
		nodeIDs[r.NodeID] = true
	}

	assert.True(t, nodeIDs["b"])
	assert.True(t, nodeIDs["c"])
	assert.False(t, nodeIDs["d"], "d should be excluded by relation filter")
	assert.Len(t, results, 2)
}

// --- Scenario: TraverseAt INCOMING Richtung ---

func TestTraverseAt_Incoming(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	for _, id := range []string{"a", "b", "c"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// b→a and c→a
	setupEdgeWithHLC(t, s, "e1", "b", "a", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "c", "a", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)

	results, err := g.TraverseAt("a", TraversalOption{
		At:        hlc.NewHLC(1000, 0, 0),
		Direction: Incoming,
	})
	require.NoError(t, err)

	nodeIDs := map[string]bool{}
	for _, r := range results {
		nodeIDs[r.NodeID] = true
	}

	assert.True(t, nodeIDs["b"])
	assert.True(t, nodeIDs["c"])
	assert.Len(t, results, 2)
}

// --- Scenario: PathQuery findet kürzesten Pfad zu Zeitpunkt T ---

func TestPathQuery_ShortestPath(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	for _, id := range []string{"a", "b", "c", "d", "e"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// Long path: a → b → c → d
	setupEdgeWithHLC(t, s, "e1", "a", "b", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e2", "b", "c", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e3", "c", "d", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	// Short path: a → e → d
	setupEdgeWithHLC(t, s, "e4", "a", "e", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	setupEdgeWithHLC(t, s, "e5", "e", "d", "calls",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)

	result, err := g.PathQuery("a", "d", PathQueryOption{
		At:        hlc.NewHLC(1000, 0, 0),
		Direction: Outgoing,
	})
	require.NoError(t, err)

	assert.Equal(t, 2, result.Length)
	assert.Equal(t, []string{"a", "e", "d"}, result.NodeIDs)
}

// --- Scenario: PathQuery no path ---

func TestPathQuery_NoPath(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	for _, id := range []string{"a", "b"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// No edges between a and b.
	_, err := g.PathQuery("a", "b", PathQueryOption{
		At:        hlc.NewHLC(1000, 0, 0),
		Direction: Outgoing,
	})
	assert.ErrorIs(t, err, ErrNoPath)
}

// --- Scenario: Traversal mit Node-Properties im Ergebnis ---

func TestTraverseAt_WithNodeProperties(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	setupNodeWithHLC(t, s, "pump_3", "pump", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	setupNodeWithHLC(t, s, "press_7", "press", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)

	// Set property at time 800.
	s.SetWithHLC("node:press_7:prop:temperature", hlc.NewHLC(800, 0, 0), []byte("89.7"), false)

	// Edge pump_3 → press_7.
	setupEdgeWithHLC(t, s, "e1", "pump_3", "press_7", "supplies",
		hlc.NewHLC(500, 0, 0), hlc.MaxHLC)

	results, err := g.TraverseAt("pump_3", TraversalOption{
		At:                    hlc.NewHLC(1000, 0, 0),
		Direction:             Outgoing,
		IncludeNodeProperties: true,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, "press_7", results[0].NodeID)
	assert.Equal(t, []byte("89.7"), results[0].Properties["temperature"])
}

// --- Scenario: Traversal eines nicht-existierenden Zeitpunkts ---

func TestTraverseAt_NoValidEdges(t *testing.T) {
	_, s := newTestGraph(t)
	g := New(s, hlc.NewClock(0))

	for _, id := range []string{"a", "b"} {
		setupNodeWithHLC(t, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// Edge valid from 5000 to 9000 only.
	setupEdgeWithHLC(t, s, "e1", "a", "b", "calls",
		hlc.NewHLC(5000, 0, 0), hlc.NewHLC(9000, 0, 0))

	results, err := g.TraverseAt("a", TraversalOption{
		At:        hlc.NewHLC(1000, 0, 0),
		Direction: Outgoing,
	})
	require.NoError(t, err)
	assert.Empty(t, results)
}

// --- Scenario: Performance – 3-Hop Traversal unter 50ms ---

func BenchmarkTraverseAt_3Hop(b *testing.B) {
	_, s := newBenchGraph(b)
	g := New(s, hlc.NewClock(0))

	const numNodes = 10_000
	const numEdges = 50_000

	// Create nodes.
	for i := range numNodes {
		id := fmt.Sprintf("n%d", i)
		setupNodeWithHLCBench(b, s, id, "test", hlc.NewHLC(0, 0, 0), hlc.MaxHLC)
	}

	// Create edges with pseudo-random connections.
	for i := range numEdges {
		from := fmt.Sprintf("n%d", i%numNodes)
		to := fmt.Sprintf("n%d", (i*7+13)%numNodes)
		if from == to {
			to = fmt.Sprintf("n%d", (i+1)%numNodes)
		}
		setupEdgeWithHLCBench(b, s, fmt.Sprintf("e%d", i), from, to, "calls",
			hlc.NewHLC(500, 0, 0), hlc.MaxHLC)
	}

	at := hlc.NewHLC(1000, 0, 0)
	opts := TraversalOption{
		At:        at,
		MaxDepth:  3,
		Direction: Outgoing,
	}

	b.ResetTimer()
	for range b.N {
		_, err := g.TraverseAt("n0", opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// newBenchGraph creates a Graph for benchmarks.
func newBenchGraph(b *testing.B) (*Graph, *store.Store) {
	b.Helper()
	clock := hlc.NewClock(0)
	// Use a no-op WAL writer for benchmarks.
	s := store.NewWithoutWAL(clock)
	g := New(s, clock)
	return g, s
}

func setupNodeWithHLCBench(b *testing.B, s *store.Store, id, nodeType string, validFrom, validTo hlc.HLC) {
	b.Helper()
	meta := NodeMeta{
		ID:        id,
		Type:      nodeType,
		ValidFrom: validFrom,
		ValidTo:   validTo,
	}
	data, _ := json.Marshal(meta)
	s.SetWithHLC("node:"+id+":meta", validFrom, data, false)
	emptyAdj, _ := json.Marshal(AdjacencyList{EdgeIDs: []string{}})
	s.SetWithHLC("graph:adj:"+id+":out", validFrom, emptyAdj, false)
	s.SetWithHLC("graph:adj:"+id+":in", validFrom, emptyAdj, false)
}

func setupEdgeWithHLCBench(b *testing.B, s *store.Store, id, from, to, relation string, validFrom, validTo hlc.HLC) {
	b.Helper()
	meta := EdgeMeta{
		ID:        id,
		From:      from,
		To:        to,
		Relation:  relation,
		ValidFrom: validFrom,
		ValidTo:   validTo,
	}
	data, _ := json.Marshal(meta)
	s.SetWithHLC("edge:"+id+":meta", validFrom, data, false)

	// Update adjacency lists.
	updateAdjListBench(b, s, "graph:adj:"+from+":out", id, validFrom)
	updateAdjListBench(b, s, "graph:adj:"+to+":in", id, validFrom)
}

func updateAdjListBench(b *testing.B, s *store.Store, key, edgeID string, at hlc.HLC) {
	b.Helper()
	var adj AdjacencyList
	r := s.Get(key)
	if r.Found {
		_ = json.Unmarshal(r.Value, &adj)
	}
	adj.EdgeIDs = append(adj.EdgeIDs, edgeID)
	data, _ := json.Marshal(adj)
	s.SetWithHLC(key, at, data, false)
}
