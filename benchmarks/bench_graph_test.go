package benchmarks

import (
	"fmt"
	"testing"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// BenchmarkTraverseAt_3Hop benchmarks a 3-hop BFS traversal on a graph
// with many nodes and edges.
// Target: < 50 ms.
func BenchmarkTraverseAt_3Hop(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)

	// Build a graph: create nodes and connect them in a chain/fan pattern.
	const numNodes = 1000
	const edgesPerNode = 5

	nodeIDs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		id := fmt.Sprintf("node-%04d", i)
		nodeIDs[i] = id
		_, err := g.CreateNodeWithID(id, "sensor", nil)
		if err != nil {
			b.Fatalf("create node: %v", err)
		}
	}

	// Create edges: each node connects to the next `edgesPerNode` nodes.
	now := clock.Now()
	for i := 0; i < numNodes; i++ {
		for j := 1; j <= edgesPerNode; j++ {
			target := (i + j) % numNodes
			_, err := g.CreateEdge(
				nodeIDs[i], nodeIDs[target], "connects",
				now, hlc.MaxHLC, nil,
			)
			if err != nil {
				b.Fatalf("create edge: %v", err)
			}
		}
	}

	queryTS := clock.Now()
	opts := graph.TraversalOption{
		At:        queryTS,
		MaxDepth:  3,
		Direction: graph.Outgoing,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := g.TraverseAt(nodeIDs[0], opts)
		if err != nil {
			b.Fatalf("traverse: %v", err)
		}
	}
}

// BenchmarkTraverseAt_1Hop benchmarks a single-hop BFS traversal.
func BenchmarkTraverseAt_1Hop(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)

	const numNodes = 100
	nodeIDs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		id := fmt.Sprintf("node-%04d", i)
		nodeIDs[i] = id
		_, err := g.CreateNodeWithID(id, "device", nil)
		if err != nil {
			b.Fatalf("create node: %v", err)
		}
	}

	// Star topology: node-0 connects to all others.
	now := clock.Now()
	for i := 1; i < numNodes; i++ {
		_, err := g.CreateEdge(nodeIDs[0], nodeIDs[i], "monitors", now, hlc.MaxHLC, nil)
		if err != nil {
			b.Fatalf("create edge: %v", err)
		}
	}

	queryTS := clock.Now()
	opts := graph.TraversalOption{
		At:        queryTS,
		MaxDepth:  1,
		Direction: graph.Outgoing,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := g.TraverseAt(nodeIDs[0], opts)
		if err != nil {
			b.Fatalf("traverse: %v", err)
		}
		if len(results) != numNodes-1 {
			b.Fatalf("expected %d results, got %d", numNodes-1, len(results))
		}
	}
}

// BenchmarkCreateNode benchmarks node creation throughput.
func BenchmarkCreateNode(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := g.CreateNodeWithID(fmt.Sprintf("bench-node-%d", i), "sensor", nil)
		if err != nil {
			b.Fatalf("create node: %v", err)
		}
	}
}
