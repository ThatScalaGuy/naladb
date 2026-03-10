package benchmarks

import (
	"fmt"
	"testing"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/query"
	"github.com/thatscalaguy/naladb/internal/store"
)

// BenchmarkQuery_HistoryLast10 benchmarks a HISTORY query via NalaQL.
func BenchmarkQuery_HistoryLast10(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)
	registry := meta.NewRegistry()
	s.SetMeta(registry)

	// Write 1000 versions for a single key.
	const key = "sensor:bench"
	for i := 0; i < 1000; i++ {
		s.SetWithHLC(key, clock.Now(), []byte(fmt.Sprintf(`{"v":%d}`, i)), false)
	}

	exec := query.NewExecutor(s, g, registry, clock)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := exec.Execute(`GET HISTORY("sensor:bench") LAST 10`)
		if err != nil {
			b.Fatalf("query error: %v", err)
		}
		if len(rows) != 10 {
			b.Fatalf("expected 10 rows, got %d", len(rows))
		}
	}
}

// BenchmarkQuery_HistoryAll benchmarks a full HISTORY query without LAST limit.
func BenchmarkQuery_HistoryAll(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)
	registry := meta.NewRegistry()
	s.SetMeta(registry)

	const key = "sensor:full"
	for i := 0; i < 100; i++ {
		s.SetWithHLC(key, clock.Now(), []byte(fmt.Sprintf(`{"v":%d}`, i)), false)
	}

	exec := query.NewExecutor(s, g, registry, clock)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := exec.Execute(`GET HISTORY("sensor:full")`)
		if err != nil {
			b.Fatalf("query error: %v", err)
		}
		if len(rows) != 100 {
			b.Fatalf("expected 100 rows, got %d", len(rows))
		}
	}
}

// BenchmarkQuery_MatchNode benchmarks a MATCH node pattern query.
func BenchmarkQuery_MatchNode(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)
	g := graph.New(s, clock)
	registry := meta.NewRegistry()
	s.SetMeta(registry)

	// Create some nodes.
	for i := 0; i < 50; i++ {
		_, err := g.CreateNodeWithID(fmt.Sprintf("n%d", i), "sensor", nil)
		if err != nil {
			b.Fatalf("create node: %v", err)
		}
	}

	exec := query.NewExecutor(s, g, registry, clock)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(`MATCH (n:sensor) RETURN n`)
		if err != nil {
			b.Fatalf("query error: %v", err)
		}
	}
}
