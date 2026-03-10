package benchmarks

import (
	"fmt"
	"sync"
	"testing"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// BenchmarkGet_CurrentValue benchmarks in-memory current-value reads.
// Target: p99 < 1 µs for O(1) index lookup.
func BenchmarkGet_CurrentValue(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)

	// Populate 100,000 keys.
	const numKeys = 100_000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%06d", i)
		keys[i] = key
		s.SetWithHLC(key, clock.Now(), []byte("value"), false)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(keys[i%numKeys])
			i++
		}
	})
}

// BenchmarkSet_WriteThroughput benchmarks write throughput without WAL.
// Target: > 500,000 writes/s with 8 goroutines.
func BenchmarkSet_WriteThroughput(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			s.SetWithHLC(key, clock.Now(), []byte("value-data"), false)
			i++
		}
	})
}

// BenchmarkSet_WriteThroughput_8Goroutines benchmarks write throughput
// with exactly 8 concurrent goroutines.
func BenchmarkSet_WriteThroughput_8Goroutines(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)

	b.ResetTimer()

	var wg sync.WaitGroup
	opsPerGoroutine := b.N / 8
	if opsPerGoroutine == 0 {
		opsPerGoroutine = 1
	}

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d-key-%d", gid, i)
				s.SetWithHLC(key, clock.Now(), []byte("value-data"), false)
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkHistory_Last100 benchmarks History retrieval for a key with
// 10,000 versions, fetching the last 100.
// Target: < 10 ms.
func BenchmarkHistory_Last100(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)

	// Write 10,000 versions for a single key.
	const key = "history-key"
	const numVersions = 10_000
	for i := 0; i < numVersions; i++ {
		s.SetWithHLC(key, clock.Now(), []byte(fmt.Sprintf("version-%d", i)), false)
	}

	opts := store.HistoryOptions{
		Limit:   100,
		Reverse: true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := s.History(key, opts)
		if len(result) != 100 {
			b.Fatalf("expected 100 entries, got %d", len(result))
		}
	}
}

// BenchmarkGetAt_PointInTime benchmarks point-in-time reads using binary
// search over a key's version log.
func BenchmarkGetAt_PointInTime(b *testing.B) {
	clock := hlc.NewClock(0)
	s := store.NewWithoutWAL(clock)

	const key = "time-travel-key"
	const numVersions = 1000
	timestamps := make([]hlc.HLC, numVersions)
	for i := 0; i < numVersions; i++ {
		ts := clock.Now()
		timestamps[i] = ts
		s.SetWithHLC(key, ts, []byte(fmt.Sprintf("v%d", i)), false)
	}

	// Query at the midpoint.
	queryTS := timestamps[numVersions/2]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := s.GetAt(key, queryTS)
		if !r.Found {
			b.Fatal("expected to find value")
		}
	}
}
