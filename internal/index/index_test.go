package index

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

func ts(wall int64, logical uint16) hlc.HLC {
	return hlc.NewHLC(wall, 1, logical)
}

func TestNew(t *testing.T) {
	idx := New()
	if idx.Len() != 0 {
		t.Fatalf("new index should be empty, got Len=%d", idx.Len())
	}
}

func TestPutAndGet(t *testing.T) {
	idx := New()
	ok := idx.Put("k1", ts(100, 0), []byte("v1"), false)
	if !ok {
		t.Fatal("first Put should succeed")
	}

	e, found := idx.Get("k1")
	if !found {
		t.Fatal("Get should find k1")
	}
	if string(e.Value) != "v1" {
		t.Fatalf("expected v1, got %s", e.Value)
	}
}

func TestGetMissing(t *testing.T) {
	idx := New()
	_, found := idx.Get("missing")
	if found {
		t.Fatal("Get on missing key should return false")
	}
}

func TestPutRejectsStaleWrite(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(200, 0), []byte("new"), false)
	ok := idx.Put("k1", ts(100, 0), []byte("old"), false)
	if ok {
		t.Fatal("Put with older HLC should be rejected")
	}

	e, _ := idx.Get("k1")
	if string(e.Value) != "new" {
		t.Fatalf("value should remain 'new', got %s", e.Value)
	}
}

func TestPutAcceptsEqualHLC(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(100, 0), []byte("v1"), false)
	ok := idx.Put("k1", ts(100, 0), []byte("v2"), false)
	if !ok {
		t.Fatal("Put with equal HLC should succeed")
	}

	e, _ := idx.Get("k1")
	if string(e.Value) != "v2" {
		t.Fatalf("expected v2, got %s", e.Value)
	}
}

func TestPutAcceptsNewerHLC(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(100, 0), []byte("v1"), false)
	ok := idx.Put("k1", ts(200, 0), []byte("v2"), false)
	if !ok {
		t.Fatal("Put with newer HLC should succeed")
	}

	e, _ := idx.Get("k1")
	if string(e.Value) != "v2" {
		t.Fatalf("expected v2, got %s", e.Value)
	}
}

func TestDelete(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(100, 0), []byte("v1"), false)

	ok := idx.Delete("k1", ts(200, 0))
	if !ok {
		t.Fatal("Delete should succeed")
	}

	_, found := idx.Get("k1")
	if found {
		t.Fatal("Get should not find deleted key")
	}
}

func TestDeleteRejectsStaleTimestamp(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(200, 0), []byte("v1"), false)

	ok := idx.Delete("k1", ts(100, 0))
	if ok {
		t.Fatal("Delete with older HLC should be rejected")
	}

	_, found := idx.Get("k1")
	if !found {
		t.Fatal("key should still be accessible after rejected delete")
	}
}

func TestGetReturnsFalseForTombstone(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(100, 0), []byte("v1"), true)

	_, found := idx.Get("k1")
	if found {
		t.Fatal("Get should return false for tombstoned entry")
	}
}

func TestGetEntry(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(100, 0), []byte("v1"), false)

	e, ok := idx.GetEntry("k1")
	if !ok {
		t.Fatal("GetEntry should find k1")
	}
	if string(e.Value) != "v1" || e.Tombstone {
		t.Fatal("GetEntry returned wrong data")
	}
}

func TestGetEntryReturnsTombstone(t *testing.T) {
	idx := New()
	idx.Put("k1", ts(100, 0), nil, true)

	e, ok := idx.GetEntry("k1")
	if !ok {
		t.Fatal("GetEntry should find tombstoned key")
	}
	if !e.Tombstone {
		t.Fatal("entry should be tombstoned")
	}
}

func TestGetEntryMissing(t *testing.T) {
	idx := New()
	_, ok := idx.GetEntry("missing")
	if ok {
		t.Fatal("GetEntry on missing key should return false")
	}
}

func TestPutEntry(t *testing.T) {
	idx := New()
	e := Entry{
		HLC:     ts(100, 0),
		Value:   []byte("blob-ref-123"),
		BlobRef: true,
	}
	ok := idx.PutEntry("k1", e)
	if !ok {
		t.Fatal("PutEntry should succeed")
	}

	got, found := idx.GetEntry("k1")
	if !found {
		t.Fatal("should find entry")
	}
	if !got.BlobRef {
		t.Fatal("BlobRef should be true")
	}
	if string(got.Value) != "blob-ref-123" {
		t.Fatalf("expected blob-ref-123, got %s", got.Value)
	}
}

func TestPutEntryRejectsStale(t *testing.T) {
	idx := New()
	idx.PutEntry("k1", Entry{HLC: ts(200, 0), Value: []byte("new")})

	ok := idx.PutEntry("k1", Entry{HLC: ts(100, 0), Value: []byte("old")})
	if ok {
		t.Fatal("PutEntry with older HLC should be rejected")
	}
}

func TestLen(t *testing.T) {
	idx := New()
	idx.Put("a", ts(1, 0), []byte("1"), false)
	idx.Put("b", ts(2, 0), []byte("2"), false)
	idx.Put("c", ts(3, 0), nil, true) // tombstone counts

	if idx.Len() != 3 {
		t.Fatalf("expected Len=3, got %d", idx.Len())
	}
}

func TestShardDistribution(t *testing.T) {
	idx := New()
	n := 10000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		idx.Put(key, ts(int64(i), 0), []byte("v"), false)
	}

	sizes := idx.ShardSizes()
	expected := float64(n) / float64(numShards)

	var maxDev float64
	for _, sz := range sizes {
		dev := math.Abs(float64(sz)-expected) / expected
		if dev > maxDev {
			maxDev = dev
		}
	}

	// With FNV-1a and 10k keys across 256 shards, no shard should deviate
	// more than 3x from the mean (~39 keys per shard).
	if maxDev > 2.0 {
		t.Fatalf("shard distribution too uneven: max deviation %.1f%% from mean", maxDev*100)
	}

	total := 0
	for _, sz := range sizes {
		total += sz
	}
	if total != n {
		t.Fatalf("shard sizes sum to %d, expected %d", total, n)
	}
}

func TestConcurrentAccess(t *testing.T) {
	idx := New()
	const goroutines = 16
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func() {
			defer wg.Done()
			for i := range opsPerGoroutine {
				key := fmt.Sprintf("g%d-k%d", g, i)
				idx.Put(key, ts(int64(i), 0), []byte("v"), false)
				idx.Get(key)
				idx.GetEntry(key)
			}
		}()
	}

	wg.Wait()

	expected := goroutines * opsPerGoroutine
	if idx.Len() != expected {
		t.Fatalf("expected %d entries, got %d", expected, idx.Len())
	}
}

func TestConcurrentWritesSameKey(t *testing.T) {
	idx := New()
	const goroutines = 32
	const ops = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func() {
			defer wg.Done()
			for i := range ops {
				wall := int64(g*ops + i)
				idx.Put("contested", ts(wall, 0), fmt.Appendf(nil, "%d", wall), false)
			}
		}()
	}

	wg.Wait()

	// The entry should hold the highest wall time written.
	e, found := idx.Get("contested")
	if !found {
		t.Fatal("contested key should exist")
	}

	maxWall := int64((goroutines - 1) * ops + (ops - 1))
	if e.HLC.WallMicros() != maxWall {
		t.Fatalf("expected wall=%d, got %d", maxWall, e.HLC.WallMicros())
	}
}
