package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/blob"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/wal"
)

// newTestStore creates a Store backed by a temporary WAL file.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "wal-*.bin")
	require.NoError(t, err)
	w := wal.NewWriter(f, wal.WriterOptions{})
	clock := hlc.NewClock(0)
	t.Cleanup(func() { w.Close() })
	return New(clock, w)
}

// --- Scenario: Set und Get eines Werts ---

func TestStore_SetAndGet(t *testing.T) {
	s := newTestStore(t)

	ts, err := s.Set("sensor:temp_1:prop:value", []byte("72.3"))
	require.NoError(t, err)
	assert.False(t, ts.IsZero(), "timestamp should be a valid HLC value")

	r := s.Get("sensor:temp_1:prop:value")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("72.3"), r.Value)
	assert.Equal(t, ts, r.HLC)
	assert.False(t, r.HLC.IsZero(), "timestamp should be a valid HLC value")
}

// --- Scenario: Get eines nicht-existierenden Keys ---

func TestStore_GetNonExistent(t *testing.T) {
	s := newTestStore(t)

	r := s.Get("nicht:vorhanden")
	assert.False(t, r.Found)
}

// --- Scenario: Set überschreibt mit neuerem Timestamp ---

func TestStore_SetOverwritesWithNewerTimestamp(t *testing.T) {
	s := newTestStore(t)

	ts1, err := s.Set("k", []byte("v1"))
	require.NoError(t, err)

	ts2, err := s.Set("k", []byte("v2"))
	require.NoError(t, err)

	assert.True(t, ts1.Before(ts2), "t2 should be after t1")

	r := s.Get("k")
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v2"), r.Value)

	// Old value should still be in the version log (WAL-backed history).
	history := s.History("k", HistoryOptions{})
	require.Len(t, history, 2)
	assert.Equal(t, []byte("v1"), history[0].Value)
	assert.Equal(t, []byte("v2"), history[1].Value)
}

// --- Scenario: Delete setzt Tombstone ---

func TestStore_DeleteSetsTombstone(t *testing.T) {
	s := newTestStore(t)

	_, err := s.Set("k", []byte("v1"))
	require.NoError(t, err)

	_, err = s.Delete("k")
	require.NoError(t, err)

	r := s.Get("k")
	assert.False(t, r.Found, "deleted key should not be found")

	// Verify tombstone record exists in version log.
	history := s.History("k", HistoryOptions{})
	require.Len(t, history, 2)
	assert.True(t, history[1].Tombstone, "last entry should be a tombstone")
}

func TestStore_DeleteTombstoneInWAL(t *testing.T) {
	dir := t.TempDir()
	walPath := dir + "/test.wal"

	f, err := os.Create(walPath)
	require.NoError(t, err)
	w := wal.NewWriter(f, wal.WriterOptions{})
	clock := hlc.NewClock(0)
	s := New(clock, w)

	_, err = s.Set("k", []byte("v1"))
	require.NoError(t, err)

	_, err = s.Delete("k")
	require.NoError(t, err)

	require.NoError(t, w.Close())

	// Read back the WAL and verify tombstone record.
	rf, err := os.Open(walPath)
	require.NoError(t, err)
	defer rf.Close()

	reader := wal.NewReader(rf)
	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 2)

	assert.False(t, records[0].Flags.IsTombstone(), "first record should not be tombstone")
	assert.True(t, records[1].Flags.IsTombstone(), "second record should be tombstone")
	assert.Equal(t, []byte("k"), records[1].Key)
}

// --- Scenario: GetAt liefert Point-in-Time-Wert ---

func TestStore_GetAtPointInTime(t *testing.T) {
	s := newTestStore(t)

	// Write three versions with known HLC timestamps.
	s.SetWithHLC("k", hlc.NewHLC(1000, 0, 0), []byte("v1"), false)
	s.SetWithHLC("k", hlc.NewHLC(2000, 0, 0), []byte("v2"), false)
	s.SetWithHLC("k", hlc.NewHLC(3000, 0, 0), []byte("v3"), false)

	r := s.GetAt("k", hlc.NewHLC(1500, 0, 0))
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v1"), r.Value)
}

// --- Scenario: GetAt vor erstem Write gibt not-found ---

func TestStore_GetAtBeforeFirstWrite(t *testing.T) {
	s := newTestStore(t)

	s.SetWithHLC("k", hlc.NewHLC(1000, 0, 0), []byte("v1"), false)

	r := s.GetAt("k", hlc.NewHLC(500, 0, 0))
	assert.False(t, r.Found)
}

// --- Scenario: History liefert alle Versionen ---

func TestStore_HistoryAllVersions(t *testing.T) {
	s := newTestStore(t)

	for i := 0; i < 50; i++ {
		ts := hlc.NewHLC(int64(i+1)*1000, 0, 0)
		s.SetWithHLC("k", ts, []byte(fmt.Sprintf("v%d", i)), false)
	}

	history := s.History("k", HistoryOptions{})
	require.Len(t, history, 50)

	// Verify ascending timestamp order.
	for i := 1; i < len(history); i++ {
		assert.True(t, history[i-1].HLC.Before(history[i].HLC),
			"entries should be in ascending timestamp order")
	}
}

// --- Scenario: History mit Zeitfenster ---

func TestStore_HistoryTimeWindow(t *testing.T) {
	s := newTestStore(t)

	for _, us := range []int64{1000, 2000, 3000, 4000, 5000} {
		ts := hlc.NewHLC(us, 0, 0)
		s.SetWithHLC("k", ts, []byte(fmt.Sprintf("v%d", us)), false)
	}

	history := s.History("k", HistoryOptions{
		From: hlc.NewHLC(2000, 0, 0),
		To:   hlc.NewHLC(4000, 0, 0),
	})

	require.Len(t, history, 3)
	assert.Equal(t, hlc.NewHLC(2000, 0, 0), history[0].HLC)
	assert.Equal(t, hlc.NewHLC(3000, 0, 0), history[1].HLC)
	assert.Equal(t, hlc.NewHLC(4000, 0, 0), history[2].HLC)
}

// --- Scenario: History mit Limit und Reverse ---

func TestStore_HistoryLimitAndReverse(t *testing.T) {
	s := newTestStore(t)

	for i := 0; i < 100; i++ {
		ts := hlc.NewHLC(int64(i+1)*1000, 0, 0)
		s.SetWithHLC("k", ts, []byte(fmt.Sprintf("v%d", i)), false)
	}

	history := s.History("k", HistoryOptions{
		Limit:   10,
		Reverse: true,
	})

	require.Len(t, history, 10)

	// Should be the 10 newest entries, newest first.
	assert.Equal(t, hlc.NewHLC(100_000, 0, 0), history[0].HLC)
	assert.Equal(t, hlc.NewHLC(91_000, 0, 0), history[9].HLC)

	// Verify descending order.
	for i := 1; i < len(history); i++ {
		assert.True(t, history[i].HLC.Before(history[i-1].HLC),
			"entries should be in descending timestamp order")
	}
}

// --- Scenario: Sharded Index verteilt Keys gleichmäßig ---

func TestStore_ShardedIndexDistribution(t *testing.T) {
	s := newTestStore(t)

	const numKeys = 100_000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key:%d", i)
		s.SetWithHLC(key, hlc.NewHLC(int64(i+1), 0, 0), []byte("v"), false)
	}

	sizes := s.idx.ShardSizes()

	// Calculate mean and standard deviation.
	var sum float64
	for _, sz := range sizes {
		sum += float64(sz)
	}
	mean := sum / float64(len(sizes))

	var varianceSum float64
	for _, sz := range sizes {
		diff := float64(sz) - mean
		varianceSum += diff * diff
	}
	stddev := math.Sqrt(varianceSum / float64(len(sizes)))

	// Standard deviation should be less than 10% of the mean.
	assert.Less(t, stddev, mean*0.10,
		"shard size stddev (%.2f) should be < 10%% of mean (%.2f)", stddev, mean)
}

// --- Scenario: Concurrent Reads und Writes sind race-free ---

func TestStore_ConcurrentReadsAndWrites(t *testing.T) {
	s := newTestStore(t)

	const writers = 10
	const readers = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup

	// Writers.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				_, err := s.Set("shared-key", []byte(fmt.Sprintf("w%d-i%d", id, i)))
				assert.NoError(t, err)
			}
		}(w)
	}

	// Readers.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				r := s.Get("shared-key")
				// Value is either not found (if no write happened yet) or a valid written value.
				if r.Found {
					assert.NotEmpty(t, r.Value)
				}
			}
		}()
	}

	wg.Wait()

	// After all writes, key should exist.
	r := s.Get("shared-key")
	assert.True(t, r.Found)
}

// --- Scenario: BatchSet schreibt atomar ---

func TestStore_BatchSetAtomic(t *testing.T) {
	s := newTestStore(t)

	entries := map[string][]byte{
		"batch:a": []byte("va"),
		"batch:b": []byte("vb"),
		"batch:c": []byte("vc"),
		"batch:d": []byte("vd"),
		"batch:e": []byte("ve"),
	}

	timestamps, err := s.BatchSet(entries)
	require.NoError(t, err)
	require.Len(t, timestamps, 5)

	// All values should be readable.
	for key, expectedValue := range entries {
		r := s.Get(key)
		assert.True(t, r.Found, "key %s should be found", key)
		assert.Equal(t, expectedValue, r.Value, "key %s value mismatch", key)
	}

	// All timestamps should be ascending (consecutive from same clock).
	for i := 1; i < len(timestamps); i++ {
		assert.True(t, timestamps[i-1].Before(timestamps[i]),
			"timestamp[%d] should be before timestamp[%d]", i-1, i)
	}
}

// --- Additional edge-case tests ---

func TestStore_GetAtExactTimestamp(t *testing.T) {
	s := newTestStore(t)

	ts := hlc.NewHLC(2000, 0, 0)
	s.SetWithHLC("k", ts, []byte("v2"), false)

	r := s.GetAt("k", ts)
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v2"), r.Value)
}

func TestStore_GetAtAfterDelete(t *testing.T) {
	s := newTestStore(t)

	s.SetWithHLC("k", hlc.NewHLC(1000, 0, 0), []byte("v1"), false)
	s.SetWithHLC("k", hlc.NewHLC(2000, 0, 0), nil, true) // tombstone

	r := s.GetAt("k", hlc.NewHLC(1500, 0, 0))
	assert.True(t, r.Found)
	assert.Equal(t, []byte("v1"), r.Value)

	r = s.GetAt("k", hlc.NewHLC(2500, 0, 0))
	assert.False(t, r.Found)
}

func TestStore_HistoryEmptyKey(t *testing.T) {
	s := newTestStore(t)

	history := s.History("nonexistent", HistoryOptions{})
	assert.Nil(t, history)
}

func TestStore_MultipleKeysIndependent(t *testing.T) {
	s := newTestStore(t)

	_, err := s.Set("a", []byte("1"))
	require.NoError(t, err)
	_, err = s.Set("b", []byte("2"))
	require.NoError(t, err)

	ra := s.Get("a")
	rb := s.Get("b")
	assert.Equal(t, []byte("1"), ra.Value)
	assert.Equal(t, []byte("2"), rb.Value)

	_, err = s.Delete("a")
	require.NoError(t, err)

	ra = s.Get("a")
	rb = s.Get("b")
	assert.False(t, ra.Found)
	assert.True(t, rb.Found)
}

// --- AP-06: Blob Store Integration Tests ---

// newTestStoreWithBlob creates a Store with a blob store attached.
func newTestStoreWithBlob(t *testing.T) (*Store, *blob.Store) {
	t.Helper()
	dir := t.TempDir()

	f, err := os.CreateTemp(dir, "wal-*.bin")
	require.NoError(t, err)
	w := wal.NewWriter(f, wal.WriterOptions{})
	clock := hlc.NewClock(0)
	t.Cleanup(func() { w.Close() })

	blobDir := filepath.Join(dir, "blobs")
	bs, err := blob.NewStore(blobDir)
	require.NoError(t, err)

	s := New(clock, w)
	s.SetBlobStore(bs)
	return s, bs
}

func TestStore_SmallValueInline(t *testing.T) {
	s, _ := newTestStoreWithBlob(t)

	value := bytes.Repeat([]byte("x"), 1000) // 1 KiB, well under 64 KiB
	_, err := s.Set("small:key", value)
	require.NoError(t, err)

	r := s.Get("small:key")
	assert.True(t, r.Found)
	assert.Equal(t, value, r.Value)

	// Verify the index entry is NOT a blob ref.
	entry, ok := s.idx.GetEntry("small:key")
	assert.True(t, ok)
	assert.False(t, entry.BlobRef, "small value should not be a blob ref")
}

func TestStore_LargeValueBlobTransparent(t *testing.T) {
	s, bs := newTestStoreWithBlob(t)

	value := make([]byte, 100_000) // ~100 KiB, exceeds 64 KiB limit
	for i := range value {
		value[i] = byte(i % 256)
	}

	_, err := s.Set("large:key", value)
	require.NoError(t, err)

	// Get should transparently return the original 100 KiB value.
	r := s.Get("large:key")
	assert.True(t, r.Found)
	assert.Equal(t, value, r.Value)

	// Verify the index entry IS a blob ref.
	entry, ok := s.idx.GetEntry("large:key")
	assert.True(t, ok)
	assert.True(t, entry.BlobRef, "large value should be a blob ref")
	assert.Equal(t, wal.BlobRefSize, len(entry.Value), "index value should be a 40-byte blob ref")

	// Verify the blob file exists on disk.
	hash := sha256.Sum256(value)
	hexHash := hex.EncodeToString(hash[:])
	blobPath := filepath.Join(bs.Dir(), hexHash[:2], hexHash+".blob")
	_, err = os.Stat(blobPath)
	assert.NoError(t, err, "blob file should exist on disk")
}

func TestStore_BlobDedup(t *testing.T) {
	s, _ := newTestStoreWithBlob(t)

	value := make([]byte, 100_000)
	for i := range value {
		value[i] = byte(i % 251)
	}

	_, err := s.Set("k1", value)
	require.NoError(t, err)
	_, err = s.Set("k2", value)
	require.NoError(t, err)

	// Both keys should return the same value.
	r1 := s.Get("k1")
	r2 := s.Get("k2")
	assert.Equal(t, value, r1.Value)
	assert.Equal(t, value, r2.Value)
}

func TestStore_DeleteDerefBlob(t *testing.T) {
	s, bs := newTestStoreWithBlob(t)

	value := make([]byte, 100_000)
	for i := range value {
		value[i] = byte(i % 137)
	}

	_, err := s.Set("k1", value)
	require.NoError(t, err)

	entry, ok := s.idx.GetEntry("k1")
	require.True(t, ok)
	ref, err := blob.UnmarshalRef(entry.Value)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), bs.RefCount(ref))

	_, err = s.Delete("k1")
	require.NoError(t, err)

	// Ref count should be 0 after delete.
	assert.Equal(t, uint32(0), bs.RefCount(ref))

	// GC with minAge=0 should remove the blob.
	removed, err := bs.GC(0)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
}

func TestStore_LargeValueInGetAt(t *testing.T) {
	s, _ := newTestStoreWithBlob(t)

	value := make([]byte, 100_000)
	for i := range value {
		value[i] = byte(i % 99)
	}

	ts, err := s.Set("k", value)
	require.NoError(t, err)

	// GetAt should resolve the blob transparently.
	r := s.GetAt("k", ts)
	assert.True(t, r.Found)
	assert.Equal(t, value, r.Value)
}

func TestStore_LargeValueInHistory(t *testing.T) {
	s, _ := newTestStoreWithBlob(t)

	value := make([]byte, 100_000)
	for i := range value {
		value[i] = byte(i % 77)
	}

	_, err := s.Set("k", value)
	require.NoError(t, err)

	history := s.History("k", HistoryOptions{})
	require.Len(t, history, 1)
	assert.Equal(t, value, history[0].Value)
}

// --- AP-06: KeyMeta Integration Tests ---

func TestStore_KeyMetaUpdatedOnSet(t *testing.T) {
	s := newTestStore(t)
	r := meta.NewRegistry()
	s.SetMeta(r)

	values := []string{"20.0", "22.0", "21.0", "25.0", "19.0"}
	for _, v := range values {
		_, err := s.Set("sensor:temp:value", []byte(v))
		require.NoError(t, err)
	}

	km := r.Get("sensor:temp:value")
	require.NotNil(t, km)
	assert.Equal(t, uint64(5), km.TotalWrites)
	assert.InDelta(t, 19.0, km.MinValue, 0.01)
	assert.InDelta(t, 25.0, km.MaxValue, 0.01)
	assert.InDelta(t, 21.4, km.AvgValue, 0.01)
	assert.Greater(t, km.StdDevValue, 0.0)
}

func TestStore_KeyMetaUpdatedOnDelete(t *testing.T) {
	s := newTestStore(t)
	r := meta.NewRegistry()
	s.SetMeta(r)

	_, err := s.Set("k", []byte("v1"))
	require.NoError(t, err)
	_, err = s.Delete("k")
	require.NoError(t, err)

	km := r.Get("k")
	require.NotNil(t, km)
	assert.Equal(t, uint64(2), km.TotalWrites) // set + delete
}
