package segment

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/wal"
)

func TestRemoveTombstonePairs_LatestTombstone(t *testing.T) {
	records := []*wal.Record{
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("key1"), Value: []byte("v1")},
		{HLC: hlc.NewHLC(2, 0, 0), Key: []byte("key1"), Flags: wal.FlagTombstone},
	}
	result := removeTombstonePairs(records)
	assert.Empty(t, result, "key with latest tombstone should be fully removed")
}

func TestRemoveTombstonePairs_NoTombstone(t *testing.T) {
	records := []*wal.Record{
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("key1"), Value: []byte("v1")},
		{HLC: hlc.NewHLC(2, 0, 0), Key: []byte("key1"), Value: []byte("v2")},
	}
	result := removeTombstonePairs(records)
	assert.Len(t, result, 2, "no tombstones, all records kept")
}

func TestRemoveTombstonePairs_TombstoneInMiddle(t *testing.T) {
	records := []*wal.Record{
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("key1"), Value: []byte("v1")},
		{HLC: hlc.NewHLC(2, 0, 0), Key: []byte("key1"), Flags: wal.FlagTombstone},
		{HLC: hlc.NewHLC(3, 0, 0), Key: []byte("key1"), Value: []byte("v3")},
	}
	result := removeTombstonePairs(records)
	require.Len(t, result, 1, "only the write after tombstone kept")
	assert.Equal(t, []byte("v3"), result[0].Value)
}

func TestRemoveTombstonePairs_MixedKeys(t *testing.T) {
	// 10 records: 2 keys with tombstones, 6 live records
	records := []*wal.Record{
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("a"), Value: []byte("a1")},
		{HLC: hlc.NewHLC(2, 0, 0), Key: []byte("a"), Flags: wal.FlagTombstone},
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("b"), Value: []byte("b1")},
		{HLC: hlc.NewHLC(2, 0, 0), Key: []byte("b"), Value: []byte("b2")},
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("c"), Value: []byte("c1")},
		{HLC: hlc.NewHLC(2, 0, 0), Key: []byte("c"), Flags: wal.FlagTombstone},
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("d"), Value: []byte("d1")},
		{HLC: hlc.NewHLC(2, 0, 0), Key: []byte("d"), Value: []byte("d2")},
		{HLC: hlc.NewHLC(3, 0, 0), Key: []byte("d"), Value: []byte("d3")},
		{HLC: hlc.NewHLC(1, 0, 0), Key: []byte("e"), Value: []byte("e1")},
	}
	result := removeTombstonePairs(records)
	// a: tombstone at latest → removed (0)
	// b: no tombstone → kept (2)
	// c: tombstone at latest → removed (0)
	// d: no tombstone → kept (3)
	// e: no tombstone → kept (1)
	assert.Len(t, result, 6)
}

func TestRemoveTombstonePairs_Empty(t *testing.T) {
	result := removeTombstonePairs(nil)
	assert.Nil(t, result)
}

func TestRemoveTombstonePairs_LargeScale(t *testing.T) {
	// 10000 records: 2000 tombstones for 2000 previous writes → 6000 remaining
	records := make([]*wal.Record, 0, 10000)

	// 2000 keys with write + tombstone
	for i := range 2000 {
		key := fmt.Appendf(nil, "dead-%04d", i)
		records = append(records,
			&wal.Record{HLC: hlc.NewHLC(int64(i), 0, 0), Key: key, Value: []byte("v")},
			&wal.Record{HLC: hlc.NewHLC(int64(i)+10000, 0, 0), Key: key, Flags: wal.FlagTombstone},
		)
	}

	// 6000 live records
	for i := range 6000 {
		key := fmt.Appendf(nil, "live-%04d", i)
		records = append(records, &wal.Record{
			HLC:   hlc.NewHLC(int64(i), 0, 0),
			Key:   key,
			Value: []byte("v"),
		})
	}

	// Sort by (key, HLC ascending) as required.
	sortRecords(records)

	result := removeTombstonePairs(records)
	assert.Len(t, result, 6000, "10000 - 2000 tombstones - 2000 writes = 6000")
}

func TestCompactor_L0Overflow(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 1024)
	require.NoError(t, err)

	// Create 5 segments (exceeds MaxL0Segments=4).
	for seg := range 5 {
		for i := range 10 {
			ts := hlc.NewHLC(int64(seg*100+i), 0, 0)
			key := fmt.Appendf(nil, "key-%d-%d", seg, i)
			err := mgr.Append(ts, 0, key, []byte("value"))
			require.NoError(t, err)
		}
		require.NoError(t, mgr.Rotate())
	}

	segments := mgr.Segments()
	require.Len(t, segments, 5, "should have 5 L0 segments")
	for _, seg := range segments {
		assert.Equal(t, 0, seg.Meta.Level, "all should be L0")
	}

	// Run compaction.
	compactor := NewCompactor(mgr, CompactionConfig{MaxL0Segments: 4})
	require.NoError(t, compactor.CheckAndCompact())

	// Should have 1 L1 segment.
	segments = mgr.Segments()
	require.Len(t, segments, 1, "5 L0 should be merged into 1 L1")
	assert.Equal(t, 1, segments[0].Meta.Level, "merged segment should be L1")
	assert.Equal(t, uint64(50), segments[0].Meta.RecordCount, "all 50 records preserved")
}

func TestCompactor_PreservesActiveData(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 512)
	require.NoError(t, err)

	// Write data: 100 unique keys across 5 segments, no tombstones.
	for seg := range 5 {
		for i := range 20 {
			ts := hlc.NewHLC(int64(seg*100+i), 0, 0)
			key := fmt.Appendf(nil, "key-%03d", seg*20+i)
			err := mgr.Append(ts, 0, key, []byte("value"))
			require.NoError(t, err)
		}
		require.NoError(t, mgr.Rotate())
	}

	compactor := NewCompactor(mgr, CompactionConfig{MaxL0Segments: 4})
	require.NoError(t, compactor.CheckAndCompact())

	// Verify all keys are accessible.
	segments := mgr.Segments()
	require.Len(t, segments, 1)
	assert.Equal(t, uint64(100), segments[0].Meta.RecordCount)

	// Verify each key can be retrieved.
	for i := range 100 {
		key := fmt.Sprintf("key-%03d", i)
		ts := hlc.NewHLC(999999, 0, 0) // far future
		rec, err := segments[0].GetAt(key, ts)
		require.NoError(t, err)
		require.NotNil(t, rec, "key %s should exist", key)
		assert.Equal(t, []byte("value"), rec.Value)
	}
}

func TestCompactor_TombstonePairRemoval(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 8192)
	require.NoError(t, err)

	// Write 20 keys, then tombstone 10 of them across different segments.
	for i := range 20 {
		ts := hlc.NewHLC(int64(i), 0, 0)
		key := fmt.Appendf(nil, "key-%03d", i)
		err := mgr.Append(ts, 0, key, []byte("value"))
		require.NoError(t, err)
	}
	require.NoError(t, mgr.Rotate())

	// Tombstone keys 0-9 in a second segment.
	for i := range 10 {
		ts := hlc.NewHLC(int64(100+i), 0, 0)
		key := fmt.Appendf(nil, "key-%03d", i)
		err := mgr.Append(ts, wal.FlagTombstone, key, nil)
		require.NoError(t, err)
	}
	require.NoError(t, mgr.Rotate())

	// Create more segments to exceed L0 threshold.
	for seg := range 3 {
		ts := hlc.NewHLC(int64(200+seg), 0, 0)
		key := fmt.Appendf(nil, "extra-%d", seg)
		err := mgr.Append(ts, 0, key, []byte("v"))
		require.NoError(t, err)
		require.NoError(t, mgr.Rotate())
	}

	require.Len(t, mgr.Segments(), 5)

	compactor := NewCompactor(mgr, CompactionConfig{MaxL0Segments: 4})
	require.NoError(t, compactor.CheckAndCompact())

	segments := mgr.Segments()
	require.Len(t, segments, 1)

	// 10 live keys + 3 extra keys = 13 records (tombstoned keys removed).
	assert.Equal(t, uint64(13), segments[0].Meta.RecordCount)

	// Verify tombstoned keys are gone.
	for i := range 10 {
		key := fmt.Sprintf("key-%03d", i)
		rec, err := segments[0].GetAt(key, hlc.NewHLC(999999, 0, 0))
		require.NoError(t, err)
		assert.Nil(t, rec, "tombstoned key %s should not exist after compaction", key)
	}

	// Verify live keys exist.
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("key-%03d", i)
		rec, err := segments[0].GetAt(key, hlc.NewHLC(999999, 0, 0))
		require.NoError(t, err)
		require.NotNil(t, rec, "live key %s should exist after compaction", key)
	}
}

func TestCompactor_ConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 512)
	require.NoError(t, err)

	// Create initial data: 5 L0 segments.
	for seg := range 5 {
		for i := range 10 {
			ts := hlc.NewHLC(int64(seg*100+i), 0, 0)
			key := fmt.Appendf(nil, "key-%d", i)
			err := mgr.Append(ts, 0, key, []byte("value"))
			require.NoError(t, err)
		}
		require.NoError(t, mgr.Rotate())
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// Concurrent reads during compaction.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			ts := hlc.NewHLC(int64(999+i), 0, 0)
			_, err := mgr.GetAt("key-0", ts)
			if err != nil {
				errCh <- fmt.Errorf("read error: %w", err)
				return
			}
		}
	}()

	// Concurrent writes during compaction.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 50 {
			ts := hlc.NewHLC(int64(10000+i), 0, 0)
			key := fmt.Appendf(nil, "new-key-%d", i)
			if err := mgr.Append(ts, 0, key, []byte("v")); err != nil {
				errCh <- fmt.Errorf("write error: %w", err)
				return
			}
		}
	}()

	// Run compaction concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		compactor := NewCompactor(mgr, CompactionConfig{MaxL0Segments: 4})
		if err := compactor.CheckAndCompact(); err != nil {
			errCh <- fmt.Errorf("compaction error: %w", err)
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}
}

func TestCompactor_BelowThreshold(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 512)
	require.NoError(t, err)

	// Create 3 segments (below threshold of 4).
	for seg := range 3 {
		ts := hlc.NewHLC(int64(seg), 0, 0)
		err := mgr.Append(ts, 0, []byte("key"), []byte("value"))
		require.NoError(t, err)
		require.NoError(t, mgr.Rotate())
	}

	compactor := NewCompactor(mgr, CompactionConfig{MaxL0Segments: 4})
	require.NoError(t, compactor.CheckAndCompact())

	// No compaction should have occurred.
	assert.Len(t, mgr.Segments(), 3)
}

func TestCompactor_AllTombstones(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 8192)
	require.NoError(t, err)

	// Write keys and then tombstone them all.
	for seg := range 5 {
		for i := range 5 {
			ts := hlc.NewHLC(int64(seg*100+i), 0, 0)
			key := fmt.Appendf(nil, "key-%d", i)
			var flags wal.Flags
			if seg >= 3 {
				flags = wal.FlagTombstone
			}
			err := mgr.Append(ts, flags, key, []byte("v"))
			require.NoError(t, err)
		}
		require.NoError(t, mgr.Rotate())
	}

	compactor := NewCompactor(mgr, CompactionConfig{MaxL0Segments: 4})
	require.NoError(t, compactor.CheckAndCompact())

	// All records should be tombstone pairs → empty result.
	segments := mgr.Segments()
	assert.Empty(t, segments, "all tombstone pairs should be removed")
}

func TestManager_SwapSegments(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 1024)
	require.NoError(t, err)

	// Create 3 segments.
	for i := range 3 {
		ts := hlc.NewHLC(int64(i), 0, 0)
		err := mgr.Append(ts, 0, []byte("k"), []byte("v"))
		require.NoError(t, err)
		require.NoError(t, mgr.Rotate())
	}
	require.Len(t, mgr.Segments(), 3)

	// Swap first two for a new one.
	segs := mgr.Segments()
	newSeg := &Segment{ID: mgr.AllocateID(), Dir: dir, Meta: Metadata{Level: 1}}
	mgr.SwapSegments([]uint64{segs[0].ID, segs[1].ID}, newSeg)

	result := mgr.Segments()
	require.Len(t, result, 2, "removed 2, added 1")
	assert.Equal(t, segs[2].ID, result[0].ID)
	assert.Equal(t, newSeg.ID, result[1].ID)
}

func TestManager_RemoveSegments(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 1024)
	require.NoError(t, err)

	// Create 3 segments.
	for i := range 3 {
		ts := hlc.NewHLC(int64(i), 0, 0)
		err := mgr.Append(ts, 0, []byte("k"), []byte("v"))
		require.NoError(t, err)
		require.NoError(t, mgr.Rotate())
	}

	segs := mgr.Segments()
	require.Len(t, segs, 3)

	// Remove the middle segment.
	mgr.RemoveSegments([]uint64{segs[1].ID})

	result := mgr.Segments()
	require.Len(t, result, 2)
	assert.Equal(t, segs[0].ID, result[0].ID)
	assert.Equal(t, segs[2].ID, result[1].ID)

	// Verify files were deleted.
	_, err = os.Stat(segs[1].LogPath())
	assert.True(t, os.IsNotExist(err), "segment files should be deleted")
}

func TestSegment_ReadAll(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 1024)
	require.NoError(t, err)

	for i := range 10 {
		ts := hlc.NewHLC(int64(i), 0, 0)
		key := fmt.Appendf(nil, "key-%02d", i)
		err := mgr.Append(ts, 0, key, []byte("value"))
		require.NoError(t, err)
	}
	require.NoError(t, mgr.Rotate())

	segs := mgr.Segments()
	require.Len(t, segs, 1)

	records, err := segs[0].ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, 10)
}

// sortRecords sorts records by (key, HLC ascending) for testing.
func sortRecords(records []*wal.Record) {
	for i := 0; i < len(records); i++ {
		for j := i + 1; j < len(records); j++ {
			ki, kj := string(records[i].Key), string(records[j].Key)
			if ki > kj || (ki == kj && records[j].HLC.Before(records[i].HLC)) {
				records[i], records[j] = records[j], records[i]
			}
		}
	}
}
