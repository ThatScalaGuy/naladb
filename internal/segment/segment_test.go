package segment

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/wal"
)

// --- Bloom Filter Tests ---

func TestBloomFilter_AddAndTest(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	bf.Add("alpha")
	bf.Add("beta")
	bf.Add("gamma")

	assert.True(t, bf.Test("alpha"))
	assert.True(t, bf.Test("beta"))
	assert.True(t, bf.Test("gamma"))

	// Keys never added should (almost certainly) not be found.
	falsePositives := 0
	for i := 0; i < 1000; i++ {
		if bf.Test(fmt.Sprintf("missing-%d", i)) {
			falsePositives++
		}
	}
	// With 3 items and 0.01 fp rate, false positives should be very rare.
	assert.Less(t, falsePositives, 50, "too many false positives")
}

func TestBloomFilter_WriteReadRoundtrip(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	for i := 0; i < 100; i++ {
		bf.Add(fmt.Sprintf("key-%d", i))
	}

	var buf bytes.Buffer
	_, err := bf.WriteTo(&buf)
	require.NoError(t, err)

	// Deserialize using a fresh bloom filter.
	bf2 := NewBloomFilter(1, 0.01)
	_, err = bf2.ReadFrom(&buf)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		assert.True(t, bf2.Test(fmt.Sprintf("key-%d", i)))
	}
}

func TestBloomFilter_FileRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bloom")

	bf := NewBloomFilter(50, 0.01)
	bf.Add("x")
	bf.Add("y")
	bf.Add("z")

	require.NoError(t, bf.WriteToFile(path))

	loaded, err := LoadBloomFilter(path)
	require.NoError(t, err)

	assert.True(t, loaded.Test("x"))
	assert.True(t, loaded.Test("y"))
	assert.True(t, loaded.Test("z"))
	assert.False(t, loaded.Test("w"))
}

// --- Sparse Index Tests ---

func TestSparseIndex_Lookup(t *testing.T) {
	si := NewSparseIndex()
	si.Add("apple", 0)
	si.Add("cherry", 1000)
	si.Add("mango", 2000)
	si.Add("peach", 3000)

	tests := []struct {
		key    string
		expect int64
	}{
		{"apple", 0},     // exact match on first entry
		{"banana", 0},    // between apple and cherry → start from apple
		{"cherry", 1000}, // exact match
		{"date", 1000},   // between cherry and mango → start from cherry
		{"mango", 2000},  // exact match
		{"orange", 2000}, // between mango and peach
		{"peach", 3000},  // exact match on last entry
		{"zebra", 3000},  // after all entries → start from peach
		{"aardvark", 0},  // before all entries → start from beginning
	}

	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			offset := si.Lookup(tc.key)
			assert.Equal(t, tc.expect, offset)
		})
	}
}

func TestSparseIndex_LookupEmpty(t *testing.T) {
	si := NewSparseIndex()
	assert.Equal(t, int64(0), si.Lookup("anything"))
}

func TestSparseIndex_WriteReadRoundtrip(t *testing.T) {
	si := NewSparseIndex()
	si.Add("alpha", 0)
	si.Add("beta", 500)
	si.Add("gamma", 1200)

	var buf bytes.Buffer
	_, err := si.WriteTo(&buf)
	require.NoError(t, err)

	si2 := NewSparseIndex()
	_, err = si2.ReadFrom(&buf)
	require.NoError(t, err)

	require.Len(t, si2.Entries, 3)
	assert.Equal(t, "alpha", si2.Entries[0].Key)
	assert.Equal(t, int64(0), si2.Entries[0].Offset)
	assert.Equal(t, "beta", si2.Entries[1].Key)
	assert.Equal(t, int64(500), si2.Entries[1].Offset)
	assert.Equal(t, "gamma", si2.Entries[2].Key)
	assert.Equal(t, int64(1200), si2.Entries[2].Offset)
}

func TestSparseIndex_FileRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.idx")

	si := NewSparseIndex()
	si.Add("key-a", 100)
	si.Add("key-b", 200)

	require.NoError(t, si.WriteToFile(path))

	loaded, err := LoadSparseIndex(path)
	require.NoError(t, err)
	require.Len(t, loaded.Entries, 2)
	assert.Equal(t, "key-a", loaded.Entries[0].Key)
	assert.Equal(t, int64(200), loaded.Entries[1].Offset)
}

// --- Segment Rotation Tests (Gherkin Scenario 1) ---

func TestSegment_RotationOnSizeLimit(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(1048576) // 1 MiB

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)
	defer mgr.Close()

	value := make([]byte, 990)
	for i := range value {
		value[i] = byte(i % 256)
	}

	// Write records until rotation triggers.
	for i := 0; i < 2000; i++ {
		ts := hlc.NewHLC(int64(i+1), 0, 0)
		key := fmt.Sprintf("key-%05d", i)
		require.NoError(t, mgr.Append(ts, 0, []byte(key), value))
	}

	segments := mgr.Segments()
	require.True(t, len(segments) >= 1, "expected at least 1 finalized segment")

	seg := segments[0]

	// Finalized segment files must exist.
	assert.FileExists(t, seg.LogPath())
	assert.FileExists(t, seg.IdxPath())
	assert.FileExists(t, seg.BloomPath())
	assert.FileExists(t, seg.MetaPath())

	// Active segment must exist.
	assert.FileExists(t, filepath.Join(dir, "seg-current.log"))
	assert.FileExists(t, filepath.Join(dir, "seg-current.idx"))

	// New active segment was created (not the same as the finalized one).
	assert.True(t, mgr.ActiveSize() < maxBytes,
		"new active segment should be smaller than maxBytes")
}

// --- Sparse Index Lookup Tests (Gherkin Scenario 2) ---

func TestSegment_SparseIndexEnablesLookup(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024) // large limit, won't auto-rotate

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	// Write 10,000 records.
	for i := 0; i < 10000; i++ {
		ts := hlc.NewHLC(int64(i+1), 0, 0)
		key := fmt.Sprintf("key-%05d", i)
		val := fmt.Sprintf("value-%05d", i)
		require.NoError(t, mgr.Append(ts, 0, []byte(key), []byte(val)))
	}

	require.NoError(t, mgr.Rotate())
	mgr.Close()

	segments := mgr.Segments()
	require.Len(t, segments, 1)
	seg := segments[0]

	// Sparse index should have entries.
	assert.True(t, seg.sparse.Len() > 0, "sparse index should have entries")

	// GetAt for a known key should use binary search and return correct value.
	ts := hlc.NewHLC(5001, 0, 0) // key-05000 was written at this HLC
	rec, err := seg.GetAt("key-05000", ts)
	require.NoError(t, err)
	require.NotNil(t, rec, "expected to find key-05000")
	assert.Equal(t, "key-05000", string(rec.Key))
	assert.Equal(t, "value-05000", string(rec.Value))

	// Verify sparse index binary search complexity: the number of index entries
	// should be roughly (total_bytes / BlockSize), and lookup requires
	// log2(entries) comparisons.
	expectedBlocks := seg.Meta.SizeBytes / BlockSize
	assert.True(t, int64(seg.sparse.Len()) <= expectedBlocks+2,
		"sparse index entries (%d) should be proportional to blocks (%d)",
		seg.sparse.Len(), expectedBlocks)
}

// --- Bloom Filter Tests (Gherkin Scenario 3) ---

func TestSegment_BloomFilterEffective(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024)

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	keys := []string{"a", "b", "c"}
	for i, k := range keys {
		ts := hlc.NewHLC(int64(i+1), 0, 0)
		require.NoError(t, mgr.Append(ts, 0, []byte(k), []byte("val")))
	}

	require.NoError(t, mgr.Rotate())
	mgr.Close()

	segments := mgr.Segments()
	require.Len(t, segments, 1)
	seg := segments[0]

	// Key "d" should not be in the filter (true negative).
	assert.False(t, seg.MayContainKey("d"), "bloom filter should report 'd' as not present")

	// Key "a" should possibly be in the filter.
	assert.True(t, seg.MayContainKey("a"), "bloom filter should report 'a' as possibly present")
	assert.True(t, seg.MayContainKey("b"))
	assert.True(t, seg.MayContainKey("c"))
}

// --- Metadata Tests (Gherkin Scenario 4) ---

func TestSegment_MetadataCorrect(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024)

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	minHLC := hlc.NewHLC(1000, 0, 0)
	maxHLC := hlc.NewHLC(9999, 0, 0)

	// Write records spanning HLC 1000 to 9999.
	for i := int64(1000); i <= 9999; i++ {
		ts := hlc.NewHLC(i, 0, 0)
		key := fmt.Sprintf("k-%04d", i)
		require.NoError(t, mgr.Append(ts, 0, []byte(key), []byte("v")))
	}

	require.NoError(t, mgr.Rotate())
	mgr.Close()

	segments := mgr.Segments()
	require.Len(t, segments, 1)
	seg := segments[0]

	assert.Equal(t, uint64(minHLC), seg.Meta.MinTS, "min_ts mismatch")
	assert.Equal(t, uint64(maxHLC), seg.Meta.MaxTS, "max_ts mismatch")
	assert.Equal(t, uint64(9000), seg.Meta.RecordCount, "record_count mismatch")
	assert.True(t, seg.Meta.SizeBytes > 0, "size_bytes should be positive")

	// Verify the metadata was persisted to disk.
	reloaded, err := OpenSegment(dir, seg.ID)
	require.NoError(t, err)
	assert.Equal(t, seg.Meta, reloaded.Meta)
}

// --- GetAt Multi-Segment Tests (Gherkin Scenario 5) ---

func TestSegment_GetAtMultiSegment(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024)

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	// Segment 1: HLC range [100, 199], keys: "k", "x"
	for i := int64(100); i < 200; i++ {
		ts := hlc.NewHLC(i, 0, 0)
		key := "k"
		if i%2 == 0 {
			key = "x"
		}
		require.NoError(t, mgr.Append(ts, 0, []byte(key), []byte(fmt.Sprintf("s1-%d", i))))
	}
	require.NoError(t, mgr.Rotate())

	// Segment 2: HLC range [200, 299], keys: "y" only (no "k")
	for i := int64(200); i < 300; i++ {
		ts := hlc.NewHLC(i, 0, 0)
		require.NoError(t, mgr.Append(ts, 0, []byte("y"), []byte(fmt.Sprintf("s2-%d", i))))
	}
	require.NoError(t, mgr.Rotate())

	// Segment 3: HLC range [300, 399], keys: "k", "z"
	for i := int64(300); i < 400; i++ {
		ts := hlc.NewHLC(i, 0, 0)
		key := "k"
		if i%2 == 0 {
			key = "z"
		}
		require.NoError(t, mgr.Append(ts, 0, []byte(key), []byte(fmt.Sprintf("s3-%d", i))))
	}
	require.NoError(t, mgr.Rotate())
	mgr.Close()

	require.Len(t, mgr.Segments(), 3)

	// Query GetAt("k", t) where t is in Segment 1's range.
	// Segment 3 should be skipped (min_ts > t).
	// Segment 2 should be skipped (bloom: no "k").
	// Only Segment 1 should be searched.
	queryTS := hlc.NewHLC(150, 0, 0)
	rec, err := mgr.GetAt("k", queryTS)
	require.NoError(t, err)
	require.NotNil(t, rec, "expected to find key 'k' at HLC 150")
	assert.Equal(t, "k", string(rec.Key))
	// The value should be from segment 1.
	assert.Contains(t, string(rec.Value), "s1-")
}

func TestSegment_GetAtReturnsLatestVersion(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024)

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	// Write multiple versions of the same key.
	for i := int64(1); i <= 10; i++ {
		ts := hlc.NewHLC(i, 0, 0)
		require.NoError(t, mgr.Append(ts, 0, []byte("mykey"), []byte(fmt.Sprintf("v%d", i))))
	}
	require.NoError(t, mgr.Rotate())
	mgr.Close()

	// Query at HLC 5 should return version 5.
	rec, err := mgr.GetAt("mykey", hlc.NewHLC(5, 0, 0))
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "v5", string(rec.Value))

	// Query at HLC 10 should return version 10.
	rec, err = mgr.GetAt("mykey", hlc.NewHLC(10, 0, 0))
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "v10", string(rec.Value))

	// Query at HLC 0 should return nothing.
	rec, err = mgr.GetAt("mykey", hlc.NewHLC(0, 0, 0))
	require.NoError(t, err)
	assert.Nil(t, rec)
}

// --- File Naming Convention Tests (Gherkin Scenario 6) ---

func TestSegment_FileNamingConvention(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(1024) // small limit to force rotation

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	value := make([]byte, 200)

	// Write enough records to create 3 finalized segments.
	hlcCounter := int64(1)
	for seg := 0; seg < 3; seg++ {
		for i := 0; i < 20; i++ {
			ts := hlc.NewHLC(hlcCounter, 0, 0)
			hlcCounter++
			key := fmt.Sprintf("k%d", i)
			require.NoError(t, mgr.Append(ts, 0, []byte(key), value))
		}
		require.NoError(t, mgr.Rotate())
	}
	mgr.Close()

	segments := mgr.Segments()
	require.True(t, len(segments) >= 3, "expected at least 3 finalized segments, got %d", len(segments))

	// Verify naming convention for first 3 segments.
	for i := 1; i <= 3; i++ {
		prefix := fmt.Sprintf("seg-%06d", i)
		assert.FileExists(t, filepath.Join(dir, prefix+".log"))
		assert.FileExists(t, filepath.Join(dir, prefix+".idx"))
		assert.FileExists(t, filepath.Join(dir, prefix+".bloom"))
		assert.FileExists(t, filepath.Join(dir, prefix+".meta"))
	}

	// Active segment files should exist.
	assert.FileExists(t, filepath.Join(dir, "seg-current.log"))
	assert.FileExists(t, filepath.Join(dir, "seg-current.idx"))
}

// --- Segment Reload Tests ---

func TestSegment_ReloadFromDisk(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024)

	// Create and finalize a segment.
	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		ts := hlc.NewHLC(int64(i+1), 0, 0)
		key := fmt.Sprintf("key-%03d", i)
		require.NoError(t, mgr.Append(ts, 0, []byte(key), []byte("data")))
	}
	require.NoError(t, mgr.Rotate())
	mgr.Close()

	// Create a new manager that discovers the existing segment.
	mgr2, err := NewManager(dir, maxBytes)
	require.NoError(t, err)
	defer mgr2.Close()

	segments := mgr2.Segments()
	require.Len(t, segments, 1)

	// Verify data is accessible.
	rec, err := mgr2.GetAt("key-050", hlc.NewHLC(51, 0, 0))
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "key-050", string(rec.Key))
}

// --- Tombstone Tests ---

func TestSegment_GetAtWithTombstone(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024)

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)

	// Write a value, then a tombstone.
	ts1 := hlc.NewHLC(1, 0, 0)
	require.NoError(t, mgr.Append(ts1, 0, []byte("delme"), []byte("alive")))

	ts2 := hlc.NewHLC(2, 0, 0)
	require.NoError(t, mgr.Append(ts2, wal.FlagTombstone, []byte("delme"), nil))

	require.NoError(t, mgr.Rotate())
	mgr.Close()

	// At HLC 1, the key was alive.
	rec, err := mgr.GetAt("delme", hlc.NewHLC(1, 0, 0))
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "alive", string(rec.Value))
	assert.False(t, rec.Flags.IsTombstone())

	// At HLC 2, the key was deleted (tombstone).
	rec, err = mgr.GetAt("delme", hlc.NewHLC(2, 0, 0))
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.True(t, rec.Flags.IsTombstone())
}

// --- Concurrent Access Tests ---

func TestSegment_ConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	maxBytes := int64(50 * 1024 * 1024)

	mgr, err := NewManager(dir, maxBytes)
	require.NoError(t, err)
	defer mgr.Close()

	done := make(chan error, 10)
	for g := 0; g < 10; g++ {
		go func(id int) {
			for i := 0; i < 100; i++ {
				ts := hlc.NewHLC(int64(id*1000+i+1), 0, 0)
				key := fmt.Sprintf("g%d-k%d", id, i)
				if err := mgr.Append(ts, 0, []byte(key), []byte("v")); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}(g)
	}

	for i := 0; i < 10; i++ {
		require.NoError(t, <-done)
	}
}

// --- Property-Based Tests ---

func TestSparseIndex_PropertyRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 100).Draw(t, "n")
		si := NewSparseIndex()

		var lastOffset int64
		for i := 0; i < n; i++ {
			key := rapid.StringMatching(`[a-z]{1,20}`).Draw(t, "key")
			offset := lastOffset + rapid.Int64Range(1, 10000).Draw(t, "gap")
			si.Add(key, offset)
			lastOffset = offset
		}

		var buf bytes.Buffer
		_, err := si.WriteTo(&buf)
		require.NoError(t, err)

		si2 := NewSparseIndex()
		_, err = si2.ReadFrom(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)

		require.Len(t, si2.Entries, len(si.Entries))
		for i := range si.Entries {
			assert.Equal(t, si.Entries[i].Key, si2.Entries[i].Key)
			assert.Equal(t, si.Entries[i].Offset, si2.Entries[i].Offset)
		}
	})
}

func TestBloomFilter_PropertyNoFalseNegatives(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 500).Draw(t, "n")
		bf := NewBloomFilter(uint(n), 0.01)

		keys := make([]string, n)
		for i := 0; i < n; i++ {
			keys[i] = rapid.StringMatching(`[a-z0-9]{1,30}`).Draw(t, "key")
			bf.Add(keys[i])
		}

		// No false negatives: every added key must test positive.
		for _, k := range keys {
			assert.True(t, bf.Test(k), "false negative for key %q", k)
		}
	})
}

// --- Edge Cases ---

func TestSegment_RotateEmptySegment(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 1024)
	require.NoError(t, err)
	defer mgr.Close()

	// Rotating an empty segment should be a no-op.
	require.NoError(t, mgr.Rotate())
	assert.Len(t, mgr.Segments(), 0)
}

func TestSegment_GetAtNonexistentKey(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 50*1024*1024)
	require.NoError(t, err)

	ts := hlc.NewHLC(1, 0, 0)
	require.NoError(t, mgr.Append(ts, 0, []byte("exists"), []byte("v")))
	require.NoError(t, mgr.Rotate())
	mgr.Close()

	rec, err := mgr.GetAt("nonexistent", hlc.NewHLC(1, 0, 0))
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestSegment_GetAtNoSegments(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewManager(dir, 50*1024*1024)
	require.NoError(t, err)
	defer mgr.Close()

	rec, err := mgr.GetAt("anything", hlc.NewHLC(1, 0, 0))
	require.NoError(t, err)
	assert.Nil(t, rec)
}

// Ensure doc.go placeholder doesn't conflict.
func TestSegment_PackageExists(t *testing.T) {
	// Compilation of this file proves the package exists.
	_ = os.TempDir()
}
