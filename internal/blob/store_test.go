package blob

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestBlobStore_PutGetRoundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)

	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	ref, err := s.Put(data)
	require.NoError(t, err)
	assert.Equal(t, uint32(100_000), ref.Size)

	// Verify blob file exists on disk.
	hash := sha256.Sum256(data)
	assert.Equal(t, hash, ref.Hash)
	blobPath := s.blobPath(hash)
	_, statErr := os.Stat(blobPath)
	assert.NoError(t, statErr, "blob file should exist on disk")

	// Get should return the original data.
	got, err := s.Get(ref)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBlobStore_ContentAddressedDedup(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)

	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i % 251)
	}

	ref1, err := s.Put(data)
	require.NoError(t, err)

	ref2, err := s.Put(data)
	require.NoError(t, err)

	// Both refs should have the same hash.
	assert.Equal(t, ref1.Hash, ref2.Hash)

	// Reference count should be 2.
	assert.Equal(t, uint32(2), s.RefCount(ref1))

	// Only one blob file should exist.
	count := countBlobFiles(t, dir)
	assert.Equal(t, 1, count, "identical content should produce exactly one blob file")
}

func TestBlobStore_GarbageCollection(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)

	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i % 137)
	}

	ref, err := s.Put(data)
	require.NoError(t, err)

	// Deref and GC with minAge=0 (immediate).
	require.NoError(t, s.Deref(ref))

	removed, err := s.GC(0)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)

	// Blob should no longer be accessible.
	_, err = s.Get(ref)
	assert.ErrorIs(t, err, ErrBlobNotFound)
}

func TestBlobStore_GC_RespectsMinAge(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)

	data := []byte("test-blob-data-that-is-big-enough")
	ref, err := s.Put(data)
	require.NoError(t, err)

	require.NoError(t, s.Deref(ref))

	// GC with a very large minAge should not remove the blob.
	removed, err := s.GC(24 * 60 * 60 * 1_000_000_000) // 24h in ns
	require.NoError(t, err)
	assert.Equal(t, 0, removed)

	// Blob should still be accessible.
	got, err := s.Get(ref)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBlobStore_GC_DoesNotRemoveReferenced(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)

	data := []byte("still-in-use")
	ref, err := s.Put(data)
	require.NoError(t, err)

	// Do NOT deref. GC should not remove it.
	removed, err := s.GC(0)
	require.NoError(t, err)
	assert.Equal(t, 0, removed)

	got, err := s.Get(ref)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestBlobStore_DerefMultipleRefs(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)

	data := []byte("shared-content")
	ref1, err := s.Put(data)
	require.NoError(t, err)

	_, err = s.Put(data) // ref count = 2
	require.NoError(t, err)

	// Deref once: ref count = 1.
	require.NoError(t, s.Deref(ref1))
	assert.Equal(t, uint32(1), s.RefCount(ref1))

	// GC should not remove (still referenced).
	removed, err := s.GC(0)
	require.NoError(t, err)
	assert.Equal(t, 0, removed)

	// Deref again: ref count = 0.
	require.NoError(t, s.Deref(ref1))
	assert.Equal(t, uint32(0), s.RefCount(ref1))

	// Now GC should remove it.
	removed, err = s.GC(0)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
}

func TestRef_MarshalUnmarshal_Roundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var hash [32]byte
		for i := range hash {
			hash[i] = rapid.Byte().Draw(t, "hash_byte")
		}
		size := rapid.Uint32().Draw(t, "size")
		flags := rapid.Uint32().Draw(t, "flags")

		ref := Ref{Hash: hash, Size: size, Flags: flags}
		data := ref.MarshalBinary()
		assert.Len(t, data, RefSize)

		got, err := UnmarshalRef(data)
		require.NoError(t, err)
		assert.Equal(t, ref, got)
	})
}

func TestUnmarshalRef_InvalidLength(t *testing.T) {
	_, err := UnmarshalRef([]byte{1, 2, 3})
	assert.ErrorIs(t, err, ErrInvalidRef)
}

func TestBlobStore_GetNotFound(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStore(dir)
	require.NoError(t, err)

	ref := Ref{Hash: sha256.Sum256([]byte("nonexistent")), Size: 11}
	_, err = s.Get(ref)
	assert.ErrorIs(t, err, ErrBlobNotFound)
}

// countBlobFiles counts .blob files recursively under dir.
func countBlobFiles(t *testing.T, dir string) int {
	t.Helper()
	count := 0
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".blob" {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}
