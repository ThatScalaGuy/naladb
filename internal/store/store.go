// Package store implements the temporal key-value store with Set, Get, GetAt,
// Delete, History, and BatchSet operations.
//
// The store combines the WAL for durability, a sharded in-memory index for O(1)
// current-value lookups, and an in-memory version log per key for point-in-time
// queries and history retrieval.
package store

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/thatscalaguy/naladb/internal/blob"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/index"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/segment"
	"github.com/thatscalaguy/naladb/internal/wal"
)

// ErrKeyNotFound indicates a key lookup returned no result.
var ErrKeyNotFound = errors.New("naladb: key not found")

// Result holds the value and metadata returned by a read operation.
type Result struct {
	Value     []byte
	HLC       hlc.HLC
	Found     bool
	Tombstone bool
}

// HistoryEntry represents a single version in a key's history.
type HistoryEntry struct {
	HLC       hlc.HLC
	Value     []byte
	Tombstone bool
}

// HistoryOptions controls History query behavior.
type HistoryOptions struct {
	From    hlc.HLC
	To      hlc.HLC
	Limit   int
	Reverse bool
}

// version is a single timestamped value in the version log.
type version struct {
	ts        hlc.HLC
	value     []byte
	tombstone bool
	blobRef   bool
}

// Store is the temporal key-value store. It is safe for concurrent use.
type Store struct {
	clock *hlc.Clock
	idx   *index.Index
	wal   *wal.Writer

	// vlog maps each key to its sorted list of versions (ascending by HLC).
	vmu        sync.RWMutex
	vlog       map[string][]version
	sortedKeys []string // sorted key list for O(log n) prefix scans

	meta     *meta.Registry   // per-key inline statistics (optional)
	blob     *blob.Store      // content-addressed blob store (optional)
	segments *segment.Manager // segment storage for on-disk persistence (optional)
}

// New creates a new Store with the given HLC clock and WAL writer.
func New(clock *hlc.Clock, w *wal.Writer) *Store {
	return &Store{
		clock: clock,
		idx:   index.New(),
		wal:   w,
		vlog:  make(map[string][]version),
	}
}

// NewWithoutWAL creates a store without a WAL writer. Only SetWithHLC is
// functional (no durability). Useful for benchmarks and read-only scenarios.
func NewWithoutWAL(clock *hlc.Clock) *Store {
	return &Store{
		clock: clock,
		idx:   index.New(),
		vlog:  make(map[string][]version),
	}
}

// SetMeta attaches a KeyMeta registry to the store. When set, per-key
// statistics are updated on every write.
func (s *Store) SetMeta(r *meta.Registry) {
	s.meta = r
}

// SetBlobStore attaches a blob store to the store. When set, values exceeding
// the WAL inline limit (64 KiB) are stored as content-addressed blobs.
func (s *Store) SetBlobStore(b *blob.Store) {
	s.blob = b
}

// SetSegments attaches a segment manager to the store. When set, every write
// is also appended to the segment manager for on-disk persistence. The segment
// manager auto-rotates segments when the size limit is reached.
func (s *Store) SetSegments(m *segment.Manager) {
	s.segments = m
}

// Set writes a key-value pair. The value is persisted to the WAL and the
// in-memory index is updated. Values exceeding 64 KiB are transparently
// stored in the blob store. Returns the HLC timestamp assigned to the write.
func (s *Store) Set(key string, value []byte) (hlc.HLC, error) {
	ts := s.clock.Now()
	if err := s.setInternal(key, value, ts); err != nil {
		return 0, err
	}
	return ts, nil
}

// Get retrieves the current value for a key. If the key does not exist or
// has been deleted, Result.Found is false. Blob references are transparently
// resolved to their original content.
func (s *Store) Get(key string) Result {
	entry, ok := s.idx.Get(key)
	if !ok {
		return Result{Found: false}
	}
	value := s.resolveBlob(entry.Value, entry.BlobRef)
	return Result{
		Value: value,
		HLC:   entry.HLC,
		Found: true,
	}
}

// Delete marks a key as deleted by writing a tombstone to the WAL and index.
// If the current value is a blob reference, its reference count is decremented.
// Returns the HLC timestamp of the tombstone.
func (s *Store) Delete(key string) (hlc.HLC, error) {
	ts := s.clock.Now()

	// Deref blob if the current value is a blob reference.
	if s.blob != nil {
		if entry, ok := s.idx.GetEntry(key); ok && entry.BlobRef {
			if ref, err := blob.UnmarshalRef(entry.Value); err == nil {
				_ = s.blob.Deref(ref)
			}
		}
	}

	if err := s.walAppend(ts, wal.FlagTombstone, []byte(key), nil); err != nil {
		return 0, err
	}

	s.idx.Delete(key, ts)
	s.appendVersion(key, ts, nil, true, false)

	if s.meta != nil {
		s.meta.Update(key, ts.WallMicros(), nil)
	}

	return ts, nil
}

// GetAt retrieves the value of a key at a specific point in time.
// It returns the latest version with an HLC <= the given timestamp.
func (s *Store) GetAt(key string, at hlc.HLC) Result {
	s.vmu.RLock()
	versions, ok := s.vlog[key]
	if !ok {
		s.vmu.RUnlock()
		return Result{Found: false}
	}
	// Binary search for the latest version <= at.
	i := sort.Search(len(versions), func(i int) bool {
		return !versions[i].ts.Before(at) && versions[i].ts != at
	})
	// i is the first index where ts > at, so i-1 is the last version <= at.
	if i == 0 {
		s.vmu.RUnlock()
		return Result{Found: false}
	}
	v := versions[i-1]
	s.vmu.RUnlock()

	if v.tombstone {
		return Result{Found: false, Tombstone: true}
	}
	return Result{
		Value: s.resolveBlob(v.value, v.blobRef),
		HLC:   v.ts,
		Found: true,
	}
}

// History returns the version history for a key within the specified options.
func (s *Store) History(key string, opts HistoryOptions) []HistoryEntry {
	s.vmu.RLock()
	versions, ok := s.vlog[key]
	if !ok {
		s.vmu.RUnlock()
		return nil
	}

	// Find the range [from, to] using binary search.
	start := 0
	if opts.From != 0 {
		start = sort.Search(len(versions), func(i int) bool {
			return !versions[i].ts.Before(opts.From)
		})
	}

	end := len(versions)
	if opts.To != 0 {
		end = sort.Search(len(versions), func(i int) bool {
			return !versions[i].ts.Before(opts.To) && versions[i].ts != opts.To
		})
	}

	if start >= end {
		s.vmu.RUnlock()
		return nil
	}

	// Copy the slice within the range.
	filtered := make([]version, end-start)
	copy(filtered, versions[start:end])
	s.vmu.RUnlock()

	// Apply reverse.
	if opts.Reverse {
		for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
			filtered[i], filtered[j] = filtered[j], filtered[i]
		}
	}

	// Apply limit.
	if opts.Limit > 0 && len(filtered) > opts.Limit {
		filtered = filtered[:opts.Limit]
	}

	result := make([]HistoryEntry, len(filtered))
	for i, v := range filtered {
		result[i] = HistoryEntry{
			HLC:       v.ts,
			Value:     s.resolveBlob(v.value, v.blobRef),
			Tombstone: v.tombstone,
		}
	}
	return result
}

// BatchSet writes multiple key-value pairs atomically. All entries receive
// consecutive HLC timestamps from the same clock, ensuring ascending order.
func (s *Store) BatchSet(entries map[string][]byte) ([]hlc.HLC, error) {
	// Sort keys for deterministic ordering.
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	timestamps := make([]hlc.HLC, 0, len(keys))

	for _, key := range keys {
		ts := s.clock.Now()
		if err := s.setInternal(key, entries[key], ts); err != nil {
			return nil, err
		}
		timestamps = append(timestamps, ts)
	}

	return timestamps, nil
}

// setInternal performs the core write logic shared by Set and BatchSet:
// blob externalization, WAL append, index update, and meta tracking.
func (s *Store) setInternal(key string, value []byte, ts hlc.HLC) error {
	storeValue := value
	flags := wal.Flags(0)
	isBlobRef := false

	if s.blob != nil && len(value) > wal.InlineValueLimit {
		ref, err := s.blob.Put(value)
		if err != nil {
			return fmt.Errorf("store: blob put: %w", err)
		}
		storeValue = ref.MarshalBinary()
		flags |= wal.FlagBlobRef
		isBlobRef = true
	}

	if err := s.walAppend(ts, flags, []byte(key), storeValue); err != nil {
		return err
	}

	s.idx.Put(key, ts, storeValue, false)
	if isBlobRef {
		s.setBlobRefFlag(key)
	}
	s.appendVersion(key, ts, storeValue, false, isBlobRef)

	if s.meta != nil {
		s.meta.Update(key, ts.WallMicros(), value)
	}

	return nil
}

// SetWithHLC writes a key-value pair with a specific HLC timestamp.
// This is used for recovery and replication, bypassing clock generation.
func (s *Store) SetWithHLC(key string, ts hlc.HLC, value []byte, tombstone bool) {
	// Check if the value is a blob reference by its size. During WAL replay,
	// blob-ref records will have FlagBlobRef set, but SetWithHLC receives
	// the raw value. We detect blob refs by checking the flags on the WAL
	// record before calling this method.
	if tombstone {
		s.idx.Delete(key, ts)
	} else {
		s.idx.Put(key, ts, value, false)
	}
	s.appendVersion(key, ts, value, tombstone, false)
}

// SetWithHLCAndFlags writes a key-value pair with a specific HLC timestamp
// and WAL flags. This is used for recovery when the blob-ref flag must be
// preserved.
func (s *Store) SetWithHLCAndFlags(key string, ts hlc.HLC, value []byte, tombstone bool, blobRef bool) {
	if tombstone {
		s.idx.Delete(key, ts)
	} else {
		s.idx.Put(key, ts, value, false)
		if blobRef {
			s.setBlobRefFlag(key)
		}
	}
	s.appendVersion(key, ts, value, tombstone, blobRef)
}

func (s *Store) walAppend(ts hlc.HLC, flags wal.Flags, key, value []byte) error {
	if s.wal != nil {
		if err := s.wal.Append(ts, flags, key, value); err != nil {
			return err
		}
	}
	if s.segments != nil {
		if err := s.segments.Append(ts, flags, key, value); err != nil {
			return fmt.Errorf("store: segment append: %w", err)
		}
	}
	return nil
}

// resolveBlob transparently fetches blob content if the value is a blob ref.
func (s *Store) resolveBlob(value []byte, isBlobRef bool) []byte {
	if !isBlobRef || s.blob == nil {
		return value
	}
	ref, err := blob.UnmarshalRef(value)
	if err != nil {
		return value
	}
	data, err := s.blob.Get(ref)
	if err != nil {
		return value
	}
	return data
}

// setBlobRefFlag sets the BlobRef flag on the current index entry for a key.
func (s *Store) setBlobRefFlag(key string) {
	entry, ok := s.idx.GetEntry(key)
	if !ok {
		return
	}
	entry.BlobRef = true
	s.idx.PutEntry(key, entry)
}

// ScanPrefix returns all keys that start with the given prefix.
// Keys are returned in sorted order. Uses binary search on the sorted key
// index for O(log n + m) performance where m is the number of matching keys.
func (s *Store) ScanPrefix(prefix string) []string {
	s.vmu.RLock()
	defer s.vmu.RUnlock()

	// Binary search for the first key >= prefix.
	i := sort.SearchStrings(s.sortedKeys, prefix)

	var keys []string
	for i < len(s.sortedKeys) && strings.HasPrefix(s.sortedKeys[i], prefix) {
		keys = append(keys, s.sortedKeys[i])
		i++
	}
	return keys
}

// Stats holds summary statistics for the store.
type Stats struct {
	Keys       int // total unique keys (including tombstoned)
	Versions   int // total version entries across all keys
	Tombstones int // keys whose latest version is a tombstone
}

// Stats returns summary statistics for the store.
func (s *Store) Stats() Stats {
	s.vmu.RLock()
	defer s.vmu.RUnlock()

	st := Stats{Keys: len(s.vlog)}
	for _, versions := range s.vlog {
		st.Versions += len(versions)
		if len(versions) > 0 && versions[len(versions)-1].tombstone {
			st.Tombstones++
		}
	}
	return st
}

func (s *Store) appendVersion(key string, ts hlc.HLC, value []byte, tombstone bool, blobRef bool) {
	v := version{ts: ts, value: value, tombstone: tombstone, blobRef: blobRef}

	s.vmu.Lock()
	defer s.vmu.Unlock()

	versions := s.vlog[key]

	// Maintain sorted key index for new keys.
	if len(versions) == 0 {
		i := sort.SearchStrings(s.sortedKeys, key)
		s.sortedKeys = append(s.sortedKeys, "")
		copy(s.sortedKeys[i+1:], s.sortedKeys[i:])
		s.sortedKeys[i] = key
	}

	// Fast path: append if this is the latest version (common case).
	if len(versions) == 0 || !ts.Before(versions[len(versions)-1].ts) {
		s.vlog[key] = append(versions, v)
		return
	}

	// Slow path: insert in sorted position.
	i := sort.Search(len(versions), func(i int) bool {
		return !versions[i].ts.Before(ts)
	})
	versions = append(versions, version{})
	copy(versions[i+1:], versions[i:])
	versions[i] = v
	s.vlog[key] = versions
}
