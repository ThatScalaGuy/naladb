// Package index implements the sharded in-memory index with 256 virtual shards.
//
// The index provides O(1) current-value lookups by mapping keys to their latest
// value and HLC timestamp. Keys are distributed across 256 shards using FNV-1a
// hashing to minimize lock contention under concurrent access.
package index

import (
	"hash/fnv"
	"sync"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

const numShards = 256

// Entry holds the current value and timestamp for a key.
type Entry struct {
	HLC       hlc.HLC
	Value     []byte
	Tombstone bool
	BlobRef   bool // true if Value contains a blob reference
}

// shard is a single partition of the index protected by its own mutex.
type shard struct {
	mu    sync.RWMutex
	items map[string]Entry
}

// Index is a sharded in-memory map from key to its latest Entry.
// It uses 256 virtual shards with FNV-1a hashing for distribution.
type Index struct {
	shards [numShards]shard
}

// New creates a new empty Index.
func New() *Index {
	idx := &Index{}
	for i := range idx.shards {
		idx.shards[i].items = make(map[string]Entry)
	}
	return idx
}

// Put inserts or updates a key in the index. The update is applied only if
// the provided HLC is greater than or equal to the existing entry's HLC
// (or the key does not exist yet). Returns true if the entry was actually updated.
func (idx *Index) Put(key string, ts hlc.HLC, value []byte, tombstone bool) bool {
	s := idx.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.items[key]; ok && ts.Before(existing.HLC) {
		return false
	}

	s.items[key] = Entry{
		HLC:       ts,
		Value:     value,
		Tombstone: tombstone,
	}
	return true
}

// Get retrieves the current entry for a key. Returns the entry and true if
// found and not tombstoned, or a zero Entry and false otherwise.
func (idx *Index) Get(key string) (Entry, bool) {
	s := idx.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.items[key]
	if !ok || e.Tombstone {
		return Entry{}, false
	}
	return e, true
}

// GetEntry retrieves the raw entry for a key including tombstones.
// Returns the entry and true if the key exists (even if tombstoned).
func (idx *Index) GetEntry(key string) (Entry, bool) {
	s := idx.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.items[key]
	return e, ok
}

// PutEntry inserts or updates a key with a full Entry. This allows setting
// all fields including BlobRef. The update is applied only if the entry's HLC
// is >= the existing entry's HLC.
func (idx *Index) PutEntry(key string, e Entry) bool {
	s := idx.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.items[key]; ok && e.HLC.Before(existing.HLC) {
		return false
	}

	s.items[key] = e
	return true
}

// Delete marks a key as tombstoned in the index.
func (idx *Index) Delete(key string, ts hlc.HLC) bool {
	return idx.Put(key, ts, nil, true)
}

// Len returns the total number of entries (including tombstones) across all shards.
func (idx *Index) Len() int {
	total := 0
	for i := range idx.shards {
		idx.shards[i].mu.RLock()
		total += len(idx.shards[i].items)
		idx.shards[i].mu.RUnlock()
	}
	return total
}

// ShardSizes returns the number of entries in each shard. This is useful for
// verifying even distribution of keys.
func (idx *Index) ShardSizes() [numShards]int {
	var sizes [numShards]int
	for i := range idx.shards {
		idx.shards[i].mu.RLock()
		sizes[i] = len(idx.shards[i].items)
		idx.shards[i].mu.RUnlock()
	}
	return sizes
}

func (idx *Index) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &idx.shards[h.Sum32()%numShards]
}
