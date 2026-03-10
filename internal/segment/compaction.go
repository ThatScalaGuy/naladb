// Package segment's compaction support: level-based segment merging with
// tombstone pair removal.
package segment

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/thatscalaguy/naladb/internal/wal"
)

// CompactionConfig controls compaction behavior.
type CompactionConfig struct {
	// MaxL0Segments is the number of L0 segments that trigger compaction.
	// When the L0 count exceeds this threshold, all L0 segments are merged
	// into a single L1 segment. Default: 4.
	MaxL0Segments int
}

// DefaultCompactionConfig returns the default compaction configuration.
func DefaultCompactionConfig() CompactionConfig {
	return CompactionConfig{MaxL0Segments: 4}
}

// Compactor performs level-based segment compaction in the background.
// It merges multiple L0 segments into a single L1 segment using merge-sort,
// removing tombstone+write pairs in the process.
type Compactor struct {
	mgr    *Manager
	config CompactionConfig

	mu      sync.Mutex
	running bool
}

// NewCompactor creates a new Compactor for the given segment manager.
func NewCompactor(mgr *Manager, config CompactionConfig) *Compactor {
	if config.MaxL0Segments <= 0 {
		config.MaxL0Segments = 4
	}
	return &Compactor{
		mgr:    mgr,
		config: config,
	}
}

// CheckAndCompact triggers compaction if L0 segment count exceeds the threshold.
// Returns immediately if a compaction is already running.
func (c *Compactor) CheckAndCompact() error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	segments := c.mgr.Segments()
	var l0 []*Segment
	for _, seg := range segments {
		if seg.Meta.Level == 0 {
			l0 = append(l0, seg)
		}
	}

	if len(l0) <= c.config.MaxL0Segments {
		return nil
	}

	return c.compact(l0, 1)
}

// compact merges the given segments into a single segment at the target level.
func (c *Compactor) compact(segments []*Segment, targetLevel int) error {
	// Read all records from input segments.
	var allRecords []*wal.Record
	for _, seg := range segments {
		records, err := seg.ReadAll()
		if err != nil {
			return fmt.Errorf("compaction: read segment %d: %w", seg.ID, err)
		}
		allRecords = append(allRecords, records...)
	}

	// Sort by (key, HLC ascending).
	sort.Slice(allRecords, func(i, j int) bool {
		ki, kj := string(allRecords[i].Key), string(allRecords[j].Key)
		if ki != kj {
			return ki < kj
		}
		return allRecords[i].HLC.Before(allRecords[j].HLC)
	})

	// Remove tombstone pairs.
	compacted := removeTombstonePairs(allRecords)

	removeIDs := make([]uint64, len(segments))
	for i, seg := range segments {
		removeIDs[i] = seg.ID
	}

	if len(compacted) == 0 {
		c.mgr.SwapSegments(removeIDs, nil)
		for _, seg := range segments {
			removeSegmentFiles(seg)
		}
		return nil
	}

	// Build new segment at the target level.
	newSeg, err := c.buildSegment(compacted, targetLevel)
	if err != nil {
		return err
	}

	// Atomically swap old segments for the new one.
	c.mgr.SwapSegments(removeIDs, newSeg)

	// Clean up old segment files.
	for _, seg := range segments {
		removeSegmentFiles(seg)
	}

	return nil
}

// removeTombstonePairs removes tombstone+write pairs from sorted records.
// Records must be sorted by (key, HLC ascending).
//
// For each key:
//   - If the latest version is a tombstone, all versions are dropped (key is dead).
//   - If a tombstone exists in the middle, versions at or before it are dropped,
//     but versions after it are kept.
func removeTombstonePairs(records []*wal.Record) []*wal.Record {
	if len(records) == 0 {
		return nil
	}

	var result []*wal.Record
	i := 0
	for i < len(records) {
		key := string(records[i].Key)

		// Find end of this key's versions.
		j := i
		for j < len(records) && string(records[j].Key) == key {
			j++
		}
		versions := records[i:j]

		// If the latest version is a tombstone, drop everything for this key.
		if versions[len(versions)-1].Flags.IsTombstone() {
			i = j
			continue
		}

		// Find the last tombstone in the versions.
		lastTombstone := -1
		for k := len(versions) - 1; k >= 0; k-- {
			if versions[k].Flags.IsTombstone() {
				lastTombstone = k
				break
			}
		}

		if lastTombstone >= 0 {
			// Keep only versions after the last tombstone.
			result = append(result, versions[lastTombstone+1:]...)
		} else {
			// No tombstones, keep all versions.
			result = append(result, versions...)
		}

		i = j
	}

	return result
}

// buildSegment writes a new segment from the given sorted records.
func (c *Compactor) buildSegment(records []*wal.Record, level int) (*Segment, error) {
	id := c.mgr.AllocateID()
	return writeSegment(records, id, c.mgr.Dir(), level, 0, 0)
}

// removeSegmentFiles deletes all files for a segment (best-effort).
func removeSegmentFiles(seg *Segment) {
	os.Remove(seg.LogPath())
	os.Remove(seg.IdxPath())
	os.Remove(seg.BloomPath())
	os.Remove(seg.MetaPath())
}
