// Package segment implements immutable segment storage with sparse index,
// bloom filters, and automatic rotation for the NalaDB storage engine.
//
// Each finalized segment consists of four files:
//   - .log:   Sorted WAL records (sorted by key, then by HLC within each key)
//   - .idx:   Sparse index for O(log n) key lookup via binary search
//   - .bloom: Bloom filter for fast negative membership testing
//   - .meta:  JSON metadata (min_ts, max_ts, record_count, size_bytes)
package segment

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/wal"
)

// BlockSize is the byte interval at which sparse index entries are created
// during segment finalization.
const BlockSize = 4096

// DefaultBloomFP is the default false positive rate for bloom filters.
const DefaultBloomFP = 0.01

// Metadata holds summary information for a finalized segment.
type Metadata struct {
	MinTS       uint64 `json:"min_ts"`
	MaxTS       uint64 `json:"max_ts"`
	RecordCount uint64 `json:"record_count"`
	SizeBytes   int64  `json:"size_bytes"`
	Level       int    `json:"level"`
}

// Segment represents a finalized, immutable segment with sorted records,
// a sparse index for O(log n) key lookup, and a bloom filter for fast
// negative membership testing.
type Segment struct {
	ID   uint64
	Dir  string
	Meta Metadata

	bloom  *BloomFilter
	sparse *SparseIndex
}

// LogPath returns the path to the segment's .log file.
func (s *Segment) LogPath() string {
	return filepath.Join(s.Dir, fmt.Sprintf("seg-%06d.log", s.ID))
}

// IdxPath returns the path to the segment's .idx file.
func (s *Segment) IdxPath() string {
	return filepath.Join(s.Dir, fmt.Sprintf("seg-%06d.idx", s.ID))
}

// BloomPath returns the path to the segment's .bloom file.
func (s *Segment) BloomPath() string {
	return filepath.Join(s.Dir, fmt.Sprintf("seg-%06d.bloom", s.ID))
}

// MetaPath returns the path to the segment's .meta file.
func (s *Segment) MetaPath() string {
	return filepath.Join(s.Dir, fmt.Sprintf("seg-%06d.meta", s.ID))
}

// MayContainKey checks the bloom filter for possible key membership.
func (s *Segment) MayContainKey(key string) bool {
	return s.bloom.Test(key)
}

// InTimeRange checks if the given HLC timestamp falls within this segment's
// [MinTS, MaxTS] range.
func (s *Segment) InTimeRange(ts hlc.HLC) bool {
	return uint64(ts) >= s.Meta.MinTS && uint64(ts) <= s.Meta.MaxTS
}

// GetAt retrieves the latest value for key with HLC <= ts from this segment.
// Returns nil if the key is not found or has no version <= ts.
//
// The method uses the sparse index to find the starting block via binary search,
// then scans forward through the sorted records.
func (s *Segment) GetAt(key string, ts hlc.HLC) (*wal.Record, error) {
	if !s.MayContainKey(key) {
		return nil, nil
	}

	offset := s.sparse.Lookup(key)

	f, err := os.Open(s.LogPath())
	if err != nil {
		return nil, fmt.Errorf("segment: open log: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("segment: seek: %w", err)
	}

	reader := wal.NewReader(f)
	var best *wal.Record

	for {
		rec, err := reader.Next()
		if err != nil {
			if err == io.EOF || err == wal.ErrTruncatedRecord {
				break
			}
			return nil, fmt.Errorf("segment: read record: %w", err)
		}

		recKey := string(rec.Key)

		// Records are sorted by key; stop when past the target.
		if recKey > key {
			break
		}
		if recKey < key {
			continue
		}

		// recKey == key. Versions are ascending by HLC.
		if ts.Before(rec.HLC) {
			break // all subsequent versions for this key will also exceed ts
		}

		// rec.HLC <= ts; this is a candidate.
		best = &wal.Record{
			HLC:   rec.HLC,
			Flags: rec.Flags,
			Key:   append([]byte(nil), rec.Key...),
			Value: append([]byte(nil), rec.Value...),
		}
	}

	return best, nil
}

// ReadAll reads all records from the segment's .log file.
func (s *Segment) ReadAll() ([]*wal.Record, error) {
	f, err := os.Open(s.LogPath())
	if err != nil {
		return nil, fmt.Errorf("segment: open log: %w", err)
	}
	defer f.Close()

	reader := wal.NewReader(f)
	return reader.ReadAll()
}

// OpenSegment loads a finalized segment from disk by reading its metadata,
// bloom filter, and sparse index.
func OpenSegment(dir string, id uint64) (*Segment, error) {
	seg := &Segment{ID: id, Dir: dir}

	metaData, err := os.ReadFile(seg.MetaPath())
	if err != nil {
		return nil, fmt.Errorf("segment: read meta: %w", err)
	}
	if err := json.Unmarshal(metaData, &seg.Meta); err != nil {
		return nil, fmt.Errorf("segment: parse meta: %w", err)
	}

	seg.bloom, err = LoadBloomFilter(seg.BloomPath())
	if err != nil {
		return nil, err
	}

	seg.sparse, err = LoadSparseIndex(seg.IdxPath())
	if err != nil {
		return nil, err
	}

	return seg, nil
}

// writeSegment writes sorted records into a new segment, building bloom filter,
// sparse index, and metadata files. Records must be pre-sorted by (key, HLC ascending).
// If overrideMinTS/overrideMaxTS are non-zero they replace the computed timestamps.
func writeSegment(records []*wal.Record, id uint64, dir string, level int, overrideMinTS, overrideMaxTS hlc.HLC) (*Segment, error) {
	seg := &Segment{ID: id, Dir: dir}

	// Collect unique keys for bloom filter sizing.
	uniqueKeys := make(map[string]struct{})
	for _, rec := range records {
		uniqueKeys[string(rec.Key)] = struct{}{}
	}

	n := uint(len(uniqueKeys))
	if n == 0 {
		n = 1
	}
	bf := NewBloomFilter(n, DefaultBloomFP)
	for key := range uniqueKeys {
		bf.Add(key)
	}

	// Write sorted records to .log and build sparse index.
	sparse := NewSparseIndex()

	logFile, err := os.Create(seg.LogPath())
	if err != nil {
		return nil, fmt.Errorf("segment: create log: %w", err)
	}

	bufWriter := bufio.NewWriter(logFile)
	var offset int64
	var lastIndexedOffset int64 = -BlockSize // force first entry
	var minTS, maxTS hlc.HLC

	for i, rec := range records {
		if i == 0 {
			minTS = rec.HLC
			maxTS = rec.HLC
		} else {
			if rec.HLC.Before(minTS) {
				minTS = rec.HLC
			}
			if maxTS.Before(rec.HLC) {
				maxTS = rec.HLC
			}
		}

		if offset-lastIndexedOffset >= BlockSize || offset == 0 {
			sparse.Add(string(rec.Key), offset)
			lastIndexedOffset = offset
		}

		if err := rec.Encode(bufWriter); err != nil {
			logFile.Close()
			return nil, fmt.Errorf("segment: encode record: %w", err)
		}

		offset += int64(wal.HeaderSize + len(rec.Key) + len(rec.Value))
	}

	if err := bufWriter.Flush(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("segment: flush log: %w", err)
	}
	if err := logFile.Sync(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("segment: sync log: %w", err)
	}
	logFile.Close()

	if overrideMinTS != 0 {
		minTS = overrideMinTS
	}
	if overrideMaxTS != 0 {
		maxTS = overrideMaxTS
	}

	meta := Metadata{
		MinTS:       uint64(minTS),
		MaxTS:       uint64(maxTS),
		RecordCount: uint64(len(records)),
		SizeBytes:   offset,
		Level:       level,
	}

	if err := sparse.WriteToFile(seg.IdxPath()); err != nil {
		return nil, err
	}
	if err := bf.WriteToFile(seg.BloomPath()); err != nil {
		return nil, err
	}
	if err := writeMetadata(seg.MetaPath(), meta); err != nil {
		return nil, err
	}

	seg.Meta = meta
	seg.bloom = bf
	seg.sparse = sparse

	return seg, nil
}

// writeMetadata persists segment metadata as JSON.
func writeMetadata(path string, meta Metadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("segment: marshal meta: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("segment: write meta: %w", err)
	}
	return nil
}
