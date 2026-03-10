package segment

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/wal"
)

// activeSegment tracks the current writable segment.
type activeSegment struct {
	file  *os.File
	buf   *bufio.Writer
	size  int64
	count uint64
	minTS hlc.HLC
	maxTS hlc.HLC
}

// RotateCallback is called after a segment rotation completes successfully.
type RotateCallback func()

// Manager manages segment rotation and multi-segment queries.
// It writes records to an active segment and automatically finalizes it
// when the size limit is reached.
type Manager struct {
	dir      string
	maxBytes int64

	mu       sync.Mutex
	nextID   uint64
	active   *activeSegment
	segments []*Segment // finalized segments, sorted by ID ascending
	onRotate RotateCallback
}

// NewManager creates a segment manager. It scans dir for existing finalized
// segments and opens a new active segment for writing.
func NewManager(dir string, maxBytes int64) (*Manager, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("segment: create dir: %w", err)
	}

	m := &Manager{
		dir:      dir,
		maxBytes: maxBytes,
		nextID:   1,
	}

	if err := m.loadSegments(); err != nil {
		return nil, err
	}

	if err := m.openActive(); err != nil {
		return nil, err
	}

	return m, nil
}

// Append writes a record to the active segment. If the segment size exceeds
// maxBytes after the write, a rotation is triggered automatically.
func (m *Manager) Append(ts hlc.HLC, flags wal.Flags, key, value []byte) error {
	m.mu.Lock()

	rec := &wal.Record{
		HLC:   ts,
		Flags: flags,
		Key:   key,
		Value: value,
	}

	if err := rec.Encode(m.active.buf); err != nil {
		m.mu.Unlock()
		return fmt.Errorf("segment: encode record: %w", err)
	}

	recordSize := int64(wal.HeaderSize + len(key) + len(value))
	m.active.size += recordSize
	m.active.count++

	if m.active.minTS == 0 || ts.Before(m.active.minTS) {
		m.active.minTS = ts
	}
	if m.active.maxTS == 0 || m.active.maxTS.Before(ts) {
		m.active.maxTS = ts
	}

	var cb RotateCallback
	if m.active.size >= m.maxBytes {
		if err := m.rotateLocked(); err != nil {
			m.mu.Unlock()
			return fmt.Errorf("segment: rotate: %w", err)
		}
		cb = m.onRotate
	}
	m.mu.Unlock()

	if cb != nil {
		cb()
	}
	return nil
}

// Rotate forces a segment rotation. The current active segment is finalized
// and a new active segment is created.
func (m *Manager) Rotate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rotateLocked()
}

// GetAt queries all finalized segments for the latest value of key with HLC <= ts.
// Segments are filtered by time range (min_ts) and bloom filter before scanning.
// Searches from newest to oldest and returns on first match.
//
// If a segment file was deleted by concurrent compaction, the search retries
// with a fresh segment list.
func (m *Manager) GetAt(key string, ts hlc.HLC) (*wal.Record, error) {
	const maxRetries = 3
	for attempt := range maxRetries {
		m.mu.Lock()
		segments := make([]*Segment, len(m.segments))
		copy(segments, m.segments)
		m.mu.Unlock()

		rec, err := m.searchSegments(segments, key, ts)
		if err != nil && errors.Is(err, os.ErrNotExist) && attempt < maxRetries-1 {
			// A segment was deleted by concurrent compaction; retry
			// with a fresh snapshot that includes the compacted segment.
			continue
		}
		return rec, err
	}
	return nil, nil // unreachable
}

// searchSegments scans the given segment snapshot newest-to-oldest.
func (m *Manager) searchSegments(segments []*Segment, key string, ts hlc.HLC) (*wal.Record, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]

		// Skip if all records in this segment are after the query time.
		minTS := hlc.HLC(seg.Meta.MinTS)
		if ts.Before(minTS) {
			continue
		}

		// Skip if key is definitely not in this segment.
		if !seg.MayContainKey(key) {
			continue
		}

		rec, err := seg.GetAt(key, ts)
		if err != nil {
			return nil, err
		}

		if rec != nil {
			return rec, nil
		}
	}

	return nil, nil
}

// Segments returns a copy of the finalized segment list.
func (m *Manager) Segments() []*Segment {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]*Segment, len(m.segments))
	copy(result, m.segments)
	return result
}

// OnRotate registers a callback that is invoked after each segment rotation.
func (m *Manager) OnRotate(cb RotateCallback) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onRotate = cb
}

// ActiveSize returns the current active segment's byte size.
func (m *Manager) ActiveSize() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active.size
}

// Close flushes and closes the active segment without finalizing it.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == nil {
		return nil
	}

	if err := m.active.buf.Flush(); err != nil {
		return fmt.Errorf("segment: flush active: %w", err)
	}
	if err := m.active.file.Sync(); err != nil {
		return fmt.Errorf("segment: sync active: %w", err)
	}
	return m.active.file.Close()
}

func (m *Manager) rotateLocked() error {
	if m.active.count == 0 {
		return nil
	}

	// Flush and close the active segment file.
	if err := m.active.buf.Flush(); err != nil {
		return fmt.Errorf("segment: flush: %w", err)
	}
	if err := m.active.file.Sync(); err != nil {
		return fmt.Errorf("segment: sync: %w", err)
	}
	if err := m.active.file.Close(); err != nil {
		return fmt.Errorf("segment: close: %w", err)
	}

	seg, err := m.finalize()
	if err != nil {
		return err
	}

	m.segments = append(m.segments, seg)

	// Remove old active segment files.
	if err := os.Remove(m.currentLogPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("segment: remove old active log: %w", err)
	}
	if err := os.Remove(m.currentIdxPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("segment: remove old active idx: %w", err)
	}

	return m.openActive()
}

func (m *Manager) finalize() (*Segment, error) {
	// Read all records from the active log.
	f, err := os.Open(m.currentLogPath())
	if err != nil {
		return nil, fmt.Errorf("segment: open active log: %w", err)
	}

	reader := wal.NewReader(f)
	records, err := reader.ReadAll()
	f.Close()
	if err != nil {
		return nil, fmt.Errorf("segment: read active records: %w", err)
	}

	// Sort by (key, HLC ascending).
	sort.Slice(records, func(i, j int) bool {
		ki, kj := string(records[i].Key), string(records[j].Key)
		if ki != kj {
			return ki < kj
		}
		return records[i].HLC.Before(records[j].HLC)
	})

	id := m.nextID
	m.nextID++

	return writeSegment(records, id, m.dir, 0, m.active.minTS, m.active.maxTS)
}

func (m *Manager) openActive() error {
	logPath := m.currentLogPath()
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("segment: open active log: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("segment: stat active log: %w", err)
	}

	// Create empty .idx file for the active segment.
	idxPath := m.currentIdxPath()
	idxFile, err := os.Create(idxPath)
	if err != nil {
		f.Close()
		return fmt.Errorf("segment: create active idx: %w", err)
	}
	idxFile.Close()

	m.active = &activeSegment{
		file: f,
		buf:  bufio.NewWriterSize(f, 64*1024),
		size: info.Size(),
	}

	return nil
}

func (m *Manager) loadSegments() error {
	matches, err := filepath.Glob(filepath.Join(m.dir, "seg-??????.meta"))
	if err != nil {
		return fmt.Errorf("segment: scan dir: %w", err)
	}

	sort.Strings(matches)

	for _, metaPath := range matches {
		base := filepath.Base(metaPath)
		var id uint64
		if _, err := fmt.Sscanf(base, "seg-%06d.meta", &id); err != nil {
			continue
		}

		seg, err := OpenSegment(m.dir, id)
		if err != nil {
			return fmt.Errorf("segment: load segment %d: %w", id, err)
		}

		m.segments = append(m.segments, seg)
		if id >= m.nextID {
			m.nextID = id + 1
		}
	}

	return nil
}

// Dir returns the segment storage directory.
func (m *Manager) Dir() string {
	return m.dir
}

// AllocateID reserves and returns the next segment ID.
func (m *Manager) AllocateID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextID
	m.nextID++
	return id
}

// SwapSegments atomically removes the segments with the given IDs and adds
// a new segment. This is used by the compactor to replace L0 segments with
// a merged L1 segment. The add parameter may be nil if all records were
// tombstone pairs.
func (m *Manager) SwapSegments(removeIDs []uint64, add *Segment) {
	m.mu.Lock()
	defer m.mu.Unlock()

	removeSet := make(map[uint64]bool, len(removeIDs))
	for _, id := range removeIDs {
		removeSet[id] = true
	}

	var kept []*Segment
	for _, seg := range m.segments {
		if !removeSet[seg.ID] {
			kept = append(kept, seg)
		}
	}

	if add != nil {
		kept = append(kept, add)
		sort.Slice(kept, func(i, j int) bool {
			return kept[i].ID < kept[j].ID
		})
	}

	m.segments = kept
}

// RemoveSegments removes segments by ID and deletes their files.
// Used by the expiry scanner to delete fully expired segments.
func (m *Manager) RemoveSegments(ids []uint64) {
	m.mu.Lock()

	removeSet := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		removeSet[id] = true
	}

	var kept []*Segment
	var removed []*Segment
	for _, seg := range m.segments {
		if removeSet[seg.ID] {
			removed = append(removed, seg)
		} else {
			kept = append(kept, seg)
		}
	}
	m.segments = kept
	m.mu.Unlock()

	for _, seg := range removed {
		removeSegmentFiles(seg)
	}
}

func (m *Manager) currentLogPath() string {
	return filepath.Join(m.dir, "seg-current.log")
}

func (m *Manager) currentIdxPath() string {
	return filepath.Join(m.dir, "seg-current.idx")
}
