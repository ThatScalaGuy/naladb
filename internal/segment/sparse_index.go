package segment

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// SparseEntry maps a key to its byte offset in the segment log file.
type SparseEntry struct {
	Key    string
	Offset int64
}

// SparseIndex provides O(log n) key lookup via binary search over sampled entries.
// Entries are sampled at regular byte intervals from the sorted segment log.
type SparseIndex struct {
	Entries []SparseEntry
}

// NewSparseIndex creates an empty sparse index.
func NewSparseIndex() *SparseIndex {
	return &SparseIndex{}
}

// Add appends an entry to the sparse index.
func (si *SparseIndex) Add(key string, offset int64) {
	si.Entries = append(si.Entries, SparseEntry{Key: key, Offset: offset})
}

// Lookup returns the byte offset from which to start scanning for the given key.
// It finds the largest entry with Key <= the target key using binary search.
// Returns 0 if the key precedes all entries (scan from start).
func (si *SparseIndex) Lookup(key string) int64 {
	if len(si.Entries) == 0 {
		return 0
	}
	// Find the first entry with Key > key.
	i := sort.Search(len(si.Entries), func(i int) bool {
		return si.Entries[i].Key > key
	})
	if i == 0 {
		return 0
	}
	return si.Entries[i-1].Offset
}

// Len returns the number of entries in the sparse index.
func (si *SparseIndex) Len() int {
	return len(si.Entries)
}

// WriteTo serializes the sparse index to w.
//
// Binary format (little-endian):
//
//	NumEntries: uint32
//	For each entry:
//	  KeyLen: uint16
//	  Key:    KeyLen bytes
//	  Offset: int64 (8 bytes)
func (si *SparseIndex) WriteTo(w io.Writer) (int64, error) {
	var written int64

	if err := binary.Write(w, binary.LittleEndian, uint32(len(si.Entries))); err != nil {
		return written, fmt.Errorf("segment: write index count: %w", err)
	}
	written += 4

	for _, e := range si.Entries {
		keyBytes := []byte(e.Key)

		if err := binary.Write(w, binary.LittleEndian, uint16(len(keyBytes))); err != nil {
			return written, fmt.Errorf("segment: write key length: %w", err)
		}
		written += 2

		n, err := w.Write(keyBytes)
		if err != nil {
			return written, fmt.Errorf("segment: write key: %w", err)
		}
		written += int64(n)

		if err := binary.Write(w, binary.LittleEndian, e.Offset); err != nil {
			return written, fmt.Errorf("segment: write offset: %w", err)
		}
		written += 8
	}

	return written, nil
}

// ReadFrom deserializes a sparse index from r.
func (si *SparseIndex) ReadFrom(r io.Reader) (int64, error) {
	var read int64

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return read, fmt.Errorf("segment: read index count: %w", err)
	}
	read += 4

	si.Entries = make([]SparseEntry, 0, count)

	for i := uint32(0); i < count; i++ {
		var keyLen uint16
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return read, fmt.Errorf("segment: read key length: %w", err)
		}
		read += 2

		keyBytes := make([]byte, keyLen)
		n, err := io.ReadFull(r, keyBytes)
		if err != nil {
			return read, fmt.Errorf("segment: read key: %w", err)
		}
		read += int64(n)

		var offset int64
		if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
			return read, fmt.Errorf("segment: read offset: %w", err)
		}
		read += 8

		si.Entries = append(si.Entries, SparseEntry{Key: string(keyBytes), Offset: offset})
	}

	return read, nil
}

// WriteToFile writes the sparse index to the given file path.
func (si *SparseIndex) WriteToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("segment: create index file: %w", err)
	}
	defer f.Close()

	if _, err := si.WriteTo(f); err != nil {
		return err
	}
	return f.Sync()
}

// LoadSparseIndex reads a sparse index from the given file path.
func LoadSparseIndex(path string) (*SparseIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment: open index file: %w", err)
	}
	defer f.Close()

	si := NewSparseIndex()
	if _, err := si.ReadFrom(f); err != nil {
		return nil, err
	}
	return si, nil
}
