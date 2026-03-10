// Package blob implements a content-addressed blob store for values exceeding
// the WAL inline limit (64 KiB). Blobs are identified by their SHA-256 hash,
// providing automatic deduplication. Reference counting and garbage collection
// manage blob lifecycle.
package blob

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RefSize is the size of a serialized blob reference in bytes:
// SHA-256 hash (32) + original size (4) + flags (4) = 40.
const RefSize = 40

var (
	// ErrBlobNotFound indicates the referenced blob does not exist on disk.
	ErrBlobNotFound = errors.New("blob: not found")

	// ErrInvalidRef indicates the blob reference data is malformed.
	ErrInvalidRef = errors.New("blob: invalid reference")
)

// Ref is a blob reference stored in the WAL value field when FlagBlobRef is
// set. It is exactly 40 bytes when serialized.
type Ref struct {
	Hash  [32]byte // SHA-256 of the original content
	Size  uint32   // original uncompressed size in bytes
	Flags uint32   // reserved for future use
}

// MarshalBinary serializes the Ref into a 40-byte slice.
func (r Ref) MarshalBinary() []byte {
	buf := make([]byte, RefSize)
	copy(buf[:32], r.Hash[:])
	binary.LittleEndian.PutUint32(buf[32:36], r.Size)
	binary.LittleEndian.PutUint32(buf[36:40], r.Flags)
	return buf
}

// UnmarshalRef deserializes a 40-byte slice into a Ref.
func UnmarshalRef(data []byte) (Ref, error) {
	if len(data) != RefSize {
		return Ref{}, ErrInvalidRef
	}
	var ref Ref
	copy(ref.Hash[:], data[:32])
	ref.Size = binary.LittleEndian.Uint32(data[32:36])
	ref.Flags = binary.LittleEndian.Uint32(data[36:40])
	return ref, nil
}

// blobEntry tracks the reference count and creation time for a blob.
type blobEntry struct {
	refs    uint32
	created time.Time
}

// Store is a content-addressed blob store backed by the filesystem. Blobs are
// identified by their SHA-256 hash and stored as immutable files. Identical
// content is automatically deduplicated via reference counting.
type Store struct {
	dir string
	mu  sync.Mutex
	// entries tracks reference counts and creation times per blob hash.
	entries map[[32]byte]*blobEntry
}

// NewStore creates a new blob store rooted at dir. The directory is created
// if it does not exist.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("blob: create store dir: %w", err)
	}
	return &Store{
		dir:     dir,
		entries: make(map[[32]byte]*blobEntry),
	}, nil
}

// Dir returns the root directory of the blob store.
func (s *Store) Dir() string {
	return s.dir
}

// Put writes data to the blob store. If a blob with the same SHA-256 hash
// already exists, its reference count is incremented and the existing Ref is
// returned (content-addressed deduplication). Returns the Ref to store in the
// WAL value field.
func (s *Store) Put(data []byte) (Ref, error) {
	hash := sha256.Sum256(data)

	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.entries[hash]; ok {
		entry.refs++
		return Ref{Hash: hash, Size: uint32(len(data))}, nil
	}

	// Write the blob file.
	path := s.blobPath(hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Ref{}, fmt.Errorf("blob: create prefix dir: %w", err)
	}

	// Atomic write: temp file + rename.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return Ref{}, fmt.Errorf("blob: write temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return Ref{}, fmt.Errorf("blob: rename: %w", err)
	}

	s.entries[hash] = &blobEntry{refs: 1, created: time.Now()}

	return Ref{Hash: hash, Size: uint32(len(data))}, nil
}

// Get retrieves blob content by its Ref. Returns ErrBlobNotFound if the blob
// has been garbage collected or never existed.
func (s *Store) Get(ref Ref) ([]byte, error) {
	path := s.blobPath(ref.Hash)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBlobNotFound
		}
		return nil, fmt.Errorf("blob: read: %w", err)
	}
	return data, nil
}

// Deref decrements the reference count for a blob. When the count reaches
// zero, the blob becomes eligible for garbage collection.
func (s *Store) Deref(ref Ref) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[ref.Hash]
	if !ok {
		return nil
	}
	if entry.refs > 0 {
		entry.refs--
	}
	return nil
}

// GC removes blobs whose reference count has reached zero and whose creation
// time exceeds minAge. Returns the number of blobs removed.
func (s *Store) GC(minAge time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for hash, entry := range s.entries {
		if entry.refs > 0 {
			continue
		}
		if time.Since(entry.created) < minAge {
			continue
		}

		path := s.blobPath(hash)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("blob: gc remove: %w", err)
		}
		delete(s.entries, hash)
		removed++
	}
	return removed, nil
}

// RefCount returns the current reference count for a blob, or 0 if unknown.
func (s *Store) RefCount(ref Ref) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.entries[ref.Hash]; ok {
		return entry.refs
	}
	return 0
}

// blobPath returns the filesystem path for a blob with the given hash.
// Files are organized into 256 subdirectories by the first byte of the hash.
func (s *Store) blobPath(hash [32]byte) string {
	hexHash := hex.EncodeToString(hash[:])
	return filepath.Join(s.dir, hexHash[:2], hexHash+".blob")
}
