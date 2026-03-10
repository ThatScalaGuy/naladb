// Package wal implements a Write-Ahead Log with binary record format,
// CRC32 integrity, Snappy compression, and crash recovery support.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// HeaderSize is the fixed portion of a WAL record before the variable-length
// key/value payload: CRC32(4) + HLC(8) + Flags(1) + KeyLen(2) + ValLen(4) = 19 bytes.
const HeaderSize = 19

// MaxKeySize is the maximum allowed key length in bytes.
const MaxKeySize = 512

// InlineValueLimit is the maximum value size stored inline in the WAL.
// Values larger than this should use a blob reference.
const InlineValueLimit = 64 * 1024 // 64 KiB

// CompressionThreshold is the minimum value size (bytes) for auto-compression.
const CompressionThreshold = 256

// BlobRefSize is the fixed size of a blob reference: SHA-256(32) + Size(8) = 40 bytes.
const BlobRefSize = 40

var (
	// ErrCorruptRecord indicates a CRC32 mismatch during record reading.
	ErrCorruptRecord = errors.New("naladb: corrupt WAL record")

	// ErrKeyTooLong indicates a key exceeds MaxKeySize bytes.
	ErrKeyTooLong = errors.New("naladb: key exceeds 512 bytes")

	// ErrTruncatedRecord indicates an incomplete record at the end of a WAL file,
	// typically caused by a crash during writing.
	ErrTruncatedRecord = errors.New("naladb: truncated WAL record")
)

// Flags represents the single-byte flags field in a WAL record.
type Flags uint8

const (
	// FlagTombstone marks the record as a deletion tombstone (bit 0).
	FlagTombstone Flags = 1 << iota
	// FlagCompressed indicates the value is Snappy-compressed (bit 1).
	FlagCompressed
	// FlagBlobRef indicates the value field contains a blob reference (bit 2).
	FlagBlobRef
)

// IsTombstone reports whether the tombstone flag is set.
func (f Flags) IsTombstone() bool { return f&FlagTombstone != 0 }

// IsCompressed reports whether the compression flag is set.
func (f Flags) IsCompressed() bool { return f&FlagCompressed != 0 }

// IsBlobRef reports whether the blob-reference flag is set.
func (f Flags) IsBlobRef() bool { return f&FlagBlobRef != 0 }

// Record represents a single WAL entry.
type Record struct {
	HLC   hlc.HLC
	Flags Flags
	Key   []byte
	Value []byte
}

// Encode serializes the record into binary format and writes it to w.
//
// Wire format (little-endian):
//
//	CRC32(4) | HLC(8) | Flags(1) | KeyLen(2) | ValLen(4) | Key | Value
//
// CRC32 covers bytes from HLC through the end of Value.
func (r *Record) Encode(w io.Writer) error {
	keyLen := len(r.Key)
	valLen := len(r.Value)

	// Build payload: everything after CRC32.
	payloadSize := 8 + 1 + 2 + 4 + keyLen + valLen
	payload := make([]byte, payloadSize)

	binary.LittleEndian.PutUint64(payload[0:8], uint64(r.HLC))
	payload[8] = byte(r.Flags)
	binary.LittleEndian.PutUint16(payload[9:11], uint16(keyLen))
	binary.LittleEndian.PutUint32(payload[11:15], uint32(valLen))
	copy(payload[15:15+keyLen], r.Key)
	copy(payload[15+keyLen:], r.Value)

	// Compute CRC32-IEEE over the payload.
	checksum := crc32.ChecksumIEEE(payload)

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], checksum)

	if _, err := w.Write(crcBuf[:]); err != nil {
		return fmt.Errorf("wal: write CRC: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}

	return nil
}

// Decode reads a single record from r and validates its CRC32 checksum.
//
// Returns:
//   - (*Record, nil) on success
//   - (nil, io.EOF) at clean end of file
//   - (nil, ErrTruncatedRecord) for an incomplete record (crash recovery)
//   - (nil, ErrCorruptRecord) on CRC32 mismatch
func Decode(r io.Reader) (*Record, error) {
	var hdr [HeaderSize]byte
	_, err := io.ReadFull(r, hdr[:])
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return nil, ErrTruncatedRecord
		}
		return nil, fmt.Errorf("wal: read header: %w", err)
	}

	storedCRC := binary.LittleEndian.Uint32(hdr[0:4])
	hlcVal := hlc.HLC(binary.LittleEndian.Uint64(hdr[4:12]))
	flags := Flags(hdr[12])
	keyLen := binary.LittleEndian.Uint16(hdr[13:15])
	valLen := binary.LittleEndian.Uint32(hdr[15:19])

	// Read variable-length key + value.
	kvData := make([]byte, int(keyLen)+int(valLen))
	if len(kvData) > 0 {
		if _, err := io.ReadFull(r, kvData); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return nil, ErrTruncatedRecord
			}
			return nil, fmt.Errorf("wal: read payload: %w", err)
		}
	}

	// Verify CRC32 over header[4:] + kvData.
	h := crc32.NewIEEE()
	h.Write(hdr[4:])
	h.Write(kvData)
	if h.Sum32() != storedCRC {
		return nil, ErrCorruptRecord
	}

	return &Record{
		HLC:   hlcVal,
		Flags: flags,
		Key:   kvData[:keyLen],
		Value: kvData[keyLen:],
	}, nil
}
