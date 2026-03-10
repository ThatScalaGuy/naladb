package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
)

// Reader reads WAL records sequentially from a file.
type Reader struct {
	r    *bufio.Reader
	file *os.File
}

// NewReader creates a new WAL Reader for the given file.
func NewReader(file *os.File) *Reader {
	return &Reader{
		r:    bufio.NewReaderSize(file, 64*1024),
		file: file,
	}
}

// Next reads the next record from the WAL.
//
// Returns:
//   - (*Record, nil) on success
//   - (nil, io.EOF) at clean end of file
//   - (nil, ErrTruncatedRecord) for incomplete record at end of file
//   - (nil, ErrCorruptRecord) on CRC32 mismatch
//
// If the compressed flag is set, the value is automatically decompressed
// and the flag is cleared on the returned record.
func (rd *Reader) Next() (*Record, error) {
	rec, err := Decode(rd.r)
	if err != nil {
		return nil, err
	}

	if rec.Flags.IsCompressed() {
		decoded, decErr := snappy.Decode(nil, rec.Value)
		if decErr != nil {
			return nil, fmt.Errorf("wal: decompress value: %w", decErr)
		}
		rec.Value = decoded
		rec.Flags &^= FlagCompressed
	}

	return rec, nil
}

// ReadAll reads all valid records from the WAL.
// A truncated record at the end is not considered an error;
// all valid records up to that point are returned with err == nil.
func (rd *Reader) ReadAll() ([]*Record, error) {
	var records []*Record
	for {
		rec, err := rd.Next()
		if err != nil {
			if err == io.EOF || err == ErrTruncatedRecord {
				return records, nil
			}
			return records, err
		}
		records = append(records, rec)
	}
}

// Close closes the underlying file.
func (rd *Reader) Close() error {
	return rd.file.Close()
}
