package wal

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/golang/snappy"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// CompressionMode controls when values are compressed.
type CompressionMode int

const (
	// CompressionNone disables compression.
	CompressionNone CompressionMode = iota
	// CompressionAuto compresses values above CompressionThreshold.
	CompressionAuto
	// CompressionAlways compresses all non-empty values.
	CompressionAlways
)

// WriterOptions configures a WAL Writer.
type WriterOptions struct {
	// SyncInterval controls how often the WAL is fsynced to disk.
	// Zero means sync after every write.
	SyncInterval time.Duration

	// Compression controls value compression behavior.
	Compression CompressionMode
}

// Writer appends records to a WAL file with optional batch-fsync.
type Writer struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
	opts WriterOptions

	closed   bool
	dirty    bool
	quit     chan struct{}
	syncDone chan struct{}
}

// NewWriter creates a new WAL Writer that appends to the given file.
func NewWriter(file *os.File, opts WriterOptions) *Writer {
	w := &Writer{
		file: file,
		buf:  bufio.NewWriterSize(file, 64*1024),
		opts: opts,
	}

	if opts.SyncInterval > 0 {
		w.quit = make(chan struct{})
		w.syncDone = make(chan struct{})
		go w.syncLoop()
	}

	return w
}

// Append writes a record to the WAL. It validates the key size,
// optionally compresses the value, and encodes the record.
func (w *Writer) Append(ts hlc.HLC, flags Flags, key, value []byte) error {
	if len(key) > MaxKeySize {
		return ErrKeyTooLong
	}
	if len(key) == 0 {
		return fmt.Errorf("wal: key must not be empty")
	}

	finalValue := value
	finalFlags := flags

	if w.shouldCompress(value) {
		compressed := snappy.Encode(nil, value)
		if len(compressed) < len(value) {
			finalValue = compressed
			finalFlags |= FlagCompressed
		}
	}

	rec := &Record{
		HLC:   ts,
		Flags: finalFlags,
		Key:   key,
		Value: finalValue,
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: writer is closed")
	}

	if err := rec.Encode(w.buf); err != nil {
		return fmt.Errorf("wal: encode record: %w", err)
	}

	w.dirty = true

	if w.opts.SyncInterval == 0 {
		return w.syncLocked()
	}

	return nil
}

// Sync forces a flush and fsync. Safe for concurrent use.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

// Close flushes pending data, fsyncs, stops the sync goroutine, and closes the file.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true

	// Stop the background syncer if running.
	if w.quit != nil {
		close(w.quit)
		w.mu.Unlock()
		<-w.syncDone // Wait for goroutine to exit (it needs the lock).
		w.mu.Lock()
	}

	err := w.syncLocked()
	closeErr := w.file.Close()
	w.mu.Unlock()

	if err != nil {
		return err
	}
	return closeErr
}

// Truncate resets the WAL file to zero length. This is safe to call after
// all records have been persisted to segments. The writer remains usable.
func (w *Writer) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("wal: writer is closed")
	}

	// Flush any buffered data first, then truncate.
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: flush before truncate: %w", err)
	}
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal: seek after truncate: %w", err)
	}
	w.buf.Reset(w.file)
	w.dirty = false
	return nil
}

func (w *Writer) shouldCompress(value []byte) bool {
	switch w.opts.Compression {
	case CompressionAlways:
		return len(value) > 0
	case CompressionAuto:
		return len(value) >= CompressionThreshold
	default:
		return false
	}
}

func (w *Writer) syncLoop() {
	defer close(w.syncDone)
	ticker := time.NewTicker(w.opts.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.quit:
			return
		case <-ticker.C:
			w.mu.Lock()
			if w.dirty && !w.closed {
				_ = w.syncLocked()
			}
			w.mu.Unlock()
		}
	}
}

func (w *Writer) syncLocked() error {
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.dirty = false
	return nil
}
