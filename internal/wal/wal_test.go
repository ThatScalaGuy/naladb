package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// --- helpers ---

func createTempWAL(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func openForWrite(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	require.NoError(t, err)
	return f
}

func openForRead(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	return f
}

// --- Gherkin Scenario: Single record write and read ---

func TestRecord_WriteAndRead_Single(t *testing.T) {
	path := createTempWAL(t)

	ts := hlc.NewHLC(1_000_000, 1, 0)
	key := []byte("sensor:temp_1:prop:value")
	var valBuf [8]byte
	binary.LittleEndian.PutUint64(valBuf[:], 72<<32) // float64 approximation
	value := valBuf[:]

	// Write
	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	require.NoError(t, w.Append(ts, 0, key, value))
	require.NoError(t, w.Close())

	// Read
	rf := openForRead(t, path)
	r := NewReader(rf)
	defer r.Close()

	records, err := r.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ts, records[0].HLC)
	assert.Equal(t, key, records[0].Key)
	assert.Equal(t, value, records[0].Value)
}

// --- Gherkin Scenario: Binary layout correctness ---

func TestRecord_BinaryLayout(t *testing.T) {
	ts := hlc.NewHLC(42, 3, 7)
	key := []byte("testkey")
	value := []byte("testvalue")

	rec := &Record{HLC: ts, Flags: FlagTombstone, Key: key, Value: value}

	var buf bytes.Buffer
	require.NoError(t, rec.Encode(&buf))

	data := buf.Bytes()

	// Total size = 19 (header) + 7 (key) + 9 (value) = 35
	require.Len(t, data, HeaderSize+len(key)+len(value))

	// CRC32 at offset 0 (4 bytes)
	_ = binary.LittleEndian.Uint32(data[0:4])

	// HLC at offset 4 (8 bytes)
	gotHLC := hlc.HLC(binary.LittleEndian.Uint64(data[4:12]))
	assert.Equal(t, ts, gotHLC)

	// Flags at offset 12 (1 byte)
	assert.Equal(t, byte(FlagTombstone), data[12])

	// KeyLen at offset 13 (2 bytes)
	gotKeyLen := binary.LittleEndian.Uint16(data[13:15])
	assert.Equal(t, uint16(len(key)), gotKeyLen)

	// ValLen at offset 15 (4 bytes)
	gotValLen := binary.LittleEndian.Uint32(data[15:19])
	assert.Equal(t, uint32(len(value)), gotValLen)

	// Key at offset 19
	assert.Equal(t, key, data[19:19+len(key)])

	// Value at offset 19+keyLen
	assert.Equal(t, value, data[19+len(key):])
}

// --- Gherkin Scenario: CRC32 detects corruption ---

func TestRecord_CRC32_DetectsCorruption(t *testing.T) {
	path := createTempWAL(t)

	ts := hlc.NewHLC(1000, 0, 0)
	key := []byte("key")
	value := []byte("hello world value")

	// Write a valid record.
	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	require.NoError(t, w.Append(ts, 0, key, value))
	require.NoError(t, w.Close())

	// Corrupt a byte in the value region.
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Value starts at HeaderSize + len(key)
	valOffset := HeaderSize + len(key)
	require.Less(t, valOffset, len(data))
	data[valOffset] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0644))

	// Reader should detect corruption.
	rf := openForRead(t, path)
	reader := NewReader(rf)
	defer reader.Close()

	_, err = reader.Next()
	assert.ErrorIs(t, err, ErrCorruptRecord)
}

// --- Gherkin Scenario: Flags roundtrip ---

func TestFlags_Roundtrip(t *testing.T) {
	path := createTempWAL(t)

	ts := hlc.NewHLC(500, 2, 10)
	flags := FlagTombstone | FlagCompressed // no BlobRef

	// Write with compression disabled to preserve flags exactly.
	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{Compression: CompressionNone})
	require.NoError(t, w.Append(ts, flags, []byte("flagtest"), []byte("data")))
	require.NoError(t, w.Close())

	// Read back raw (without auto-decompression) via Decode.
	rf := openForRead(t, path)
	defer rf.Close()
	rec, err := Decode(rf)
	require.NoError(t, err)

	assert.True(t, rec.Flags.IsTombstone())
	assert.True(t, rec.Flags.IsCompressed())
	assert.False(t, rec.Flags.IsBlobRef())
}

// --- Gherkin Scenario: Snappy compression for large values ---

func TestWriter_SnappyCompression_Auto(t *testing.T) {
	path := createTempWAL(t)

	ts := hlc.NewHLC(2000, 0, 0)
	key := []byte("bigval")

	// Create a 10,000-byte value with repetitive data (highly compressible).
	value := make([]byte, 10_000)
	for i := range value {
		value[i] = byte(i % 256)
	}

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{Compression: CompressionAuto})
	require.NoError(t, w.Append(ts, 0, key, value))
	require.NoError(t, w.Close())

	// File should be smaller than raw value + header.
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Less(t, info.Size(), int64(10_000+HeaderSize))

	// The reader should auto-decompress and return the original value.
	rf := openForRead(t, path)
	reader := NewReader(rf)
	defer reader.Close()

	rec, err := reader.Next()
	require.NoError(t, err)
	assert.Equal(t, value, rec.Value)
	assert.False(t, rec.Flags.IsCompressed(), "compressed flag should be cleared after decompression")
}

// --- Gherkin Scenario: Batch write with fsync ---

func TestWriter_BatchSync(t *testing.T) {
	path := createTempWAL(t)

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{SyncInterval: 1 * time.Millisecond})

	const count = 1000
	for i := range count {
		ts := hlc.NewHLC(int64(i+1), 0, 0)
		key := []byte("batch-key")
		value := []byte("batch-value")
		require.NoError(t, w.Append(ts, 0, key, value))
	}

	require.NoError(t, w.Close())

	// All records must be readable.
	rf := openForRead(t, path)
	reader := NewReader(rf)
	defer reader.Close()

	records, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, count)
}

// --- Gherkin Scenario: WAL crash recovery ---

func TestReader_CrashRecovery(t *testing.T) {
	path := createTempWAL(t)

	// Write 100 complete records.
	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	for i := range 100 {
		ts := hlc.NewHLC(int64(i+1), 0, 0)
		require.NoError(t, w.Append(ts, 0, []byte("key"), []byte("val")))
	}
	require.NoError(t, w.Close())

	// Append an incomplete record (simulating a crash mid-write).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Reader should return all 100 valid records without error.
	rf := openForRead(t, path)
	reader := NewReader(rf)
	defer reader.Close()

	records, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, 100)
}

// --- Gherkin Scenario: Key size limit enforcement ---

func TestWriter_KeyTooLong(t *testing.T) {
	path := createTempWAL(t)

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	defer w.Close()

	bigKey := make([]byte, 513)
	for i := range bigKey {
		bigKey[i] = 'a'
	}

	err := w.Append(hlc.NewHLC(1, 0, 0), 0, bigKey, []byte("val"))
	assert.ErrorIs(t, err, ErrKeyTooLong)
}

// --- Gherkin Scenario: Blob reference ---

func TestRecord_BlobRef(t *testing.T) {
	path := createTempWAL(t)

	ts := hlc.NewHLC(9999, 0, 0)
	key := []byte("blobkey")

	// Construct a synthetic 40-byte blob reference.
	blobRef := make([]byte, BlobRefSize)
	for i := range 32 {
		blobRef[i] = byte(i) // SHA-256 hash
	}
	binary.LittleEndian.PutUint64(blobRef[32:40], 100_000) // original size

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	require.NoError(t, w.Append(ts, FlagBlobRef, key, blobRef))
	require.NoError(t, w.Close())

	rf := openForRead(t, path)
	reader := NewReader(rf)
	defer reader.Close()

	rec, err := reader.Next()
	require.NoError(t, err)
	assert.True(t, rec.Flags.IsBlobRef())
	assert.Len(t, rec.Value, BlobRefSize)
	assert.Equal(t, blobRef, rec.Value)
}

// --- Additional tests ---

func TestRecord_EmptyValue(t *testing.T) {
	var buf bytes.Buffer

	rec := &Record{
		HLC:   hlc.NewHLC(1, 0, 0),
		Key:   []byte("key"),
		Value: nil,
	}
	require.NoError(t, rec.Encode(&buf))

	decoded, err := Decode(&buf)
	require.NoError(t, err)
	assert.Equal(t, rec.Key, decoded.Key)
	assert.Empty(t, decoded.Value)
}

func TestRecord_MaxKeySize(t *testing.T) {
	path := createTempWAL(t)

	key := make([]byte, MaxKeySize) // exactly 512
	for i := range key {
		key[i] = 'k'
	}

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	require.NoError(t, w.Append(hlc.NewHLC(1, 0, 0), 0, key, []byte("v")))
	require.NoError(t, w.Close())

	rf := openForRead(t, path)
	reader := NewReader(rf)
	defer reader.Close()

	rec, err := reader.Next()
	require.NoError(t, err)
	assert.Equal(t, key, rec.Key)
}

func TestWriter_CloseTwice(t *testing.T) {
	path := createTempWAL(t)

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	require.NoError(t, w.Close())
	assert.NoError(t, w.Close()) // second close should be a no-op
}

func TestReader_EmptyFile(t *testing.T) {
	path := createTempWAL(t)

	// Create an empty file.
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rf := openForRead(t, path)
	reader := NewReader(rf)
	defer reader.Close()

	records, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestWriter_EmptyKey(t *testing.T) {
	path := createTempWAL(t)

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	defer w.Close()

	err := w.Append(hlc.NewHLC(1, 0, 0), 0, []byte{}, []byte("val"))
	assert.Error(t, err)
}

func TestWriter_ClosedWriter(t *testing.T) {
	path := createTempWAL(t)

	wf := openForWrite(t, path)
	w := NewWriter(wf, WriterOptions{})
	require.NoError(t, w.Close())

	err := w.Append(hlc.NewHLC(1, 0, 0), 0, []byte("key"), []byte("val"))
	assert.Error(t, err)
}

// --- Property-based tests ---

func TestProperty_EncodeDecodeRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		wallMicros := rapid.Int64Range(0, hlc.MaxWallMicros).Draw(t, "wallMicros")
		nodeID := rapid.Uint8Range(0, hlc.MaxNodeID).Draw(t, "nodeID")
		logical := rapid.Uint16Range(0, hlc.MaxLogical).Draw(t, "logical")
		ts := hlc.NewHLC(wallMicros, nodeID, logical)

		flagBits := rapid.Uint8Range(0, 7).Draw(t, "flags")
		flags := Flags(flagBits)

		keyLen := rapid.IntRange(1, MaxKeySize).Draw(t, "keyLen")
		key := rapid.SliceOfN(rapid.Byte(), keyLen, keyLen).Draw(t, "key")

		valLen := rapid.IntRange(0, 1024).Draw(t, "valLen")
		value := rapid.SliceOfN(rapid.Byte(), valLen, valLen).Draw(t, "value")

		rec := &Record{HLC: ts, Flags: flags, Key: key, Value: value}

		var buf bytes.Buffer
		err := rec.Encode(&buf)
		require.NoError(t, err)

		decoded, err := Decode(&buf)
		require.NoError(t, err)

		assert.Equal(t, rec.HLC, decoded.HLC)
		assert.Equal(t, rec.Flags, decoded.Flags)
		assert.Equal(t, rec.Key, decoded.Key)
		assert.Equal(t, rec.Value, decoded.Value)
	})
}

func TestProperty_CRC32Integrity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ts := hlc.NewHLC(rapid.Int64Range(0, 1_000_000).Draw(t, "wall"), 0, 0)
		key := []byte("propkey")
		valLen := rapid.IntRange(1, 512).Draw(t, "valLen")
		value := rapid.SliceOfN(rapid.Byte(), valLen, valLen).Draw(t, "value")

		rec := &Record{HLC: ts, Key: key, Value: value}

		var buf bytes.Buffer
		require.NoError(t, rec.Encode(&buf))

		data := buf.Bytes()
		require.Greater(t, len(data), HeaderSize)

		// Flip a random bit in the payload (after CRC field).
		flipPos := 4 + rand.IntN(len(data)-4)
		flipBit := byte(1 << rand.IntN(8))
		data[flipPos] ^= flipBit

		_, err := Decode(bytes.NewReader(data))
		// A bit flip may corrupt length fields, causing truncation instead of
		// CRC mismatch. Both indicate the record was not silently accepted.
		isCorrupt := errors.Is(err, ErrCorruptRecord)
		isTruncated := errors.Is(err, ErrTruncatedRecord)
		assert.True(t, isCorrupt || isTruncated,
			"expected ErrCorruptRecord or ErrTruncatedRecord, got: %v", err)
	})
}
