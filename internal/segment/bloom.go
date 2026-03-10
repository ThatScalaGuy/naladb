package segment

import (
	"fmt"
	"io"
	"os"

	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilter wraps a probabilistic set membership filter for segment keys.
type BloomFilter struct {
	filter *bloom.BloomFilter
}

// NewBloomFilter creates a bloom filter sized for n expected items
// with a false positive rate of fp.
func NewBloomFilter(n uint, fp float64) *BloomFilter {
	return &BloomFilter{
		filter: bloom.NewWithEstimates(n, fp),
	}
}

// Add inserts a key into the bloom filter.
func (bf *BloomFilter) Add(key string) {
	bf.filter.Add([]byte(key))
}

// Test reports whether key may be in the set.
// False means definitely not present; true means possibly present.
func (bf *BloomFilter) Test(key string) bool {
	return bf.filter.Test([]byte(key))
}

// WriteTo serializes the bloom filter to w.
func (bf *BloomFilter) WriteTo(w io.Writer) (int64, error) {
	return bf.filter.WriteTo(w)
}

// ReadFrom deserializes a bloom filter from r.
func (bf *BloomFilter) ReadFrom(r io.Reader) (int64, error) {
	return bf.filter.ReadFrom(r)
}

// WriteToFile writes the bloom filter to the given file path.
func (bf *BloomFilter) WriteToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("segment: create bloom file: %w", err)
	}
	defer f.Close()

	if _, err := bf.WriteTo(f); err != nil {
		return fmt.Errorf("segment: write bloom: %w", err)
	}
	return f.Sync()
}

// LoadBloomFilter reads a bloom filter from the given file path.
func LoadBloomFilter(path string) (*BloomFilter, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment: open bloom file: %w", err)
	}
	defer f.Close()

	bf := &BloomFilter{filter: &bloom.BloomFilter{}}
	if _, err := bf.ReadFrom(f); err != nil {
		return nil, fmt.Errorf("segment: read bloom: %w", err)
	}
	return bf, nil
}
