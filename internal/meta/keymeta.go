// Package meta implements per-key inline statistics that are updated on every
// write. Statistics include Welford online mean/variance, EWMA write rate, and
// HyperLogLog cardinality estimation, enabling META queries without a separate
// aggregation pass.
package meta

import (
	"math"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/axiomhq/hyperloglog"
)

// ewmaAlpha is the smoothing factor for the exponentially weighted moving
// average of the write rate. A smaller alpha produces a smoother estimate.
const ewmaAlpha = 0.05

// KeyMeta holds inline statistics for a single key. All fields are updated
// incrementally on every write.
type KeyMeta struct {
	Key         string
	TotalWrites uint64

	// Timing
	FirstSeenUs int64   // wall-time µs of first write
	LastSeenUs  int64   // wall-time µs of most recent write
	WriteRateHz float64 // EWMA write frequency (writes/second)

	// Welford online statistics (numeric values only)
	MinValue    float64
	MaxValue    float64
	AvgValue    float64
	StdDevValue float64
	numericN    uint64  // count of numeric values seen
	welfordM2   float64 // Welford M2 accumulator

	// Cardinality
	Cardinality uint32 // HyperLogLog estimated distinct values

	// Size tracking
	SizeBytes uint64

	hll *hyperloglog.Sketch
}

// newKeyMeta creates a new KeyMeta for the given key with initialized HLL.
func newKeyMeta(key string) *KeyMeta {
	return &KeyMeta{
		Key:      key,
		MinValue: math.Inf(1),
		MaxValue: math.Inf(-1),
		hll:      hyperloglog.New(),
	}
}

// updateWelford updates running mean, variance, min, and max using Welford's
// online algorithm. This is numerically stable for large sample sizes.
func (km *KeyMeta) updateWelford(val float64) {
	km.numericN++
	n := float64(km.numericN)

	delta := val - km.AvgValue
	km.AvgValue += delta / n
	delta2 := val - km.AvgValue
	km.welfordM2 += delta * delta2

	if n > 1 {
		km.StdDevValue = math.Sqrt(km.welfordM2 / n)
	}

	if val < km.MinValue {
		km.MinValue = val
	}
	if val > km.MaxValue {
		km.MaxValue = val
	}
}

// updateWriteRate computes the EWMA write rate from inter-arrival times.
func (km *KeyMeta) updateWriteRate(wallMicros int64) {
	if km.TotalWrites == 1 {
		// First write: initialize timing, no interval yet.
		km.FirstSeenUs = wallMicros
		km.LastSeenUs = wallMicros
		return
	}

	intervalUs := wallMicros - km.LastSeenUs
	if intervalUs <= 0 {
		intervalUs = 1 // avoid division by zero
	}

	instantHz := 1_000_000.0 / float64(intervalUs)

	if km.TotalWrites == 2 {
		// Second write: initialize EWMA directly.
		km.WriteRateHz = instantHz
	} else {
		km.WriteRateHz = ewmaAlpha*instantHz + (1.0-ewmaAlpha)*km.WriteRateHz
	}

	km.LastSeenUs = wallMicros
}

// updateCardinality inserts value bytes into the HyperLogLog sketch.
func (km *KeyMeta) updateCardinality(value []byte) {
	km.hll.Insert(value)
	km.Cardinality = uint32(km.hll.Estimate())
}

// Registry manages KeyMeta entries for all keys with concurrent-safe access.
type Registry struct {
	mu    sync.RWMutex
	metas map[string]*KeyMeta
}

// NewRegistry creates a new empty KeyMeta registry.
func NewRegistry() *Registry {
	return &Registry{
		metas: make(map[string]*KeyMeta),
	}
}

// Update records a write event for the given key. It updates all inline
// statistics: write count, timing, numeric stats (if value parses as float64),
// cardinality, and cumulative size.
func (r *Registry) Update(key string, wallMicros int64, value []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	km, ok := r.metas[key]
	if !ok {
		km = newKeyMeta(key)
		r.metas[key] = km
	}

	km.TotalWrites++
	km.SizeBytes += uint64(len(value))

	km.updateWriteRate(wallMicros)

	if value != nil {
		km.updateCardinality(value)

		if val, err := strconv.ParseFloat(string(value), 64); err == nil {
			km.updateWelford(val)
		}
	}
}

// Get returns the KeyMeta for a key, or nil if not tracked.
func (r *Registry) Get(key string) *KeyMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metas[key]
}

// Len returns the total number of tracked keys.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.metas)
}

// Match returns all KeyMeta entries whose keys match the given glob pattern
// and pass the optional filter function.
func (r *Registry) Match(pattern string, filter func(*KeyMeta) bool) []*KeyMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*KeyMeta, 0, len(r.metas))
	for key, km := range r.metas {
		matched, err := filepath.Match(pattern, key)
		if err != nil || !matched {
			continue
		}
		if filter != nil && !filter(km) {
			continue
		}
		result = append(result, km)
	}
	return result
}
