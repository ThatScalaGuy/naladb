package meta

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestKeyMeta_WritesUpdateStats(t *testing.T) {
	r := NewRegistry()
	key := "sensor:temp:value"
	values := []float64{20.0, 22.0, 21.0, 25.0, 19.0}

	baseUs := int64(1_000_000)
	for i, v := range values {
		r.Update(key, baseUs+int64(i)*10_000, []byte(fmt.Sprintf("%.1f", v)))
	}

	km := r.Get(key)
	require.NotNil(t, km)

	assert.Equal(t, uint64(5), km.TotalWrites)
	assert.InDelta(t, 19.0, km.MinValue, 0.001)
	assert.InDelta(t, 25.0, km.MaxValue, 0.001)
	assert.InDelta(t, 21.4, km.AvgValue, 0.01)
	assert.Greater(t, km.StdDevValue, 0.0)
	assert.Less(t, km.FirstSeenUs, km.LastSeenUs)
}

func TestKeyMeta_WriteRateHz_EWMA(t *testing.T) {
	r := NewRegistry()
	key := "rate:test"

	// 100 writes at 10ms intervals = 100 Hz
	intervalUs := int64(10_000) // 10ms in µs
	for i := range 100 {
		r.Update(key, int64(i)*intervalUs, []byte("1"))
	}

	km := r.Get(key)
	require.NotNil(t, km)

	// EWMA converges toward 100 Hz. Allow ±10%.
	assert.InDelta(t, 100.0, km.WriteRateHz, 10.0,
		"WriteRateHz should be approximately 100 Hz, got %f", km.WriteRateHz)
}

func TestKeyMeta_Cardinality_HyperLogLog(t *testing.T) {
	r := NewRegistry()
	key := "cardinality:test"

	baseUs := int64(1_000_000)
	for i := range 10_000 {
		val := fmt.Sprintf("value_%d", i%500) // 500 distinct values
		r.Update(key, baseUs+int64(i), []byte(val))
	}

	km := r.Get(key)
	require.NotNil(t, km)

	// HyperLogLog with ~1.6% error. Allow ±2% (±10 from 500).
	assert.InDelta(t, 500, float64(km.Cardinality), 10.0,
		"Cardinality should be approximately 500, got %d", km.Cardinality)
}

func TestRegistry_Match_FilterByWriteRate(t *testing.T) {
	r := NewRegistry()

	// Create 100 keys with varying write rates.
	// Keys 0-49: low rate (100ms intervals = 10 Hz)
	// Keys 50-99: high rate (1ms intervals = 1000 Hz)
	for i := range 100 {
		key := fmt.Sprintf("sensor:%d:prop:temperature", i)
		intervalUs := int64(100_000) // 100ms = 10 Hz
		if i >= 50 {
			intervalUs = 1_000 // 1ms = 1000 Hz
		}
		// Write enough samples for EWMA to converge.
		for j := range 50 {
			r.Update(key, int64(j)*intervalUs, []byte("42"))
		}
	}

	// Query for keys matching pattern with write rate > 10.0
	results := r.Match("sensor:*:prop:temperature", func(km *KeyMeta) bool {
		return km.WriteRateHz > 10.0
	})

	// Should only get the high-rate keys (50-99).
	assert.GreaterOrEqual(t, len(results), 45, "should have most high-rate keys")
	for _, km := range results {
		assert.Greater(t, km.WriteRateHz, 10.0,
			"key %s has WriteRateHz %f, expected > 10.0", km.Key, km.WriteRateHz)
	}
}

func TestKeyMeta_NonNumericValues(t *testing.T) {
	r := NewRegistry()
	key := "text:key"

	r.Update(key, 1000, []byte("hello"))
	r.Update(key, 2000, []byte("world"))

	km := r.Get(key)
	require.NotNil(t, km)

	assert.Equal(t, uint64(2), km.TotalWrites)
	// Welford stats should be at initial state (no numeric updates).
	assert.Equal(t, math.Inf(1), km.MinValue)
	assert.Equal(t, math.Inf(-1), km.MaxValue)
}

func TestKeyMeta_MixedNumericAndNonNumeric(t *testing.T) {
	r := NewRegistry()
	key := "mixed:key"

	r.Update(key, 1000, []byte("10.0"))
	r.Update(key, 2000, []byte("not-a-number"))
	r.Update(key, 3000, []byte("20.0"))

	km := r.Get(key)
	require.NotNil(t, km)

	assert.Equal(t, uint64(3), km.TotalWrites)
	assert.InDelta(t, 15.0, km.AvgValue, 0.01) // avg of 10.0 and 20.0
	assert.InDelta(t, 10.0, km.MinValue, 0.01)
	assert.InDelta(t, 20.0, km.MaxValue, 0.01)
}

func TestKeyMeta_Get_NotTracked(t *testing.T) {
	r := NewRegistry()
	assert.Nil(t, r.Get("nonexistent"))
}

func TestKeyMeta_Welford_Rapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		values := rapid.SliceOfN(
			rapid.Float64Range(-1e6, 1e6),
			2, 1000,
		).Draw(t, "values")

		km := newKeyMeta("test")
		for _, v := range values {
			km.numericN++
			n := float64(km.numericN)
			delta := v - km.AvgValue
			km.AvgValue += delta / n
			delta2 := v - km.AvgValue
			km.welfordM2 += delta * delta2
			if n > 1 {
				km.StdDevValue = math.Sqrt(km.welfordM2 / n)
			}
			if v < km.MinValue {
				km.MinValue = v
			}
			if v > km.MaxValue {
				km.MaxValue = v
			}
		}

		// Verify against direct computation.
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		expectedMean := sum / float64(len(values))

		sumSqDiff := 0.0
		for _, v := range values {
			d := v - expectedMean
			sumSqDiff += d * d
		}
		expectedStdDev := math.Sqrt(sumSqDiff / float64(len(values)))

		if math.Abs(expectedMean) > 1e-10 {
			assert.InDelta(t, expectedMean, km.AvgValue, math.Abs(expectedMean)*1e-6,
				"Welford mean should match direct computation")
		}
		if expectedStdDev > 1e-10 {
			assert.InDelta(t, expectedStdDev, km.StdDevValue, expectedStdDev*1e-4,
				"Welford stddev should match direct computation")
		}
	})
}
