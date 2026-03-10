package ttl

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownsampleAvg_BasicBuckets(t *testing.T) {
	// 10 points at 1-second intervals, aggregate at 5-second intervals.
	points := make([]DataPoint, 0, 10)
	for i := range 10 {
		points = append(points, DataPoint{
			Timestamp: int64(i) * 1_000_000, // i seconds in µs
			Value:     float64(i),
		})
	}

	result := Downsample(points, 5*time.Second, StrategyAvg)
	require.Len(t, result, 2, "10 seconds / 5s interval = 2 buckets")

	// First bucket: avg(0,1,2,3,4) = 2.0
	assert.InDelta(t, 2.0, result[0].Value, 0.001)
	// Second bucket: avg(5,6,7,8,9) = 7.0
	assert.InDelta(t, 7.0, result[1].Value, 0.001)
}

func TestDownsampleAvg_SingleBucket(t *testing.T) {
	points := []DataPoint{
		{Timestamp: 0, Value: 10},
		{Timestamp: 1_000_000, Value: 20},
		{Timestamp: 2_000_000, Value: 30},
	}

	result := Downsample(points, 10*time.Second, StrategyAvg)
	require.Len(t, result, 1)
	assert.InDelta(t, 20.0, result[0].Value, 0.001)
}

func TestDownsampleMinMax(t *testing.T) {
	points := make([]DataPoint, 0, 10)
	for i := range 10 {
		points = append(points, DataPoint{
			Timestamp: int64(i) * 1_000_000,
			Value:     float64(i),
		})
	}

	result := Downsample(points, 5*time.Second, StrategyMinMax)
	// 2 buckets × 2 points (min, max) = 4 points.
	require.Len(t, result, 4)

	// First bucket: min=0, max=4
	assert.InDelta(t, 0.0, result[0].Value, 0.001)
	assert.InDelta(t, 4.0, result[1].Value, 0.001)

	// Second bucket: min=5, max=9
	assert.InDelta(t, 5.0, result[2].Value, 0.001)
	assert.InDelta(t, 9.0, result[3].Value, 0.001)
}

func TestDownsampleLTTB(t *testing.T) {
	// Create a sine wave with 100 points, downsample to ~10.
	points := make([]DataPoint, 0, 100)
	for i := range 100 {
		points = append(points, DataPoint{
			Timestamp: int64(i) * 1_000_000,
			Value:     math.Sin(float64(i) * 0.1),
		})
	}

	result := Downsample(points, 10*time.Second, StrategyLTTB)
	// Should have fewer points than original.
	assert.Less(t, len(result), len(points))
	// Should include first and last points.
	assert.Equal(t, points[0].Timestamp, result[0].Timestamp)
	assert.Equal(t, points[99].Timestamp, result[len(result)-1].Timestamp)
}

func TestDownsample_EmptyInput(t *testing.T) {
	result := Downsample(nil, time.Second, StrategyAvg)
	assert.Nil(t, result)
}

func TestDownsampleAvg_HourlyAggregation(t *testing.T) {
	// Simulate 3600 per-second readings over 1 hour.
	points := make([]DataPoint, 0, 3600)
	for i := range 3600 {
		points = append(points, DataPoint{
			Timestamp: int64(i) * 1_000_000,
			Value:     float64(i % 60), // periodic pattern
		})
	}

	result := Downsample(points, time.Hour, StrategyAvg)
	require.Len(t, result, 1, "3600s of data / 1h interval = 1 bucket")
}

func TestDownsampleAvg_PerSecondToPerHour(t *testing.T) {
	// 24 hours of per-second data → 24 hourly buckets.
	hour := int64(3_600_000_000) // 1 hour in µs
	points := make([]DataPoint, 0, 1440)
	for h := range 24 {
		for s := range 60 {
			points = append(points, DataPoint{
				Timestamp: int64(h)*hour + int64(s)*1_000_000,
				Value:     float64(h*60 + s),
			})
		}
	}

	result := Downsample(points, time.Hour, StrategyAvg)
	assert.Equal(t, 24, len(result), "24 hours → 24 buckets")
}
