package ttl

import (
	"math"
	"sort"
	"time"
)

// DownsampleStrategy defines how data points are aggregated during downsampling.
type DownsampleStrategy int

const (
	// StrategyAvg computes the average value per time interval.
	StrategyAvg DownsampleStrategy = iota
	// StrategyMinMax preserves the minimum and maximum values per interval.
	StrategyMinMax
	// StrategyLTTB uses the Largest Triangle Three Buckets algorithm for
	// visually accurate downsampling.
	StrategyLTTB
)

// DataPoint represents a timestamped numeric value for downsampling.
type DataPoint struct {
	Timestamp int64 // Unix microseconds
	Value     float64
}

// Downsample aggregates data points according to the given strategy and interval.
// Points must not be empty. The returned points are in chronological order.
func Downsample(points []DataPoint, interval time.Duration, strategy DownsampleStrategy) []DataPoint {
	if len(points) == 0 {
		return nil
	}

	switch strategy {
	case StrategyAvg:
		return downsampleAvg(points, interval)
	case StrategyMinMax:
		return downsampleMinMax(points, interval)
	case StrategyLTTB:
		targetCount := estimateTargetCount(points, interval)
		if targetCount >= len(points) {
			return points
		}
		return downsampleLTTB(points, targetCount)
	default:
		return downsampleAvg(points, interval)
	}
}

// downsampleAvg computes the average value per time interval.
func downsampleAvg(points []DataPoint, interval time.Duration) []DataPoint {
	intervalMicros := interval.Microseconds()
	if intervalMicros <= 0 {
		return points
	}

	sorted := make([]DataPoint, len(points))
	copy(sorted, points)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	var result []DataPoint
	bucketStart := sorted[0].Timestamp
	var sum float64
	var count int

	for _, p := range sorted {
		if p.Timestamp >= bucketStart+intervalMicros {
			if count > 0 {
				result = append(result, DataPoint{
					Timestamp: bucketStart + intervalMicros/2,
					Value:     sum / float64(count),
				})
			}
			bucketStart = bucketStart + ((p.Timestamp-bucketStart)/intervalMicros)*intervalMicros
			sum = 0
			count = 0
		}
		sum += p.Value
		count++
	}

	if count > 0 {
		result = append(result, DataPoint{
			Timestamp: bucketStart + intervalMicros/2,
			Value:     sum / float64(count),
		})
	}

	return result
}

// downsampleMinMax preserves the min and max values per time interval.
// Each interval produces two data points (min, max).
func downsampleMinMax(points []DataPoint, interval time.Duration) []DataPoint {
	intervalMicros := interval.Microseconds()
	if intervalMicros <= 0 {
		return points
	}

	sorted := make([]DataPoint, len(points))
	copy(sorted, points)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	var result []DataPoint
	bucketStart := sorted[0].Timestamp
	minVal := math.Inf(1)
	maxVal := math.Inf(-1)
	count := 0

	flush := func() {
		if count > 0 {
			mid := bucketStart + intervalMicros/2
			result = append(result,
				DataPoint{Timestamp: mid, Value: minVal},
				DataPoint{Timestamp: mid + 1, Value: maxVal},
			)
		}
	}

	for _, p := range sorted {
		if p.Timestamp >= bucketStart+intervalMicros {
			flush()
			bucketStart = bucketStart + ((p.Timestamp-bucketStart)/intervalMicros)*intervalMicros
			minVal = math.Inf(1)
			maxVal = math.Inf(-1)
			count = 0
		}
		if p.Value < minVal {
			minVal = p.Value
		}
		if p.Value > maxVal {
			maxVal = p.Value
		}
		count++
	}

	flush()
	return result
}

// downsampleLTTB implements the Largest Triangle Three Buckets algorithm.
// It selects targetCount representative points from the input while
// preserving visual shape.
func downsampleLTTB(points []DataPoint, targetCount int) []DataPoint {
	n := len(points)
	if targetCount >= n || targetCount < 3 {
		return points
	}

	sorted := make([]DataPoint, n)
	copy(sorted, points)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	result := make([]DataPoint, 0, targetCount)
	// Always include first point.
	result = append(result, sorted[0])

	bucketSize := float64(n-2) / float64(targetCount-2)
	prevSelected := 0

	for i := 1; i < targetCount-1; i++ {
		bucketStart := int(float64(i-1)*bucketSize) + 1
		bucketEnd := int(float64(i)*bucketSize) + 1
		if bucketEnd > n-1 {
			bucketEnd = n - 1
		}

		// Compute average of next bucket for triangle area calculation.
		nextBucketStart := bucketEnd
		nextBucketEnd := int(float64(i+1)*bucketSize) + 1
		if nextBucketEnd > n {
			nextBucketEnd = n
		}

		var avgX, avgY float64
		nextCount := 0
		for j := nextBucketStart; j < nextBucketEnd; j++ {
			avgX += float64(sorted[j].Timestamp)
			avgY += sorted[j].Value
			nextCount++
		}
		if nextCount > 0 {
			avgX /= float64(nextCount)
			avgY /= float64(nextCount)
		}

		// Select point with largest triangle area in current bucket.
		maxArea := -1.0
		selectedIdx := bucketStart
		prevX := float64(sorted[prevSelected].Timestamp)
		prevY := sorted[prevSelected].Value

		for j := bucketStart; j < bucketEnd; j++ {
			area := math.Abs(
				(prevX-avgX)*(sorted[j].Value-prevY)-
					(prevX-float64(sorted[j].Timestamp))*(avgY-prevY)) * 0.5
			if area > maxArea {
				maxArea = area
				selectedIdx = j
			}
		}

		result = append(result, sorted[selectedIdx])
		prevSelected = selectedIdx
	}

	// Always include last point.
	result = append(result, sorted[n-1])
	return result
}

// estimateTargetCount calculates how many points the time span would produce
// at the given interval.
func estimateTargetCount(points []DataPoint, interval time.Duration) int {
	if len(points) < 2 {
		return len(points)
	}

	sorted := make([]DataPoint, len(points))
	copy(sorted, points)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp < sorted[j].Timestamp
	})

	span := sorted[len(sorted)-1].Timestamp - sorted[0].Timestamp
	intervalMicros := interval.Microseconds()
	if intervalMicros <= 0 {
		return len(points)
	}

	count := int(span/intervalMicros) + 1
	if count < 3 {
		count = 3
	}
	return count
}
