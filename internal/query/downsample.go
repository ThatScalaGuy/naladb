package query

import (
	"math"
)

// DataPoint represents a single time-series data point for downsampling.
type DataPoint struct {
	Timestamp int64 // microseconds
	Value     float64
}

// LTTB implements the Largest Triangle Three Buckets downsampling algorithm.
// It selects n representative points from the input data while preserving
// the visual shape of the time series.
func LTTB(data []DataPoint, n int) []DataPoint {
	if n <= 0 || len(data) == 0 {
		return nil
	}
	if n >= len(data) {
		out := make([]DataPoint, len(data))
		copy(out, data)
		return out
	}
	if n < 3 {
		// With fewer than 3 buckets, just return first and last.
		return []DataPoint{data[0], data[len(data)-1]}
	}

	result := make([]DataPoint, 0, n)
	// Always include the first point.
	result = append(result, data[0])

	bucketSize := float64(len(data)-2) / float64(n-2)

	prevSelected := 0

	for i := 1; i < n-1; i++ {
		// Calculate bucket boundaries.
		bucketStart := int(math.Floor(float64(i-1)*bucketSize)) + 1
		bucketEnd := min(int(math.Floor(float64(i)*bucketSize))+1, len(data)-1)

		// Calculate the average point of the next bucket.
		nextBucketStart := int(math.Floor(float64(i)*bucketSize)) + 1
		nextBucketEnd := min(int(math.Floor(float64(i+1)*bucketSize))+1, len(data)-1)

		avgX := 0.0
		avgY := 0.0
		nextCount := 0
		for j := nextBucketStart; j <= nextBucketEnd && j < len(data); j++ {
			avgX += float64(data[j].Timestamp)
			avgY += data[j].Value
			nextCount++
		}
		if nextCount > 0 {
			avgX /= float64(nextCount)
			avgY /= float64(nextCount)
		}

		// Find the point in the current bucket that forms the largest triangle.
		maxArea := -1.0
		bestIdx := bucketStart

		prevX := float64(data[prevSelected].Timestamp)
		prevY := data[prevSelected].Value

		for j := bucketStart; j <= bucketEnd && j < len(data); j++ {
			area := math.Abs(
				(prevX-avgX)*(data[j].Value-prevY)-
					(prevX-float64(data[j].Timestamp))*(avgY-prevY),
			) * 0.5
			if area > maxArea {
				maxArea = area
				bestIdx = j
			}
		}

		result = append(result, data[bestIdx])
		prevSelected = bestIdx
	}

	// Always include the last point.
	result = append(result, data[len(data)-1])

	return result
}

// MinMaxDownsample reduces data to n points by selecting the min and max
// value in each bucket. Returns up to 2*buckets points (min+max per bucket).
func MinMaxDownsample(data []DataPoint, buckets int) []DataPoint {
	if buckets <= 0 || len(data) == 0 {
		return nil
	}
	if len(data) <= buckets*2 {
		out := make([]DataPoint, len(data))
		copy(out, data)
		return out
	}

	bucketSize := float64(len(data)) / float64(buckets)
	result := make([]DataPoint, 0, buckets*2)

	for i := range buckets {
		start := int(math.Floor(float64(i) * bucketSize))
		end := min(int(math.Floor(float64(i+1)*bucketSize)), len(data))
		if start >= end {
			continue
		}

		minIdx, maxIdx := start, start
		for j := start + 1; j < end; j++ {
			if data[j].Value < data[minIdx].Value {
				minIdx = j
			}
			if data[j].Value > data[maxIdx].Value {
				maxIdx = j
			}
		}

		// Add min before max to preserve temporal order.
		if minIdx <= maxIdx {
			result = append(result, data[minIdx], data[maxIdx])
		} else {
			result = append(result, data[maxIdx], data[minIdx])
		}
	}

	return result
}

// AvgDownsample reduces data to n points by averaging each bucket.
func AvgDownsample(data []DataPoint, n int) []DataPoint {
	if n <= 0 || len(data) == 0 {
		return nil
	}
	if n >= len(data) {
		out := make([]DataPoint, len(data))
		copy(out, data)
		return out
	}

	bucketSize := float64(len(data)) / float64(n)
	result := make([]DataPoint, 0, n)

	for i := range n {
		start := int(math.Floor(float64(i) * bucketSize))
		end := min(int(math.Floor(float64(i+1)*bucketSize)), len(data))
		if start >= end {
			continue
		}

		sumT := int64(0)
		sumV := 0.0
		count := 0
		for j := start; j < end; j++ {
			sumT += data[j].Timestamp
			sumV += data[j].Value
			count++
		}

		result = append(result, DataPoint{
			Timestamp: sumT / int64(count),
			Value:     sumV / float64(count),
		})
	}

	return result
}
