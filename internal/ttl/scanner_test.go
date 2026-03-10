package ttl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/segment"
)

// makeSegment creates a test segment with MaxTS from a wall time relative to
// the NalaDB epoch. Use toRelativeMicros() to convert time.Time values.
func makeSegment(id uint64, maxTSRelativeMicros int64) *segment.Segment {
	maxTS := hlc.NewHLC(maxTSRelativeMicros, 0, 0)
	return &segment.Segment{
		ID: id,
		Meta: segment.Metadata{
			MaxTS: uint64(maxTS),
			MinTS: uint64(hlc.NewHLC(maxTSRelativeMicros-1000000, 0, 0)),
		},
	}
}

// toRelativeMicros converts a time.Time to microseconds relative to the
// NalaDB epoch, matching the HLC wall time format.
func toRelativeMicros(t time.Time) int64 {
	return t.UnixMicro() - hlc.Epoch
}

func TestScanner_ExpiredSegment(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scanner := NewScanner(WithNowFunc(func() time.Time { return now }))

	// Segment from 100 days ago.
	seg := makeSegment(1, toRelativeMicros(now.Add(-100*24*time.Hour)))

	// Policy: 90 days retention.
	policies := []RetentionPolicy{
		{TTL: 90 * 24 * time.Hour},
	}

	result := scanner.Scan([]*segment.Segment{seg}, policies)
	require.Len(t, result.ExpiredSegmentIDs, 1)
	assert.Equal(t, uint64(1), result.ExpiredSegmentIDs[0])
}

func TestScanner_ActiveSegment(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scanner := NewScanner(WithNowFunc(func() time.Time { return now }))

	// Segment from 30 days ago (within 90-day retention).
	seg := makeSegment(1, toRelativeMicros(now.Add(-30*24*time.Hour)))

	policies := []RetentionPolicy{
		{TTL: 90 * 24 * time.Hour},
	}

	result := scanner.Scan([]*segment.Segment{seg}, policies)
	assert.Empty(t, result.ExpiredSegmentIDs)
}

func TestScanner_NoPolicy(t *testing.T) {
	scanner := NewScanner()

	seg := makeSegment(1, 1000)
	result := scanner.Scan([]*segment.Segment{seg}, nil)
	assert.Empty(t, result.ExpiredSegmentIDs)
}

func TestScanner_ZeroTTL(t *testing.T) {
	scanner := NewScanner()

	seg := makeSegment(1, 1000)
	policies := []RetentionPolicy{
		{TTL: 0}, // unlimited retention
	}

	result := scanner.Scan([]*segment.Segment{seg}, policies)
	assert.Empty(t, result.ExpiredSegmentIDs)
}

func TestScanner_MultiplePolicies(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scanner := NewScanner(WithNowFunc(func() time.Time { return now }))

	// Segment from 50 days ago.
	seg := makeSegment(1, toRelativeMicros(now.Add(-50*24*time.Hour)))

	// Two policies: 30 days and 90 days.
	// The shortest (30d) wins → segment is expired.
	policies := []RetentionPolicy{
		{TTL: 90 * 24 * time.Hour},
		{TTL: 30 * 24 * time.Hour},
	}

	result := scanner.Scan([]*segment.Segment{seg}, policies)
	require.Len(t, result.ExpiredSegmentIDs, 1)
}

func TestScanner_MixedSegments(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scanner := NewScanner(WithNowFunc(func() time.Time { return now }))

	segments := []*segment.Segment{
		makeSegment(1, toRelativeMicros(now.Add(-100*24*time.Hour))), // expired
		makeSegment(2, toRelativeMicros(now.Add(-50*24*time.Hour))),  // active
		makeSegment(3, toRelativeMicros(now.Add(-200*24*time.Hour))), // expired
	}

	policies := []RetentionPolicy{
		{TTL: 90 * 24 * time.Hour},
	}

	result := scanner.Scan(segments, policies)
	require.Len(t, result.ExpiredSegmentIDs, 2)
	assert.Contains(t, result.ExpiredSegmentIDs, uint64(1))
	assert.Contains(t, result.ExpiredSegmentIDs, uint64(3))
}
