package ttl

import (
	"time"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/segment"
)

// Scanner checks segments against retention policies to identify fully expired
// segments that can be bulk-deleted without per-record tombstones.
type Scanner struct {
	nowFunc func() time.Time
}

// ScannerOption configures a Scanner.
type ScannerOption func(*Scanner)

// WithNowFunc sets the time source for the scanner (useful for testing).
func WithNowFunc(fn func() time.Time) ScannerOption {
	return func(s *Scanner) {
		s.nowFunc = fn
	}
}

// NewScanner creates a new expiry scanner.
func NewScanner(opts ...ScannerOption) *Scanner {
	s := &Scanner{
		nowFunc: time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ScanResult holds the outcome of an expiry scan.
type ScanResult struct {
	ExpiredSegmentIDs []uint64
}

// Scan checks all segments against the given retention policies.
// A segment is considered fully expired if its MaxTS wall-clock time is
// older than the shortest TTL across all applicable policies.
func (s *Scanner) Scan(segments []*segment.Segment, policies []RetentionPolicy) ScanResult {
	var result ScanResult
	if len(policies) == 0 {
		return result
	}

	now := s.nowFunc()
	// Convert to NalaDB epoch (HLC wall time is relative to hlc.Epoch).
	nowRelativeMicros := now.UnixMicro() - hlc.Epoch

	// Find the shortest TTL across all policies (most aggressive retention).
	var minTTL time.Duration
	for _, pol := range policies {
		if pol.TTL > 0 && (minTTL <= 0 || pol.TTL < minTTL) {
			minTTL = pol.TTL
		}
	}
	if minTTL <= 0 {
		return result
	}

	cutoffMicros := nowRelativeMicros - minTTL.Microseconds()

	for _, seg := range segments {
		maxTS := hlc.HLC(seg.Meta.MaxTS)
		if maxTS.WallMicros() < cutoffMicros {
			result.ExpiredSegmentIDs = append(result.ExpiredSegmentIDs, seg.ID)
		}
	}

	return result
}
