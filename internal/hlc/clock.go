package hlc

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// PhysicalClock abstracts the system clock for testability.
// It must return microseconds relative to the NalaDB epoch.
type PhysicalClock func() int64

// Epoch is the NalaDB reference time: January 1, 2025 00:00:00 UTC,
// expressed as microseconds since the Unix epoch.
// All HLC wall times are stored as microseconds since this epoch.
// With 48-bit precision this provides ~8.9 years of range (through ~2034).
const Epoch int64 = 1_735_689_600_000_000

// DefaultMaxSkew is the default maximum tolerated clock skew between cluster
// nodes: 1 second (1 000 000 µs). If a remote HLC wall time exceeds the
// local physical clock by more than this value, Clock.Update returns
// ErrClockSkew and the local state is not modified.
//
// Set to 0 to disable skew checking.
const DefaultMaxSkew int64 = 1_000_000

// ErrClockSkew is returned by Clock.Update when the remote timestamp's wall
// time exceeds the local physical clock by more than the configured maximum
// skew. This typically indicates that the remote node's system clock is
// misconfigured or that NTP synchronization has drifted too far.
var ErrClockSkew = errors.New("hlc: remote clock skew exceeds maximum tolerance")

// SystemClock returns the current wall-clock time as microseconds since
// the NalaDB epoch.
func SystemClock() int64 {
	return time.Now().UnixMicro() - Epoch
}

// Clock maintains HLC state for a single node, providing monotonically
// increasing timestamps. It is safe for concurrent use.
type Clock struct {
	mu       sync.Mutex
	nodeID   uint8
	last     HLC
	physNow  PhysicalClock
	maxSkew  int64 // maximum tolerated clock skew in µs (0 = disabled)
}

// NewClock creates a new HLC clock for the given node ID.
// The node ID must be in the range 0–15. The clock is initialized with
// DefaultMaxSkew; use SetMaxSkew to adjust.
func NewClock(nodeID uint8) *Clock {
	return &Clock{
		nodeID:  nodeID & MaxNodeID,
		physNow: SystemClock,
		maxSkew: DefaultMaxSkew,
	}
}

// NewClockWithPhysical creates a new HLC clock with a custom physical clock
// source. This is primarily useful for testing.
func NewClockWithPhysical(nodeID uint8, phys PhysicalClock) *Clock {
	return &Clock{
		nodeID:  nodeID & MaxNodeID,
		physNow: phys,
		maxSkew: DefaultMaxSkew,
	}
}

// SetMaxSkew sets the maximum tolerated clock skew. If a remote HLC's wall
// time exceeds the local physical clock by more than d, Clock.Update will
// return ErrClockSkew. A duration of 0 disables the check.
func (c *Clock) SetMaxSkew(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxSkew = d.Microseconds()
}

// MaxSkew returns the currently configured maximum clock skew tolerance.
func (c *Clock) MaxSkew() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Duration(c.maxSkew) * time.Microsecond
}

// Now generates a new HLC timestamp for a local event.
//
// Algorithm:
//  1. Read physical time pt.
//  2. If pt > last.WallMicros: new timestamp = (pt, nodeID, 0).
//  3. If pt <= last.WallMicros: new timestamp = (last.WallMicros, nodeID, last.Logical+1).
//
// This guarantees monotonically increasing timestamps even when the
// physical clock is not strictly monotonic.
func (c *Clock) Now() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.physNow()
	lastWall := c.last.WallMicros()

	var ts HLC
	if pt > lastWall {
		ts = NewHLC(pt, c.nodeID, 0)
	} else {
		ts = NewHLC(lastWall, c.nodeID, c.last.Logical()+1)
	}

	c.last = ts
	return ts
}

// Update processes a remote HLC timestamp and returns a new local timestamp
// that is guaranteed to be greater than both the current local state and the
// remote timestamp.
//
// If the remote wall time exceeds the local physical clock by more than the
// configured maximum skew (see SetMaxSkew), Update returns ErrClockSkew and
// the local clock state is not modified. This prevents a single misconfigured
// node from permanently pushing the cluster's timestamps into the future.
//
// Algorithm:
//  1. Read physical time pt.
//  2. Reject if remote wall time is too far ahead (clock skew check).
//  3. new wall = max(pt, last.WallMicros, remote.WallMicros).
//  4. Determine logical counter based on which wall times matched.
//  5. Construct new timestamp with (newWall, nodeID, logical).
func (c *Clock) Update(remote HLC) (HLC, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.physNow()
	lastWall := c.last.WallMicros()
	remoteWall := remote.WallMicros()

	// Clock skew check: reject if remote wall time is too far ahead.
	if c.maxSkew > 0 && remoteWall-pt > c.maxSkew {
		return 0, fmt.Errorf("%w: remote wall %d exceeds local physical %d by %d µs (max %d µs)",
			ErrClockSkew, remoteWall, pt, remoteWall-pt, c.maxSkew)
	}

	var newWall int64
	var logical uint16

	switch {
	case pt > lastWall && pt > remoteWall:
		// Physical clock is ahead — reset logical.
		newWall = pt
		logical = 0
	case lastWall == remoteWall:
		// Both are tied — take the max logical and increment.
		newWall = lastWall
		l := c.last.Logical()
		rl := remote.Logical()
		if rl > l {
			l = rl
		}
		logical = l + 1
	case lastWall > remoteWall:
		// Local is ahead — increment local logical.
		newWall = lastWall
		logical = c.last.Logical() + 1
	default:
		// Remote is ahead — increment remote logical.
		newWall = remoteWall
		logical = remote.Logical() + 1
	}

	ts := NewHLC(newWall, c.nodeID, logical)
	c.last = ts
	return ts, nil
}

// Last returns the most recently generated HLC timestamp.
func (c *Clock) Last() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}
