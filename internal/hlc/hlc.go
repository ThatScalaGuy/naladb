// Package hlc implements a Hybrid Logical Clock with microsecond precision.
//
// The HLC is packed into 8 bytes: 48-bit wall-time (µs) + 4-bit NodeID + 12-bit logical counter.
package hlc

import "fmt"

// HLC represents a Hybrid Logical Clock timestamp packed into a single uint64.
//
// Bit layout (MSB to LSB):
//
//	[63..16] 48 bits — wall-clock time in microseconds
//	[15..12]  4 bits — node ID (0–15)
//	[11.. 0] 12 bits — logical counter (0–4095)
//
// This layout ensures that natural uint64 ordering matches the
// (WallTime, NodeID, Logical) tuple ordering.
type HLC uint64

// MaxWallMicros is the maximum wall-time value (2^48 - 1).
const MaxWallMicros = (1 << 48) - 1

// MaxNodeID is the maximum node ID value (2^4 - 1 = 15).
const MaxNodeID = (1 << 4) - 1

// MaxLogical is the maximum logical counter value (2^12 - 1 = 4095).
const MaxLogical = (1 << 12) - 1

// MaxHLC is the maximum possible HLC value, used to represent open-ended
// validity intervals (e.g., a node that has not been deleted).
const MaxHLC = HLC(^uint64(0))

// NewHLC creates a new HLC from the given wall-clock time (microseconds),
// node ID (0–15), and logical counter (0–4095).
func NewHLC(wallMicros int64, nodeID uint8, logical uint16) HLC {
	return HLC(uint64(wallMicros)<<16 | uint64(nodeID&0xF)<<12 | uint64(logical&0xFFF))
}

// WallMicros returns the 48-bit wall-clock time in microseconds.
func (h HLC) WallMicros() int64 {
	return int64(uint64(h) >> 16)
}

// NodeID returns the 4-bit node ID.
func (h HLC) NodeID() uint8 {
	return uint8((uint64(h) >> 12) & 0xF)
}

// Logical returns the 12-bit logical counter.
func (h HLC) Logical() uint16 {
	return uint16(uint64(h) & 0xFFF)
}

// Before reports whether h is strictly before other in causal ordering.
// The ordering is determined by the packed uint64 value, which naturally
// orders by (WallTime, NodeID, Logical).
func (h HLC) Before(other HLC) bool {
	return uint64(h) < uint64(other)
}

// IsZero reports whether the HLC is the zero value.
func (h HLC) IsZero() bool {
	return h == 0
}

// String returns a human-readable representation of the HLC.
func (h HLC) String() string {
	return fmt.Sprintf("HLC{wall=%d, node=%d, logical=%d}", h.WallMicros(), h.NodeID(), h.Logical())
}
