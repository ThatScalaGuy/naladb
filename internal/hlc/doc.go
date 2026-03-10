// Package hlc implements a Hybrid Logical Clock with microsecond precision.
//
// The HLC is packed into 8 bytes: 48-bit wall-time (µs) + 4-bit NodeID + 12-bit logical counter.
package hlc
