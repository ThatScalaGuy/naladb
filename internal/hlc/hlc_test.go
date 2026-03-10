package hlc

import (
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// ---------------------------------------------------------------------------
// Scenario: HLC erstellen und Felder auslesen
// ---------------------------------------------------------------------------

func TestNewHLC_FieldAccess(t *testing.T) {
	// 140613200000000 represents a realistic epoch-relative wall time
	// (roughly mid-2029 with the NalaDB epoch of Jan 1, 2025).
	h := NewHLC(140613200000000, 3, 42)

	assert.Equal(t, int64(140613200000000), h.WallMicros())
	assert.Equal(t, uint8(3), h.NodeID())
	assert.Equal(t, uint16(42), h.Logical())
	// Packed value is deterministic — verify via round-trip.
	expected := NewHLC(140613200000000, 3, 42)
	assert.Equal(t, expected, h)
}

// ---------------------------------------------------------------------------
// Scenario: HLC-Ordnung respektiert Wall-Time
// ---------------------------------------------------------------------------

func TestHLC_Before_WallTime(t *testing.T) {
	a := NewHLC(100, 0, 0)
	b := NewHLC(200, 0, 0)

	assert.True(t, a.Before(b))
	assert.False(t, b.Before(a))
}

// ---------------------------------------------------------------------------
// Scenario: HLC-Ordnung bei gleicher Wall-Time nutzt Logical Counter
// ---------------------------------------------------------------------------

func TestHLC_Before_Logical(t *testing.T) {
	a := NewHLC(100, 0, 5)
	b := NewHLC(100, 0, 10)

	assert.True(t, a.Before(b))
	assert.False(t, b.Before(a))
}

// ---------------------------------------------------------------------------
// Scenario: HLC-Ordnung bei gleicher Wall-Time und gleichem Logical nutzt NodeID
// ---------------------------------------------------------------------------

func TestHLC_Before_NodeID(t *testing.T) {
	a := NewHLC(100, 1, 5)
	b := NewHLC(100, 2, 5)

	assert.True(t, a.Before(b))
	assert.False(t, b.Before(a))
}

// ---------------------------------------------------------------------------
// Scenario: Lokales Event generiert neuen HLC-Timestamp
// ---------------------------------------------------------------------------

func TestClock_Now_LocalEvent(t *testing.T) {
	var physTime int64 = 1000
	clock := NewClockWithPhysical(1, func() int64 { return physTime })

	ts := clock.Now()

	assert.True(t, ts.WallMicros() >= 1000)
	assert.Equal(t, uint8(1), ts.NodeID())
	assert.Equal(t, uint16(0), ts.Logical())
}

// ---------------------------------------------------------------------------
// Scenario: Empfang einer Remote-HLC aktualisiert lokale Clock
// ---------------------------------------------------------------------------

func TestClock_Update_RemoteHLC(t *testing.T) {
	var physTime int64 = 1000
	clock := NewClockWithPhysical(1, func() int64 { return physTime })
	clock.SetMaxSkew(0) // disable skew check for this test

	remote := NewHLC(1500, 2, 3)
	ts, err := clock.Update(remote)
	require.NoError(t, err)

	assert.True(t, ts.WallMicros() >= 1500)
	// Remote wall is ahead of physical — so wall should be 1500, logical > 3.
	assert.Equal(t, int64(1500), ts.WallMicros())
	assert.Equal(t, uint16(4), ts.Logical())
	assert.Equal(t, uint8(1), ts.NodeID())
}

func TestClock_Update_RemoteSameWall(t *testing.T) {
	var physTime int64 = 500
	clock := NewClockWithPhysical(1, func() int64 { return physTime })

	// Generate a local timestamp first at wall=500.
	first := clock.Now()
	require.Equal(t, int64(500), first.WallMicros())

	// Now receive a remote with same wall but higher logical.
	remote := NewHLC(500, 2, 10)
	ts, err := clock.Update(remote)
	require.NoError(t, err)

	assert.Equal(t, int64(500), ts.WallMicros())
	assert.True(t, ts.Logical() > 10, "logical should be > remote logical when wall times match")
}

func TestClock_Update_PhysicalAhead(t *testing.T) {
	var physTime int64 = 2000
	clock := NewClockWithPhysical(1, func() int64 { return physTime })

	remote := NewHLC(1000, 2, 5)
	ts, err := clock.Update(remote)
	require.NoError(t, err)

	assert.Equal(t, int64(2000), ts.WallMicros())
	assert.Equal(t, uint16(0), ts.Logical())
}

// ---------------------------------------------------------------------------
// Scenario: Clock-Skew-Erkennung verhindert Zeitsprünge
// ---------------------------------------------------------------------------

func TestClock_Update_RejectsExcessiveSkew(t *testing.T) {
	var physTime int64 = 1000
	clock := NewClockWithPhysical(1, func() int64 { return physTime })
	// Default max skew is 1s (1_000_000 µs).

	// Remote is 2 seconds ahead — exceeds the 1s default.
	remote := NewHLC(2_000_001, 2, 0)
	_, err := clock.Update(remote)

	require.ErrorIs(t, err, ErrClockSkew)
	// Local clock state must not have changed.
	assert.True(t, clock.Last().IsZero(), "clock state should be unchanged after skew rejection")
}

func TestClock_Update_AcceptsWithinSkew(t *testing.T) {
	var physTime int64 = 1000
	clock := NewClockWithPhysical(1, func() int64 { return physTime })
	// Default max skew is 1s (1_000_000 µs).

	// Remote is 999_999 µs ahead — within the 1s tolerance.
	remote := NewHLC(999_999, 2, 0)
	ts, err := clock.Update(remote)

	require.NoError(t, err)
	assert.Equal(t, int64(999_999), ts.WallMicros())
}

func TestClock_Update_SkewCheckDisabled(t *testing.T) {
	var physTime int64 = 1000
	clock := NewClockWithPhysical(1, func() int64 { return physTime })
	clock.SetMaxSkew(0) // disable

	// Remote is far ahead but skew check is disabled.
	remote := NewHLC(100_000_000, 2, 0)
	ts, err := clock.Update(remote)

	require.NoError(t, err)
	assert.Equal(t, int64(100_000_000), ts.WallMicros())
}

func TestClock_Update_SkewCheckCustomDuration(t *testing.T) {
	var physTime int64 = 1000
	clock := NewClockWithPhysical(1, func() int64 { return physTime })
	clock.SetMaxSkew(500 * time.Millisecond) // 500ms = 500_000 µs

	// Remote is 600ms ahead — exceeds 500ms tolerance.
	remote := NewHLC(601_000, 2, 0)
	_, err := clock.Update(remote)
	require.ErrorIs(t, err, ErrClockSkew)

	// Remote is 400ms ahead — within 500ms tolerance.
	remote = NewHLC(401_000, 2, 0)
	ts, err := clock.Update(remote)
	require.NoError(t, err)
	assert.Equal(t, int64(401_000), ts.WallMicros())
}

func TestClock_MaxSkew_Getter(t *testing.T) {
	clock := NewClock(0)
	assert.Equal(t, time.Second, clock.MaxSkew())

	clock.SetMaxSkew(500 * time.Millisecond)
	assert.Equal(t, 500*time.Millisecond, clock.MaxSkew())

	clock.SetMaxSkew(0)
	assert.Equal(t, time.Duration(0), clock.MaxSkew())
}

// ---------------------------------------------------------------------------
// Scenario: Monotonie ist garantiert
// ---------------------------------------------------------------------------

func TestClock_Monotonicity_10000(t *testing.T) {
	var physTime int64 = 1000
	clock := NewClockWithPhysical(1, func() int64 { return atomic.LoadInt64(&physTime) })

	var prev HLC
	for i := 0; i < 10000; i++ {
		ts := clock.Now()
		if i > 0 {
			require.True(t, prev.Before(ts), "timestamp %d not monotonic: %v >= %v", i, prev, ts)
		}
		prev = ts
		// Occasionally advance physical time to exercise both branches.
		if i%100 == 0 {
			atomic.AddInt64(&physTime, 1)
		}
	}
}

// ---------------------------------------------------------------------------
// Scenario Outline: Bit-Layout Korrektheit
// ---------------------------------------------------------------------------

func TestHLC_BitLayout_Roundtrip(t *testing.T) {
	tests := []struct {
		wall    int64
		node    uint8
		logical uint16
	}{
		{0, 0, 0},
		{MaxWallMicros, MaxNodeID, MaxLogical},
		{140613200000000, 7, 2048},
		{1, 1, 1},
	}

	for _, tt := range tests {
		h := NewHLC(tt.wall, tt.node, tt.logical)
		assert.Equal(t, tt.wall, h.WallMicros(), "wall mismatch for %+v", tt)
		assert.Equal(t, tt.node, h.NodeID(), "node mismatch for %+v", tt)
		assert.Equal(t, tt.logical, h.Logical(), "logical mismatch for %+v", tt)
	}
}

// ---------------------------------------------------------------------------
// Scenario: Property-Based Test für Ordnungsrelation
// ---------------------------------------------------------------------------

func TestHLC_Property_OrderConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		wall1 := rapid.Int64Range(0, MaxWallMicros).Draw(t, "wall1")
		node1 := rapid.Uint8Range(0, MaxNodeID).Draw(t, "node1")
		log1 := rapid.Uint16Range(0, MaxLogical).Draw(t, "log1")

		wall2 := rapid.Int64Range(0, MaxWallMicros).Draw(t, "wall2")
		node2 := rapid.Uint8Range(0, MaxNodeID).Draw(t, "node2")
		log2 := rapid.Uint16Range(0, MaxLogical).Draw(t, "log2")

		a := NewHLC(wall1, node1, log1)
		b := NewHLC(wall2, node2, log2)

		// uint64 ordering must match tuple ordering.
		aVal := uint64(a)
		bVal := uint64(b)

		if aVal < bVal {
			if !a.Before(b) {
				t.Fatalf("Before inconsistency: a=%v b=%v", a, b)
			}
		} else if aVal > bVal {
			if !b.Before(a) {
				t.Fatalf("Before inconsistency: a=%v b=%v", a, b)
			}
		} else {
			if a.Before(b) || b.Before(a) {
				t.Fatalf("equal values should not be Before each other: a=%v b=%v", a, b)
			}
		}
	})
}

func TestHLC_Property_PackUnpackRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		wall := rapid.Int64Range(0, MaxWallMicros).Draw(t, "wall")
		node := rapid.Uint8Range(0, MaxNodeID).Draw(t, "node")
		logical := rapid.Uint16Range(0, MaxLogical).Draw(t, "logical")

		h := NewHLC(wall, node, logical)

		if h.WallMicros() != wall {
			t.Fatalf("wall roundtrip failed: got %d, want %d", h.WallMicros(), wall)
		}
		if h.NodeID() != node {
			t.Fatalf("node roundtrip failed: got %d, want %d", h.NodeID(), node)
		}
		if h.Logical() != logical {
			t.Fatalf("logical roundtrip failed: got %d, want %d", h.Logical(), logical)
		}
	})
}

func TestHLC_Property_SortOrderMatchesTuple(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(2, 200).Draw(t, "n")
		hlcs := make([]HLC, n)
		for i := range hlcs {
			wall := rapid.Int64Range(0, MaxWallMicros).Draw(t, "wall")
			node := rapid.Uint8Range(0, MaxNodeID).Draw(t, "node")
			logical := rapid.Uint16Range(0, MaxLogical).Draw(t, "logical")
			hlcs[i] = NewHLC(wall, node, logical)
		}

		// Sort by uint64 value.
		sort.Slice(hlcs, func(i, j int) bool {
			return uint64(hlcs[i]) < uint64(hlcs[j])
		})

		// Verify sorted order matches tuple ordering.
		for i := 1; i < len(hlcs); i++ {
			prev, cur := hlcs[i-1], hlcs[i]
			pW, cW := prev.WallMicros(), cur.WallMicros()
			pN, cN := prev.NodeID(), cur.NodeID()
			pL, cL := prev.Logical(), cur.Logical()

			if pW > cW {
				t.Fatalf("wall ordering violated at index %d", i)
			}
			if pW == cW && pN > cN {
				t.Fatalf("node ordering violated at index %d", i)
			}
			if pW == cW && pN == cN && pL > cL {
				t.Fatalf("logical ordering violated at index %d", i)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Additional edge-case tests
// ---------------------------------------------------------------------------

func TestHLC_IsZero(t *testing.T) {
	assert.True(t, HLC(0).IsZero())
	assert.False(t, NewHLC(1, 0, 0).IsZero())
}

func TestHLC_String(t *testing.T) {
	h := NewHLC(1000, 3, 42)
	assert.Equal(t, "HLC{wall=1000, node=3, logical=42}", h.String())
}

func TestClock_Now_AdvancingPhysical(t *testing.T) {
	var physTime int64 = 100
	clock := NewClockWithPhysical(5, func() int64 {
		return atomic.AddInt64(&physTime, 1)
	})

	ts1 := clock.Now()
	ts2 := clock.Now()
	ts3 := clock.Now()

	require.True(t, ts1.Before(ts2))
	require.True(t, ts2.Before(ts3))
	// With advancing clock, logical should reset.
	assert.Equal(t, uint16(0), ts1.Logical())
	assert.Equal(t, uint16(0), ts2.Logical())
	assert.Equal(t, uint16(0), ts3.Logical())
}

func TestClock_Now_StalePhysical(t *testing.T) {
	// Physical clock stuck — logical counter should increment.
	clock := NewClockWithPhysical(1, func() int64 { return 1000 })

	ts1 := clock.Now()
	ts2 := clock.Now()
	ts3 := clock.Now()

	assert.Equal(t, int64(1000), ts1.WallMicros())
	assert.Equal(t, uint16(0), ts1.Logical())
	assert.Equal(t, uint16(1), ts2.Logical())
	assert.Equal(t, uint16(2), ts3.Logical())
	require.True(t, ts1.Before(ts2))
	require.True(t, ts2.Before(ts3))
}

func TestClock_ConcurrentNow(t *testing.T) {
	clock := NewClock(1)

	const goroutines = 10
	const iterations = 1000
	results := make(chan HLC, goroutines*iterations)

	for g := 0; g < goroutines; g++ {
		go func() {
			for i := 0; i < iterations; i++ {
				results <- clock.Now()
			}
		}()
	}

	// All timestamps must be unique (the mutex serializes access).
	seen := make(map[HLC]bool, goroutines*iterations)
	for i := 0; i < goroutines*iterations; i++ {
		ts := <-results
		require.False(t, seen[ts], "duplicate timestamp: %v", ts)
		seen[ts] = true
	}
	assert.Len(t, seen, goroutines*iterations)
}

func TestNewClock_NodeIDMasked(t *testing.T) {
	// NodeID > 15 should be masked to lower 4 bits.
	clock := NewClock(0xFF) // 0xFF & 0xF = 15
	ts := clock.Now()
	assert.Equal(t, uint8(15), ts.NodeID())
}

func TestClock_Last(t *testing.T) {
	clock := NewClockWithPhysical(1, func() int64 { return 1000 })

	assert.True(t, clock.Last().IsZero())

	ts := clock.Now()
	assert.Equal(t, ts, clock.Last())
}
