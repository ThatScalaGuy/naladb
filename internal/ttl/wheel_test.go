package ttl

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWheel_ScheduleAndTick(t *testing.T) {
	w := NewWheel(100, time.Millisecond)

	w.Schedule("key1", 5*time.Millisecond)
	assert.Equal(t, 1, w.Len())

	// Tick 4 times → no expiry yet.
	for range 4 {
		expired := w.Tick()
		assert.Empty(t, expired)
	}

	// 5th tick → key1 expires.
	expired := w.Tick()
	require.Len(t, expired, 1)
	assert.Equal(t, "key1", expired[0])
	assert.Equal(t, 0, w.Len())
}

func TestWheel_Cancel(t *testing.T) {
	w := NewWheel(100, time.Millisecond)

	w.Schedule("key1", 5*time.Millisecond)
	assert.Equal(t, 1, w.Len())

	w.Cancel("key1")
	assert.Equal(t, 0, w.Len())

	// Tick past the original TTL → nothing expires.
	for range 10 {
		expired := w.Tick()
		assert.Empty(t, expired)
	}
}

func TestWheel_CancelNonExistent(t *testing.T) {
	w := NewWheel(100, time.Millisecond)
	w.Cancel("nonexistent") // should not panic
}

func TestWheel_MultipleKeys_SameTTL(t *testing.T) {
	w := NewWheel(1024, time.Millisecond)

	// Schedule 1000 keys with TTL=10ms.
	for i := range 1000 {
		w.Schedule(fmt.Sprintf("key-%d", i), 10*time.Millisecond)
	}
	assert.Equal(t, 1000, w.Len())

	// Tick 9 times → no expiry.
	for range 9 {
		expired := w.Tick()
		assert.Empty(t, expired)
	}

	// 10th tick → all 1000 keys expire.
	expired := w.Tick()
	assert.Len(t, expired, 1000)
	assert.Equal(t, 0, w.Len())
}

func TestWheel_Reschedule(t *testing.T) {
	w := NewWheel(100, time.Millisecond)

	w.Schedule("key1", 3*time.Millisecond)
	// Reschedule to later TTL.
	w.Schedule("key1", 7*time.Millisecond)
	assert.Equal(t, 1, w.Len(), "reschedule should not duplicate")

	// Tick 3 times → should NOT expire (was rescheduled).
	for range 3 {
		expired := w.Tick()
		assert.Empty(t, expired)
	}

	// Tick 4 more times → should expire at tick 7.
	for range 3 {
		expired := w.Tick()
		assert.Empty(t, expired)
	}
	expired := w.Tick()
	require.Len(t, expired, 1)
	assert.Equal(t, "key1", expired[0])
}

func TestWheel_DifferentTTLs(t *testing.T) {
	w := NewWheel(100, time.Millisecond)

	w.Schedule("fast", 2*time.Millisecond)
	w.Schedule("slow", 5*time.Millisecond)

	// Tick 2 → fast expires.
	w.Tick()
	expired := w.Tick()
	require.Len(t, expired, 1)
	assert.Equal(t, "fast", expired[0])

	// Tick 3 more → slow expires at tick 5.
	w.Tick()
	w.Tick()
	expired = w.Tick()
	require.Len(t, expired, 1)
	assert.Equal(t, "slow", expired[0])
}

func TestWheel_TTLExceedsSize(t *testing.T) {
	w := NewWheel(10, time.Millisecond)

	// TTL of 20ms exceeds wheel size of 10ms → clamped to size-1=9.
	w.Schedule("key1", 20*time.Millisecond)

	for range 8 {
		expired := w.Tick()
		assert.Empty(t, expired)
	}

	expired := w.Tick()
	require.Len(t, expired, 1)
	assert.Equal(t, "key1", expired[0])
}

func TestWheel_ZeroTTL(t *testing.T) {
	w := NewWheel(10, time.Millisecond)
	w.Schedule("key1", 0) // zero TTL → minimum 1 tick.
	expired := w.Tick()
	require.Len(t, expired, 1)
	assert.Equal(t, "key1", expired[0])
}
