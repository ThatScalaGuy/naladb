package ttl

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/segment"
)

func TestManager_StartStop(t *testing.T) {
	cfg := Config{
		WheelSize:       1024,
		WheelResolution: time.Millisecond,
		TickInterval:    time.Millisecond,
		ScanInterval:    time.Hour, // don't scan during this test
	}

	mgr := NewManager(cfg, nil, nil, nil, nil)
	mgr.Start()
	// Double start should be safe.
	mgr.Start()

	mgr.Stop()
	// Double stop should be safe.
	mgr.Stop()
}

func TestManager_ScheduleAndExpire(t *testing.T) {
	var mu sync.Mutex
	var deleted []string

	deleteFn := func(key string) error {
		mu.Lock()
		deleted = append(deleted, key)
		mu.Unlock()
		return nil
	}

	cfg := Config{
		WheelSize:       1024,
		WheelResolution: time.Millisecond,
		TickInterval:    time.Millisecond,
		ScanInterval:    time.Hour,
	}

	mgr := NewManager(cfg, deleteFn, nil, nil, nil)

	// Schedule 5 keys with short TTL.
	for i := range 5 {
		mgr.Schedule(keyName(i), 5*time.Millisecond)
	}

	mgr.Start()
	defer mgr.Stop()

	// Wait for keys to expire.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deleted) >= 5
	}, time.Second, time.Millisecond)

	mu.Lock()
	assert.Len(t, deleted, 5)
	mu.Unlock()
}

func TestManager_LeaderOnly(t *testing.T) {
	var deleteCount atomic.Int64

	deleteFn := func(key string) error {
		deleteCount.Add(1)
		return nil
	}

	var isLeader atomic.Bool
	isLeader.Store(false)

	cfg := Config{
		WheelSize:       1024,
		WheelResolution: time.Millisecond,
		TickInterval:    time.Millisecond,
		ScanInterval:    time.Hour,
	}

	mgr := NewManager(cfg, deleteFn, nil, nil, func() bool {
		return isLeader.Load()
	})

	mgr.Schedule("key1", 2*time.Millisecond)
	mgr.Start()
	defer mgr.Stop()

	// Wait enough time for the key to have expired if ticks were processed.
	time.Sleep(20 * time.Millisecond)

	// Should NOT have deleted because not leader.
	assert.Equal(t, int64(0), deleteCount.Load(), "non-leader should not delete")

	// Become leader.
	isLeader.Store(true)

	// Schedule another key with short TTL.
	mgr.Schedule("key2", 2*time.Millisecond)

	require.Eventually(t, func() bool {
		return deleteCount.Load() >= 1
	}, time.Second, time.Millisecond)
}

func TestManager_Cancel(t *testing.T) {
	var deleteCount atomic.Int64

	deleteFn := func(key string) error {
		deleteCount.Add(1)
		return nil
	}

	cfg := Config{
		WheelSize:       1024,
		WheelResolution: time.Millisecond,
		TickInterval:    time.Millisecond,
		ScanInterval:    time.Hour,
	}

	mgr := NewManager(cfg, deleteFn, nil, nil, nil)
	mgr.Schedule("key1", 10*time.Millisecond)
	mgr.Cancel("key1")

	mgr.Start()
	defer mgr.Stop()

	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, int64(0), deleteCount.Load(), "canceled key should not be deleted")
}

func TestManager_ScanExpiredSegments(t *testing.T) {
	var mu sync.Mutex
	var removedIDs []uint64

	removeFn := func(ids []uint64) error {
		mu.Lock()
		removedIDs = append(removedIDs, ids...)
		mu.Unlock()
		return nil
	}

	now := time.Date(2025, 4, 15, 0, 0, 0, 0, time.UTC)
	oldRelativeMicros := now.Add(-100*24*time.Hour).UnixMicro() - hlc.Epoch

	seg := &segment.Segment{
		ID: 42,
		Meta: segment.Metadata{
			MaxTS: uint64(hlc.NewHLC(oldRelativeMicros, 0, 0)),
		},
	}

	segmentsFn := func() []*segment.Segment {
		return []*segment.Segment{seg}
	}

	cfg := Config{
		WheelSize:       1024,
		WheelResolution: time.Millisecond,
		TickInterval:    time.Hour, // don't tick
		ScanInterval:    5 * time.Millisecond,
		Policies: []RetentionPolicy{
			{TTL: 90 * 24 * time.Hour},
		},
	}

	// Override scanner's time source.
	mgr := NewManager(cfg, nil, removeFn, segmentsFn, nil)
	mgr.scanner = NewScanner(WithNowFunc(func() time.Time { return now }))

	mgr.Start()
	defer mgr.Stop()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(removedIDs) > 0
	}, time.Second, time.Millisecond)

	mu.Lock()
	assert.Contains(t, removedIDs, uint64(42))
	mu.Unlock()
}

func keyName(i int) string {
	return "key-" + string(rune('a'+i))
}
