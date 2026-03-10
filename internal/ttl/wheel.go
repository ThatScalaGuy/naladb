package ttl

import (
	"sync"
	"time"
)

// Wheel is a timing wheel for scheduling key expiry with millisecond granularity.
// Keys are placed into slots based on their TTL; each tick advances the wheel
// and returns keys in the current slot.
type Wheel struct {
	slots      []map[string]struct{}
	current    int
	size       int
	resolution time.Duration

	mu       sync.Mutex
	keySlots map[string]int // maps key to its slot index for O(1) cancel
}

// NewWheel creates a timing wheel with the given number of slots and time
// resolution per slot. A wheel with size=1024 and resolution=100ms can
// schedule TTLs up to ~102 seconds.
func NewWheel(size int, resolution time.Duration) *Wheel {
	if size <= 0 {
		size = 1024
	}
	if resolution <= 0 {
		resolution = time.Millisecond
	}

	slots := make([]map[string]struct{}, size)
	for i := range slots {
		slots[i] = make(map[string]struct{})
	}

	return &Wheel{
		slots:      slots,
		size:       size,
		resolution: resolution,
		keySlots:   make(map[string]int),
	}
}

// Schedule adds a key to expire after the given TTL duration.
// If the key is already scheduled, it is moved to the new slot.
func (w *Wheel) Schedule(key string, ttl time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Cancel existing schedule if any.
	if oldSlot, ok := w.keySlots[key]; ok {
		delete(w.slots[oldSlot], key)
	}

	ticks := int(ttl / w.resolution)
	if ticks <= 0 {
		ticks = 1
	}
	if ticks >= w.size {
		ticks = w.size - 1
	}

	slot := (w.current + ticks) % w.size
	w.slots[slot][key] = struct{}{}
	w.keySlots[key] = slot
}

// Cancel removes a key from the timing wheel.
func (w *Wheel) Cancel(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if slot, ok := w.keySlots[key]; ok {
		delete(w.slots[slot], key)
		delete(w.keySlots, key)
	}
}

// Tick advances the wheel by one slot and returns all expired keys.
// The expired keys are removed from the wheel.
func (w *Wheel) Tick() []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.current = (w.current + 1) % w.size
	expired := w.slots[w.current]

	if len(expired) == 0 {
		return nil
	}

	keys := make([]string, 0, len(expired))
	for k := range expired {
		keys = append(keys, k)
		delete(w.keySlots, k)
	}

	w.slots[w.current] = make(map[string]struct{})
	return keys
}

// Len returns the total number of scheduled keys.
func (w *Wheel) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.keySlots)
}
