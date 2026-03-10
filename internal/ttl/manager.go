package ttl

import (
	"sync"
	"time"

	"github.com/thatscalaguy/naladb/internal/segment"
)

// RetentionPolicy defines retention and downsampling rules for keys
// matching a prefix.
type RetentionPolicy struct {
	Prefix             string
	TTL                time.Duration
	DownsampleAfter    time.Duration
	DownsampleStrategy DownsampleStrategy
	DownsampleInterval time.Duration
}

// DeleteFunc writes a tombstone for an expired key.
type DeleteFunc func(key string) error

// SegmentRemoveFunc removes segments by their IDs.
type SegmentRemoveFunc func(ids []uint64) error

// Config controls TTL manager behavior.
type Config struct {
	WheelSize       int
	WheelResolution time.Duration
	TickInterval    time.Duration
	ScanInterval    time.Duration
	Policies        []RetentionPolicy
}

// DefaultConfig returns reasonable default TTL configuration.
func DefaultConfig() Config {
	return Config{
		WheelSize:       65536,
		WheelResolution: 100 * time.Millisecond,
		TickInterval:    100 * time.Millisecond,
		ScanInterval:    time.Minute,
	}
}

// Manager orchestrates TTL expiry via timing wheel and segment scanning.
// It is designed to run only on the RAFT leader.
type Manager struct {
	wheel   *Wheel
	scanner *Scanner
	config  Config

	deleteFn   DeleteFunc
	removeFn   SegmentRemoveFunc
	segmentsFn func() []*segment.Segment
	isLeader   func() bool

	mu     sync.Mutex
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewManager creates a new TTL manager.
func NewManager(
	cfg Config,
	deleteFn DeleteFunc,
	removeFn SegmentRemoveFunc,
	segmentsFn func() []*segment.Segment,
	isLeader func() bool,
) *Manager {
	return &Manager{
		wheel:      NewWheel(cfg.WheelSize, cfg.WheelResolution),
		scanner:    NewScanner(),
		config:     cfg,
		deleteFn:   deleteFn,
		removeFn:   removeFn,
		segmentsFn: segmentsFn,
		isLeader:   isLeader,
	}
}

// Start begins the TTL management loop in a background goroutine.
func (m *Manager) Start() {
	m.mu.Lock()
	if m.stopCh != nil {
		m.mu.Unlock()
		return
	}
	m.stopCh = make(chan struct{})
	m.doneCh = make(chan struct{})
	m.mu.Unlock()

	go m.run()
}

// Stop halts the TTL management loop and waits for it to finish.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopCh == nil {
		m.mu.Unlock()
		return
	}
	close(m.stopCh)
	m.mu.Unlock()

	<-m.doneCh

	m.mu.Lock()
	m.stopCh = nil
	m.doneCh = nil
	m.mu.Unlock()
}

// Schedule adds a key to the timing wheel for expiry after the given TTL.
func (m *Manager) Schedule(key string, ttl time.Duration) {
	m.wheel.Schedule(key, ttl)
}

// Cancel removes a key from the timing wheel.
func (m *Manager) Cancel(key string) {
	m.wheel.Cancel(key)
}

// Wheel returns the underlying timing wheel (for testing).
func (m *Manager) Wheel() *Wheel {
	return m.wheel
}

func (m *Manager) run() {
	defer close(m.doneCh)

	tickInterval := m.config.TickInterval
	if tickInterval <= 0 {
		tickInterval = 100 * time.Millisecond
	}

	scanInterval := m.config.ScanInterval
	if scanInterval <= 0 {
		scanInterval = time.Minute
	}

	tickTicker := time.NewTicker(tickInterval)
	defer tickTicker.Stop()

	scanTicker := time.NewTicker(scanInterval)
	defer scanTicker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-tickTicker.C:
			if m.isLeader == nil || m.isLeader() {
				m.processTick()
			}
		case <-scanTicker.C:
			if m.isLeader == nil || m.isLeader() {
				m.processScan()
			}
		}
	}
}

func (m *Manager) processTick() {
	expired := m.wheel.Tick()
	for _, key := range expired {
		if m.deleteFn != nil {
			_ = m.deleteFn(key)
		}
	}
}

func (m *Manager) processScan() {
	if m.segmentsFn == nil || m.removeFn == nil {
		return
	}

	segments := m.segmentsFn()
	result := m.scanner.Scan(segments, m.config.Policies)

	if len(result.ExpiredSegmentIDs) > 0 {
		_ = m.removeFn(result.ExpiredSegmentIDs)
	}
}
