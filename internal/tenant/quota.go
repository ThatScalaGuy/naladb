package tenant

import "sync/atomic"

// QuotaTracker tracks resource usage for a tenant and enforces limits.
type QuotaTracker struct {
	nodeCount atomic.Int64
	edgeCount atomic.Int64
	maxNodes  int64
	maxEdges  int64
}

// NewQuotaTracker creates a quota tracker from a tenant config.
// A nil config results in unlimited quotas.
func NewQuotaTracker(cfg *Config) *QuotaTracker {
	qt := &QuotaTracker{}
	if cfg != nil {
		qt.maxNodes = cfg.MaxNodes
		qt.maxEdges = cfg.MaxEdges
	}
	return qt
}

// CheckNode checks if creating a new node would exceed the quota.
func (qt *QuotaTracker) CheckNode() error {
	if qt.maxNodes <= 0 {
		return nil
	}
	if qt.nodeCount.Load() >= qt.maxNodes {
		return ErrTenantQuotaExceeded
	}
	return nil
}

// IncrementNodes increments the node count by one.
func (qt *QuotaTracker) IncrementNodes() {
	qt.nodeCount.Add(1)
}

// DecrementNodes decrements the node count by one.
func (qt *QuotaTracker) DecrementNodes() {
	qt.nodeCount.Add(-1)
}

// CheckEdge checks if creating a new edge would exceed the quota.
func (qt *QuotaTracker) CheckEdge() error {
	if qt.maxEdges <= 0 {
		return nil
	}
	if qt.edgeCount.Load() >= qt.maxEdges {
		return ErrTenantQuotaExceeded
	}
	return nil
}

// IncrementEdges increments the edge count by one.
func (qt *QuotaTracker) IncrementEdges() {
	qt.edgeCount.Add(1)
}

// DecrementEdges decrements the edge count by one.
func (qt *QuotaTracker) DecrementEdges() {
	qt.edgeCount.Add(-1)
}

// NodeCount returns the current node count.
func (qt *QuotaTracker) NodeCount() int64 {
	return qt.nodeCount.Load()
}

// EdgeCount returns the current edge count.
func (qt *QuotaTracker) EdgeCount() int64 {
	return qt.edgeCount.Load()
}
