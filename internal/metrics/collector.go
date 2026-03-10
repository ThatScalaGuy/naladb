package metrics

import (
	"os"
	"path/filepath"
	"time"
)

// IndexLenFunc returns the number of keys in the in-memory index.
type IndexLenFunc func() int

// SegmentCountFunc returns the number of finalized segments.
type SegmentCountFunc func() int

// SegmentBytesFunc returns total bytes across all segments.
type SegmentBytesFunc func() int64

// RaftStatsFunc returns RAFT state information.
type RaftStatsFunc func() (term uint64, commitIndex uint64, isLeader bool)

// BlobDirFunc returns the blob store directory path.
type BlobDirFunc func() string

// StoreStatsFunc returns store-level statistics (keys, versions, tombstones).
type StoreStatsFunc func() (keys int, versions int, tombstones int)

// GraphStatsFunc returns graph-level statistics (nodes, activeNodes, edges, activeEdges).
type GraphStatsFunc func() (nodes int, activeNodes int, edges int, activeEdges int)

// CollectorConfig holds the functions used to collect gauge metrics.
type CollectorConfig struct {
	IndexLen     IndexLenFunc
	SegmentCount SegmentCountFunc
	SegmentBytes SegmentBytesFunc
	RaftStats    RaftStatsFunc
	BlobDir      BlobDirFunc
	StoreStats   StoreStatsFunc
	GraphStats   GraphStatsFunc
}

// Collector periodically updates gauge metrics from external sources.
type Collector struct {
	metrics *Metrics
	config  CollectorConfig
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// NewCollector creates a new metric collector.
func NewCollector(m *Metrics, cfg CollectorConfig) *Collector {
	return &Collector{
		metrics: m,
		config:  cfg,
	}
}

// Start begins periodic gauge collection in a background goroutine.
func (c *Collector) Start(interval time.Duration) {
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	go c.run(interval)
}

// Stop halts the collector and waits for it to finish.
func (c *Collector) Stop() {
	if c.stopCh == nil {
		return
	}
	close(c.stopCh)
	<-c.doneCh
}

// Collect performs a single collection pass, updating all gauge metrics.
func (c *Collector) Collect() {
	if c.config.IndexLen != nil {
		c.metrics.KeysTotal.Set(float64(c.config.IndexLen()))
	}

	if c.config.SegmentCount != nil {
		c.metrics.SegmentsTotal.Set(float64(c.config.SegmentCount()))
	}

	if c.config.SegmentBytes != nil {
		c.metrics.SegmentBytes.Set(float64(c.config.SegmentBytes()))
	}

	if c.config.RaftStats != nil {
		term, commitIndex, isLeader := c.config.RaftStats()
		c.metrics.RaftTerm.Set(float64(term))
		c.metrics.RaftCommitIndex.Set(float64(commitIndex))
		if isLeader {
			c.metrics.RaftIsLeader.Set(1)
		} else {
			c.metrics.RaftIsLeader.Set(0)
		}
	}

	if c.config.BlobDir != nil {
		c.metrics.BlobStoreBytes.Set(float64(dirSize(c.config.BlobDir())))
	}

	if c.config.StoreStats != nil {
		keys, versions, tombstones := c.config.StoreStats()
		c.metrics.VersionsTotal.Set(float64(versions))
		c.metrics.TombstonesTotal.Set(float64(tombstones))
		// Also update KeysTotal from store stats if IndexLen is not set.
		if c.config.IndexLen == nil {
			c.metrics.KeysTotal.Set(float64(keys))
		}
	}

	if c.config.GraphStats != nil {
		nodes, activeNodes, edges, activeEdges := c.config.GraphStats()
		c.metrics.NodesTotal.Set(float64(nodes))
		c.metrics.NodesActive.Set(float64(activeNodes))
		c.metrics.EdgesTotal.Set(float64(edges))
		c.metrics.EdgesActive.Set(float64(activeEdges))
	}
}

func (c *Collector) run(interval time.Duration) {
	defer close(c.doneCh)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial collection.
	c.Collect()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.Collect()
		}
	}
}

// dirSize returns the total size in bytes of all files in a directory tree.
func dirSize(path string) int64 {
	if path == "" {
		return 0
	}
	var total int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}
