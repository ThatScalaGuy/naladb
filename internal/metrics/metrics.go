// Package metrics provides Prometheus metric definitions for NalaDB.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds all Prometheus metrics for NalaDB.
type Metrics struct {
	WritesTotal        prometheus.Counter
	ReadsTotal         prometheus.Counter
	WriteDuration      prometheus.Histogram
	ReadDuration       prometheus.Histogram
	KeysTotal          prometheus.Gauge
	SegmentsTotal      prometheus.Gauge
	SegmentBytes       prometheus.Gauge
	RaftTerm           prometheus.Gauge
	RaftCommitIndex    prometheus.Gauge
	RaftIsLeader       prometheus.Gauge
	GRPCRequestsTotal  *prometheus.CounterVec
	TTLExpiredTotal    prometheus.Counter
	CompactionDuration prometheus.Histogram
	BlobStoreBytes     prometheus.Gauge
	NodesTotal         prometheus.Gauge
	NodesActive        prometheus.Gauge
	EdgesTotal         prometheus.Gauge
	EdgesActive        prometheus.Gauge
	VersionsTotal      prometheus.Gauge
	TombstonesTotal    prometheus.Gauge
}

// New creates and registers all NalaDB metrics with the given registry.
// If reg is nil, prometheus.DefaultRegisterer is used.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	m := &Metrics{
		WritesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "naladb_writes_total",
			Help: "Total number of write operations.",
		}),
		ReadsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "naladb_reads_total",
			Help: "Total number of read operations.",
		}),
		WriteDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "naladb_write_duration_seconds",
			Help:    "Histogram of write operation durations.",
			Buckets: prometheus.DefBuckets,
		}),
		ReadDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "naladb_read_duration_seconds",
			Help:    "Histogram of read operation durations.",
			Buckets: prometheus.DefBuckets,
		}),
		KeysTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_keys_total",
			Help: "Total number of keys in the in-memory index.",
		}),
		SegmentsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_segments_total",
			Help: "Total number of finalized segments.",
		}),
		SegmentBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_segment_bytes",
			Help: "Total bytes across all finalized segments.",
		}),
		RaftTerm: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_raft_term",
			Help: "Current RAFT term.",
		}),
		RaftCommitIndex: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_raft_commit_index",
			Help: "Current RAFT commit index.",
		}),
		RaftIsLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_raft_is_leader",
			Help: "1 if this node is the RAFT leader, 0 otherwise.",
		}),
		GRPCRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "naladb_grpc_requests_total",
			Help: "Total number of gRPC requests by method.",
		}, []string{"method"}),
		TTLExpiredTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "naladb_ttl_expired_total",
			Help: "Total number of keys expired by TTL.",
		}),
		CompactionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "naladb_compaction_duration_seconds",
			Help:    "Histogram of compaction operation durations.",
			Buckets: prometheus.DefBuckets,
		}),
		BlobStoreBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_blob_store_bytes",
			Help: "Total bytes stored in the blob store.",
		}),
		NodesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_nodes_total",
			Help: "Total number of graph nodes (including deleted).",
		}),
		NodesActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_nodes_active",
			Help: "Number of active (non-deleted) graph nodes.",
		}),
		EdgesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_edges_total",
			Help: "Total number of graph edges (including deleted).",
		}),
		EdgesActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_edges_active",
			Help: "Number of active (non-deleted) graph edges.",
		}),
		VersionsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_versions_total",
			Help: "Total number of version entries across all keys.",
		}),
		TombstonesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "naladb_tombstones_total",
			Help: "Number of tombstoned (deleted) keys.",
		}),
	}

	reg.MustRegister(
		m.WritesTotal,
		m.ReadsTotal,
		m.WriteDuration,
		m.ReadDuration,
		m.KeysTotal,
		m.SegmentsTotal,
		m.SegmentBytes,
		m.RaftTerm,
		m.RaftCommitIndex,
		m.RaftIsLeader,
		m.GRPCRequestsTotal,
		m.TTLExpiredTotal,
		m.CompactionDuration,
		m.BlobStoreBytes,
		m.NodesTotal,
		m.NodesActive,
		m.EdgesTotal,
		m.EdgesActive,
		m.VersionsTotal,
		m.TombstonesTotal,
	)

	return m
}
