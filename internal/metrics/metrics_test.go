package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_RegistersAllMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	require.NotNil(t, m)

	// Initialize the CounterVec so it shows up in Gather.
	m.GRPCRequestsTotal.WithLabelValues("/test").Inc()

	families, err := reg.Gather()
	require.NoError(t, err)

	expected := map[string]bool{
		"naladb_writes_total":                false,
		"naladb_reads_total":                 false,
		"naladb_write_duration_seconds":      false,
		"naladb_read_duration_seconds":       false,
		"naladb_keys_total":                  false,
		"naladb_segments_total":              false,
		"naladb_segment_bytes":               false,
		"naladb_raft_term":                   false,
		"naladb_raft_commit_index":           false,
		"naladb_raft_is_leader":              false,
		"naladb_grpc_requests_total":         false,
		"naladb_ttl_expired_total":           false,
		"naladb_compaction_duration_seconds": false,
		"naladb_blob_store_bytes":            false,
	}

	for _, fam := range families {
		if _, ok := expected[fam.GetName()]; ok {
			expected[fam.GetName()] = true
		}
	}

	for name, found := range expected {
		assert.True(t, found, "metric %s not found in registry", name)
	}
}

func TestCollector_Collect(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	c := NewCollector(m, CollectorConfig{
		IndexLen:     func() int { return 42 },
		SegmentCount: func() int { return 3 },
		SegmentBytes: func() int64 { return 1024 },
		RaftStats: func() (uint64, uint64, bool) {
			return 5, 100, true
		},
	})

	c.Collect()

	families, err := reg.Gather()
	require.NoError(t, err)

	vals := make(map[string]float64)
	for _, fam := range families {
		for _, m := range fam.GetMetric() {
			if m.GetGauge() != nil {
				vals[fam.GetName()] = m.GetGauge().GetValue()
			}
		}
	}

	assert.Equal(t, float64(42), vals["naladb_keys_total"])
	assert.Equal(t, float64(3), vals["naladb_segments_total"])
	assert.Equal(t, float64(1024), vals["naladb_segment_bytes"])
	assert.Equal(t, float64(5), vals["naladb_raft_term"])
	assert.Equal(t, float64(100), vals["naladb_raft_commit_index"])
	assert.Equal(t, float64(1), vals["naladb_raft_is_leader"])
}

func TestCollector_StartStop(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	callCount := 0
	c := NewCollector(m, CollectorConfig{
		IndexLen: func() int {
			callCount++
			return callCount
		},
	})

	c.Start(10 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	c.Stop()

	assert.Greater(t, callCount, 1, "collector should have called IndexLen multiple times")
}
