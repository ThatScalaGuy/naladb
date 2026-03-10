package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	assert.Equal(t, ":7301", cfg.Cluster.ListenAddr)
	assert.Equal(t, ":9090", cfg.Metrics.Addr)
	assert.Equal(t, uint(0), cfg.HLC.NodeID)
	assert.Equal(t, "1s", cfg.HLC.MaxClockSkew)
	assert.Equal(t, "data/wal", cfg.Storage.WALDir)
	assert.Equal(t, "data/segments", cfg.Storage.SegmentDir)
	assert.False(t, cfg.Raft.Enabled)
	assert.Equal(t, ":7400", cfg.Raft.BindAddr)
	assert.Equal(t, "data/raft", cfg.Raft.DataDir)
}

func TestLoad(t *testing.T) {
	yaml := `
cluster:
  listen_addr: ":8000"
hlc:
  node_id: 5
  max_clock_skew: "2s"
storage:
  wal_dir: /data/wal
  segment_dir: /data/segments
raft:
  enabled: true
  node_id: "node-1"
  data_dir: /data/raft
  bind_addr: "0.0.0.0:7400"
  advertise_addr: "host1:7400"
  bootstrap: true
  peers:
    - id: "node-0"
      address: "host0:7400"
    - id: "node-1"
      address: "host1:7400"
  grpc_peers:
    - id: "node-0"
      address: "host0:7301"
    - id: "node-1"
      address: "host1:7301"
metrics:
  addr: ":9191"
`
	path := filepath.Join(t.TempDir(), "naladb.yml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)

	assert.Equal(t, ":8000", cfg.Cluster.ListenAddr)
	assert.Equal(t, uint(5), cfg.HLC.NodeID)
	assert.Equal(t, "2s", cfg.HLC.MaxClockSkew)
	assert.Equal(t, "/data/wal", cfg.Storage.WALDir)
	assert.Equal(t, "/data/segments", cfg.Storage.SegmentDir)
	assert.True(t, cfg.Raft.Enabled)
	assert.Equal(t, "node-1", cfg.Raft.NodeID)
	assert.Equal(t, "/data/raft", cfg.Raft.DataDir)
	assert.Equal(t, "0.0.0.0:7400", cfg.Raft.BindAddr)
	assert.Equal(t, "host1:7400", cfg.Raft.AdvertiseAddr)
	assert.True(t, cfg.Raft.Bootstrap)
	assert.Equal(t, ":9191", cfg.Metrics.Addr)

	assert.Len(t, cfg.Raft.Peers, 2)
	assert.Equal(t, "node-0", cfg.Raft.Peers[0].ID)
	assert.Equal(t, "host0:7400", cfg.Raft.Peers[0].Address)
}

func TestLoad_PartialOverride(t *testing.T) {
	yaml := `
cluster:
  listen_addr: ":9999"
`
	path := filepath.Join(t.TempDir(), "partial.yml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)

	// Overridden value.
	assert.Equal(t, ":9999", cfg.Cluster.ListenAddr)

	// Defaults preserved for unset fields.
	assert.Equal(t, ":9090", cfg.Metrics.Addr)
	assert.Equal(t, "data/wal", cfg.Storage.WALDir)
	assert.Equal(t, uint(0), cfg.HLC.NodeID)
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path.yml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}

func TestLoad_InvalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yml")
	require.NoError(t, os.WriteFile(path, []byte(":::invalid"), 0o644))

	_, err := Load(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

func TestParseMaxClockSkew(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"1s", time.Second},
		{"500ms", 500 * time.Millisecond},
		{"0", 0},
		{"", time.Second},
	}
	for _, tt := range tests {
		hlc := HLCConfig{MaxClockSkew: tt.input}
		d, err := hlc.ParseMaxClockSkew()
		require.NoError(t, err)
		assert.Equal(t, tt.expected, d, "input: %q", tt.input)
	}
}

func TestPeersFlag(t *testing.T) {
	cfg := RaftConfig{
		Peers: []PeerConfig{
			{ID: "node-0", Address: "host0:7400"},
			{ID: "node-1", Address: "host1:7400"},
		},
		GRPCPeers: []PeerConfig{
			{ID: "node-0", Address: "host0:7301"},
		},
	}
	assert.Equal(t, "node-0=host0:7400,node-1=host1:7400", cfg.PeersFlag())
	assert.Equal(t, "node-0=host0:7301", cfg.GRPCPeersFlag())
}

func TestPeersFlag_Empty(t *testing.T) {
	cfg := RaftConfig{}
	assert.Equal(t, "", cfg.PeersFlag())
	assert.Equal(t, "", cfg.GRPCPeersFlag())
}
