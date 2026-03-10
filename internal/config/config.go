// Package config provides YAML configuration file loading for NalaDB.
//
// Configuration precedence (highest to lowest):
//  1. CLI flags (explicitly set)
//  2. Config file values
//  3. Built-in defaults
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level NalaDB configuration.
type Config struct {
	Cluster ClusterConfig `yaml:"cluster"`
	Raft    RaftConfig    `yaml:"raft"`
	HLC     HLCConfig     `yaml:"hlc"`
	Storage StorageConfig `yaml:"storage"`
	Metrics MetricsConfig `yaml:"metrics"`
}

// ClusterConfig holds general cluster / networking settings.
type ClusterConfig struct {
	ListenAddr string `yaml:"listen_addr"`
}

// RaftConfig holds RAFT consensus settings.
type RaftConfig struct {
	Enabled       bool         `yaml:"enabled"`
	NodeID        string       `yaml:"node_id"`
	DataDir       string       `yaml:"data_dir"`
	BindAddr      string       `yaml:"bind_addr"`
	AdvertiseAddr string       `yaml:"advertise_addr"`
	Bootstrap     bool         `yaml:"bootstrap"`
	Peers         []PeerConfig `yaml:"peers"`
	GRPCPeers     []PeerConfig `yaml:"grpc_peers"`
}

// PeerConfig identifies a single cluster peer.
type PeerConfig struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

// HLCConfig holds Hybrid Logical Clock settings.
type HLCConfig struct {
	NodeID       uint   `yaml:"node_id"`
	MaxClockSkew string `yaml:"max_clock_skew"`
}

// StorageConfig holds WAL and segment directory settings.
type StorageConfig struct {
	WALDir     string `yaml:"wal_dir"`
	SegmentDir string `yaml:"segment_dir"`
}

// MetricsConfig holds Prometheus metrics endpoint settings.
type MetricsConfig struct {
	Addr string `yaml:"addr"`
}

// Default returns a Config with the same defaults as the CLI flags.
func Default() Config {
	return Config{
		Cluster: ClusterConfig{
			ListenAddr: ":7301",
		},
		Raft: RaftConfig{
			BindAddr: ":7400",
			DataDir:  "data/raft",
		},
		HLC: HLCConfig{
			NodeID:       0,
			MaxClockSkew: "1s",
		},
		Storage: StorageConfig{
			WALDir:     "data/wal",
			SegmentDir: "data/segments",
		},
		Metrics: MetricsConfig{
			Addr: ":9090",
		},
	}
}

// Load reads a YAML config file and returns the parsed Config.
// Fields not present in the file retain their zero values; use
// [Merge] to layer the result on top of [Default].
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	cfg := Default()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// ParseMaxClockSkew parses the MaxClockSkew string as a time.Duration.
func (c HLCConfig) ParseMaxClockSkew() (time.Duration, error) {
	if c.MaxClockSkew == "" {
		return time.Second, nil
	}
	return time.ParseDuration(c.MaxClockSkew)
}

// PeersFlag converts the structured peer list to the CLI flag format
// "id=host:port,id=host:port".
func (c RaftConfig) PeersFlag() string {
	return peersToFlag(c.Peers)
}

// GRPCPeersFlag converts the structured gRPC peer list to CLI flag format.
func (c RaftConfig) GRPCPeersFlag() string {
	return peersToFlag(c.GRPCPeers)
}

func peersToFlag(peers []PeerConfig) string {
	parts := make([]string, 0, len(peers))
	for _, p := range peers {
		parts = append(parts, p.ID+"="+p.Address)
	}
	return strings.Join(parts, ",")
}
