package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// Sentinel errors for RAFT operations.
var (
	ErrNotLeader = errors.New("naladb: not the RAFT leader")
)

// ConsistencyLevel controls read consistency.
type ConsistencyLevel int

const (
	// Eventual allows reads from any node (may be stale).
	Eventual ConsistencyLevel = iota
	// Linearizable ensures reads reflect the latest committed state.
	Linearizable
	// BoundedStale allows reads from followers if within MaxStale of the leader.
	BoundedStale
)

// PeerConfig describes a RAFT cluster peer for bootstrap.
type PeerConfig struct {
	ID      string
	Address string
}

// ClusterConfig holds configuration for a RAFT cluster node.
type ClusterConfig struct {
	// NodeID is a unique identifier for this node in the cluster.
	NodeID string
	// DataDir is the directory for RAFT log and snapshot storage.
	DataDir string
	// Bootstrap indicates this node should bootstrap a new cluster.
	Bootstrap bool
	// Peers lists all initial cluster members for bootstrap. When Bootstrap is
	// true and Peers is non-empty, the cluster is bootstrapped with all listed
	// peers. When empty, only this node is included.
	Peers []PeerConfig
	// ApplyTimeout is the timeout for RAFT apply operations.
	ApplyTimeout time.Duration
	// SnapshotRetain is the number of snapshots to retain.
	SnapshotRetain int
	// SnapshotThreshold is the number of log entries before triggering a snapshot.
	SnapshotThreshold uint64
	// GRPCAddr is the gRPC listen address for this node (for leader forwarding).
	GRPCAddr string
	// MaxStaleDefault is the default maximum staleness for BOUNDED_STALE reads.
	MaxStaleDefault time.Duration
}

// DefaultClusterConfig returns a ClusterConfig with sensible defaults.
func DefaultClusterConfig(nodeID, dataDir string) ClusterConfig {
	return ClusterConfig{
		NodeID:            nodeID,
		DataDir:           dataDir,
		Bootstrap:         false,
		ApplyTimeout:      5 * time.Second,
		SnapshotRetain:    2,
		SnapshotThreshold: 8192,
	}
}

// NodeStatus describes a node's current state in the cluster.
type NodeStatus struct {
	ID      string
	Address string
	Role    string // "Leader", "Follower", "Candidate"
}

// ClusterStatus describes the overall cluster state.
type ClusterStatus struct {
	LeaderID   string
	LeaderAddr string
	Nodes      []NodeStatus
}

// Cluster manages a RAFT consensus group for NalaDB.
type Cluster struct {
	raft       *hraft.Raft
	fsm        *FSM
	store      *store.Store
	graphLayer *graph.Graph
	clock      *hlc.Clock
	config     ClusterConfig
	transport  hraft.Transport
}

// NewCluster creates and starts a RAFT cluster node with BoltDB-backed
// log and stable stores, and file-based snapshots.
func NewCluster(
	cfg ClusterConfig,
	s *store.Store,
	g *graph.Graph,
	clock *hlc.Clock,
	transport hraft.Transport,
) (*Cluster, error) {
	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("raft: create log store: %w", err)
	}

	stableStorePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.NewBoltStore(stableStorePath)
	if err != nil {
		return nil, fmt.Errorf("raft: create stable store: %w", err)
	}

	snapshotStore, err := hraft.NewFileSnapshotStore(cfg.DataDir, cfg.SnapshotRetain, nil)
	if err != nil {
		return nil, fmt.Errorf("raft: create snapshot store: %w", err)
	}

	return newClusterFromStores(cfg, s, g, clock, transport, logStore, stableStore, snapshotStore, nil)
}

// bootstrapConfiguration builds the initial RAFT configuration for bootstrap.
// If peers are configured, all peers are included. Otherwise, only this node.
func bootstrapConfiguration(cfg ClusterConfig, transport hraft.Transport) hraft.Configuration {
	if len(cfg.Peers) > 0 {
		servers := make([]hraft.Server, 0, len(cfg.Peers))
		for _, p := range cfg.Peers {
			servers = append(servers, hraft.Server{
				ID:      hraft.ServerID(p.ID),
				Address: hraft.ServerAddress(p.Address),
			})
		}
		return hraft.Configuration{Servers: servers}
	}
	return hraft.Configuration{
		Servers: []hraft.Server{
			{
				ID:      hraft.ServerID(cfg.NodeID),
				Address: transport.LocalAddr(),
			},
		},
	}
}

// NewClusterWithStores creates a RAFT cluster node with caller-provided stores.
// This is useful for testing with in-memory stores.
func NewClusterWithStores(
	cfg ClusterConfig,
	s *store.Store,
	g *graph.Graph,
	clock *hlc.Clock,
	transport hraft.Transport,
	logStore hraft.LogStore,
	stableStore hraft.StableStore,
	snapshotStore hraft.SnapshotStore,
) (*Cluster, error) {
	return newClusterFromStores(cfg, s, g, clock, transport, logStore, stableStore, snapshotStore,
		func(rc *hraft.Config) {
			// Speed up elections for testing.
			rc.HeartbeatTimeout = 150 * time.Millisecond
			rc.ElectionTimeout = 150 * time.Millisecond
			rc.LeaderLeaseTimeout = 100 * time.Millisecond
			rc.CommitTimeout = 50 * time.Millisecond
		},
	)
}

// newClusterFromStores contains the shared initialization logic for creating
// a RAFT cluster node. The optional configureRaft callback customizes the
// RAFT config (e.g., faster timeouts for testing).
func newClusterFromStores(
	cfg ClusterConfig,
	s *store.Store,
	g *graph.Graph,
	clock *hlc.Clock,
	transport hraft.Transport,
	logStore hraft.LogStore,
	stableStore hraft.StableStore,
	snapshotStore hraft.SnapshotStore,
	configureRaft func(*hraft.Config),
) (*Cluster, error) {
	fsm := NewFSM(s)

	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID = hraft.ServerID(cfg.NodeID)
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold

	if configureRaft != nil {
		configureRaft(raftConfig)
	}

	r, err := hraft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("raft: create raft instance: %w", err)
	}

	if cfg.Bootstrap {
		configuration := bootstrapConfiguration(cfg, transport)
		f := r.BootstrapCluster(configuration)
		if err := f.Error(); err != nil && !errors.Is(err, hraft.ErrCantBootstrap) {
			return nil, fmt.Errorf("raft: bootstrap cluster: %w", err)
		}
	}

	return &Cluster{
		raft:       r,
		fsm:        fsm,
		store:      s,
		graphLayer: g,
		clock:      clock,
		config:     cfg,
		transport:  transport,
	}, nil
}

// apply submits a command through RAFT consensus.
func (c *Cluster) apply(cmd RaftCommand) error {
	if c.raft.State() != hraft.Leader {
		return ErrNotLeader
	}

	data, err := MarshalCommand(cmd)
	if err != nil {
		return fmt.Errorf("raft: marshal command: %w", err)
	}

	f := c.raft.Apply(data, c.config.ApplyTimeout)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft: apply: %w", err)
	}

	resp, ok := f.Response().(*ApplyResponse)
	if ok && resp.Error != nil {
		return resp.Error
	}

	return nil
}

// Set writes a key-value pair through RAFT consensus.
func (c *Cluster) Set(key string, value []byte) (hlc.HLC, error) {
	ts := c.clock.Now()
	cmd := RaftCommand{
		Type: CmdSet,
		Ops: []KVOp{
			{Key: key, Value: value, HLC: ts},
		},
	}
	if err := c.apply(cmd); err != nil {
		return 0, err
	}
	return ts, nil
}

// Delete marks a key as deleted through RAFT consensus.
func (c *Cluster) Delete(key string) (hlc.HLC, error) {
	ts := c.clock.Now()
	cmd := RaftCommand{
		Type: CmdDelete,
		Ops: []KVOp{
			{Key: key, HLC: ts, Tombstone: true},
		},
	}
	if err := c.apply(cmd); err != nil {
		return 0, err
	}
	return ts, nil
}

// BatchSet writes multiple key-value pairs atomically through RAFT consensus.
func (c *Cluster) BatchSet(entries map[string][]byte) ([]hlc.HLC, error) {
	ops := make([]KVOp, 0, len(entries))
	hlcs := make([]hlc.HLC, 0, len(entries))

	for key, value := range entries {
		ts := c.clock.Now()
		ops = append(ops, KVOp{Key: key, Value: value, HLC: ts})
		hlcs = append(hlcs, ts)
	}

	cmd := RaftCommand{Type: CmdBatchSet, Ops: ops}
	if err := c.apply(cmd); err != nil {
		return nil, err
	}
	return hlcs, nil
}

// CreateNode creates a graph node through RAFT consensus.
// The node ID and all HLC timestamps are pre-assigned by the leader.
func (c *Cluster) CreateNode(nodeType string, props map[string][]byte) (graph.NodeMeta, error) {
	id, err := graph.GenerateNodeID()
	if err != nil {
		return graph.NodeMeta{}, fmt.Errorf("raft: generate node ID: %w", err)
	}
	return c.CreateNodeWithID(id, nodeType, props)
}

// CreateNodeWithID creates a graph node with a specified ID through RAFT.
func (c *Cluster) CreateNodeWithID(id, nodeType string, props map[string][]byte) (graph.NodeMeta, error) {
	metaTS := c.clock.Now()
	prefix := c.graphLayer.KeyPrefix()

	meta := graph.NodeMeta{
		ID:        id,
		Type:      nodeType,
		ValidFrom: metaTS,
		ValidTo:   hlc.MaxHLC,
	}

	metaData, err := json.Marshal(meta)
	if err != nil {
		return graph.NodeMeta{}, fmt.Errorf("raft: marshal node meta: %w", err)
	}

	ops := []KVOp{
		{Key: prefix + "node:" + id + ":meta", Value: metaData, HLC: metaTS},
	}

	for name, value := range props {
		ts := c.clock.Now()
		ops = append(ops, KVOp{
			Key: prefix + "node:" + id + ":prop:" + name, Value: value, HLC: ts,
		})
	}

	// Initialize empty adjacency lists.
	emptyAdj, _ := json.Marshal(graph.AdjacencyList{EdgeIDs: []string{}})
	outTS := c.clock.Now()
	inTS := c.clock.Now()
	ops = append(ops,
		KVOp{Key: prefix + "graph:adj:" + id + ":out", Value: emptyAdj, HLC: outTS},
		KVOp{Key: prefix + "graph:adj:" + id + ":in", Value: emptyAdj, HLC: inTS},
	)

	cmd := RaftCommand{Type: CmdCreateNode, Ops: ops}
	if err := c.apply(cmd); err != nil {
		return graph.NodeMeta{}, err
	}
	return meta, nil
}

// CreateEdge creates a graph edge through RAFT consensus.
func (c *Cluster) CreateEdge(from, to, relation string, validFrom, validTo hlc.HLC, props map[string][]byte) (graph.EdgeMeta, error) {
	id, err := graph.GenerateEdgeID()
	if err != nil {
		return graph.EdgeMeta{}, fmt.Errorf("raft: generate edge ID: %w", err)
	}

	metaTS := c.clock.Now()
	prefix := c.graphLayer.KeyPrefix()
	meta := graph.EdgeMeta{
		ID:        id,
		From:      from,
		To:        to,
		Relation:  relation,
		ValidFrom: validFrom,
		ValidTo:   validTo,
	}

	metaData, err := json.Marshal(meta)
	if err != nil {
		return graph.EdgeMeta{}, fmt.Errorf("raft: marshal edge meta: %w", err)
	}

	ops := []KVOp{
		{Key: prefix + "edge:" + id + ":meta", Value: metaData, HLC: metaTS},
	}

	for name, value := range props {
		ts := c.clock.Now()
		ops = append(ops, KVOp{
			Key: prefix + "edge:" + id + ":prop:" + name, Value: value, HLC: ts,
		})
	}

	// Update adjacency lists: read current, append, write back.
	// Since adjacency lists are modified, we read-modify-write here.
	outKey := prefix + "graph:adj:" + from + ":out"
	outAdj, err := c.readAdjacencyList(outKey)
	if err != nil {
		return graph.EdgeMeta{}, err
	}
	outAdj.EdgeIDs = append(outAdj.EdgeIDs, id)
	outData, _ := json.Marshal(outAdj)
	outTS := c.clock.Now()
	ops = append(ops, KVOp{Key: outKey, Value: outData, HLC: outTS})

	inKey := prefix + "graph:adj:" + to + ":in"
	inAdj, err := c.readAdjacencyList(inKey)
	if err != nil {
		return graph.EdgeMeta{}, err
	}
	inAdj.EdgeIDs = append(inAdj.EdgeIDs, id)
	inData, _ := json.Marshal(inAdj)
	inTS := c.clock.Now()
	ops = append(ops, KVOp{Key: inKey, Value: inData, HLC: inTS})

	cmd := RaftCommand{Type: CmdCreateEdge, Ops: ops}
	if err := c.apply(cmd); err != nil {
		return graph.EdgeMeta{}, err
	}
	return meta, nil
}

// readAdjacencyList reads the current adjacency list from the local store.
func (c *Cluster) readAdjacencyList(key string) (graph.AdjacencyList, error) {
	r := c.store.Get(key)
	if !r.Found {
		return graph.AdjacencyList{EdgeIDs: []string{}}, nil
	}
	var adj graph.AdjacencyList
	if err := json.Unmarshal(r.Value, &adj); err != nil {
		return graph.AdjacencyList{}, fmt.Errorf("raft: unmarshal adjacency list %q: %w", key, err)
	}
	return adj, nil
}

// Get reads a value from the local store with the specified consistency level.
func (c *Cluster) Get(key string, consistency ConsistencyLevel) (store.Result, error) {
	switch consistency {
	case Linearizable:
		return c.ReadIndex(key)
	case BoundedStale:
		return c.GetWithMaxStale(key, c.maxStale())
	default:
		return c.store.Get(key), nil
	}
}

// GetWithMaxStale performs a bounded-stale read with a custom staleness window.
// If this node is the leader or within the staleness window, reads locally.
// Otherwise returns ErrNotLeader to signal the caller should forward.
func (c *Cluster) GetWithMaxStale(key string, maxStale time.Duration) (store.Result, error) {
	if c.IsLeader() || !c.IsStaleBeyond(maxStale) {
		return c.store.Get(key), nil
	}
	return store.Result{}, ErrNotLeader
}

// maxStale returns the configured maximum staleness or a default of 5 seconds.
func (c *Cluster) maxStale() time.Duration {
	if c.config.MaxStaleDefault > 0 {
		return c.config.MaxStaleDefault
	}
	return 5 * time.Second
}

// Status returns the current cluster status.
func (c *Cluster) Status() ClusterStatus {
	leaderAddr, leaderID := c.raft.LeaderWithID()

	configFuture := c.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return ClusterStatus{
			LeaderID:   string(leaderID),
			LeaderAddr: string(leaderAddr),
		}
	}

	servers := configFuture.Configuration().Servers
	nodes := make([]NodeStatus, 0, len(servers))
	for _, server := range servers {
		role := "Follower"
		if server.ID == leaderID {
			role = "Leader"
		}
		nodes = append(nodes, NodeStatus{
			ID:      string(server.ID),
			Address: string(server.Address),
			Role:    role,
		})
	}

	return ClusterStatus{
		LeaderID:   string(leaderID),
		LeaderAddr: string(leaderAddr),
		Nodes:      nodes,
	}
}

// IsLeader returns true if this node is the current RAFT leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == hraft.Leader
}

// LeaderAddr returns the address of the current RAFT leader.
func (c *Cluster) LeaderAddr() string {
	addr, _ := c.raft.LeaderWithID()
	return string(addr)
}

// AddVoter adds a new voter node to the cluster. Must be called on the leader.
func (c *Cluster) AddVoter(id, address string) error {
	f := c.raft.AddVoter(
		hraft.ServerID(id),
		hraft.ServerAddress(address),
		0,
		c.config.ApplyTimeout,
	)
	return f.Error()
}

// RemoveServer removes a node from the cluster. Must be called on the leader.
func (c *Cluster) RemoveServer(id string) error {
	f := c.raft.RemoveServer(
		hraft.ServerID(id),
		0,
		c.config.ApplyTimeout,
	)
	return f.Error()
}

// Snapshot triggers a manual RAFT snapshot.
func (c *Cluster) Snapshot() error {
	f := c.raft.Snapshot()
	return f.Error()
}

// Shutdown gracefully shuts down the RAFT node.
func (c *Cluster) Shutdown() error {
	f := c.raft.Shutdown()
	return f.Error()
}

// Raft returns the underlying hashicorp/raft instance for advanced operations.
func (c *Cluster) Raft() *hraft.Raft {
	return c.raft
}

// FSM returns the FSM for direct access (e.g., for snapshot testing).
func (c *Cluster) FSM() *FSM {
	return c.fsm
}

// Store returns the underlying store for direct reads.
func (c *Cluster) Store() *store.Store {
	return c.store
}

// Graph returns the underlying graph layer for read operations.
func (c *Cluster) Graph() *graph.Graph {
	return c.graphLayer
}
