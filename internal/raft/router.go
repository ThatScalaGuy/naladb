package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Sentinel errors for routing.
var (
	ErrNoLeader      = errors.New("naladb: no leader available")
	ErrForwardFailed = errors.New("naladb: leader forwarding failed")
)

// Routing describes where a request should be handled.
type Routing int

const (
	// HandleLocally means this node should process the request.
	HandleLocally Routing = iota
	// ForwardToLeader means the request should be forwarded to the leader.
	ForwardToLeader
)

// Router handles leader detection and request forwarding for a RAFT cluster node.
type Router struct {
	cluster *Cluster

	mu        sync.RWMutex
	peerAddrs map[string]string // RAFT node ID -> gRPC address

	connMu   sync.Mutex
	connPool map[string]*grpc.ClientConn
}

// NewRouter creates a Router for the given cluster.
// peerAddrs maps RAFT node IDs to their gRPC endpoint addresses.
func NewRouter(c *Cluster, peerAddrs map[string]string) *Router {
	return &Router{
		cluster:   c,
		peerAddrs: peerAddrs,
		connPool:  make(map[string]*grpc.ClientConn),
	}
}

// RouteWrite determines routing for a write request.
// Writes must go to the leader; returns ForwardToLeader with the leader's
// gRPC address if this node is not the leader.
func (r *Router) RouteWrite() (Routing, string) {
	if r.cluster.IsLeader() {
		return HandleLocally, ""
	}
	leaderAddr := r.LeaderEndpoint()
	return ForwardToLeader, leaderAddr
}

// RouteRead determines routing for a read request based on consistency level.
func (r *Router) RouteRead(consistency ConsistencyLevel, maxStale time.Duration) (Routing, string) {
	switch consistency {
	case Eventual:
		return HandleLocally, ""
	case Linearizable:
		if r.cluster.IsLeader() {
			return HandleLocally, ""
		}
		return ForwardToLeader, r.LeaderEndpoint()
	case BoundedStale:
		if r.cluster.IsLeader() {
			return HandleLocally, ""
		}
		if !r.cluster.IsStaleBeyond(maxStale) {
			return HandleLocally, ""
		}
		return ForwardToLeader, r.LeaderEndpoint()
	default:
		return HandleLocally, ""
	}
}

// LeaderEndpoint returns the gRPC address of the current RAFT leader,
// or an empty string if the leader is unknown.
func (r *Router) LeaderEndpoint() string {
	_, leaderID := r.cluster.raft.LeaderWithID()
	if leaderID == "" {
		return ""
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peerAddrs[string(leaderID)]
}

// SetLeaderMetadata adds the current leader endpoint to gRPC response metadata.
func (r *Router) SetLeaderMetadata(ctx context.Context) {
	leaderAddr := r.LeaderEndpoint()
	if leaderAddr != "" {
		header := metadata.Pairs("x-naladb-leader", leaderAddr)
		_ = grpc.SetHeader(ctx, header)
	}
}

// GetConn returns a cached or new gRPC client connection to the given address.
func (r *Router) GetConn(addr string) (*grpc.ClientConn, error) {
	r.connMu.Lock()
	defer r.connMu.Unlock()

	if conn, ok := r.connPool[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("raft: dial leader %s: %w", addr, err)
	}
	r.connPool[addr] = conn
	return conn, nil
}

// UpdatePeers replaces the peer address map.
func (r *Router) UpdatePeers(peers map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peerAddrs = peers
}

// Cluster returns the underlying cluster for direct access.
func (r *Router) Cluster() *Cluster {
	return r.cluster
}

// Close closes all cached gRPC connections.
func (r *Router) Close() error {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	for addr, conn := range r.connPool {
		conn.Close()
		delete(r.connPool, addr)
	}
	return nil
}
