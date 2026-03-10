package raft

import (
	"fmt"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/thatscalaguy/naladb/internal/store"
)

// ReadIndexTimeout is the default timeout for ReadIndex barrier operations.
const ReadIndexTimeout = 5 * time.Second

// ReadIndex performs a linearizable read using the ReadIndex optimization:
//  1. Verify this node is still the leader via heartbeat quorum.
//  2. Issue a Barrier to ensure all committed entries are applied to the FSM.
//  3. Read from the local (now up-to-date) state machine.
func (c *Cluster) ReadIndex(key string) (store.Result, error) {
	if c.raft.State() != hraft.Leader {
		return store.Result{}, ErrNotLeader
	}

	if err := c.raft.VerifyLeader().Error(); err != nil {
		return store.Result{}, fmt.Errorf("raft: verify leader: %w", err)
	}

	if err := c.raft.Barrier(ReadIndexTimeout).Error(); err != nil {
		return store.Result{}, fmt.Errorf("raft: barrier: %w", err)
	}

	return c.store.Get(key), nil
}

// IsStaleBeyond reports whether this follower node's last leader contact
// is older than maxStale. Returns false if this node is the leader.
func (c *Cluster) IsStaleBeyond(maxStale time.Duration) bool {
	if c.raft.State() == hraft.Leader {
		return false
	}
	lastContact := c.raft.LastContact()
	if lastContact.IsZero() {
		return true
	}
	return time.Since(lastContact) > maxStale
}

// LastContact returns the time of the last leader heartbeat received.
func (c *Cluster) LastContact() time.Time {
	return c.raft.LastContact()
}
