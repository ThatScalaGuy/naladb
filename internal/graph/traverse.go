package graph

import (
	"errors"
	"fmt"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// Direction specifies the traversal direction for graph queries.
type Direction int

const (
	// Outgoing follows edges from source to target.
	Outgoing Direction = iota
	// Incoming follows edges from target to source.
	Incoming
	// Both follows edges in both directions.
	Both
)

// TraversalOption configures a TraverseAt query.
type TraversalOption struct {
	// At is the point-in-time HLC to evaluate edge/node validity.
	At hlc.HLC
	// MaxDepth limits the traversal depth. 0 means unlimited.
	MaxDepth int
	// Direction controls which edges to follow (default: Outgoing).
	Direction Direction
	// RelationFilter, if non-empty, restricts traversal to edges with
	// a matching Relation type.
	RelationFilter []string
	// IncludeNodeProperties requests that node properties be loaded
	// into each TraversalResult entry.
	IncludeNodeProperties bool
}

// TraversalResult represents a single node reached during traversal.
type TraversalResult struct {
	NodeID      string
	Depth       int
	ViaEdge     string
	ViaRelation string
	Properties  map[string][]byte // populated when IncludeNodeProperties is set
}

// TraverseAt performs a BFS traversal starting from the given node,
// filtering edges by temporal validity at the specified HLC timestamp.
// Only edges whose [ValidFrom, ValidTo) interval contains `at` are followed.
func (g *Graph) TraverseAt(startNodeID string, opts TraversalOption) ([]TraversalResult, error) {
	var results []TraversalResult

	err := g.bfs(startNodeID, bfsOpts{
		at:             opts.At,
		maxDepth:       opts.MaxDepth,
		direction:      opts.Direction,
		relationFilter: opts.RelationFilter,
	}, func(fromID string, nb neighbor, depth int) bool {
		result := TraversalResult{
			NodeID:      nb.nodeID,
			Depth:       depth,
			ViaEdge:     nb.edgeID,
			ViaRelation: nb.relation,
		}

		if opts.IncludeNodeProperties {
			result.Properties = g.getNodePropertiesAt(nb.nodeID, opts.At)
		}

		results = append(results, result)
		return false
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// neighbor represents a reachable neighbor through a specific edge.
type neighbor struct {
	nodeID   string
	edgeID   string
	relation string
}

// neighborsAt returns all neighbors reachable from nodeID at the given timestamp,
// considering only edges valid at that time.
func (g *Graph) neighborsAt(nodeID string, at hlc.HLC, dir Direction) ([]neighbor, error) {
	var edgeIDs []string

	if dir == Outgoing || dir == Both {
		out, err := g.getEdgeIDsAt(nodeID, at, true)
		if err != nil {
			return nil, err
		}
		edgeIDs = append(edgeIDs, out...)
	}

	if dir == Incoming || dir == Both {
		in, err := g.getEdgeIDsAt(nodeID, at, false)
		if err != nil {
			return nil, err
		}
		edgeIDs = append(edgeIDs, in...)
	}

	neighbors := make([]neighbor, 0, len(edgeIDs))
	for _, eid := range edgeIDs {
		edge, err := g.GetEdgeAt(eid, at)
		if errors.Is(err, ErrEdgeNotFound) {
			continue // edge not found at this time
		}
		if err != nil {
			return nil, fmt.Errorf("graph: get edge %q: %w", eid, err)
		}
		if edge.Deleted {
			continue
		}
		// Check temporal validity: edge must be valid at timestamp `at`.
		// Valid interval is [ValidFrom, ValidTo).
		if at.Before(edge.ValidFrom) || !at.Before(edge.ValidTo) {
			continue
		}

		// Determine neighbor node based on direction.
		neighborID := edge.To
		if nodeID == edge.To {
			neighborID = edge.From
		}

		neighbors = append(neighbors, neighbor{
			nodeID:   neighborID,
			edgeID:   eid,
			relation: edge.Relation,
		})
	}

	return neighbors, nil
}

// getEdgeIDsAt returns edge IDs from the adjacency list at a point in time.
func (g *Graph) getEdgeIDsAt(nodeID string, at hlc.HLC, outgoing bool) ([]string, error) {
	var key string
	if outgoing {
		key = g.adjOutKey(nodeID)
	} else {
		key = g.adjInKey(nodeID)
	}
	adj, err := g.getAdjacencyListAt(key, at)
	if err != nil {
		return nil, err
	}
	return adj.EdgeIDs, nil
}

// getNodePropertiesAt retrieves all known properties for a node at a point in time.
// It reads the node's current property keys and fetches their values at the given HLC.
func (g *Graph) getNodePropertiesAt(nodeID string, at hlc.HLC) map[string][]byte {
	prefix := g.keyPrefix + "node:" + nodeID + ":prop:"
	props := make(map[string][]byte)

	// Use store's key scanning to find property keys.
	keys := g.store.ScanPrefix(prefix)
	for _, key := range keys {
		propName := key[len(prefix):]
		r := g.store.GetAt(key, at)
		if r.Found && !r.Tombstone {
			props[propName] = r.Value
		}
	}

	return props
}
