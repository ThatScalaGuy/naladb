package graph

import (
	"fmt"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// ErrNoPath indicates no path was found between the specified nodes.
var ErrNoPath = fmt.Errorf("naladb: no path found")

// PathResult represents the shortest path between two nodes.
type PathResult struct {
	// NodeIDs contains the ordered sequence of node IDs from source to target.
	NodeIDs []string
	// Length is the number of hops (edges) in the path.
	Length int
}

// PathQueryOption configures a shortest-path query.
type PathQueryOption struct {
	// At is the point-in-time HLC to evaluate edge validity.
	At hlc.HLC
	// MaxDepth limits the search depth. 0 means unlimited.
	MaxDepth int
	// Direction controls which edges to follow (default: Outgoing).
	Direction Direction
	// RelationFilter, if non-empty, restricts the search to edges with
	// a matching Relation type.
	RelationFilter []string
}

// PathQuery finds the shortest path from source to target at a point in time
// using BFS. Returns ErrNoPath if no path exists.
func (g *Graph) PathQuery(from, to string, opts PathQueryOption) (PathResult, error) {
	if from == to {
		return PathResult{NodeIDs: []string{from}, Length: 0}, nil
	}

	parent := map[string]string{from: ""}
	var found bool

	err := g.bfs(from, bfsOpts{
		at:             opts.At,
		maxDepth:       opts.MaxDepth,
		direction:      opts.Direction,
		relationFilter: opts.RelationFilter,
	}, func(fromID string, nb neighbor, depth int) bool {
		parent[nb.nodeID] = fromID
		if nb.nodeID == to {
			found = true
			return true
		}
		return false
	})
	if err != nil {
		return PathResult{}, err
	}

	if !found {
		return PathResult{}, ErrNoPath
	}

	// Reconstruct path.
	path := []string{to}
	cur := to
	for cur != from {
		cur = parent[cur]
		path = append(path, cur)
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return PathResult{
		NodeIDs: path,
		Length:  len(path) - 1,
	}, nil
}
