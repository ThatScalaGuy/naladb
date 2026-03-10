package graph

import (
	"fmt"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// bfsOpts configures a BFS traversal.
type bfsOpts struct {
	at             hlc.HLC
	maxDepth       int
	direction      Direction
	relationFilter []string
}

// bfs runs BFS from startNodeID and calls visit for each discovered neighbor.
// The visit callback receives the parent node ID, the neighbor, and the depth.
// If visit returns true, BFS stops early.
func (g *Graph) bfs(startNodeID string, opts bfsOpts, visit func(fromID string, nb neighbor, depth int) bool) error {
	if opts.at.IsZero() {
		return fmt.Errorf("graph: BFS requires a non-zero HLC timestamp")
	}

	relationSet := make(map[string]struct{}, len(opts.relationFilter))
	for _, r := range opts.relationFilter {
		relationSet[r] = struct{}{}
	}

	type bfsEntry struct {
		nodeID string
		depth  int
	}

	visited := map[string]bool{startNodeID: true}
	queue := []bfsEntry{{nodeID: startNodeID, depth: 0}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if opts.maxDepth > 0 && current.depth >= opts.maxDepth {
			continue
		}

		neighbors, err := g.neighborsAt(current.nodeID, opts.at, opts.direction)
		if err != nil {
			return err
		}

		for _, nb := range neighbors {
			if len(relationSet) > 0 {
				if _, ok := relationSet[nb.relation]; !ok {
					continue
				}
			}

			if visited[nb.nodeID] {
				continue
			}
			visited[nb.nodeID] = true

			if visit(current.nodeID, nb, current.depth+1) {
				return nil
			}

			queue = append(queue, bfsEntry{nodeID: nb.nodeID, depth: current.depth + 1})
		}
	}

	return nil
}
