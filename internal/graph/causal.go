package graph

import (
	"errors"
	"fmt"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// ErrInvalidCausalQuery indicates that the CausalQuery is missing required fields.
var ErrInvalidCausalQuery = errors.New("naladb: invalid causal query")

// CausalDirection specifies the direction of causal traversal.
type CausalDirection int

const (
	// CausalForward traces the impact of a trigger event (forward in time).
	CausalForward CausalDirection = iota
	// CausalBackward traces the root cause of a trigger event (backward in time).
	CausalBackward
)

// CausalQuery configures a causal traversal starting from a trigger node.
type CausalQuery struct {
	// TriggerNodeID is the starting node of the causal chain.
	TriggerNodeID string
	// At is the HLC timestamp of the trigger event.
	At hlc.HLC
	// MaxDepth limits the number of hops (>= 1).
	MaxDepth int
	// WindowMicros is the per-hop time window in microseconds.
	WindowMicros int64
	// Direction controls forward (impact) or backward (root-cause) analysis.
	Direction CausalDirection
	// MinConfidence filters results below this threshold (0 = no filter).
	MinConfidence float64
	// RelationFilter, if non-empty, restricts traversal to matching edge relations.
	RelationFilter []string
}

// CausalResult represents a single node in a causal chain.
type CausalResult struct {
	// NodeID is the identifier of the causally linked node.
	NodeID string
	// Depth is the number of hops from the trigger node.
	Depth int
	// DeltaMicros is the time difference from the trigger event in microseconds.
	// Positive for forward, negative for backward.
	DeltaMicros int64
	// Confidence is the cumulative causal confidence (0.0 to 1.0).
	Confidence float64
	// Path is the ordered list of node IDs from trigger to this node.
	Path []string
	// ViaEdge is the edge ID through which this node was reached.
	ViaEdge string
	// ViaRelation is the relation type of the edge.
	ViaRelation string
	// ChangeTime is the HLC timestamp of the detected property change.
	ChangeTime hlc.HLC
}

// CausalTraverse performs a BFS-based causal traversal from a trigger node,
// detecting property changes within per-hop time windows and computing
// confidence scores based on temporal distance and edge weights.
func (g *Graph) CausalTraverse(query CausalQuery) ([]CausalResult, error) {
	if query.TriggerNodeID == "" || query.At.IsZero() || query.MaxDepth < 1 || query.WindowMicros <= 0 {
		return nil, fmt.Errorf("graph: %w: trigger=%q at=%v depth=%d window=%d",
			ErrInvalidCausalQuery, query.TriggerNodeID, query.At, query.MaxDepth, query.WindowMicros)
	}

	// Map CausalDirection to graph Direction for neighbor lookup.
	var graphDir Direction
	if query.Direction == CausalForward {
		graphDir = Outgoing
	} else {
		graphDir = Incoming
	}

	relationSet := make(map[string]struct{}, len(query.RelationFilter))
	for _, r := range query.RelationFilter {
		relationSet[r] = struct{}{}
	}

	type bfsEntry struct {
		nodeID     string
		depth      int
		changeTime hlc.HLC
		confidence float64
		path       []string
	}

	// Determine the trigger node's change time: look for the most recent
	// property/reading change within [At - window, At]. If found, use that
	// as the causal origin; otherwise fall back to query.At.
	triggerChangeTime := query.At
	if ct, ok := g.detectPropertyChange(query.TriggerNodeID, query.At, query.WindowMicros, CausalBackward); ok {
		triggerChangeTime = ct
	}

	visited := map[string]bool{query.TriggerNodeID: true}
	queue := []bfsEntry{{
		nodeID:     query.TriggerNodeID,
		depth:      0,
		changeTime: triggerChangeTime,
		confidence: 1.0,
		path:       []string{query.TriggerNodeID},
	}}

	var results []CausalResult

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth >= query.MaxDepth {
			continue
		}

		// Edge validity is always checked at the original trigger time.
		neighbors, err := g.neighborsAt(current.nodeID, query.At, graphDir)
		if err != nil {
			return nil, err
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

			// Detect property change within the per-hop window relative to parent.
			// Try the requested direction first; if not found, also check
			// backward (covers the common case where readings accumulate
			// continuously and the trigger's change time is near "now").
			changeTime, found := g.detectPropertyChange(nb.nodeID, current.changeTime, query.WindowMicros, query.Direction)
			if !found && query.Direction == CausalForward {
				changeTime, found = g.detectPropertyChange(nb.nodeID, current.changeTime, query.WindowMicros, CausalBackward)
			}
			if !found {
				continue
			}

			// Compute delta from parent's change time.
			var deltaMicros int64
			if query.Direction == CausalForward {
				deltaMicros = changeTime.WallMicros() - current.changeTime.WallMicros()
			} else {
				deltaMicros = current.changeTime.WallMicros() - changeTime.WallMicros()
			}

			tf := TimeFactor(deltaMicros, query.WindowMicros)
			ew := g.EdgeWeight(nb.edgeID, query.At)
			conf := CausalConfidence(current.confidence, tf, ew)

			if query.MinConfidence > 0 && conf < query.MinConfidence {
				continue
			}

			visited[nb.nodeID] = true

			path := make([]string, len(current.path)+1)
			copy(path, current.path)
			path[len(current.path)] = nb.nodeID

			// DeltaMicros in the result is relative to the original trigger.
			var resultDelta int64
			if query.Direction == CausalForward {
				resultDelta = changeTime.WallMicros() - query.At.WallMicros()
			} else {
				resultDelta = changeTime.WallMicros() - query.At.WallMicros() // negative
			}

			results = append(results, CausalResult{
				NodeID:      nb.nodeID,
				Depth:       current.depth + 1,
				DeltaMicros: resultDelta,
				Confidence:  conf,
				Path:        path,
				ViaEdge:     nb.edgeID,
				ViaRelation: nb.relation,
				ChangeTime:  changeTime,
			})

			queue = append(queue, bfsEntry{
				nodeID:     nb.nodeID,
				depth:      current.depth + 1,
				changeTime: changeTime,
				confidence: conf,
				path:       path,
			})
		}
	}

	return results, nil
}

// detectPropertyChange checks whether any property of nodeID changed within a
// time window relative to referenceTime. For forward traversal, the window is
// [referenceTime, referenceTime + windowMicros]. For backward, it is
// [referenceTime - windowMicros, referenceTime]. Returns the earliest (forward)
// or latest (backward) change time found.
func (g *Graph) detectPropertyChange(nodeID string, referenceTime hlc.HLC, windowMicros int64, dir CausalDirection) (hlc.HLC, bool) {
	prefix := g.keyPrefix + "node:" + nodeID + ":prop:"
	keys := g.store.ScanPrefix(prefix)

	// Also check the node's readingKey (bridges graph node to KV time-series).
	readingKeyProp := g.store.Get(g.nodePropKey(nodeID, "readingKey"))
	if readingKeyProp.Found && !readingKeyProp.Tombstone {
		keys = append(keys, string(readingKeyProp.Value))
	}

	var bestTime hlc.HLC
	found := false

	for _, key := range keys {
		var opts store.HistoryOptions
		if dir == CausalForward {
			opts.From = referenceTime
			opts.To = hlc.NewHLC(referenceTime.WallMicros()+windowMicros, 0, 0)
		} else {
			fromMicros := referenceTime.WallMicros() - windowMicros
			if fromMicros < 0 {
				fromMicros = 0
			}
			opts.From = hlc.NewHLC(fromMicros, 0, 0)
			opts.To = referenceTime
		}

		entries := g.store.History(key, opts)
		for _, entry := range entries {
			if !found {
				bestTime = entry.HLC
				found = true
			} else if dir == CausalForward && entry.HLC.Before(bestTime) {
				// Forward: take earliest change (closest to parent).
				bestTime = entry.HLC
			} else if dir == CausalBackward && bestTime.Before(entry.HLC) {
				// Backward: take latest change (closest to parent).
				bestTime = entry.HLC
			}
		}
	}

	return bestTime, found
}
