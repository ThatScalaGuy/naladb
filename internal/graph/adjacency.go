package graph

import (
	"encoding/json"
	"fmt"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// AdjacencyList holds the edge IDs for a node's incoming or outgoing
// connections, serialized as JSON in the temporal KV store.
type AdjacencyList struct {
	EdgeIDs []string `json:"edge_ids"`
}

// getAdjacencyList reads the current adjacency list for a key.
// Returns an empty list if the key does not exist.
func (g *Graph) getAdjacencyList(key string) (AdjacencyList, error) {
	r := g.store.Get(key)
	if !r.Found {
		return AdjacencyList{}, nil
	}
	var adj AdjacencyList
	if err := json.Unmarshal(r.Value, &adj); err != nil {
		return AdjacencyList{}, fmt.Errorf("graph: unmarshal adjacency list: %w", err)
	}
	return adj, nil
}

// getAdjacencyListAt reads the adjacency list at a specific point in time.
// Returns an empty list if no version exists at that time.
func (g *Graph) getAdjacencyListAt(key string, at hlc.HLC) (AdjacencyList, error) {
	r := g.store.GetAt(key, at)
	if !r.Found {
		return AdjacencyList{}, nil
	}
	var adj AdjacencyList
	if err := json.Unmarshal(r.Value, &adj); err != nil {
		return AdjacencyList{}, fmt.Errorf("graph: unmarshal adjacency list: %w", err)
	}
	return adj, nil
}

// addToAdjacencyList reads the current list, appends the edgeID, and writes back.
// Caller must hold g.adjMu.
func (g *Graph) addToAdjacencyList(key string, edgeID string) error {
	adj, err := g.getAdjacencyList(key)
	if err != nil {
		return err
	}
	adj.EdgeIDs = append(adj.EdgeIDs, edgeID)
	data, err := json.Marshal(adj)
	if err != nil {
		return fmt.Errorf("graph: marshal adjacency list: %w", err)
	}
	if _, err := g.store.Set(key, data); err != nil {
		return err
	}
	return nil
}

// removeFromAdjacencyList reads the current list, removes the edgeID, and writes back.
// Caller must hold g.adjMu.
func (g *Graph) removeFromAdjacencyList(key string, edgeID string) error {
	adj, err := g.getAdjacencyList(key)
	if err != nil {
		return err
	}
	filtered := make([]string, 0, len(adj.EdgeIDs))
	for _, eid := range adj.EdgeIDs {
		if eid != edgeID {
			filtered = append(filtered, eid)
		}
	}
	adj.EdgeIDs = filtered
	data, err := json.Marshal(adj)
	if err != nil {
		return fmt.Errorf("graph: marshal adjacency list: %w", err)
	}
	if _, err := g.store.Set(key, data); err != nil {
		return err
	}
	return nil
}

// GetOutgoingEdges returns edge IDs for a node's outgoing connections.
func (g *Graph) GetOutgoingEdges(nodeID string) ([]string, error) {
	adj, err := g.getAdjacencyList(g.adjOutKey(nodeID))
	if err != nil {
		return nil, err
	}
	return adj.EdgeIDs, nil
}

// GetIncomingEdges returns edge IDs for a node's incoming connections.
func (g *Graph) GetIncomingEdges(nodeID string) ([]string, error) {
	adj, err := g.getAdjacencyList(g.adjInKey(nodeID))
	if err != nil {
		return nil, err
	}
	return adj.EdgeIDs, nil
}

// GetOutgoingEdgesAt returns outgoing edge IDs at a specific point in time.
func (g *Graph) GetOutgoingEdgesAt(nodeID string, at hlc.HLC) ([]string, error) {
	adj, err := g.getAdjacencyListAt(g.adjOutKey(nodeID), at)
	if err != nil {
		return nil, err
	}
	return adj.EdgeIDs, nil
}

// GetIncomingEdgesAt returns incoming edge IDs at a specific point in time.
func (g *Graph) GetIncomingEdgesAt(nodeID string, at hlc.HLC) ([]string, error) {
	adj, err := g.getAdjacencyListAt(g.adjInKey(nodeID), at)
	if err != nil {
		return nil, err
	}
	return adj.EdgeIDs, nil
}
