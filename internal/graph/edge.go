package graph

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/thatscalaguy/naladb/internal/hlc"
)

// EdgeMeta holds the metadata for a graph edge, serialized as JSON
// and stored under the key "edge:{id}:meta" in the temporal KV store.
type EdgeMeta struct {
	ID        string  `json:"id"`
	From      string  `json:"from"`
	To        string  `json:"to"`
	Relation  string  `json:"relation"`
	ValidFrom hlc.HLC `json:"valid_from"`
	ValidTo   hlc.HLC `json:"valid_to"`
	Deleted   bool    `json:"deleted"`
}

// GenerateEdgeID creates a new UUID v7 for use as an edge identifier.
func GenerateEdgeID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("graph: generate edge ID: %w", err)
	}
	return id.String(), nil
}

// CreateEdge creates a new edge between two nodes with temporal validity.
// The edge's validity range [validFrom, validTo) must be within both nodes'
// validity ranges, otherwise ErrEdgeOutsideNodeValidity is returned.
// If validFrom is zero, the current clock time is used.
// If validTo is zero, MaxHLC is used (open-ended).
func (g *Graph) CreateEdge(from, to, relation string, validFrom, validTo hlc.HLC, props map[string][]byte) (EdgeMeta, error) {
	// Default validity: current time → open-ended.
	if validFrom == 0 {
		validFrom = g.clock.Now()
	}
	if validTo == 0 {
		validTo = hlc.MaxHLC
	}

	// Validate source node.
	fromMeta, err := g.GetNode(from)
	if err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: source node: %w", err)
	}
	if fromMeta.Deleted {
		return EdgeMeta{}, fmt.Errorf("graph: source node %q: %w", from, ErrNodeDeleted)
	}

	// Validate target node.
	toMeta, err := g.GetNode(to)
	if err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: target node: %w", err)
	}
	if toMeta.Deleted {
		return EdgeMeta{}, fmt.Errorf("graph: target node %q: %w", to, ErrNodeDeleted)
	}

	// Validate edge validity is within both nodes' validity ranges.
	// edge [validFrom, validTo) must be within node [ValidFrom, ValidTo).
	if validFrom.Before(fromMeta.ValidFrom) || fromMeta.ValidTo.Before(validTo) {
		return EdgeMeta{}, ErrEdgeOutsideNodeValidity
	}
	if validFrom.Before(toMeta.ValidFrom) || toMeta.ValidTo.Before(validTo) {
		return EdgeMeta{}, ErrEdgeOutsideNodeValidity
	}

	id, err := GenerateEdgeID()
	if err != nil {
		return EdgeMeta{}, err
	}

	meta := EdgeMeta{
		ID:        id,
		From:      from,
		To:        to,
		Relation:  relation,
		ValidFrom: validFrom,
		ValidTo:   validTo,
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: marshal edge meta: %w", err)
	}
	if _, err := g.store.Set(g.edgeMetaKey(meta.ID), data); err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: store edge meta: %w", err)
	}

	for name, value := range props {
		if _, err := g.store.Set(g.edgePropKey(meta.ID, name), value); err != nil {
			return EdgeMeta{}, fmt.Errorf("graph: store edge property %q: %w", name, err)
		}
	}

	// Update adjacency lists.
	g.adjMu.Lock()
	defer g.adjMu.Unlock()

	if err := g.addToAdjacencyList(g.adjOutKey(from), meta.ID); err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: update adj out: %w", err)
	}
	if err := g.addToAdjacencyList(g.adjInKey(to), meta.ID); err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: update adj in: %w", err)
	}

	return meta, nil
}

// GetEdge retrieves the current metadata for an edge.
func (g *Graph) GetEdge(id string) (EdgeMeta, error) {
	r := g.store.Get(g.edgeMetaKey(id))
	if !r.Found {
		return EdgeMeta{}, ErrEdgeNotFound
	}
	var meta EdgeMeta
	if err := json.Unmarshal(r.Value, &meta); err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: unmarshal edge meta: %w", err)
	}
	return meta, nil
}

// GetEdgeAt retrieves the metadata for an edge at a specific point in time.
func (g *Graph) GetEdgeAt(id string, at hlc.HLC) (EdgeMeta, error) {
	r := g.store.GetAt(g.edgeMetaKey(id), at)
	if !r.Found {
		return EdgeMeta{}, ErrEdgeNotFound
	}
	var meta EdgeMeta
	if err := json.Unmarshal(r.Value, &meta); err != nil {
		return EdgeMeta{}, fmt.Errorf("graph: unmarshal edge meta: %w", err)
	}
	return meta, nil
}

// UpdateEdge updates properties of an existing edge.
func (g *Graph) UpdateEdge(id string, props map[string][]byte) error {
	meta, err := g.GetEdge(id)
	if err != nil {
		return err
	}
	if meta.Deleted {
		return fmt.Errorf("graph: edge %q: %w", id, ErrEdgeNotFound)
	}
	for name, value := range props {
		if _, err := g.store.Set(g.edgePropKey(id, name), value); err != nil {
			return fmt.Errorf("graph: update edge property %q: %w", name, err)
		}
	}
	return nil
}

// DeleteEdge soft-deletes an edge and removes it from adjacency lists.
func (g *Graph) DeleteEdge(id string) error {
	meta, err := g.GetEdge(id)
	if err != nil {
		return err
	}
	if meta.Deleted {
		return nil
	}

	// Mark edge as deleted.
	meta.Deleted = true
	meta.ValidTo = g.clock.Now()
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("graph: marshal edge meta: %w", err)
	}
	if _, err := g.store.Set(g.edgeMetaKey(id), data); err != nil {
		return fmt.Errorf("graph: store edge meta: %w", err)
	}

	// Update adjacency lists.
	g.adjMu.Lock()
	defer g.adjMu.Unlock()

	if err := g.removeFromAdjacencyList(g.adjOutKey(meta.From), id); err != nil {
		return fmt.Errorf("graph: update adj out: %w", err)
	}
	if err := g.removeFromAdjacencyList(g.adjInKey(meta.To), id); err != nil {
		return fmt.Errorf("graph: update adj in: %w", err)
	}

	return nil
}
