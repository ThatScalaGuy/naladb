package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"

	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/store"
)

// Sentinel errors for graph operations.
var (
	ErrNodeNotFound            = errors.New("naladb: node not found")
	ErrNodeDeleted             = errors.New("naladb: node is deleted")
	ErrEdgeNotFound            = errors.New("naladb: edge not found")
	ErrEdgeOutsideNodeValidity = errors.New("naladb: edge validity outside node validity")
)

// NodeMeta holds the metadata for a graph node, serialized as JSON
// and stored under the key "node:{id}:meta" in the temporal KV store.
type NodeMeta struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	ValidFrom hlc.HLC `json:"valid_from"`
	ValidTo   hlc.HLC `json:"valid_to"`
	Deleted   bool    `json:"deleted"`
}

// Graph provides node and edge CRUD operations projected onto the
// temporal KV store. It is safe for concurrent use.
type Graph struct {
	store        *store.Store
	clock        *hlc.Clock
	adjMu        sync.Mutex // protects adjacency list read-modify-write
	keyPrefix    string     // tenant key prefix (e.g., "acme:")
	resolveProps []string   // property names checked during ResolveNodeID; if empty, all properties are scanned
}

// Option configures a Graph instance.
type Option func(*Graph)

// WithKeyPrefix sets a key prefix for all graph keys, enabling
// multi-tenant isolation via key-prefix partitioning.
func WithKeyPrefix(prefix string) Option {
	return func(g *Graph) { g.keyPrefix = prefix }
}

// WithResolveProperties configures the property names that ResolveNodeID
// checks when resolving a user-supplied identifier. By default (when no
// properties are configured) all node properties are scanned.
func WithResolveProperties(names ...string) Option {
	return func(g *Graph) { g.resolveProps = names }
}

// New creates a new Graph layer backed by the given store and clock.
func New(s *store.Store, c *hlc.Clock, opts ...Option) *Graph {
	g := &Graph{store: s, clock: c}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// KeyPrefix returns the graph's key prefix.
func (g *Graph) KeyPrefix() string { return g.keyPrefix }

// GraphStats holds summary statistics for the graph layer.
type GraphStats struct {
	Nodes        int            // total nodes (including deleted)
	ActiveNodes  int            // non-deleted nodes
	DeletedNodes int            // soft-deleted nodes
	NodesByType  map[string]int // active node count per type
	Edges        int            // total edges (including deleted)
	ActiveEdges  int            // non-deleted edges
	DeletedEdges int            // soft-deleted edges
	EdgesByRel   map[string]int // active edge count per relation
}

// Stats computes summary statistics for the graph by scanning node and edge
// metadata keys. This is not a constant-time operation; it reads all metadata.
func (g *Graph) Stats() GraphStats {
	st := GraphStats{
		NodesByType: make(map[string]int),
		EdgesByRel:  make(map[string]int),
	}

	// Scan node metadata keys.
	nodePrefix := g.keyPrefix + "node:"
	for _, key := range g.store.ScanPrefix(nodePrefix) {
		if !strings.HasSuffix(key, ":meta") {
			continue
		}
		st.Nodes++
		r := g.store.Get(key)
		if !r.Found {
			continue
		}
		var nm NodeMeta
		if err := json.Unmarshal(r.Value, &nm); err != nil {
			continue
		}
		if nm.Deleted {
			st.DeletedNodes++
		} else {
			st.ActiveNodes++
			st.NodesByType[nm.Type]++
		}
	}

	// Scan edge metadata keys.
	edgePrefix := g.keyPrefix + "edge:"
	for _, key := range g.store.ScanPrefix(edgePrefix) {
		if !strings.HasSuffix(key, ":meta") {
			continue
		}
		st.Edges++
		r := g.store.Get(key)
		if !r.Found {
			continue
		}
		var em EdgeMeta
		if err := json.Unmarshal(r.Value, &em); err != nil {
			continue
		}
		if em.Deleted {
			st.DeletedEdges++
		} else {
			st.ActiveEdges++
			st.EdgesByRel[em.Relation]++
		}
	}

	return st
}

// Now returns the current HLC timestamp from the graph's clock.
func (g *Graph) Now() hlc.HLC { return g.clock.Now() }

// Key format helpers.
func (g *Graph) nodeMetaKey(id string) string { return g.keyPrefix + "node:" + id + ":meta" }
func (g *Graph) nodePropKey(id, name string) string {
	return g.keyPrefix + "node:" + id + ":prop:" + name
}
func (g *Graph) edgeMetaKey(id string) string { return g.keyPrefix + "edge:" + id + ":meta" }
func (g *Graph) edgePropKey(id, name string) string {
	return g.keyPrefix + "edge:" + id + ":prop:" + name
}
func (g *Graph) adjOutKey(nodeID string) string { return g.keyPrefix + "graph:adj:" + nodeID + ":out" }
func (g *Graph) adjInKey(nodeID string) string  { return g.keyPrefix + "graph:adj:" + nodeID + ":in" }

// GenerateNodeID creates a new UUID v7 for use as a node identifier.
func GenerateNodeID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("graph: generate node ID: %w", err)
	}
	return id.String(), nil
}

// CreateNode creates a new graph node with a UUID v7 identifier.
func (g *Graph) CreateNode(nodeType string, props map[string][]byte) (NodeMeta, error) {
	id, err := GenerateNodeID()
	if err != nil {
		return NodeMeta{}, err
	}
	return g.createNodeInternal(id, nodeType, props)
}

// CreateNodeWithID creates a node with a caller-specified ID.
// This is useful for testing and data migration.
func (g *Graph) CreateNodeWithID(id, nodeType string, props map[string][]byte) (NodeMeta, error) {
	return g.createNodeInternal(id, nodeType, props)
}

func (g *Graph) createNodeInternal(id, nodeType string, props map[string][]byte) (NodeMeta, error) {
	now := g.clock.Now()

	meta := NodeMeta{
		ID:        id,
		Type:      nodeType,
		ValidFrom: now,
		ValidTo:   hlc.MaxHLC,
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return NodeMeta{}, fmt.Errorf("graph: marshal node meta: %w", err)
	}

	if _, err := g.store.Set(g.nodeMetaKey(id), data); err != nil {
		return NodeMeta{}, fmt.Errorf("graph: store node meta: %w", err)
	}

	for name, value := range props {
		if _, err := g.store.Set(g.nodePropKey(id, name), value); err != nil {
			return NodeMeta{}, fmt.Errorf("graph: store property %q: %w", name, err)
		}
	}

	// Initialize empty adjacency lists.
	emptyAdj, _ := json.Marshal(AdjacencyList{EdgeIDs: []string{}})
	if _, err := g.store.Set(g.adjOutKey(id), emptyAdj); err != nil {
		return NodeMeta{}, fmt.Errorf("graph: init adj out: %w", err)
	}
	if _, err := g.store.Set(g.adjInKey(id), emptyAdj); err != nil {
		return NodeMeta{}, fmt.Errorf("graph: init adj in: %w", err)
	}

	return meta, nil
}

// GetNode retrieves the current metadata for a node.
func (g *Graph) GetNode(id string) (NodeMeta, error) {
	r := g.store.Get(g.nodeMetaKey(id))
	if !r.Found {
		return NodeMeta{}, ErrNodeNotFound
	}
	var meta NodeMeta
	if err := json.Unmarshal(r.Value, &meta); err != nil {
		return NodeMeta{}, fmt.Errorf("graph: unmarshal node meta: %w", err)
	}
	return meta, nil
}

// GetNodeAt retrieves the metadata for a node at a specific point in time.
func (g *Graph) GetNodeAt(id string, at hlc.HLC) (NodeMeta, error) {
	r := g.store.GetAt(g.nodeMetaKey(id), at)
	if !r.Found {
		return NodeMeta{}, ErrNodeNotFound
	}
	var meta NodeMeta
	if err := json.Unmarshal(r.Value, &meta); err != nil {
		return NodeMeta{}, fmt.Errorf("graph: unmarshal node meta: %w", err)
	}
	return meta, nil
}

// ResolveNodeID resolves a user-supplied identifier to an internal graph node ID.
// It first tries a direct node lookup. If that fails, it scans all nodes for a
// property value match. When resolve properties are configured (via
// WithResolveProperties), only those properties are checked. Otherwise all
// properties of each node are scanned.
func (g *Graph) ResolveNodeID(identifier string) (string, error) {
	// Fast path: direct node ID.
	if _, err := g.GetNode(identifier); err == nil {
		return identifier, nil
	}

	// Slow path: scan nodes for a matching property.
	prefix := g.keyPrefix + "node:"
	metaKeys := g.store.ScanPrefix(prefix)
	for _, key := range metaKeys {
		if !strings.HasSuffix(key, ":meta") {
			continue
		}
		r := g.store.Get(key)
		if !r.Found || r.Tombstone {
			continue
		}
		var nm NodeMeta
		if err := json.Unmarshal(r.Value, &nm); err != nil || nm.Deleted {
			continue
		}

		if len(g.resolveProps) > 0 {
			// Check only the configured property names.
			for _, prop := range g.resolveProps {
				pr := g.store.Get(g.nodePropKey(nm.ID, prop))
				if pr.Found && !pr.Tombstone && string(pr.Value) == identifier {
					return nm.ID, nil
				}
			}
		} else {
			// No configured properties — scan all properties of this node.
			propPrefix := g.nodePropKey(nm.ID, "")
			for _, propKey := range g.store.ScanPrefix(propPrefix) {
				pr := g.store.Get(propKey)
				if pr.Found && !pr.Tombstone && string(pr.Value) == identifier {
					return nm.ID, nil
				}
			}
		}
	}

	return "", fmt.Errorf("graph: node %q not found", identifier)
}

// UpdateNode updates properties of an existing node.
func (g *Graph) UpdateNode(id string, props map[string][]byte) error {
	meta, err := g.GetNode(id)
	if err != nil {
		return err
	}
	if meta.Deleted {
		return ErrNodeDeleted
	}
	for name, value := range props {
		if _, err := g.store.Set(g.nodePropKey(id, name), value); err != nil {
			return fmt.Errorf("graph: update property %q: %w", name, err)
		}
	}
	return nil
}

// DeleteNode soft-deletes a node and all its connected edges.
func (g *Graph) DeleteNode(id string) error {
	meta, err := g.GetNode(id)
	if err != nil {
		return err
	}
	if meta.Deleted {
		return nil
	}

	// Mark node as deleted.
	meta.Deleted = true
	meta.ValidTo = g.clock.Now()
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("graph: marshal node meta: %w", err)
	}
	if _, err := g.store.Set(g.nodeMetaKey(id), data); err != nil {
		return fmt.Errorf("graph: store node meta: %w", err)
	}

	// Soft-delete all connected edges.
	outEdges, err := g.GetOutgoingEdges(id)
	if err != nil {
		return fmt.Errorf("graph: get outgoing edges: %w", err)
	}
	for _, edgeID := range outEdges {
		if err := g.DeleteEdge(edgeID); err != nil && !errors.Is(err, ErrEdgeNotFound) {
			return fmt.Errorf("graph: delete outgoing edge %q: %w", edgeID, err)
		}
	}

	inEdges, err := g.GetIncomingEdges(id)
	if err != nil {
		return fmt.Errorf("graph: get incoming edges: %w", err)
	}
	for _, edgeID := range inEdges {
		if err := g.DeleteEdge(edgeID); err != nil && !errors.Is(err, ErrEdgeNotFound) {
			return fmt.Errorf("graph: delete incoming edge %q: %w", edgeID, err)
		}
	}

	return nil
}
