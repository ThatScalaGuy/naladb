package query

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/store"
)

// Row represents a single result row as a map of column names to values.
type Row map[string]any

// Operator is the iterator interface for query execution.
// Operators form a pull-based pipeline: each call to Next() produces one row.
type Operator interface {
	// Next returns the next row. Returns (nil, false, nil) when exhausted.
	Next() (Row, bool, error)
	// Close releases resources held by this operator.
	Close()
}

// --- NodeScanOp ---

// NodeScanOp scans all nodes of a given type from the store.
type NodeScanOp struct {
	store    *store.Store
	variable string
	nodeType string
	at       hlc.HLC
	keys     []string
	pos      int
	done     bool
}

// NewNodeScanOp creates a scan operator for nodes of a given type.
func NewNodeScanOp(s *store.Store, variable, nodeType string, at hlc.HLC) *NodeScanOp {
	return &NodeScanOp{
		store:    s,
		variable: variable,
		nodeType: nodeType,
		at:       at,
	}
}

func (op *NodeScanOp) Next() (Row, bool, error) {
	if !op.done && op.keys == nil {
		op.keys = op.store.ScanPrefix("node:")
		// Filter to only :meta keys.
		filtered := op.keys[:0]
		for _, k := range op.keys {
			if strings.HasSuffix(k, ":meta") {
				filtered = append(filtered, k)
			}
		}
		op.keys = filtered
	}

	for op.pos < len(op.keys) {
		key := op.keys[op.pos]
		op.pos++

		var r store.Result
		if op.at.IsZero() {
			r = op.store.Get(key)
		} else {
			r = op.store.GetAt(key, op.at)
		}
		if !r.Found || r.Tombstone {
			continue
		}

		var nm graph.NodeMeta
		if err := json.Unmarshal(r.Value, &nm); err != nil {
			continue
		}
		if nm.Deleted {
			continue
		}
		if op.nodeType != "" && nm.Type != op.nodeType {
			continue
		}

		// Check temporal validity.
		if !op.at.IsZero() {
			if op.at.Before(nm.ValidFrom) || !op.at.Before(nm.ValidTo) {
				continue
			}
		}

		row := Row{
			op.variable + ".id":     nm.ID,
			op.variable + ".__type": nm.Type, // metadata type for pattern matching
			op.variable + ".__id":   nm.ID,   // internal ID for joins
		}

		// Load properties.
		propPrefix := "node:" + nm.ID + ":prop:"
		propKeys := op.store.ScanPrefix(propPrefix)
		for _, pk := range propKeys {
			propName := pk[len(propPrefix):]
			var pr store.Result
			if op.at.IsZero() {
				pr = op.store.Get(pk)
			} else {
				pr = op.store.GetAt(pk, op.at)
			}
			if pr.Found && !pr.Tombstone {
				row[op.variable+"."+propName] = tryParseValue(pr.Value)
			}
		}

		return row, true, nil
	}

	op.done = true
	return nil, false, nil
}

func (op *NodeScanOp) Close() {}

// --- EdgeJoinOp ---

// EdgeJoinOp joins an input operator with edges matching a pattern.
type EdgeJoinOp struct {
	input     Operator
	store     *store.Store
	fromVar   string
	edgeVar   string
	toVar     string
	relation  string
	direction Direction
	at        hlc.HLC

	currentRow Row
	edgeIDs    []string
	edgePos    int
}

// NewEdgeJoinOp creates an operator that joins nodes through edges.
func NewEdgeJoinOp(input Operator, s *store.Store, fromVar, edgeVar, toVar string, relation string, dir Direction, at hlc.HLC) *EdgeJoinOp {
	return &EdgeJoinOp{
		input:     input,
		store:     s,
		fromVar:   fromVar,
		edgeVar:   edgeVar,
		toVar:     toVar,
		relation:  relation,
		direction: dir,
		at:        at,
	}
}

func (op *EdgeJoinOp) Next() (Row, bool, error) {
	for {
		// Try to produce a row from current edge list.
		for op.edgePos < len(op.edgeIDs) {
			edgeID := op.edgeIDs[op.edgePos]
			op.edgePos++

			var er store.Result
			edgeKey := "edge:" + edgeID + ":meta"
			if op.at.IsZero() {
				er = op.store.Get(edgeKey)
			} else {
				er = op.store.GetAt(edgeKey, op.at)
			}
			if !er.Found || er.Tombstone {
				continue
			}

			var em graph.EdgeMeta
			if err := json.Unmarshal(er.Value, &em); err != nil {
				continue
			}
			if em.Deleted {
				continue
			}
			if op.relation != "" && em.Relation != op.relation {
				continue
			}

			// Check temporal validity.
			if !op.at.IsZero() {
				if op.at.Before(em.ValidFrom) || !op.at.Before(em.ValidTo) {
					continue
				}
			}

			// Determine the target node.
			fromID, _ := op.currentRow[op.fromVar+".__id"].(string)
			var targetID string
			switch op.direction {
			case DirectionOutgoing:
				if em.From != fromID {
					continue
				}
				targetID = em.To
			case DirectionIncoming:
				if em.To != fromID {
					continue
				}
				targetID = em.From
			default: // Both
				if em.From == fromID {
					targetID = em.To
				} else if em.To == fromID {
					targetID = em.From
				} else {
					continue
				}
			}

			// Load target node.
			targetMeta, err := op.loadNodeMeta(targetID)
			if err != nil {
				continue
			}

			// Build result row.
			row := make(Row, len(op.currentRow)+10)
			maps.Copy(row, op.currentRow)

			// Edge bindings.
			if op.edgeVar != "" {
				row[op.edgeVar+".id"] = em.ID
				row[op.edgeVar+".relation"] = em.Relation
				row[op.edgeVar+".__id"] = em.ID

				// Load edge properties.
				edgePropPrefix := "edge:" + em.ID + ":prop:"
				edgePropKeys := op.store.ScanPrefix(edgePropPrefix)
				for _, pk := range edgePropKeys {
					propName := pk[len(edgePropPrefix):]
					var pr store.Result
					if op.at.IsZero() {
						pr = op.store.Get(pk)
					} else {
						pr = op.store.GetAt(pk, op.at)
					}
					if pr.Found && !pr.Tombstone {
						row[op.edgeVar+"."+propName] = tryParseValue(pr.Value)
					}
				}
			}

			// Target node bindings.
			row[op.toVar+".id"] = targetMeta.ID
			row[op.toVar+".__type"] = targetMeta.Type
			row[op.toVar+".__id"] = targetMeta.ID

			// Load target node properties.
			propPrefix := "node:" + targetID + ":prop:"
			propKeys := op.store.ScanPrefix(propPrefix)
			for _, pk := range propKeys {
				propName := pk[len(propPrefix):]
				var pr store.Result
				if op.at.IsZero() {
					pr = op.store.Get(pk)
				} else {
					pr = op.store.GetAt(pk, op.at)
				}
				if pr.Found && !pr.Tombstone {
					row[op.toVar+"."+propName] = tryParseValue(pr.Value)
				}
			}

			return row, true, nil
		}

		// Fetch next input row.
		inputRow, ok, err := op.input.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		op.currentRow = inputRow
		op.edgePos = 0

		fromID, _ := inputRow[op.fromVar+".__id"].(string)
		if fromID == "" {
			op.edgeIDs = nil
			continue
		}

		// Load edge IDs from adjacency lists.
		op.edgeIDs = op.loadEdgeIDs(fromID)
	}
}

func (op *EdgeJoinOp) loadEdgeIDs(nodeID string) []string {
	var allEdgeIDs []string

	if op.direction == DirectionOutgoing || op.direction == DirectionBoth {
		adjKey := "graph:adj:" + nodeID + ":out"
		allEdgeIDs = append(allEdgeIDs, op.readAdjList(adjKey)...)
	}
	if op.direction == DirectionIncoming || op.direction == DirectionBoth {
		adjKey := "graph:adj:" + nodeID + ":in"
		allEdgeIDs = append(allEdgeIDs, op.readAdjList(adjKey)...)
	}

	return allEdgeIDs
}

func (op *EdgeJoinOp) readAdjList(key string) []string {
	var r store.Result
	if op.at.IsZero() {
		r = op.store.Get(key)
	} else {
		r = op.store.GetAt(key, op.at)
	}
	if !r.Found || r.Tombstone {
		return nil
	}

	type adjList struct {
		EdgeIDs []string `json:"edge_ids"`
	}
	var adj adjList
	if err := json.Unmarshal(r.Value, &adj); err != nil {
		return nil
	}
	return adj.EdgeIDs
}

func (op *EdgeJoinOp) loadNodeMeta(nodeID string) (graph.NodeMeta, error) {
	key := "node:" + nodeID + ":meta"
	var r store.Result
	if op.at.IsZero() {
		r = op.store.Get(key)
	} else {
		r = op.store.GetAt(key, op.at)
	}
	if !r.Found || r.Tombstone {
		return graph.NodeMeta{}, fmt.Errorf("node not found: %s", nodeID)
	}

	var nm graph.NodeMeta
	if err := json.Unmarshal(r.Value, &nm); err != nil {
		return graph.NodeMeta{}, err
	}
	return nm, nil
}

func (op *EdgeJoinOp) Close() {
	op.input.Close()
}

// --- FilterOp ---

// FilterOp filters rows based on a WHERE expression.
type FilterOp struct {
	input Operator
	expr  Expr
}

// NewFilterOp creates a filter operator.
func NewFilterOp(input Operator, expr Expr) *FilterOp {
	return &FilterOp{input: input, expr: expr}
}

func (op *FilterOp) Next() (Row, bool, error) {
	for {
		row, ok, err := op.input.Next()
		if err != nil || !ok {
			return nil, false, err
		}

		result := evalExpr(op.expr, row)
		if toBool(result) {
			return row, true, nil
		}
	}
}

func (op *FilterOp) Close() { op.input.Close() }

// --- ProjectOp ---

// ProjectOp projects specific columns from a row.
type ProjectOp struct {
	input Operator
	items []Expr
}

// NewProjectOp creates a projection operator.
func NewProjectOp(input Operator, items []Expr) *ProjectOp {
	return &ProjectOp{input: input, items: items}
}

func (op *ProjectOp) Next() (Row, bool, error) {
	row, ok, err := op.input.Next()
	if err != nil || !ok {
		return nil, false, err
	}

	// Handle RETURN * — pass through all non-internal fields.
	if len(op.items) == 1 {
		if ident, ok := op.items[0].(*Identifier); ok && ident.Name == "*" {
			result := make(Row, len(row))
			for k, v := range row {
				if !strings.HasPrefix(k[strings.LastIndex(k, ".")+1:], "__") {
					result[k] = v
				}
			}
			return result, true, nil
		}
	}

	result := make(Row, len(op.items))
	for _, item := range op.items {
		key := exprKey(item)
		val := evalExpr(item, row)
		result[key] = val
	}

	return result, true, nil
}

func (op *ProjectOp) Close() { op.input.Close() }

// --- SortOp ---

// SortOp sorts all rows by the ORDER BY clause (materializing sort).
type SortOp struct {
	input Operator
	items []OrderByItem
	rows  []Row
	pos   int
	done  bool
}

// NewSortOp creates a sort operator.
func NewSortOp(input Operator, items []OrderByItem) *SortOp {
	return &SortOp{input: input, items: items}
}

func (op *SortOp) Next() (Row, bool, error) {
	if !op.done {
		// Materialize all input rows.
		for {
			row, ok, err := op.input.Next()
			if err != nil {
				return nil, false, err
			}
			if !ok {
				break
			}
			op.rows = append(op.rows, row)
		}

		// Sort.
		sort.SliceStable(op.rows, func(i, j int) bool {
			for _, item := range op.items {
				vi := evalExpr(item.Expr, op.rows[i])
				vj := evalExpr(item.Expr, op.rows[j])
				cmp := compareValues(vi, vj)
				if cmp == 0 {
					continue
				}
				if item.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})

		op.done = true
	}

	if op.pos >= len(op.rows) {
		return nil, false, nil
	}

	row := op.rows[op.pos]
	op.pos++
	return row, true, nil
}

func (op *SortOp) Close() { op.input.Close() }

// --- LimitOp ---

// LimitOp limits output to n rows.
type LimitOp struct {
	input   Operator
	limit   int64
	emitted int64
}

// NewLimitOp creates a limit operator.
func NewLimitOp(input Operator, n int64) *LimitOp {
	return &LimitOp{input: input, limit: n}
}

func (op *LimitOp) Next() (Row, bool, error) {
	if op.emitted >= op.limit {
		return nil, false, nil
	}

	row, ok, err := op.input.Next()
	if err != nil || !ok {
		return nil, false, err
	}

	op.emitted++
	return row, true, nil
}

func (op *LimitOp) Close() { op.input.Close() }

// --- OffsetOp ---

// OffsetOp skips the first n rows from the input.
type OffsetOp struct {
	input   Operator
	offset  int64
	skipped int64
}

// NewOffsetOp creates an offset operator that skips n rows.
func NewOffsetOp(input Operator, n int64) *OffsetOp {
	return &OffsetOp{input: input, offset: n}
}

func (op *OffsetOp) Next() (Row, bool, error) {
	for op.skipped < op.offset {
		_, ok, err := op.input.Next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		op.skipped++
	}
	return op.input.Next()
}

func (op *OffsetOp) Close() { op.input.Close() }

// --- CausalScanOp ---

// CausalScanOp wraps graph.CausalTraverse as an iterator.
type CausalScanOp struct {
	store   *store.Store
	results []graph.CausalResult
	pos     int
	names   map[string]string // UUID -> display name cache
}

// NewCausalScanOp creates a causal scan operator.
func NewCausalScanOp(s *store.Store, g *graph.Graph, query graph.CausalQuery) (*CausalScanOp, error) {
	results, err := g.CausalTraverse(query)
	if err != nil {
		return nil, err
	}
	return &CausalScanOp{store: s, results: results, names: make(map[string]string)}, nil
}

// resolveNodeName looks up a human-readable name for a graph node UUID.
// It checks sensorId, machineId, zoneId, name, and id properties in order.
func (op *CausalScanOp) resolveNodeName(nodeID string) string {
	if name, ok := op.names[nodeID]; ok {
		return name
	}
	for _, prop := range []string{"sensorId", "machineId", "zoneId", "name", "id"} {
		r := op.store.Get("node:" + nodeID + ":prop:" + prop)
		if r.Found && !r.Tombstone && len(r.Value) > 0 {
			name := string(r.Value)
			op.names[nodeID] = name
			return name
		}
	}
	op.names[nodeID] = nodeID
	return nodeID
}

func (op *CausalScanOp) Next() (Row, bool, error) {
	if op.pos >= len(op.results) {
		return nil, false, nil
	}

	r := op.results[op.pos]
	op.pos++

	// Resolve path UUIDs to display names.
	pathNames := make([]string, len(r.Path))
	for i, id := range r.Path {
		pathNames[i] = op.resolveNodeName(id)
	}

	row := Row{
		"node_id":    op.resolveNodeName(r.NodeID),
		"depth":      float64(r.Depth),
		"delta":      float64(r.DeltaMicros),
		"confidence": r.Confidence,
		"path":       strings.Join(pathNames, " -> "),
		"via_edge":   r.ViaEdge,
		"via_rel":    r.ViaRelation,
	}

	return row, true, nil
}

func (op *CausalScanOp) Close() {}

// --- DiffScanOp ---

// DiffEdge represents an edge difference between two timestamps.
type DiffEdge struct {
	EdgeID   string
	From     string
	To       string
	Relation string
	Change   string // "added", "removed", or "changed"
	Weight1  float64
	Weight2  float64
}

// DiffScanOp compares graph edges between two timestamps.
type DiffScanOp struct {
	results []Row
	pos     int
}

// NewDiffScanOp creates a diff scan operator.
func NewDiffScanOp(s *store.Store, pattern *GraphPattern, startTS, endTS hlc.HLC) *DiffScanOp {
	op := &DiffScanOp{}

	// Collect all edge meta keys.
	edgeKeys := s.ScanPrefix("edge:")
	var metaKeys []string
	for _, k := range edgeKeys {
		if strings.HasSuffix(k, ":meta") {
			metaKeys = append(metaKeys, k)
		}
	}

	// Get node/edge type filters from the pattern.
	var nodeType, relation string
	if pattern != nil && len(pattern.Elements) >= 1 {
		if np, ok := pattern.Elements[0].(*NodePattern); ok {
			nodeType = np.Type
		}
	}
	if pattern != nil && len(pattern.Elements) >= 2 {
		if ep, ok := pattern.Elements[1].(*EdgePattern); ok {
			relation = ep.Relation
		}
	}

	for _, key := range metaKeys {
		rStart := s.GetAt(key, startTS)
		rEnd := s.GetAt(key, endTS)

		var emStart, emEnd graph.EdgeMeta
		hasStart := rStart.Found && !rStart.Tombstone && json.Unmarshal(rStart.Value, &emStart) == nil && !emStart.Deleted
		hasEnd := rEnd.Found && !rEnd.Tombstone && json.Unmarshal(rEnd.Value, &emEnd) == nil && !emEnd.Deleted

		if !hasStart && !hasEnd {
			continue
		}

		// Apply relation filter.
		if relation != "" {
			edgeRel := ""
			if hasEnd {
				edgeRel = emEnd.Relation
			} else {
				edgeRel = emStart.Relation
			}
			if edgeRel != relation {
				continue
			}
		}

		// Apply node type filter.
		if nodeType != "" {
			em := emEnd
			if !hasEnd {
				em = emStart
			}
			if !nodeTypeMatch(s, em.From, nodeType, startTS) && !nodeTypeMatch(s, em.From, nodeType, endTS) {
				continue
			}
		}

		if !hasStart && hasEnd {
			row := Row{
				"edge_id":  emEnd.ID,
				"from":     emEnd.From,
				"to":       emEnd.To,
				"relation": emEnd.Relation,
				"change":   "added",
			}
			op.results = append(op.results, row)
		} else if hasStart && !hasEnd {
			row := Row{
				"edge_id":  emStart.ID,
				"from":     emStart.From,
				"to":       emStart.To,
				"relation": emStart.Relation,
				"change":   "removed",
			}
			op.results = append(op.results, row)
		} else {
			// Both exist — check for weight changes.
			w1 := readEdgeWeight(s, emStart.ID, startTS)
			w2 := readEdgeWeight(s, emEnd.ID, endTS)
			if w1 != w2 {
				row := Row{
					"edge_id":       emEnd.ID,
					"from":          emEnd.From,
					"to":            emEnd.To,
					"relation":      emEnd.Relation,
					"change":        "changed",
					"weight_before": w1,
					"weight_after":  w2,
				}
				op.results = append(op.results, row)
			}
		}
	}

	return op
}

func nodeTypeMatch(s *store.Store, nodeID, expectedType string, at hlc.HLC) bool {
	key := "node:" + nodeID + ":meta"
	r := s.GetAt(key, at)
	if !r.Found || r.Tombstone {
		return false
	}
	var nm graph.NodeMeta
	if err := json.Unmarshal(r.Value, &nm); err != nil {
		return false
	}
	return nm.Type == expectedType
}

func readEdgeWeight(s *store.Store, edgeID string, at hlc.HLC) float64 {
	key := "edge:" + edgeID + ":prop:weight"
	r := s.GetAt(key, at)
	if !r.Found || r.Tombstone {
		return 0
	}
	v, err := strconv.ParseFloat(string(r.Value), 64)
	if err != nil {
		return 0
	}
	return v
}

func (op *DiffScanOp) Next() (Row, bool, error) {
	if op.pos >= len(op.results) {
		return nil, false, nil
	}
	row := op.results[op.pos]
	op.pos++
	return row, true, nil
}

func (op *DiffScanOp) Close() {}

// --- MetaScanOp ---

// MetaScanOp scans KeyMeta entries matching a pattern.
type MetaScanOp struct {
	results []*meta.KeyMeta
	pos     int
}

// NewMetaScanOp creates a meta scan operator.
func NewMetaScanOp(registry *meta.Registry, pattern string) *MetaScanOp {
	results := registry.Match(pattern, nil)
	return &MetaScanOp{results: results}
}

func (op *MetaScanOp) Next() (Row, bool, error) {
	if op.pos >= len(op.results) {
		return nil, false, nil
	}

	km := op.results[op.pos]
	op.pos++

	row := Row{
		"key":          km.Key,
		"total_writes": float64(km.TotalWrites),
		"write_rate":   km.WriteRateHz,
		"min_value":    km.MinValue,
		"max_value":    km.MaxValue,
		"avg_value":    km.AvgValue,
		"stddev_value": km.StdDevValue,
		"cardinality":  float64(km.Cardinality),
		"size_bytes":   float64(km.SizeBytes),
	}

	return row, true, nil
}

func (op *MetaScanOp) Close() {}

// --- HistoryScanOp ---

// HistoryScanOp scans key history with optional downsampling.
type HistoryScanOp struct {
	results []Row
	pos     int
}

// NewHistoryScanOp creates a history scan operator.
func NewHistoryScanOp(s *store.Store, key string, opts store.HistoryOptions, ds *DownsampleClause) *HistoryScanOp {
	entries := s.History(key, opts)

	op := &HistoryScanOp{}

	if ds != nil && len(entries) > 0 {
		// Convert to DataPoints.
		points := make([]DataPoint, len(entries))
		for i, e := range entries {
			points[i] = DataPoint{
				Timestamp: e.HLC.WallMicros(),
				Value:     parseFloat(e.Value),
			}
		}

		var sampled []DataPoint
		switch strings.ToUpper(ds.Strategy) {
		case "LTTB":
			sampled = LTTB(points, int(ds.Buckets))
		case "MINMAX":
			sampled = MinMaxDownsample(points, int(ds.Buckets))
		case "AVG":
			sampled = AvgDownsample(points, int(ds.Buckets))
		default:
			sampled = LTTB(points, int(ds.Buckets))
		}

		for _, dp := range sampled {
			op.results = append(op.results, Row{
				"timestamp": dp.Timestamp,
				"value":     dp.Value,
			})
		}
	} else {
		for _, e := range entries {
			row := Row{
				"timestamp": e.HLC.WallMicros(),
				"value":     tryParseValue(e.Value),
				"tombstone": e.Tombstone,
			}
			op.results = append(op.results, row)
		}
	}

	return op
}

func (op *HistoryScanOp) Next() (Row, bool, error) {
	if op.pos >= len(op.results) {
		return nil, false, nil
	}
	row := op.results[op.pos]
	op.pos++
	return row, true, nil
}

func (op *HistoryScanOp) Close() {}

// --- SliceOp ---

// SliceOp wraps a pre-materialized slice of rows as an Operator.
type SliceOp struct {
	rows []Row
	pos  int
}

// NewSliceOp creates an operator from a pre-built slice of rows.
func NewSliceOp(rows []Row) *SliceOp {
	return &SliceOp{rows: rows}
}

func (op *SliceOp) Next() (Row, bool, error) {
	if op.pos >= len(op.rows) {
		return nil, false, nil
	}
	row := op.rows[op.pos]
	op.pos++
	return row, true, nil
}

func (op *SliceOp) Close() {}

// --- ShowNodesScanOp ---

// ShowNodesScanOp lists all active graph nodes with their type and properties.
type ShowNodesScanOp struct {
	store *store.Store
	keys  []string
	pos   int
}

// NewShowNodesScanOp creates a scan operator that lists all nodes.
func NewShowNodesScanOp(s *store.Store) *ShowNodesScanOp {
	allKeys := s.ScanPrefix("node:")
	var metaKeys []string
	for _, k := range allKeys {
		if strings.HasSuffix(k, ":meta") {
			metaKeys = append(metaKeys, k)
		}
	}
	sort.Strings(metaKeys)
	return &ShowNodesScanOp{store: s, keys: metaKeys}
}

func (op *ShowNodesScanOp) Next() (Row, bool, error) {
	for op.pos < len(op.keys) {
		key := op.keys[op.pos]
		op.pos++

		r := op.store.Get(key)
		if !r.Found || r.Tombstone {
			continue
		}

		var nm graph.NodeMeta
		if err := json.Unmarshal(r.Value, &nm); err != nil {
			continue
		}
		if nm.Deleted {
			continue
		}

		// Collect property names.
		propPrefix := "node:" + nm.ID + ":prop:"
		propKeys := op.store.ScanPrefix(propPrefix)
		props := make([]string, 0, len(propKeys))
		for _, pk := range propKeys {
			props = append(props, pk[len(propPrefix):])
		}
		sort.Strings(props)

		row := Row{
			"id":         nm.ID,
			"type":       nm.Type,
			"properties": strings.Join(props, ", "),
		}
		return row, true, nil
	}
	return nil, false, nil
}

func (op *ShowNodesScanOp) Close() {}

// --- ShowEdgesScanOp ---

// ShowEdgesScanOp lists all active graph edges.
type ShowEdgesScanOp struct {
	store *store.Store
	keys  []string
	pos   int
}

// NewShowEdgesScanOp creates a scan operator that lists all edges.
func NewShowEdgesScanOp(s *store.Store) *ShowEdgesScanOp {
	allKeys := s.ScanPrefix("edge:")
	var metaKeys []string
	for _, k := range allKeys {
		if strings.HasSuffix(k, ":meta") {
			metaKeys = append(metaKeys, k)
		}
	}
	sort.Strings(metaKeys)
	return &ShowEdgesScanOp{store: s, keys: metaKeys}
}

func (op *ShowEdgesScanOp) Next() (Row, bool, error) {
	for op.pos < len(op.keys) {
		key := op.keys[op.pos]
		op.pos++

		r := op.store.Get(key)
		if !r.Found || r.Tombstone {
			continue
		}

		var em graph.EdgeMeta
		if err := json.Unmarshal(r.Value, &em); err != nil {
			continue
		}
		if em.Deleted {
			continue
		}

		row := Row{
			"id":       em.ID,
			"from":     em.From,
			"to":       em.To,
			"relation": em.Relation,
		}
		return row, true, nil
	}
	return nil, false, nil
}

func (op *ShowEdgesScanOp) Close() {}

// --- ShowKeysScanOp ---

// ShowKeysScanOp lists all keys in the store.
type ShowKeysScanOp struct {
	keys []string
	pos  int
}

// NewShowKeysScanOp creates a scan operator that lists all keys.
func NewShowKeysScanOp(s *store.Store) *ShowKeysScanOp {
	keys := s.ScanPrefix("")
	sort.Strings(keys)
	return &ShowKeysScanOp{keys: keys}
}

func (op *ShowKeysScanOp) Next() (Row, bool, error) {
	if op.pos >= len(op.keys) {
		return nil, false, nil
	}
	key := op.keys[op.pos]
	op.pos++
	return Row{"key": key}, true, nil
}

func (op *ShowKeysScanOp) Close() {}

// --- DescribeNodeScanOp ---

// DescribeNodeScanOp returns nodes with all their property values.
// If targetID is set, only that specific node is returned.
type DescribeNodeScanOp struct {
	store    *store.Store
	targetID string
	at       hlc.HLC
	keys     []string
	pos      int
	done     bool
}

// NewDescribeNodeScanOp creates a describe operator for nodes.
func NewDescribeNodeScanOp(s *store.Store, targetID string, at hlc.HLC) *DescribeNodeScanOp {
	return &DescribeNodeScanOp{
		store:    s,
		targetID: targetID,
		at:       at,
	}
}

func (op *DescribeNodeScanOp) Next() (Row, bool, error) {
	if !op.done && op.keys == nil {
		if op.targetID != "" {
			op.keys = []string{"node:" + op.targetID + ":meta"}
		} else {
			allKeys := op.store.ScanPrefix("node:")
			var metaKeys []string
			for _, k := range allKeys {
				if strings.HasSuffix(k, ":meta") {
					metaKeys = append(metaKeys, k)
				}
			}
			sort.Strings(metaKeys)
			op.keys = metaKeys
		}
	}

	for op.pos < len(op.keys) {
		key := op.keys[op.pos]
		op.pos++

		var r store.Result
		if op.at.IsZero() {
			r = op.store.Get(key)
		} else {
			r = op.store.GetAt(key, op.at)
		}
		if !r.Found || r.Tombstone {
			continue
		}

		var nm graph.NodeMeta
		if err := json.Unmarshal(r.Value, &nm); err != nil {
			continue
		}
		if nm.Deleted {
			continue
		}

		row := Row{
			"id":         nm.ID,
			"type":       nm.Type,
			"valid_from": uint64(nm.ValidFrom),
			"valid_to":   uint64(nm.ValidTo),
		}

		// Load all properties with values (don't overwrite metadata columns).
		propPrefix := "node:" + nm.ID + ":prop:"
		propKeys := op.store.ScanPrefix(propPrefix)
		for _, pk := range propKeys {
			propName := pk[len(propPrefix):]
			if _, reserved := row[propName]; reserved {
				continue
			}
			var pr store.Result
			if op.at.IsZero() {
				pr = op.store.Get(pk)
			} else {
				pr = op.store.GetAt(pk, op.at)
			}
			if pr.Found && !pr.Tombstone {
				row[propName] = tryParseValue(pr.Value)
			}
		}

		return row, true, nil
	}

	op.done = true
	return nil, false, nil
}

func (op *DescribeNodeScanOp) Close() {}

// --- DescribeEdgeScanOp ---

// DescribeEdgeScanOp returns edges with all their property values.
// If targetID is set, only that specific edge is returned.
type DescribeEdgeScanOp struct {
	store    *store.Store
	targetID string
	at       hlc.HLC
	keys     []string
	pos      int
	done     bool
}

// NewDescribeEdgeScanOp creates a describe operator for edges.
func NewDescribeEdgeScanOp(s *store.Store, targetID string, at hlc.HLC) *DescribeEdgeScanOp {
	return &DescribeEdgeScanOp{
		store:    s,
		targetID: targetID,
		at:       at,
	}
}

func (op *DescribeEdgeScanOp) Next() (Row, bool, error) {
	if !op.done && op.keys == nil {
		if op.targetID != "" {
			op.keys = []string{"edge:" + op.targetID + ":meta"}
		} else {
			allKeys := op.store.ScanPrefix("edge:")
			var metaKeys []string
			for _, k := range allKeys {
				if strings.HasSuffix(k, ":meta") {
					metaKeys = append(metaKeys, k)
				}
			}
			sort.Strings(metaKeys)
			op.keys = metaKeys
		}
	}

	for op.pos < len(op.keys) {
		key := op.keys[op.pos]
		op.pos++

		var r store.Result
		if op.at.IsZero() {
			r = op.store.Get(key)
		} else {
			r = op.store.GetAt(key, op.at)
		}
		if !r.Found || r.Tombstone {
			continue
		}

		var em graph.EdgeMeta
		if err := json.Unmarshal(r.Value, &em); err != nil {
			continue
		}
		if em.Deleted {
			continue
		}

		row := Row{
			"id":         em.ID,
			"from":       em.From,
			"to":         em.To,
			"relation":   em.Relation,
			"valid_from": uint64(em.ValidFrom),
			"valid_to":   uint64(em.ValidTo),
		}

		// Load all properties with values (don't overwrite metadata columns).
		propPrefix := "edge:" + em.ID + ":prop:"
		propKeys := op.store.ScanPrefix(propPrefix)
		for _, pk := range propKeys {
			propName := pk[len(propPrefix):]
			if _, reserved := row[propName]; reserved {
				continue
			}
			var pr store.Result
			if op.at.IsZero() {
				pr = op.store.Get(pk)
			} else {
				pr = op.store.GetAt(pk, op.at)
			}
			if pr.Found && !pr.Tombstone {
				row[propName] = tryParseValue(pr.Value)
			}
		}

		return row, true, nil
	}

	op.done = true
	return nil, false, nil
}

func (op *DescribeEdgeScanOp) Close() {}

// --- FetchOp ---

// FetchOp enriches each row from the input with KV data based on FETCH items.
type FetchOp struct {
	input Operator
	store *store.Store
	items []FetchItem
}

// NewFetchOp creates a fetch operator.
func NewFetchOp(input Operator, s *store.Store, items []FetchItem) *FetchOp {
	return &FetchOp{input: input, store: s, items: items}
}

func (op *FetchOp) Next() (Row, bool, error) {
	row, ok, err := op.input.Next()
	if err != nil || !ok {
		return nil, false, err
	}

	for _, item := range op.items {
		keyVal := evalExpr(item.KeyExpr, row)
		keyStr := fmt.Sprintf("%v", keyVal)

		switch item.Mode {
		case "latest":
			r := op.store.Get(keyStr)
			if r.Found && !r.Tombstone {
				row[item.Alias] = tryParseValue(r.Value)
			} else {
				row[item.Alias] = nil
			}
			// Also store under __latest: key so inline latest() can find it.
			row["__latest:"+keyStr] = row[item.Alias]

		case "history":
			opts := store.HistoryOptions{}
			if item.From != "" {
				if from, err := parseTimestamp(item.From); err == nil {
					opts.From = from
				}
			}
			if item.To != "" {
				if to, err := parseTimestamp(item.To); err == nil {
					opts.To = to
				}
			}
			if item.Last != nil {
				opts.Limit = int(item.Last.Value)
				opts.Reverse = true
			}
			entries := op.store.History(keyStr, opts)
			// Build a compact JSON-like representation.
			var points []string
			for _, e := range entries {
				ts := e.HLC.WallMicros()
				val := tryParseValue(e.Value)
				points = append(points, fmt.Sprintf("%d:%.4f", ts, toFloatSafe(val)))
			}
			row[item.Alias] = strings.Join(points, ";")
		}
	}

	return row, true, nil
}

func (op *FetchOp) Close() { op.input.Close() }

// toFloatSafe converts a value to float64, defaulting to 0.
func toFloatSafe(v any) float64 {
	f, ok := toFloat(v)
	if ok {
		return f
	}
	return 0
}

// --- LatestResolveOp ---

// LatestResolveOp resolves inline latest() function calls in RETURN expressions
// by pre-fetching values from the KV store.
type LatestResolveOp struct {
	input     Operator
	store     *store.Store
	funcCalls []*FunctionCall // latest() calls found in RETURN items
}

// NewLatestResolveOp creates an operator that resolves inline latest() calls.
func NewLatestResolveOp(input Operator, s *store.Store, calls []*FunctionCall) *LatestResolveOp {
	return &LatestResolveOp{input: input, store: s, funcCalls: calls}
}

func (op *LatestResolveOp) Next() (Row, bool, error) {
	row, ok, err := op.input.Next()
	if err != nil || !ok {
		return nil, false, err
	}

	for _, fc := range op.funcCalls {
		if len(fc.Args) != 1 {
			continue
		}
		keyVal := evalExpr(fc.Args[0], row)
		keyStr := fmt.Sprintf("%v", keyVal)
		r := op.store.Get(keyStr)
		if r.Found && !r.Tombstone {
			row["__latest:"+keyStr] = tryParseValue(r.Value)
		} else {
			row["__latest:"+keyStr] = nil
		}
	}

	return row, true, nil
}

func (op *LatestResolveOp) Close() { op.input.Close() }

// --- Expression evaluation helpers ---

// evalExpr evaluates an AST expression against a row.
func evalExpr(expr Expr, row Row) any {
	switch e := expr.(type) {
	case *Identifier:
		if v, ok := row[e.Name]; ok {
			return v
		}
		return nil

	case *StringLiteral:
		return e.Value

	case *IntLiteral:
		return float64(e.Value)

	case *FloatLiteral:
		return e.Value

	case *BoolLiteral:
		return e.Value

	case *PropertyAccess:
		key := exprKey(expr)
		if v, ok := row[key]; ok {
			return v
		}
		return nil

	case *BinaryExpr:
		left := evalExpr(e.Left, row)
		right := evalExpr(e.Right, row)

		switch e.Op {
		case TOKEN_EQ:
			return compareValues(left, right) == 0
		case TOKEN_NEQ:
			return compareValues(left, right) != 0
		case TOKEN_GT:
			return compareValues(left, right) > 0
		case TOKEN_LT:
			return compareValues(left, right) < 0
		case TOKEN_GTE:
			return compareValues(left, right) >= 0
		case TOKEN_LTE:
			return compareValues(left, right) <= 0
		case TOKEN_AND:
			return toBool(left) && toBool(right)
		case TOKEN_OR:
			return toBool(left) || toBool(right)
		case TOKEN_PLUS:
			// String concatenation.
			return fmt.Sprintf("%v", left) + fmt.Sprintf("%v", right)
		}

	case *UnaryExpr:
		if e.Op == TOKEN_NOT {
			return !toBool(evalExpr(e.Operand, row))
		}

	case *FunctionCall:
		// Handle latest() as an inline function.
		if strings.ToUpper(e.Name) == "LATEST" && len(e.Args) == 1 {
			// The FetchOp should have already resolved this; check the row.
			key := "__latest:" + exprKey(e.Args[0])
			if v, ok := row[key]; ok {
				return v
			}
			// Try evaluating the key expression and looking it up directly.
			keyVal := evalExpr(e.Args[0], row)
			if keyStr, ok := keyVal.(string); ok {
				key = "__latest:" + keyStr
				if v, ok2 := row[key]; ok2 {
					return v
				}
			}
		}
		return nil
	}

	return nil
}

// exprKey generates a column key from an expression.
func exprKey(expr Expr) string {
	switch e := expr.(type) {
	case *Identifier:
		return e.Name
	case *PropertyAccess:
		return exprKey(e.Object) + "." + e.Property
	case *StringLiteral:
		return e.Value
	case *IntLiteral:
		return strconv.FormatInt(e.Value, 10)
	case *FloatLiteral:
		return strconv.FormatFloat(e.Value, 'f', -1, 64)
	case *FunctionCall:
		return e.Name + "(" + strings.Join(funcArgKeys(e.Args), ", ") + ")"
	case *BinaryExpr:
		return exprKey(e.Left) + e.Op.String() + exprKey(e.Right)
	default:
		return fmt.Sprintf("%v", expr)
	}
}

func funcArgKeys(args []Expr) []string {
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = exprKey(a)
	}
	return keys
}

// compareValues compares two values, returning -1, 0, or 1.
func compareValues(a, b any) int {
	fa, aIsFloat := toFloat(a)
	fb, bIsFloat := toFloat(b)

	if aIsFloat && bIsFloat {
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		default:
			return 0
		}
	}

	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)

	switch {
	case sa < sb:
		return -1
	case sa > sb:
		return 1
	default:
		return 0
	}
}

// toBool converts a value to boolean.
func toBool(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case string:
		return val != ""
	case nil:
		return false
	default:
		return true
	}
}

// toFloat converts a value to float64 if possible.
func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// tryParseValue attempts to parse a byte slice as a numeric value.
func tryParseValue(b []byte) any {
	s := string(b)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// parseFloat parses bytes as float64, returning 0 on failure.
func parseFloat(b []byte) float64 {
	f, _ := strconv.ParseFloat(string(b), 64)
	return f
}
