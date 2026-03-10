package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/thatscalaguy/naladb/internal/graph"
	"github.com/thatscalaguy/naladb/internal/hlc"
	"github.com/thatscalaguy/naladb/internal/meta"
	"github.com/thatscalaguy/naladb/internal/store"
)

// Planner transforms parsed AST statements into executable operator trees.
type Planner struct {
	store *store.Store
	graph *graph.Graph
	meta  *meta.Registry
	clock *hlc.Clock
}

// NewPlanner creates a new query planner.
func NewPlanner(s *store.Store, g *graph.Graph, m *meta.Registry, c *hlc.Clock) *Planner {
	return &Planner{
		store: s,
		graph: g,
		meta:  m,
		clock: c,
	}
}

// Plan creates an operator tree from a parsed statement.
func (p *Planner) Plan(stmt Statement) (Operator, error) {
	switch s := stmt.(type) {
	case *MatchStatement:
		return p.planMatch(s)
	case *CausalStatement:
		return p.planCausal(s)
	case *DiffStatement:
		return p.planDiff(s)
	case *HistoryStatement:
		return p.planHistory(s)
	case *MetaStatement:
		return p.planMeta(s)
	case *ShowStatement:
		return p.planShow(s)
	case *DescribeStatement:
		return p.planDescribe(s)
	default:
		return nil, fmt.Errorf("planner: unsupported statement type %T", stmt)
	}
}

// planMatch builds the operator tree for a MATCH statement.
// Pipeline: NodeScan → EdgeJoin(s) → Filter → Sort → Limit → Project
func (p *Planner) planMatch(stmt *MatchStatement) (Operator, error) {
	at := p.resolveTimestamp(stmt.At)

	if stmt.Pattern == nil || len(stmt.Pattern.Elements) == 0 {
		return nil, fmt.Errorf("planner: MATCH requires at least one node pattern")
	}

	// First element must be a NodePattern.
	firstNode, ok := stmt.Pattern.Elements[0].(*NodePattern)
	if !ok {
		return nil, fmt.Errorf("planner: first pattern element must be a node")
	}

	var op Operator = NewNodeScanOp(p.store, firstNode.Variable, firstNode.Type, at)

	// Process remaining pattern elements (alternating edge, node pairs).
	for i := 1; i+1 < len(stmt.Pattern.Elements); i += 2 {
		edge, ok := stmt.Pattern.Elements[i].(*EdgePattern)
		if !ok {
			return nil, fmt.Errorf("planner: expected edge pattern at position %d", i)
		}
		node, ok := stmt.Pattern.Elements[i+1].(*NodePattern)
		if !ok {
			return nil, fmt.Errorf("planner: expected node pattern at position %d", i+1)
		}

		fromVar := ""
		if i == 1 {
			fromVar = firstNode.Variable
		} else {
			prevNode := stmt.Pattern.Elements[i-1].(*NodePattern)
			fromVar = prevNode.Variable
		}

		// Wrap with EdgeJoinOp — node type filtering happens inside via post-filter.
		joinOp := NewEdgeJoinOp(op, p.store, fromVar, edge.Variable, node.Variable, edge.Relation, edge.Direction, at)
		op = joinOp

		// If the target node has a type filter, add a filter operator.
		if node.Type != "" {
			typeFilter := &BinaryExpr{
				Left:  &PropertyAccess{Object: &Identifier{Name: node.Variable}, Property: "__type"},
				Op:    TOKEN_EQ,
				Right: &StringLiteral{Value: node.Type},
			}
			op = NewFilterOp(op, typeFilter)
		}
	}

	// WHERE filter.
	if stmt.Where != nil {
		op = NewFilterOp(op, stmt.Where.Expr)
	}

	// FETCH clause — enrich rows with KV data.
	if stmt.Fetch != nil && len(stmt.Fetch.Items) > 0 {
		op = NewFetchOp(op, p.store, stmt.Fetch.Items)
	}

	// Detect inline latest() calls in RETURN items and resolve them.
	if stmt.Return != nil {
		var latestCalls []*FunctionCall
		for _, item := range stmt.Return.Items {
			latestCalls = append(latestCalls, findLatestCalls(item)...)
		}
		if len(latestCalls) > 0 {
			op = NewLatestResolveOp(op, p.store, latestCalls)
		}
	}

	// ORDER BY.
	if stmt.OrderBy != nil {
		op = NewSortOp(op, stmt.OrderBy.Items)
	}

	// OFFSET.
	if stmt.Offset != nil {
		op = NewOffsetOp(op, stmt.Offset.Value)
	}

	// LIMIT.
	if stmt.Limit != nil {
		op = NewLimitOp(op, stmt.Limit.Value)
	}

	// RETURN projection.
	if stmt.Return != nil {
		op = NewProjectOp(op, stmt.Return.Items)
	}

	return op, nil
}

// findLatestCalls recursively finds all latest() function calls in an expression.
func findLatestCalls(expr Expr) []*FunctionCall {
	switch e := expr.(type) {
	case *FunctionCall:
		if strings.ToUpper(e.Name) == "LATEST" {
			return []*FunctionCall{e}
		}
		var calls []*FunctionCall
		for _, arg := range e.Args {
			calls = append(calls, findLatestCalls(arg)...)
		}
		return calls
	case *BinaryExpr:
		left := findLatestCalls(e.Left)
		right := findLatestCalls(e.Right)
		return append(left, right...)
	case *UnaryExpr:
		return findLatestCalls(e.Operand)
	case *PropertyAccess:
		return findLatestCalls(e.Object)
	default:
		return nil
	}
}

// planCausal builds the operator tree for a CAUSAL statement.
func (p *Planner) planCausal(stmt *CausalStatement) (Operator, error) {
	if p.graph == nil {
		return nil, fmt.Errorf("planner: graph layer required for CAUSAL queries")
	}

	depth := 5 // default
	if stmt.Depth != nil {
		depth = int(stmt.Depth.Value)
	}

	windowMicros := int64(30_000_000) // 30s default
	if stmt.Window != "" {
		wm, err := parseDurationMicros(stmt.Window)
		if err != nil {
			return nil, fmt.Errorf("planner: invalid window %q: %w", stmt.Window, err)
		}
		windowMicros = wm
	}

	at := p.resolveTimestamp(stmt.At)
	if at.IsZero() {
		at = p.clock.Now()
	}

	triggerID, err := p.graph.ResolveNodeID(stmt.TriggerNode)
	if err != nil {
		return nil, fmt.Errorf("planner: resolve trigger node %q: %w", stmt.TriggerNode, err)
	}

	query := graph.CausalQuery{
		TriggerNodeID: triggerID,
		At:            at,
		MaxDepth:      depth,
		WindowMicros:  windowMicros,
		Direction:     graph.CausalForward,
	}

	scan, err := NewCausalScanOp(p.store, p.graph, query)
	if err != nil {
		return nil, fmt.Errorf("planner: causal traverse: %w", err)
	}

	var op Operator = scan

	if stmt.Where != nil {
		op = NewFilterOp(op, stmt.Where.Expr)
	}
	if stmt.OrderBy != nil {
		op = NewSortOp(op, stmt.OrderBy.Items)
	}
	if stmt.Offset != nil {
		op = NewOffsetOp(op, stmt.Offset.Value)
	}
	if stmt.Limit != nil {
		op = NewLimitOp(op, stmt.Limit.Value)
	}
	if stmt.Return != nil {
		op = NewProjectOp(op, stmt.Return.Items)
	}

	return op, nil
}

// planDiff builds the operator tree for a DIFF statement.
func (p *Planner) planDiff(stmt *DiffStatement) (Operator, error) {
	startTS, err := parseTimestamp(stmt.Start)
	if err != nil {
		return nil, fmt.Errorf("planner: invalid start timestamp %q: %w", stmt.Start, err)
	}
	endTS, err := parseTimestamp(stmt.End)
	if err != nil {
		return nil, fmt.Errorf("planner: invalid end timestamp %q: %w", stmt.End, err)
	}

	var op Operator = NewDiffScanOp(p.store, stmt.Pattern, startTS, endTS)

	if stmt.Where != nil {
		op = NewFilterOp(op, stmt.Where.Expr)
	}
	if stmt.Offset != nil {
		op = NewOffsetOp(op, stmt.Offset.Value)
	}
	if stmt.Limit != nil {
		op = NewLimitOp(op, stmt.Limit.Value)
	}
	if stmt.Return != nil {
		op = NewProjectOp(op, stmt.Return.Items)
	}

	return op, nil
}

// planHistory builds the operator tree for a GET history() statement.
func (p *Planner) planHistory(stmt *HistoryStatement) (Operator, error) {
	opts := store.HistoryOptions{}

	if stmt.From != "" {
		from, err := parseTimestamp(stmt.From)
		if err != nil {
			return nil, fmt.Errorf("planner: invalid FROM timestamp: %w", err)
		}
		opts.From = from
	}

	if stmt.To != "" {
		to, err := parseTimestamp(stmt.To)
		if err != nil {
			return nil, fmt.Errorf("planner: invalid TO timestamp: %w", err)
		}
		opts.To = to
	}

	if stmt.Last != nil {
		opts.Limit = int(stmt.Last.Value)
		opts.Reverse = true
	}

	var op Operator = NewHistoryScanOp(p.store, stmt.Key, opts, stmt.Downsample)

	return op, nil
}

// planMeta builds the operator tree for a META statement.
func (p *Planner) planMeta(stmt *MetaStatement) (Operator, error) {
	if p.meta == nil {
		return nil, fmt.Errorf("planner: meta registry required for META queries")
	}

	var op Operator = NewMetaScanOp(p.meta, stmt.KeyPattern)

	if stmt.Where != nil {
		op = NewFilterOp(op, stmt.Where.Expr)
	}
	if stmt.Offset != nil {
		op = NewOffsetOp(op, stmt.Offset.Value)
	}
	if stmt.Limit != nil {
		op = NewLimitOp(op, stmt.Limit.Value)
	}
	if stmt.Return != nil {
		op = NewProjectOp(op, stmt.Return.Items)
	}

	return op, nil
}

// planShow builds the operator tree for a SHOW statement.
func (p *Planner) planShow(stmt *ShowStatement) (Operator, error) {
	var op Operator
	switch stmt.Target {
	case ShowNodes:
		op = NewShowNodesScanOp(p.store)
	case ShowEdges:
		op = NewShowEdgesScanOp(p.store)
	case ShowKeys:
		op = NewShowKeysScanOp(p.store)
	default:
		return nil, fmt.Errorf("planner: unsupported SHOW target")
	}

	if stmt.Where != nil {
		op = NewFilterOp(op, stmt.Where.Expr)
	}
	if stmt.Offset != nil {
		op = NewOffsetOp(op, stmt.Offset.Value)
	}
	if stmt.Limit != nil {
		op = NewLimitOp(op, stmt.Limit.Value)
	}

	return op, nil
}

// planDescribe builds the operator tree for a DESCRIBE statement.
func (p *Planner) planDescribe(stmt *DescribeStatement) (Operator, error) {
	at := p.resolveTimestamp(stmt.At)

	var op Operator
	switch stmt.Target {
	case DescribeNodeByID:
		op = NewDescribeNodeScanOp(p.store, stmt.ID, at)
	case DescribeAllNodes:
		op = NewDescribeNodeScanOp(p.store, "", at)
	case DescribeEdgeByID:
		op = NewDescribeEdgeScanOp(p.store, stmt.ID, at)
	case DescribeAllEdges:
		op = NewDescribeEdgeScanOp(p.store, "", at)
	default:
		return nil, fmt.Errorf("planner: unsupported DESCRIBE target")
	}

	if stmt.Where != nil {
		op = NewFilterOp(op, stmt.Where.Expr)
	}
	if stmt.Offset != nil {
		op = NewOffsetOp(op, stmt.Offset.Value)
	}
	if stmt.Limit != nil {
		op = NewLimitOp(op, stmt.Limit.Value)
	}

	return op, nil
}

// resolveTimestamp converts an AT clause expression to an HLC timestamp.
func (p *Planner) resolveTimestamp(at *StringLiteral) hlc.HLC {
	if at == nil {
		if p.clock != nil {
			return p.clock.Now()
		}
		return 0
	}

	ts, err := parseTimestamp(at.Value)
	if err != nil {
		// If parsing fails, use current time.
		if p.clock != nil {
			return p.clock.Now()
		}
		return 0
	}
	return ts
}

// parseTimestamp parses an ISO 8601 timestamp string into an HLC.
func parseTimestamp(s string) (hlc.HLC, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02",
	}

	for _, format := range formats {
		t, err := time.Parse(format, s)
		if err == nil {
			wallMicros := t.UnixMicro() - hlc.Epoch
			return hlc.NewHLC(wallMicros, 0, 0), nil
		}
	}

	return 0, fmt.Errorf("unable to parse timestamp %q", s)
}

// parseDurationMicros parses a duration string (e.g. "30s", "5m", "1h") to microseconds.
func parseDurationMicros(s string) (int64, error) {
	// Try Go duration format first.
	d, err := time.ParseDuration(s)
	if err == nil {
		return d.Microseconds(), nil
	}

	// Try NalaQL format: number + unit suffix.
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid duration: %s", s)
	}

	numStr := s[:len(s)-1]
	unit := s[len(s)-1:]

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration number: %s", s)
	}

	switch strings.ToLower(unit) {
	case "s":
		return int64(num * 1_000_000), nil
	case "m":
		return int64(num * 60_000_000), nil
	case "h":
		return int64(num * 3_600_000_000), nil
	default:
		return 0, fmt.Errorf("unknown duration unit: %s", unit)
	}
}
