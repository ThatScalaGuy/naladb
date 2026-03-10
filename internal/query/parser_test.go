package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Scenario: MATCH-AT Query parsen ---

func TestParser_MatchAt(t *testing.T) {
	input := `
		MATCH (a:sensor)-[r:triggers]->(b)
		AT "2024-06-01T14:32:05Z"
		WHERE r.weight > 0.5
		RETURN a.id, b.id, r.weight
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MatchStatement)
	require.True(t, ok, "expected MatchStatement, got %T", stmt)

	// Pattern: (a:sensor)-[r:triggers]->(b)
	require.NotNil(t, m.Pattern)
	require.Len(t, m.Pattern.Elements, 3) // node, edge, node

	n1 := m.Pattern.Elements[0].(*NodePattern)
	assert.Equal(t, "a", n1.Variable)
	assert.Equal(t, "sensor", n1.Type)

	e1 := m.Pattern.Elements[1].(*EdgePattern)
	assert.Equal(t, "r", e1.Variable)
	assert.Equal(t, "triggers", e1.Relation)
	assert.Equal(t, DirectionOutgoing, e1.Direction)

	n2 := m.Pattern.Elements[2].(*NodePattern)
	assert.Equal(t, "b", n2.Variable)
	assert.Equal(t, "", n2.Type) // untyped

	// AT clause
	require.NotNil(t, m.At)
	assert.Equal(t, "2024-06-01T14:32:05Z", m.At.Value)

	// WHERE clause: r.weight > 0.5
	require.NotNil(t, m.Where)
	bin, ok := m.Where.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_GT, bin.Op)

	prop, ok := bin.Left.(*PropertyAccess)
	require.True(t, ok)
	assert.Equal(t, "weight", prop.Property)
	ident, ok := prop.Object.(*Identifier)
	require.True(t, ok)
	assert.Equal(t, "r", ident.Name)

	fl, ok := bin.Right.(*FloatLiteral)
	require.True(t, ok)
	assert.Equal(t, 0.5, fl.Value)

	// RETURN clause: 3 projections
	require.NotNil(t, m.Return)
	assert.Len(t, m.Return.Items, 3)
}

// --- Scenario: CAUSAL Query parsen ---

func TestParser_Causal(t *testing.T) {
	input := `
		CAUSAL FROM sensor_A
		AT "2024-06-01T14:32:05Z"
		DEPTH 5
		WINDOW 30s
		WHERE confidence > 0.7
		RETURN path, delta, confidence
		ORDER BY delta ASC
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	c, ok := stmt.(*CausalStatement)
	require.True(t, ok, "expected CausalStatement, got %T", stmt)

	assert.Equal(t, "sensor_A", c.TriggerNode)

	require.NotNil(t, c.At)
	assert.Equal(t, "2024-06-01T14:32:05Z", c.At.Value)

	require.NotNil(t, c.Depth)
	assert.Equal(t, int64(5), c.Depth.Value)

	assert.Equal(t, "30s", c.Window)

	// WHERE: confidence > 0.7
	require.NotNil(t, c.Where)
	bin, ok := c.Where.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_GT, bin.Op)
	left, ok := bin.Left.(*Identifier)
	require.True(t, ok)
	assert.Equal(t, "confidence", left.Name)
	right, ok := bin.Right.(*FloatLiteral)
	require.True(t, ok)
	assert.Equal(t, 0.7, right.Value)

	// RETURN: 3 items
	require.NotNil(t, c.Return)
	assert.Len(t, c.Return.Items, 3)

	// ORDER BY delta ASC
	require.NotNil(t, c.OrderBy)
	require.Len(t, c.OrderBy.Items, 1)
	obItem := c.OrderBy.Items[0]
	obIdent, ok := obItem.Expr.(*Identifier)
	require.True(t, ok)
	assert.Equal(t, "delta", obIdent.Name)
	assert.False(t, obItem.Desc)
}

// --- Scenario: DIFF Query parsen ---

func TestParser_Diff(t *testing.T) {
	input := `
		DIFF (a:device)-[r]->(b)
		BETWEEN "2024-01-01" AND "2024-06-01"
		RETURN added_edges, removed_edges, changed_weights
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	d, ok := stmt.(*DiffStatement)
	require.True(t, ok, "expected DiffStatement, got %T", stmt)

	// Pattern
	require.NotNil(t, d.Pattern)
	require.Len(t, d.Pattern.Elements, 3)

	n1 := d.Pattern.Elements[0].(*NodePattern)
	assert.Equal(t, "a", n1.Variable)
	assert.Equal(t, "device", n1.Type)

	e1 := d.Pattern.Elements[1].(*EdgePattern)
	assert.Equal(t, "r", e1.Variable)
	assert.Equal(t, "", e1.Relation)
	assert.Equal(t, DirectionOutgoing, e1.Direction)

	n2 := d.Pattern.Elements[2].(*NodePattern)
	assert.Equal(t, "b", n2.Variable)

	// BETWEEN
	assert.Equal(t, "2024-01-01", d.Start)
	assert.Equal(t, "2024-06-01", d.End)

	// RETURN: 3 items
	require.NotNil(t, d.Return)
	assert.Len(t, d.Return.Items, 3)
}

// --- Scenario: GET history parsen ---

func TestParser_History(t *testing.T) {
	input := `
		GET history("node:sensor_42:prop:temperature")
		FROM "2024-06-01" TO "2024-06-02"
		DOWNSAMPLE LTTB(500)
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	h, ok := stmt.(*HistoryStatement)
	require.True(t, ok, "expected HistoryStatement, got %T", stmt)

	assert.Equal(t, "node:sensor_42:prop:temperature", h.Key)
	assert.Equal(t, "2024-06-01", h.From)
	assert.Equal(t, "2024-06-02", h.To)

	require.NotNil(t, h.Downsample)
	assert.Equal(t, "LTTB", h.Downsample.Strategy)
	assert.Equal(t, int64(500), h.Downsample.Buckets)
}

// --- Scenario: META Query parsen ---

func TestParser_Meta(t *testing.T) {
	input := `
		META "node:sensor_*:prop:temperature"
		WHERE write_rate > 10.0
		RETURN key, write_rate, avg_interval, stddev_value
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MetaStatement)
	require.True(t, ok, "expected MetaStatement, got %T", stmt)

	assert.Equal(t, "node:sensor_*:prop:temperature", m.KeyPattern)

	// WHERE: write_rate > 10.0
	require.NotNil(t, m.Where)
	bin, ok := m.Where.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_GT, bin.Op)
	left, ok := bin.Left.(*Identifier)
	require.True(t, ok)
	assert.Equal(t, "write_rate", left.Name)
	right, ok := bin.Right.(*FloatLiteral)
	require.True(t, ok)
	assert.Equal(t, 10.0, right.Value)

	// RETURN: 4 items
	require.NotNil(t, m.Return)
	assert.Len(t, m.Return.Items, 4)
}

// --- Scenario: Syntax-Fehler erzeugen hilfreiche Fehlermeldungen ---

func TestParser_SyntaxError(t *testing.T) {
	input := "MATCH (a:sensor WHERE"

	_, err := Parse(input)
	require.Error(t, err)

	pe, ok := err.(*ParseError)
	require.True(t, ok, "expected *ParseError, got %T", err)

	// Error should point at WHERE with expected )
	assert.Equal(t, 1, pe.Line)
	assert.Greater(t, pe.Col, 0)
	assert.Equal(t, ")", pe.Expected)
	assert.Equal(t, "WHERE", pe.Got)
}

// --- Additional parser tests ---

func TestParser_MatchWithDuring(t *testing.T) {
	input := `
		MATCH (a:sensor)-[r]->(b)
		DURING "2024-01-01" TO "2024-06-01"
		RETURN a, b
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MatchStatement)
	require.True(t, ok)

	require.NotNil(t, m.During)
	assert.Equal(t, "2024-01-01", m.During.Start)
	assert.Equal(t, "2024-06-01", m.During.End)
}

func TestParser_MatchWithLimit(t *testing.T) {
	input := `
		MATCH (a)-[r]->(b)
		RETURN a
		LIMIT 10
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MatchStatement)
	require.True(t, ok)

	require.NotNil(t, m.Limit)
	assert.Equal(t, int64(10), m.Limit.Value)
}

func TestParser_WhereComplex(t *testing.T) {
	input := `
		MATCH (a)-[r]->(b)
		WHERE r.weight > 0.5 AND a.status = "active"
		RETURN a
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MatchStatement)
	require.True(t, ok)

	require.NotNil(t, m.Where)
	and, ok := m.Where.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_AND, and.Op)

	// Left: r.weight > 0.5
	left, ok := and.Left.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_GT, left.Op)

	// Right: a.status = "active"
	right, ok := and.Right.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_EQ, right.Op)
}

func TestParser_WhereOrPrecedence(t *testing.T) {
	input := `
		MATCH (a)-[r]->(b)
		WHERE a = 1 OR b = 2 AND c = 3
		RETURN a
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m := stmt.(*MatchStatement)
	require.NotNil(t, m.Where)

	// OR has lower precedence than AND, so: a=1 OR (b=2 AND c=3)
	or, ok := m.Where.Expr.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_OR, or.Op)

	// Right side should be AND
	and, ok := or.Right.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_AND, and.Op)
}

func TestParser_IncomingEdge(t *testing.T) {
	input := `MATCH (a)<-[r:type]-(b) RETURN a`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m := stmt.(*MatchStatement)
	e := m.Pattern.Elements[1].(*EdgePattern)
	assert.Equal(t, DirectionIncoming, e.Direction)
	assert.Equal(t, "r", e.Variable)
	assert.Equal(t, "type", e.Relation)
}

func TestParser_UndirectedEdge(t *testing.T) {
	input := `MATCH (a)-[r]-(b) RETURN a`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m := stmt.(*MatchStatement)
	e := m.Pattern.Elements[1].(*EdgePattern)
	assert.Equal(t, DirectionBoth, e.Direction)
}

func TestParser_OrderByDesc(t *testing.T) {
	input := `
		CAUSAL FROM sensor_A
		DEPTH 3
		RETURN delta
		ORDER BY delta DESC
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	c := stmt.(*CausalStatement)
	require.NotNil(t, c.OrderBy)
	require.Len(t, c.OrderBy.Items, 1)
	assert.True(t, c.OrderBy.Items[0].Desc)
}

func TestParser_HistoryWithLast(t *testing.T) {
	input := `GET history("key") LAST 100`

	stmt, err := Parse(input)
	require.NoError(t, err)

	h := stmt.(*HistoryStatement)
	assert.Equal(t, "key", h.Key)
	require.NotNil(t, h.Last)
	assert.Equal(t, int64(100), h.Last.Value)
}

func TestParser_NotExpression(t *testing.T) {
	input := `
		MATCH (a)-[r]->(b)
		WHERE NOT active
		RETURN a
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m := stmt.(*MatchStatement)
	require.NotNil(t, m.Where)

	unary, ok := m.Where.Expr.(*UnaryExpr)
	require.True(t, ok)
	assert.Equal(t, TOKEN_NOT, unary.Op)

	ident, ok := unary.Operand.(*Identifier)
	require.True(t, ok)
	assert.Equal(t, "active", ident.Name)
}

func TestParser_UnexpectedStartToken(t *testing.T) {
	_, err := Parse("FOOBAR something")
	require.Error(t, err)

	pe, ok := err.(*ParseError)
	require.True(t, ok)
	assert.Contains(t, pe.Message, "unexpected token")
}

func TestParser_MultipleEdges(t *testing.T) {
	input := `MATCH (a)-[r1]->(b)-[r2]->(c) RETURN a, c`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m := stmt.(*MatchStatement)
	require.Len(t, m.Pattern.Elements, 5) // a, r1, b, r2, c

	assert.Equal(t, "a", m.Pattern.Elements[0].(*NodePattern).Variable)
	assert.Equal(t, "r1", m.Pattern.Elements[1].(*EdgePattern).Variable)
	assert.Equal(t, "b", m.Pattern.Elements[2].(*NodePattern).Variable)
	assert.Equal(t, "r2", m.Pattern.Elements[3].(*EdgePattern).Variable)
	assert.Equal(t, "c", m.Pattern.Elements[4].(*NodePattern).Variable)
}

func TestParser_PropertyAccessInReturn(t *testing.T) {
	input := `
		MATCH (a:sensor)-[r]->(b)
		RETURN a.id, b.name, r.weight
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m := stmt.(*MatchStatement)
	require.NotNil(t, m.Return)
	require.Len(t, m.Return.Items, 3)

	for i, expected := range []struct{ obj, prop string }{
		{"a", "id"},
		{"b", "name"},
		{"r", "weight"},
	} {
		pa, ok := m.Return.Items[i].(*PropertyAccess)
		require.True(t, ok, "item[%d] should be PropertyAccess", i)
		assert.Equal(t, expected.obj, pa.Object.(*Identifier).Name)
		assert.Equal(t, expected.prop, pa.Property)
	}
}

// --- Scenario: SHOW statements ---

func TestParser_ShowNodes(t *testing.T) {
	stmt, err := Parse("SHOW NODES")
	require.NoError(t, err)

	s, ok := stmt.(*ShowStatement)
	require.True(t, ok, "expected ShowStatement, got %T", stmt)
	assert.Equal(t, ShowNodes, s.Target)
	assert.Nil(t, s.Where)
	assert.Nil(t, s.Limit)
}

func TestParser_ShowEdges(t *testing.T) {
	stmt, err := Parse("SHOW EDGES")
	require.NoError(t, err)

	s, ok := stmt.(*ShowStatement)
	require.True(t, ok)
	assert.Equal(t, ShowEdges, s.Target)
}

func TestParser_ShowKeys(t *testing.T) {
	stmt, err := Parse("SHOW KEYS")
	require.NoError(t, err)

	s, ok := stmt.(*ShowStatement)
	require.True(t, ok)
	assert.Equal(t, ShowKeys, s.Target)
}

func TestParser_ShowNodesWithWhereAndLimit(t *testing.T) {
	stmt, err := Parse(`SHOW NODES WHERE type = "sensor" LIMIT 10`)
	require.NoError(t, err)

	s, ok := stmt.(*ShowStatement)
	require.True(t, ok)
	assert.Equal(t, ShowNodes, s.Target)
	require.NotNil(t, s.Where)
	require.NotNil(t, s.Limit)
	assert.Equal(t, int64(10), s.Limit.Value)
}

func TestParser_ShowCaseInsensitive(t *testing.T) {
	stmt, err := Parse("show nodes")
	require.NoError(t, err)

	s, ok := stmt.(*ShowStatement)
	require.True(t, ok)
	assert.Equal(t, ShowNodes, s.Target)
}

func TestParser_MatchWithLimitAndOffset(t *testing.T) {
	input := `
		MATCH (a)-[r]->(b)
		RETURN a
		LIMIT 10
		OFFSET 20
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MatchStatement)
	require.True(t, ok)

	require.NotNil(t, m.Limit)
	assert.Equal(t, int64(10), m.Limit.Value)
	require.NotNil(t, m.Offset)
	assert.Equal(t, int64(20), m.Offset.Value)
}

func TestParser_MatchWithOffsetOnly(t *testing.T) {
	input := `
		MATCH (a)-[r]->(b)
		RETURN a
		OFFSET 5
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MatchStatement)
	require.True(t, ok)

	assert.Nil(t, m.Limit)
	require.NotNil(t, m.Offset)
	assert.Equal(t, int64(5), m.Offset.Value)
}

func TestParser_ShowNodesWithLimitAndOffset(t *testing.T) {
	stmt, err := Parse("SHOW NODES LIMIT 10 OFFSET 5")
	require.NoError(t, err)

	s, ok := stmt.(*ShowStatement)
	require.True(t, ok)
	assert.Equal(t, ShowNodes, s.Target)
	require.NotNil(t, s.Limit)
	assert.Equal(t, int64(10), s.Limit.Value)
	require.NotNil(t, s.Offset)
	assert.Equal(t, int64(5), s.Offset.Value)
}

func TestParser_CausalWithOffset(t *testing.T) {
	input := `
		CAUSAL FROM sensor_A
		DEPTH 3
		RETURN path
		LIMIT 5
		OFFSET 2
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	c, ok := stmt.(*CausalStatement)
	require.True(t, ok)

	require.NotNil(t, c.Limit)
	assert.Equal(t, int64(5), c.Limit.Value)
	require.NotNil(t, c.Offset)
	assert.Equal(t, int64(2), c.Offset.Value)
}

func TestParser_DiffWithOffset(t *testing.T) {
	input := `
		DIFF (a:device)-[r]->(b)
		BETWEEN "2024-01-01" AND "2024-06-01"
		LIMIT 10
		OFFSET 5
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	d, ok := stmt.(*DiffStatement)
	require.True(t, ok)

	require.NotNil(t, d.Limit)
	assert.Equal(t, int64(10), d.Limit.Value)
	require.NotNil(t, d.Offset)
	assert.Equal(t, int64(5), d.Offset.Value)
}

func TestParser_MetaWithOffset(t *testing.T) {
	input := `
		META "sensor:*"
		LIMIT 10
		OFFSET 3
	`

	stmt, err := Parse(input)
	require.NoError(t, err)

	m, ok := stmt.(*MetaStatement)
	require.True(t, ok)

	require.NotNil(t, m.Limit)
	assert.Equal(t, int64(10), m.Limit.Value)
	require.NotNil(t, m.Offset)
	assert.Equal(t, int64(3), m.Offset.Value)
}

// --- Scenario: DESCRIBE statements ---

func TestParser_DescribeNodeByID(t *testing.T) {
	stmt, err := Parse(`DESCRIBE NODE "sensor_42"`)
	require.NoError(t, err)

	d, ok := stmt.(*DescribeStatement)
	require.True(t, ok, "expected DescribeStatement, got %T", stmt)
	assert.Equal(t, DescribeNodeByID, d.Target)
	assert.Equal(t, "sensor_42", d.ID)
	assert.Nil(t, d.At)
	assert.Nil(t, d.Where)
	assert.Nil(t, d.Limit)
}

func TestParser_DescribeEdgeByID(t *testing.T) {
	stmt, err := Parse(`DESCRIBE EDGE "edge_abc"`)
	require.NoError(t, err)

	d, ok := stmt.(*DescribeStatement)
	require.True(t, ok)
	assert.Equal(t, DescribeEdgeByID, d.Target)
	assert.Equal(t, "edge_abc", d.ID)
}

func TestParser_DescribeNodes(t *testing.T) {
	stmt, err := Parse(`DESCRIBE NODES WHERE type = "sensor" LIMIT 10`)
	require.NoError(t, err)

	d, ok := stmt.(*DescribeStatement)
	require.True(t, ok)
	assert.Equal(t, DescribeAllNodes, d.Target)
	assert.Equal(t, "", d.ID)
	require.NotNil(t, d.Where)
	require.NotNil(t, d.Limit)
	assert.Equal(t, int64(10), d.Limit.Value)
}

func TestParser_DescribeEdges(t *testing.T) {
	stmt, err := Parse("DESCRIBE EDGES")
	require.NoError(t, err)

	d, ok := stmt.(*DescribeStatement)
	require.True(t, ok)
	assert.Equal(t, DescribeAllEdges, d.Target)
}

func TestParser_DescribeNodeWithAt(t *testing.T) {
	stmt, err := Parse(`DESCRIBE NODE "sensor_42" AT "2024-06-01T14:00:00Z"`)
	require.NoError(t, err)

	d, ok := stmt.(*DescribeStatement)
	require.True(t, ok)
	assert.Equal(t, DescribeNodeByID, d.Target)
	assert.Equal(t, "sensor_42", d.ID)
	require.NotNil(t, d.At)
	assert.Equal(t, "2024-06-01T14:00:00Z", d.At.Value)
}

func TestParser_DescribeNodesWithOffset(t *testing.T) {
	stmt, err := Parse("DESCRIBE NODES LIMIT 10 OFFSET 5")
	require.NoError(t, err)

	d, ok := stmt.(*DescribeStatement)
	require.True(t, ok)
	assert.Equal(t, DescribeAllNodes, d.Target)
	require.NotNil(t, d.Limit)
	assert.Equal(t, int64(10), d.Limit.Value)
	require.NotNil(t, d.Offset)
	assert.Equal(t, int64(5), d.Offset.Value)
}

func TestParser_DescribeCaseInsensitive(t *testing.T) {
	stmt, err := Parse(`describe node "test"`)
	require.NoError(t, err)

	d, ok := stmt.(*DescribeStatement)
	require.True(t, ok)
	assert.Equal(t, DescribeNodeByID, d.Target)
	assert.Equal(t, "test", d.ID)
}

func TestParser_DescribeInvalidTarget(t *testing.T) {
	_, err := Parse("DESCRIBE KEYS")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected NODE, NODES, EDGE, or EDGES")
}

func TestParser_ShowInvalidTarget(t *testing.T) {
	_, err := Parse("SHOW TABLES")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected NODES, EDGES, or KEYS")
}
