package query

// Node is the interface implemented by all AST nodes.
type Node interface {
	nodeType() string
}

// Expr is the interface implemented by all expression nodes.
type Expr interface {
	Node
	exprNode()
}

// Statement is the interface implemented by all statement nodes.
type Statement interface {
	Node
	stmtNode()
}

// --- Expressions ---

// Identifier represents a simple identifier (variable name).
type Identifier struct {
	Name string
}

func (i *Identifier) nodeType() string { return "Identifier" }
func (i *Identifier) exprNode()        {}

// StringLiteral represents a quoted string value.
type StringLiteral struct {
	Value string
}

func (s *StringLiteral) nodeType() string { return "StringLiteral" }
func (s *StringLiteral) exprNode()        {}

// IntLiteral represents an integer value.
type IntLiteral struct {
	Value int64
}

func (i *IntLiteral) nodeType() string { return "IntLiteral" }
func (i *IntLiteral) exprNode()        {}

// FloatLiteral represents a floating-point value.
type FloatLiteral struct {
	Value float64
}

func (f *FloatLiteral) nodeType() string { return "FloatLiteral" }
func (f *FloatLiteral) exprNode()        {}

// BoolLiteral represents a boolean value.
type BoolLiteral struct {
	Value bool
}

func (b *BoolLiteral) nodeType() string { return "BoolLiteral" }
func (b *BoolLiteral) exprNode()        {}

// PropertyAccess represents a dotted property access (e.g. a.id, r.weight).
type PropertyAccess struct {
	Object   Expr
	Property string
}

func (p *PropertyAccess) nodeType() string { return "PropertyAccess" }
func (p *PropertyAccess) exprNode()        {}

// BinaryExpr represents a binary operation (e.g. a > b, x AND y).
type BinaryExpr struct {
	Left  Expr
	Op    TokenType
	Right Expr
}

func (b *BinaryExpr) nodeType() string { return "BinaryExpr" }
func (b *BinaryExpr) exprNode()        {}

// UnaryExpr represents a unary operation (e.g. NOT x).
type UnaryExpr struct {
	Op      TokenType
	Operand Expr
}

func (u *UnaryExpr) nodeType() string { return "UnaryExpr" }
func (u *UnaryExpr) exprNode()        {}

// FunctionCall represents a function invocation (e.g. history("key"), LTTB(500)).
type FunctionCall struct {
	Name string
	Args []Expr
}

func (f *FunctionCall) nodeType() string { return "FunctionCall" }
func (f *FunctionCall) exprNode()        {}

// --- Graph Patterns ---

// Direction represents the direction of an edge in a graph pattern.
type Direction int

const (
	DirectionOutgoing Direction = iota // -[r]->
	DirectionIncoming                  // <-[r]-
	DirectionBoth                      // -[r]-
)

// String returns the human-readable direction name.
func (d Direction) String() string {
	switch d {
	case DirectionOutgoing:
		return "OUTGOING"
	case DirectionIncoming:
		return "INCOMING"
	case DirectionBoth:
		return "BOTH"
	default:
		return "UNKNOWN"
	}
}

// NodePattern represents a node in a graph pattern (e.g. (a:sensor)).
type NodePattern struct {
	Variable string // e.g. "a"
	Type     string // e.g. "sensor"; empty if untyped
}

func (n *NodePattern) nodeType() string { return "NodePattern" }

// EdgePattern represents an edge in a graph pattern (e.g. -[r:triggers]->).
type EdgePattern struct {
	Variable  string    // e.g. "r"
	Relation  string    // e.g. "triggers"; empty if untyped
	Direction Direction // OUTGOING, INCOMING, or BOTH
}

func (e *EdgePattern) nodeType() string { return "EdgePattern" }

// GraphPattern represents a full graph pattern
// (e.g. (a:sensor)-[r:triggers]->(b)).
type GraphPattern struct {
	Elements []Node // alternating NodePattern and EdgePattern
}

func (g *GraphPattern) nodeType() string { return "GraphPattern" }

// --- Clauses ---

// WhereClause wraps a WHERE filter expression.
type WhereClause struct {
	Expr Expr
}

// ReturnClause contains the projections in a RETURN clause.
type ReturnClause struct {
	Items []Expr
}

// OrderByClause represents an ORDER BY clause.
type OrderByClause struct {
	Items []OrderByItem
}

// OrderByItem is a single ordering expression with optional DESC direction.
type OrderByItem struct {
	Expr Expr
	Desc bool // true for DESC, false for ASC (default)
}

// DuringClause represents a DURING time range.
type DuringClause struct {
	Start string
	End   string
}

// DownsampleClause represents a DOWNSAMPLE strategy (e.g. LTTB(500)).
type DownsampleClause struct {
	Strategy string // e.g. "LTTB"
	Buckets  int64
}

// FetchItem represents a single FETCH clause item.
// Mode is "latest" or "history". KeyExpr is the KV key expression.
// Alias is the column name for the result (from AS clause).
type FetchItem struct {
	Mode    string // "latest" or "history"
	KeyExpr Expr   // expression that evaluates to the KV key
	Alias   string // AS alias column name
	From    string // optional FROM timestamp (history mode)
	To      string // optional TO timestamp (history mode)
	Last    *IntLiteral
}

// FetchClause contains one or more FETCH items.
type FetchClause struct {
	Items []FetchItem
}

// --- Statements ---

// MatchStatement represents a MATCH query with optional temporal and filter clauses.
type MatchStatement struct {
	Pattern *GraphPattern
	At      *StringLiteral
	During  *DuringClause
	Where   *WhereClause
	Fetch   *FetchClause
	Return  *ReturnClause
	OrderBy *OrderByClause
	Limit   *IntLiteral
	Offset  *IntLiteral
}

func (m *MatchStatement) nodeType() string { return "MatchStatement" }
func (m *MatchStatement) stmtNode()        {}

// CausalStatement represents a CAUSAL dependency traversal query.
type CausalStatement struct {
	TriggerNode string
	At          *StringLiteral
	Depth       *IntLiteral
	Window      string // duration string, e.g. "30s"
	Where       *WhereClause
	Return      *ReturnClause
	OrderBy     *OrderByClause
	Limit       *IntLiteral
	Offset      *IntLiteral
}

func (c *CausalStatement) nodeType() string { return "CausalStatement" }
func (c *CausalStatement) stmtNode()        {}

// DiffStatement represents a DIFF comparison query between two timestamps.
type DiffStatement struct {
	Pattern *GraphPattern
	Start   string // BETWEEN start timestamp
	End     string // AND end timestamp
	Where   *WhereClause
	Return  *ReturnClause
	Limit   *IntLiteral
	Offset  *IntLiteral
}

func (d *DiffStatement) nodeType() string { return "DiffStatement" }
func (d *DiffStatement) stmtNode()        {}

// HistoryStatement represents a GET history() query for key history retrieval.
type HistoryStatement struct {
	Key        string
	From       string
	To         string
	Last       *IntLiteral
	Downsample *DownsampleClause
}

func (h *HistoryStatement) nodeType() string { return "HistoryStatement" }
func (h *HistoryStatement) stmtNode()        {}

// MetaStatement represents a META statistics query.
type MetaStatement struct {
	KeyPattern string
	Where      *WhereClause
	Return     *ReturnClause
	Limit      *IntLiteral
	Offset     *IntLiteral
}

func (m *MetaStatement) nodeType() string { return "MetaStatement" }
func (m *MetaStatement) stmtNode()        {}

// ShowTarget identifies what a SHOW statement lists.
type ShowTarget int

const (
	ShowNodes ShowTarget = iota
	ShowEdges
	ShowKeys
)

// ShowStatement represents a SHOW NODES|EDGES|KEYS query.
type ShowStatement struct {
	Target ShowTarget
	Where  *WhereClause
	Limit  *IntLiteral
	Offset *IntLiteral
}

func (s *ShowStatement) nodeType() string { return "ShowStatement" }
func (s *ShowStatement) stmtNode()        {}

// DescribeTarget identifies what a DESCRIBE statement inspects.
type DescribeTarget int

const (
	DescribeNodeByID DescribeTarget = iota // DESCRIBE NODE "id"
	DescribeAllNodes                       // DESCRIBE NODES
	DescribeEdgeByID                       // DESCRIBE EDGE "id"
	DescribeAllEdges                       // DESCRIBE EDGES
)

// DescribeStatement represents a DESCRIBE NODE|NODES|EDGE|EDGES query.
type DescribeStatement struct {
	Target DescribeTarget
	ID     string         // specific ID (for NODE/EDGE)
	At     *StringLiteral // optional temporal
	Where  *WhereClause   // optional filter (for NODES/EDGES)
	Limit  *IntLiteral
	Offset *IntLiteral
}

func (d *DescribeStatement) nodeType() string { return "DescribeStatement" }
func (d *DescribeStatement) stmtNode()        {}
