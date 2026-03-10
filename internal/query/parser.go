package query

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseError represents a parser error with source position and context.
type ParseError struct {
	Message  string
	Line     int
	Col      int
	Expected string
	Got      string
}

func (e *ParseError) Error() string {
	msg := fmt.Sprintf("parse error at line %d, column %d: %s", e.Line, e.Col, e.Message)
	if e.Expected != "" {
		msg += fmt.Sprintf(" (expected %s, got %s)", e.Expected, e.Got)
	}
	return msg
}

// Parser parses NalaQL tokens into an AST using Pratt parsing for expressions
// and recursive descent for statements.
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a new Parser for the given token slice.
func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

// Parse lexes and parses a NalaQL input string into an AST statement.
func Parse(input string) (Statement, error) {
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()
	parser := NewParser(tokens)
	return parser.ParseStatement()
}

func (p *Parser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	tok := p.current()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *Parser) expect(typ TokenType) (Token, error) {
	tok := p.current()
	if tok.Type != typ {
		return tok, &ParseError{
			Message:  fmt.Sprintf("unexpected token %s", tok.Type),
			Line:     tok.Line,
			Col:      tok.Col,
			Expected: typ.String(),
			Got:      tok.Type.String(),
		}
	}
	return p.advance(), nil
}

// ParseStatement dispatches to the appropriate statement parser based on the
// leading keyword.
func (p *Parser) ParseStatement() (Statement, error) {
	tok := p.current()
	switch tok.Type {
	case TOKEN_MATCH:
		return p.parseMatch()
	case TOKEN_CAUSAL:
		return p.parseCausal()
	case TOKEN_DIFF:
		return p.parseDiff()
	case TOKEN_GET:
		return p.parseGet()
	case TOKEN_META:
		return p.parseMeta()
	case TOKEN_SHOW:
		return p.parseShow()
	case TOKEN_DESCRIBE:
		return p.parseDescribe()
	default:
		return nil, &ParseError{
			Message: fmt.Sprintf("unexpected token %s at start of statement", tok.Type),
			Line:    tok.Line,
			Col:     tok.Col,
			Got:     tok.Type.String(),
		}
	}
}

// --- MATCH statement ---

func (p *Parser) parseMatch() (*MatchStatement, error) {
	p.advance() // consume MATCH

	pattern, err := p.parseGraphPattern()
	if err != nil {
		return nil, err
	}

	stmt := &MatchStatement{Pattern: pattern}

	for p.current().Type != TOKEN_EOF {
		switch p.current().Type {
		case TOKEN_AT:
			p.advance()
			tok, err := p.expect(TOKEN_STRING)
			if err != nil {
				return nil, err
			}
			stmt.At = &StringLiteral{Value: tok.Literal}
		case TOKEN_DURING:
			p.advance()
			dur, err := p.parseDuringClause()
			if err != nil {
				return nil, err
			}
			stmt.During = dur
		case TOKEN_WHERE:
			p.advance()
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			stmt.Where = &WhereClause{Expr: expr}
		case TOKEN_FETCH:
			fc, err := p.parseFetchClause()
			if err != nil {
				return nil, err
			}
			stmt.Fetch = fc
		case TOKEN_RETURN:
			p.advance()
			ret, err := p.parseReturnClause()
			if err != nil {
				return nil, err
			}
			stmt.Return = ret
		case TOKEN_ORDER:
			ob, err := p.parseOrderByClause()
			if err != nil {
				return nil, err
			}
			stmt.OrderBy = ob
		case TOKEN_LIMIT:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Limit = &IntLiteral{Value: val}
		case TOKEN_OFFSET:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Offset = &IntLiteral{Value: val}
		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

// --- FETCH clause ---

// parseFetchClause parses one or more FETCH items.
// FETCH latest(<key_expr>) AS <alias> [, latest(<key_expr>) AS <alias>]
// FETCH history(<key_expr>) [FROM <ts> TO <ts>] [LAST <n>] AS <alias>
func (p *Parser) parseFetchClause() (*FetchClause, error) {
	p.advance() // consume FETCH

	fc := &FetchClause{}
	item, err := p.parseFetchItem()
	if err != nil {
		return nil, err
	}
	fc.Items = append(fc.Items, item)

	for p.current().Type == TOKEN_COMMA {
		p.advance()
		item, err := p.parseFetchItem()
		if err != nil {
			return nil, err
		}
		fc.Items = append(fc.Items, item)
	}

	return fc, nil
}

func (p *Parser) parseFetchItem() (FetchItem, error) {
	tok := p.current()
	var mode string

	switch {
	case tok.Type == TOKEN_LATEST:
		mode = "latest"
		p.advance()
	case tok.Type == TOKEN_HISTORY:
		mode = "history"
		p.advance()
	case tok.Type == TOKEN_IDENT && strings.ToUpper(tok.Literal) == "LATEST":
		mode = "latest"
		p.advance()
	default:
		return FetchItem{}, &ParseError{
			Message:  "expected 'latest' or 'history' in FETCH clause",
			Line:     tok.Line,
			Col:      tok.Col,
			Expected: "LATEST or HISTORY",
			Got:      tok.Type.String(),
		}
	}

	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return FetchItem{}, err
	}

	keyExpr, err := p.parseExpression(0)
	if err != nil {
		return FetchItem{}, err
	}

	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return FetchItem{}, err
	}

	item := FetchItem{
		Mode:    mode,
		KeyExpr: keyExpr,
	}

	// Optional FROM ... TO ... for history mode.
	if mode == "history" && p.current().Type == TOKEN_FROM {
		p.advance()
		fromTok, err := p.expect(TOKEN_STRING)
		if err != nil {
			return FetchItem{}, err
		}
		item.From = fromTok.Literal
		if _, err := p.expect(TOKEN_TO); err != nil {
			return FetchItem{}, err
		}
		toTok, err := p.expect(TOKEN_STRING)
		if err != nil {
			return FetchItem{}, err
		}
		item.To = toTok.Literal
	}

	// Optional LAST n for history mode.
	if mode == "history" && p.current().Type == TOKEN_LAST {
		p.advance()
		nTok, err := p.expect(TOKEN_INT)
		if err != nil {
			return FetchItem{}, err
		}
		val, _ := strconv.ParseInt(nTok.Literal, 10, 64)
		item.Last = &IntLiteral{Value: val}
	}

	// AS alias.
	if _, err := p.expect(TOKEN_AS); err != nil {
		return FetchItem{}, err
	}

	// Parse alias as dotted identifier (e.g. s.value).
	aliasTok := p.advance()
	alias := aliasTok.Literal
	for p.current().Type == TOKEN_DOT {
		p.advance()
		nextTok := p.advance()
		alias += "." + nextTok.Literal
	}
	item.Alias = alias

	return item, nil
}

// --- CAUSAL statement ---

func (p *Parser) parseCausal() (*CausalStatement, error) {
	p.advance() // consume CAUSAL

	if _, err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}

	triggerTok := p.advance()
	if triggerTok.Type != TOKEN_IDENT && triggerTok.Type != TOKEN_STRING {
		return nil, &ParseError{
			Message:  "expected identifier or string for trigger node",
			Line:     triggerTok.Line,
			Col:      triggerTok.Col,
			Expected: "IDENT or STRING",
			Got:      triggerTok.Type.String(),
		}
	}

	stmt := &CausalStatement{TriggerNode: triggerTok.Literal}

	for p.current().Type != TOKEN_EOF {
		switch p.current().Type {
		case TOKEN_AT:
			p.advance()
			tok, err := p.expect(TOKEN_STRING)
			if err != nil {
				return nil, err
			}
			stmt.At = &StringLiteral{Value: tok.Literal}
		case TOKEN_DEPTH:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Depth = &IntLiteral{Value: val}
		case TOKEN_WINDOW:
			p.advance()
			w, err := p.parseWindowValue()
			if err != nil {
				return nil, err
			}
			stmt.Window = w
		case TOKEN_WHERE:
			p.advance()
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			stmt.Where = &WhereClause{Expr: expr}
		case TOKEN_RETURN:
			p.advance()
			ret, err := p.parseReturnClause()
			if err != nil {
				return nil, err
			}
			stmt.Return = ret
		case TOKEN_ORDER:
			ob, err := p.parseOrderByClause()
			if err != nil {
				return nil, err
			}
			stmt.OrderBy = ob
		case TOKEN_LIMIT:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Limit = &IntLiteral{Value: val}
		case TOKEN_OFFSET:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Offset = &IntLiteral{Value: val}
		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

// parseWindowValue reads a window duration like "30s", "5m", or a quoted string.
func (p *Parser) parseWindowValue() (string, error) {
	tok := p.current()
	switch tok.Type {
	case TOKEN_INT:
		p.advance()
		val := tok.Literal
		// Check for duration unit suffix (e.g. 30s, 5m, 1h).
		if p.current().Type == TOKEN_IDENT {
			val += p.advance().Literal
		}
		return val, nil
	case TOKEN_STRING:
		p.advance()
		return tok.Literal, nil
	case TOKEN_IDENT:
		p.advance()
		return tok.Literal, nil
	default:
		return "", &ParseError{
			Message:  "expected duration value for WINDOW",
			Line:     tok.Line,
			Col:      tok.Col,
			Expected: "INT, STRING, or IDENT",
			Got:      tok.Type.String(),
		}
	}
}

// --- DIFF statement ---

func (p *Parser) parseDiff() (*DiffStatement, error) {
	p.advance() // consume DIFF

	pattern, err := p.parseGraphPattern()
	if err != nil {
		return nil, err
	}

	stmt := &DiffStatement{Pattern: pattern}

	for p.current().Type != TOKEN_EOF {
		switch p.current().Type {
		case TOKEN_BETWEEN:
			p.advance()
			startTok, err := p.expect(TOKEN_STRING)
			if err != nil {
				return nil, err
			}
			stmt.Start = startTok.Literal
			if _, err := p.expect(TOKEN_AND); err != nil {
				return nil, err
			}
			endTok, err := p.expect(TOKEN_STRING)
			if err != nil {
				return nil, err
			}
			stmt.End = endTok.Literal
		case TOKEN_WHERE:
			p.advance()
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			stmt.Where = &WhereClause{Expr: expr}
		case TOKEN_RETURN:
			p.advance()
			ret, err := p.parseReturnClause()
			if err != nil {
				return nil, err
			}
			stmt.Return = ret
		case TOKEN_LIMIT:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Limit = &IntLiteral{Value: val}
		case TOKEN_OFFSET:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Offset = &IntLiteral{Value: val}
		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

// --- GET / HISTORY statement ---

func (p *Parser) parseGet() (Statement, error) {
	p.advance() // consume GET

	tok := p.current()
	if tok.Type == TOKEN_HISTORY {
		return p.parseHistory()
	}

	return nil, &ParseError{
		Message:  "expected 'history' after GET",
		Line:     tok.Line,
		Col:      tok.Col,
		Expected: "HISTORY",
		Got:      tok.Type.String(),
	}
}

func (p *Parser) parseHistory() (*HistoryStatement, error) {
	p.advance() // consume HISTORY

	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	keyTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	stmt := &HistoryStatement{Key: keyTok.Literal}

	for p.current().Type != TOKEN_EOF {
		switch p.current().Type {
		case TOKEN_FROM:
			p.advance()
			fromTok, err := p.expect(TOKEN_STRING)
			if err != nil {
				return nil, err
			}
			stmt.From = fromTok.Literal
			if _, err := p.expect(TOKEN_TO); err != nil {
				return nil, err
			}
			toTok, err := p.expect(TOKEN_STRING)
			if err != nil {
				return nil, err
			}
			stmt.To = toTok.Literal
		case TOKEN_LAST:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Last = &IntLiteral{Value: val}
		case TOKEN_DOWNSAMPLE:
			p.advance()
			ds, err := p.parseDownsample()
			if err != nil {
				return nil, err
			}
			stmt.Downsample = ds
		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

func (p *Parser) parseDownsample() (*DownsampleClause, error) {
	strategyTok := p.advance()
	strategy := strategyTok.Literal

	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}
	bucketsTok, err := p.expect(TOKEN_INT)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	val, _ := strconv.ParseInt(bucketsTok.Literal, 10, 64)
	return &DownsampleClause{Strategy: strategy, Buckets: val}, nil
}

// --- META statement ---

func (p *Parser) parseMeta() (*MetaStatement, error) {
	p.advance() // consume META

	keyTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}

	stmt := &MetaStatement{KeyPattern: keyTok.Literal}

	for p.current().Type != TOKEN_EOF {
		switch p.current().Type {
		case TOKEN_WHERE:
			p.advance()
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			stmt.Where = &WhereClause{Expr: expr}
		case TOKEN_RETURN:
			p.advance()
			ret, err := p.parseReturnClause()
			if err != nil {
				return nil, err
			}
			stmt.Return = ret
		case TOKEN_LIMIT:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Limit = &IntLiteral{Value: val}
		case TOKEN_OFFSET:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Offset = &IntLiteral{Value: val}
		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

// --- SHOW statement ---

func (p *Parser) parseShow() (*ShowStatement, error) {
	p.advance() // consume SHOW

	tok := p.current()
	var target ShowTarget
	switch tok.Type {
	case TOKEN_NODES:
		target = ShowNodes
	case TOKEN_EDGES:
		target = ShowEdges
	case TOKEN_KEYS:
		target = ShowKeys
	default:
		return nil, &ParseError{
			Message:  "expected NODES, EDGES, or KEYS after SHOW",
			Line:     tok.Line,
			Col:      tok.Col,
			Expected: "NODES, EDGES, or KEYS",
			Got:      tok.Type.String(),
		}
	}
	p.advance()

	stmt := &ShowStatement{Target: target}

	for p.current().Type != TOKEN_EOF {
		switch p.current().Type {
		case TOKEN_WHERE:
			p.advance()
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			stmt.Where = &WhereClause{Expr: expr}
		case TOKEN_LIMIT:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Limit = &IntLiteral{Value: val}
		case TOKEN_OFFSET:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Offset = &IntLiteral{Value: val}
		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

// --- DESCRIBE statement ---

func (p *Parser) parseDescribe() (*DescribeStatement, error) {
	p.advance() // consume DESCRIBE

	tok := p.current()
	stmt := &DescribeStatement{}

	switch tok.Type {
	case TOKEN_NODE:
		p.advance()
		idTok, err := p.expect(TOKEN_STRING)
		if err != nil {
			return nil, err
		}
		stmt.Target = DescribeNodeByID
		stmt.ID = idTok.Literal

	case TOKEN_EDGE:
		p.advance()
		idTok, err := p.expect(TOKEN_STRING)
		if err != nil {
			return nil, err
		}
		stmt.Target = DescribeEdgeByID
		stmt.ID = idTok.Literal

	case TOKEN_NODES:
		p.advance()
		stmt.Target = DescribeAllNodes

	case TOKEN_EDGES:
		p.advance()
		stmt.Target = DescribeAllEdges

	default:
		return nil, &ParseError{
			Message:  "expected NODE, NODES, EDGE, or EDGES after DESCRIBE",
			Line:     tok.Line,
			Col:      tok.Col,
			Expected: "NODE, NODES, EDGE, or EDGES",
			Got:      tok.Type.String(),
		}
	}

	for p.current().Type != TOKEN_EOF {
		switch p.current().Type {
		case TOKEN_AT:
			p.advance()
			ts, err := p.expect(TOKEN_STRING)
			if err != nil {
				return nil, err
			}
			stmt.At = &StringLiteral{Value: ts.Literal}
		case TOKEN_WHERE:
			p.advance()
			expr, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			stmt.Where = &WhereClause{Expr: expr}
		case TOKEN_LIMIT:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Limit = &IntLiteral{Value: val}
		case TOKEN_OFFSET:
			p.advance()
			tok, err := p.expect(TOKEN_INT)
			if err != nil {
				return nil, err
			}
			val, _ := strconv.ParseInt(tok.Literal, 10, 64)
			stmt.Offset = &IntLiteral{Value: val}
		default:
			return stmt, nil
		}
	}

	return stmt, nil
}

// --- Graph pattern parsing ---

func (p *Parser) parseGraphPattern() (*GraphPattern, error) {
	pattern := &GraphPattern{}

	node, err := p.parseNodePattern()
	if err != nil {
		return nil, err
	}
	pattern.Elements = append(pattern.Elements, node)

	for p.current().Type == TOKEN_DASH || p.current().Type == TOKEN_LARROW {
		edge, err := p.parseEdgePattern()
		if err != nil {
			return nil, err
		}
		pattern.Elements = append(pattern.Elements, edge)

		node, err := p.parseNodePattern()
		if err != nil {
			return nil, err
		}
		pattern.Elements = append(pattern.Elements, node)
	}

	return pattern, nil
}

func (p *Parser) parseNodePattern() (*NodePattern, error) {
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	node := &NodePattern{}

	if p.current().Type == TOKEN_IDENT {
		node.Variable = p.advance().Literal
	}

	if p.current().Type == TOKEN_COLON {
		p.advance()
		if p.current().Type == TOKEN_IDENT {
			node.Type = p.advance().Literal
		} else {
			tok := p.current()
			return nil, &ParseError{
				Message:  "expected type name after ':'",
				Line:     tok.Line,
				Col:      tok.Col,
				Expected: "IDENT",
				Got:      tok.Type.String(),
			}
		}
	}

	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return node, nil
}

func (p *Parser) parseEdgePattern() (*EdgePattern, error) {
	edge := &EdgePattern{}
	hasLeftArrow := false

	startTok := p.current()
	switch startTok.Type {
	case TOKEN_LARROW:
		hasLeftArrow = true
		p.advance()
	case TOKEN_DASH:
		p.advance()
	default:
		return nil, &ParseError{
			Message:  "expected '-' or '<-' at start of edge pattern",
			Line:     startTok.Line,
			Col:      startTok.Col,
			Expected: "- or <-",
			Got:      startTok.Type.String(),
		}
	}

	if _, err := p.expect(TOKEN_LBRACKET); err != nil {
		return nil, err
	}

	if p.current().Type == TOKEN_IDENT {
		edge.Variable = p.advance().Literal
	}

	if p.current().Type == TOKEN_COLON {
		p.advance()
		if p.current().Type == TOKEN_IDENT {
			edge.Relation = p.advance().Literal
		}
	}

	if _, err := p.expect(TOKEN_RBRACKET); err != nil {
		return nil, err
	}

	hasRightArrow := false
	if p.current().Type == TOKEN_ARROW {
		hasRightArrow = true
		p.advance()
	} else if p.current().Type == TOKEN_DASH {
		p.advance()
	}

	switch {
	case hasLeftArrow && hasRightArrow:
		edge.Direction = DirectionBoth
	case hasLeftArrow:
		edge.Direction = DirectionIncoming
	case hasRightArrow:
		edge.Direction = DirectionOutgoing
	default:
		edge.Direction = DirectionBoth
	}

	return edge, nil
}

// --- Clause parsing ---

func (p *Parser) parseDuringClause() (*DuringClause, error) {
	startTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_TO); err != nil {
		return nil, err
	}
	endTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	return &DuringClause{Start: startTok.Literal, End: endTok.Literal}, nil
}

func (p *Parser) parseReturnClause() (*ReturnClause, error) {
	ret := &ReturnClause{}

	expr, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}
	ret.Items = append(ret.Items, expr)

	for p.current().Type == TOKEN_COMMA {
		p.advance()
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		ret.Items = append(ret.Items, expr)
	}

	return ret, nil
}

func (p *Parser) parseOrderByClause() (*OrderByClause, error) {
	p.advance() // consume ORDER
	if _, err := p.expect(TOKEN_BY); err != nil {
		return nil, err
	}

	clause := &OrderByClause{}
	item, err := p.parseOrderByItem()
	if err != nil {
		return nil, err
	}
	clause.Items = append(clause.Items, item)

	for p.current().Type == TOKEN_COMMA {
		p.advance()
		item, err := p.parseOrderByItem()
		if err != nil {
			return nil, err
		}
		clause.Items = append(clause.Items, item)
	}

	return clause, nil
}

func (p *Parser) parseOrderByItem() (OrderByItem, error) {
	expr, err := p.parseExpression(0)
	if err != nil {
		return OrderByItem{}, err
	}
	item := OrderByItem{Expr: expr}
	if p.current().Type == TOKEN_ASC {
		p.advance()
	} else if p.current().Type == TOKEN_DESC {
		p.advance()
		item.Desc = true
	}
	return item, nil
}

// --- Pratt expression parser ---

// Binding power levels (precedence).
const (
	bpNone       = 0
	bpOr         = 10
	bpAnd        = 20
	bpComparison = 30
	bpAdd        = 40
	bpProperty   = 70
)

func (p *Parser) parseExpression(minBP int) (Expr, error) {
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}

	for {
		tok := p.current()
		bp := infixBindingPower(tok.Type)
		if bp <= minBP {
			break
		}

		if tok.Type == TOKEN_DOT {
			p.advance()
			propTok := p.advance()
			// Accept identifiers and keywords as property names.
			if propTok.Literal == "" || !isIdentStart(rune(propTok.Literal[0])) {
				return nil, &ParseError{
					Message:  "expected property name after '.'",
					Line:     propTok.Line,
					Col:      propTok.Col,
					Expected: "IDENT",
					Got:      propTok.Type.String(),
				}
			}
			left = &PropertyAccess{Object: left, Property: propTok.Literal}
			continue
		}

		// Binary operator.
		p.advance()
		right, err := p.parseExpression(bp)
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: tok.Type, Right: right}
	}

	return left, nil
}

func (p *Parser) parsePrefix() (Expr, error) {
	tok := p.current()

	switch tok.Type {
	case TOKEN_IDENT:
		p.advance()
		if p.current().Type == TOKEN_LPAREN {
			return p.parseFunctionCall(tok.Literal)
		}
		return &Identifier{Name: tok.Literal}, nil

	case TOKEN_STRING:
		p.advance()
		return &StringLiteral{Value: tok.Literal}, nil

	case TOKEN_INT:
		p.advance()
		val, err := strconv.ParseInt(tok.Literal, 10, 64)
		if err != nil {
			return nil, &ParseError{
				Message: fmt.Sprintf("invalid integer: %s", tok.Literal),
				Line:    tok.Line,
				Col:     tok.Col,
			}
		}
		return &IntLiteral{Value: val}, nil

	case TOKEN_FLOAT:
		p.advance()
		val, err := strconv.ParseFloat(tok.Literal, 64)
		if err != nil {
			return nil, &ParseError{
				Message: fmt.Sprintf("invalid float: %s", tok.Literal),
				Line:    tok.Line,
				Col:     tok.Col,
			}
		}
		return &FloatLiteral{Value: val}, nil

	case TOKEN_TRUE:
		p.advance()
		return &BoolLiteral{Value: true}, nil

	case TOKEN_FALSE:
		p.advance()
		return &BoolLiteral{Value: false}, nil

	case TOKEN_NOT:
		p.advance()
		operand, err := p.parseExpression(bpComparison)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: TOKEN_NOT, Operand: operand}, nil

	case TOKEN_LPAREN:
		p.advance()
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
		return expr, nil

	case TOKEN_STAR:
		p.advance()
		return &Identifier{Name: "*"}, nil

	default:
		return nil, &ParseError{
			Message:  fmt.Sprintf("unexpected token %s in expression", tok.Type),
			Line:     tok.Line,
			Col:      tok.Col,
			Expected: "expression",
			Got:      tok.Type.String(),
		}
	}
}

func (p *Parser) parseFunctionCall(name string) (*FunctionCall, error) {
	p.advance() // consume (

	fc := &FunctionCall{Name: name}

	if p.current().Type == TOKEN_RPAREN {
		p.advance()
		return fc, nil
	}

	arg, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}
	fc.Args = append(fc.Args, arg)

	for p.current().Type == TOKEN_COMMA {
		p.advance()
		arg, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		fc.Args = append(fc.Args, arg)
	}

	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}

	return fc, nil
}

func infixBindingPower(tok TokenType) int {
	switch tok {
	case TOKEN_OR:
		return bpOr
	case TOKEN_AND:
		return bpAnd
	case TOKEN_EQ, TOKEN_NEQ, TOKEN_GT, TOKEN_LT, TOKEN_GTE, TOKEN_LTE:
		return bpComparison
	case TOKEN_PLUS:
		return bpAdd
	case TOKEN_DOT:
		return bpProperty
	default:
		return bpNone
	}
}
