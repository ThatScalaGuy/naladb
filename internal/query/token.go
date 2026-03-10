package query

import (
	"fmt"
	"strings"
)

// TokenType represents the type of a lexical token.
type TokenType int

const (
	// Special tokens.
	TOKEN_ILLEGAL TokenType = iota
	TOKEN_EOF

	// Identifiers and literals.
	TOKEN_IDENT  // variable names, unquoted identifiers
	TOKEN_STRING // "string literal"
	TOKEN_INT    // 123
	TOKEN_FLOAT  // 1.23

	// Statement keywords.
	TOKEN_MATCH
	TOKEN_AT
	TOKEN_DURING
	TOKEN_WHERE
	TOKEN_RETURN
	TOKEN_CAUSAL
	TOKEN_DIFF
	TOKEN_GET
	TOKEN_META
	TOKEN_FROM
	TOKEN_DEPTH
	TOKEN_WINDOW
	TOKEN_LIMIT
	TOKEN_SET
	TOKEN_DELETE
	TOKEN_HISTORY
	TOKEN_TRAVERSE
	TOKEN_LAST
	TOKEN_DOWNSAMPLE
	TOKEN_LTTB
	TOKEN_ORDER
	TOKEN_BY
	TOKEN_ASC
	TOKEN_DESC
	TOKEN_BETWEEN
	TOKEN_TO
	TOKEN_OFFSET
	TOKEN_SHOW
	TOKEN_DESCRIBE
	TOKEN_NODE
	TOKEN_NODES
	TOKEN_EDGE
	TOKEN_EDGES
	TOKEN_KEYS
	TOKEN_FETCH
	TOKEN_AS
	TOKEN_LATEST

	// Boolean operators.
	TOKEN_AND
	TOKEN_OR
	TOKEN_NOT

	// Boolean literals.
	TOKEN_TRUE
	TOKEN_FALSE

	// Comparison operators.
	TOKEN_EQ  // =
	TOKEN_NEQ // !=
	TOKEN_GT  // >
	TOKEN_LT  // <
	TOKEN_GTE // >=
	TOKEN_LTE // <=

	// Punctuation.
	TOKEN_LPAREN    // (
	TOKEN_RPAREN    // )
	TOKEN_LBRACKET  // [
	TOKEN_RBRACKET  // ]
	TOKEN_COLON     // :
	TOKEN_DOT       // .
	TOKEN_COMMA     // ,
	TOKEN_SEMICOLON // ;
	TOKEN_DASH      // -
	TOKEN_ARROW     // ->
	TOKEN_LARROW    // <-
	TOKEN_STAR      // *
	TOKEN_PLUS      // +
)

var tokenNames = map[TokenType]string{
	TOKEN_ILLEGAL:    "ILLEGAL",
	TOKEN_EOF:        "EOF",
	TOKEN_IDENT:      "IDENT",
	TOKEN_STRING:     "STRING",
	TOKEN_INT:        "INT",
	TOKEN_FLOAT:      "FLOAT",
	TOKEN_MATCH:      "MATCH",
	TOKEN_AT:         "AT",
	TOKEN_DURING:     "DURING",
	TOKEN_WHERE:      "WHERE",
	TOKEN_RETURN:     "RETURN",
	TOKEN_CAUSAL:     "CAUSAL",
	TOKEN_DIFF:       "DIFF",
	TOKEN_GET:        "GET",
	TOKEN_META:       "META",
	TOKEN_FROM:       "FROM",
	TOKEN_DEPTH:      "DEPTH",
	TOKEN_WINDOW:     "WINDOW",
	TOKEN_LIMIT:      "LIMIT",
	TOKEN_SET:        "SET",
	TOKEN_DELETE:     "DELETE",
	TOKEN_HISTORY:    "HISTORY",
	TOKEN_TRAVERSE:   "TRAVERSE",
	TOKEN_LAST:       "LAST",
	TOKEN_DOWNSAMPLE: "DOWNSAMPLE",
	TOKEN_LTTB:       "LTTB",
	TOKEN_ORDER:      "ORDER",
	TOKEN_BY:         "BY",
	TOKEN_ASC:        "ASC",
	TOKEN_DESC:       "DESC",
	TOKEN_BETWEEN:    "BETWEEN",
	TOKEN_TO:         "TO",
	TOKEN_OFFSET:     "OFFSET",
	TOKEN_SHOW:       "SHOW",
	TOKEN_DESCRIBE:   "DESCRIBE",
	TOKEN_NODE:       "NODE",
	TOKEN_NODES:      "NODES",
	TOKEN_EDGE:       "EDGE",
	TOKEN_EDGES:      "EDGES",
	TOKEN_KEYS:       "KEYS",
	TOKEN_FETCH:      "FETCH",
	TOKEN_AS:         "AS",
	TOKEN_LATEST:     "LATEST",
	TOKEN_AND:        "AND",
	TOKEN_OR:         "OR",
	TOKEN_NOT:        "NOT",
	TOKEN_TRUE:       "TRUE",
	TOKEN_FALSE:      "FALSE",
	TOKEN_EQ:         "=",
	TOKEN_NEQ:        "!=",
	TOKEN_GT:         ">",
	TOKEN_LT:         "<",
	TOKEN_GTE:        ">=",
	TOKEN_LTE:        "<=",
	TOKEN_LPAREN:     "(",
	TOKEN_RPAREN:     ")",
	TOKEN_LBRACKET:   "[",
	TOKEN_RBRACKET:   "]",
	TOKEN_COLON:      ":",
	TOKEN_DOT:        ".",
	TOKEN_COMMA:      ",",
	TOKEN_SEMICOLON:  ";",
	TOKEN_DASH:       "-",
	TOKEN_ARROW:      "->",
	TOKEN_LARROW:     "<-",
	TOKEN_STAR:       "*",
	TOKEN_PLUS:       "+",
}

// String returns the human-readable name of the token type.
func (t TokenType) String() string {
	if name, ok := tokenNames[t]; ok {
		return name
	}
	return fmt.Sprintf("TokenType(%d)", int(t))
}

// Token represents a lexical token with its type, literal value, and source position.
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Col     int
}

// String returns a human-readable representation of the token.
func (t Token) String() string {
	if t.Literal != "" {
		return fmt.Sprintf("%s(%q)", t.Type, t.Literal)
	}
	return t.Type.String()
}

// keywords maps uppercase keyword strings to their token types.
var keywords = map[string]TokenType{
	"MATCH":      TOKEN_MATCH,
	"AT":         TOKEN_AT,
	"DURING":     TOKEN_DURING,
	"WHERE":      TOKEN_WHERE,
	"RETURN":     TOKEN_RETURN,
	"CAUSAL":     TOKEN_CAUSAL,
	"DIFF":       TOKEN_DIFF,
	"GET":        TOKEN_GET,
	"META":       TOKEN_META,
	"FROM":       TOKEN_FROM,
	"DEPTH":      TOKEN_DEPTH,
	"WINDOW":     TOKEN_WINDOW,
	"LIMIT":      TOKEN_LIMIT,
	"SET":        TOKEN_SET,
	"DELETE":     TOKEN_DELETE,
	"HISTORY":    TOKEN_HISTORY,
	"TRAVERSE":   TOKEN_TRAVERSE,
	"LAST":       TOKEN_LAST,
	"DOWNSAMPLE": TOKEN_DOWNSAMPLE,
	"LTTB":       TOKEN_LTTB,
	"ORDER":      TOKEN_ORDER,
	"BY":         TOKEN_BY,
	"ASC":        TOKEN_ASC,
	"DESC":       TOKEN_DESC,
	"BETWEEN":    TOKEN_BETWEEN,
	"TO":         TOKEN_TO,
	"OFFSET":     TOKEN_OFFSET,
	"SHOW":       TOKEN_SHOW,
	"DESCRIBE":   TOKEN_DESCRIBE,
	"NODE":       TOKEN_NODE,
	"NODES":      TOKEN_NODES,
	"EDGE":       TOKEN_EDGE,
	"EDGES":      TOKEN_EDGES,
	"KEYS":       TOKEN_KEYS,
	"FETCH":      TOKEN_FETCH,
	"AS":         TOKEN_AS,
	"LATEST":     TOKEN_LATEST,
	"AND":        TOKEN_AND,
	"OR":         TOKEN_OR,
	"NOT":        TOKEN_NOT,
	"TRUE":       TOKEN_TRUE,
	"FALSE":      TOKEN_FALSE,
}

// LookupKeyword returns the token type for an identifier, checking if it is a
// reserved keyword. Keywords are case-insensitive.
func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return TOKEN_IDENT
}
