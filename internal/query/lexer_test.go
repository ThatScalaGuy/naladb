package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Scenario: Keywords werden korrekt erkannt ---

func TestLexer_Keywords(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"MATCH", TOKEN_MATCH},
		{"AT", TOKEN_AT},
		{"DURING", TOKEN_DURING},
		{"WHERE", TOKEN_WHERE},
		{"RETURN", TOKEN_RETURN},
		{"CAUSAL", TOKEN_CAUSAL},
		{"DIFF", TOKEN_DIFF},
		{"GET", TOKEN_GET},
		{"META", TOKEN_META},
		{"FROM", TOKEN_FROM},
		{"DEPTH", TOKEN_DEPTH},
		{"WINDOW", TOKEN_WINDOW},
		{"LIMIT", TOKEN_LIMIT},
		// Case-insensitive keywords.
		{"match", TOKEN_MATCH},
		{"Match", TOKEN_MATCH},
		{"causal", TOKEN_CAUSAL},
		{"Return", TOKEN_RETURN},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lex := NewLexer(tt.input)
			tok := lex.NextToken()
			assert.Equal(t, tt.expected, tok.Type, "input: %q", tt.input)
		})
	}
}

// --- Scenario: Graph-Pattern tokenisieren ---

func TestLexer_GraphPattern(t *testing.T) {
	input := "(a:sensor)-[r:triggers]->(b)"
	lex := NewLexer(input)
	tokens := lex.Tokenize()

	expected := []struct {
		typ     TokenType
		literal string
	}{
		{TOKEN_LPAREN, "("},
		{TOKEN_IDENT, "a"},
		{TOKEN_COLON, ":"},
		{TOKEN_IDENT, "sensor"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_DASH, "-"},
		{TOKEN_LBRACKET, "["},
		{TOKEN_IDENT, "r"},
		{TOKEN_COLON, ":"},
		{TOKEN_IDENT, "triggers"},
		{TOKEN_RBRACKET, "]"},
		{TOKEN_ARROW, "->"},
		{TOKEN_LPAREN, "("},
		{TOKEN_IDENT, "b"},
		{TOKEN_RPAREN, ")"},
		{TOKEN_EOF, ""},
	}

	require.Len(t, tokens, len(expected), "token count mismatch")
	for i, exp := range expected {
		assert.Equal(t, exp.typ, tokens[i].Type, "token[%d] type", i)
		assert.Equal(t, exp.literal, tokens[i].Literal, "token[%d] literal", i)
	}
}

// --- Scenario: String-Literale und Zahlen ---

func TestLexer_StringsAndNumbers(t *testing.T) {
	input := `WHERE r.weight > 0.5 AND name = "test"`
	lex := NewLexer(input)
	tokens := lex.Tokenize()

	expected := []struct {
		typ     TokenType
		literal string
	}{
		{TOKEN_WHERE, "WHERE"},
		{TOKEN_IDENT, "r"},
		{TOKEN_DOT, "."},
		{TOKEN_IDENT, "weight"},
		{TOKEN_GT, ">"},
		{TOKEN_FLOAT, "0.5"},
		{TOKEN_AND, "AND"},
		{TOKEN_IDENT, "name"},
		{TOKEN_EQ, "="},
		{TOKEN_STRING, "test"},
		{TOKEN_EOF, ""},
	}

	require.Len(t, tokens, len(expected), "token count mismatch")
	for i, exp := range expected {
		assert.Equal(t, exp.typ, tokens[i].Type, "token[%d] type", i)
		assert.Equal(t, exp.literal, tokens[i].Literal, "token[%d] literal", i)
	}
}

func TestLexer_IntegerLiteral(t *testing.T) {
	lex := NewLexer("42")
	tok := lex.NextToken()
	assert.Equal(t, TOKEN_INT, tok.Type)
	assert.Equal(t, "42", tok.Literal)
}

func TestLexer_FloatLiteral(t *testing.T) {
	lex := NewLexer("3.14")
	tok := lex.NextToken()
	assert.Equal(t, TOKEN_FLOAT, tok.Type)
	assert.Equal(t, "3.14", tok.Literal)
}

func TestLexer_StringEscapes(t *testing.T) {
	lex := NewLexer(`"hello\nworld"`)
	tok := lex.NextToken()
	assert.Equal(t, TOKEN_STRING, tok.Type)
	assert.Equal(t, "hello\nworld", tok.Literal)
}

func TestLexer_UnterminatedString(t *testing.T) {
	lex := NewLexer(`"unterminated`)
	tok := lex.NextToken()
	assert.Equal(t, TOKEN_ILLEGAL, tok.Type)
	assert.Equal(t, "unterminated string", tok.Literal)
}

func TestLexer_ComparisonOperators(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"=", TOKEN_EQ},
		{"!=", TOKEN_NEQ},
		{">", TOKEN_GT},
		{"<", TOKEN_LT},
		{">=", TOKEN_GTE},
		{"<=", TOKEN_LTE},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lex := NewLexer(tt.input)
			tok := lex.NextToken()
			assert.Equal(t, tt.expected, tok.Type)
			assert.Equal(t, tt.input, tok.Literal)
		})
	}
}

func TestLexer_ArrowTokens(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
		literal  string
	}{
		{"->", TOKEN_ARROW, "->"},
		{"<-", TOKEN_LARROW, "<-"},
		{"-", TOKEN_DASH, "-"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lex := NewLexer(tt.input)
			tok := lex.NextToken()
			assert.Equal(t, tt.expected, tok.Type)
			assert.Equal(t, tt.literal, tok.Literal)
		})
	}
}

func TestLexer_LineColumnTracking(t *testing.T) {
	input := "MATCH\n  (a)"
	lex := NewLexer(input)

	tok1 := lex.NextToken() // MATCH
	assert.Equal(t, 1, tok1.Line)
	assert.Equal(t, 1, tok1.Col)

	tok2 := lex.NextToken() // (
	assert.Equal(t, 2, tok2.Line)
	assert.Equal(t, 3, tok2.Col)
}

func TestLexer_SkipsLineComments(t *testing.T) {
	input := "// comment\nMATCH"
	lex := NewLexer(input)
	tok := lex.NextToken()
	assert.Equal(t, TOKEN_MATCH, tok.Type)
	assert.Equal(t, 2, tok.Line)
}

func TestLexer_Punctuation(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"(", TOKEN_LPAREN},
		{")", TOKEN_RPAREN},
		{"[", TOKEN_LBRACKET},
		{"]", TOKEN_RBRACKET},
		{":", TOKEN_COLON},
		{".", TOKEN_DOT},
		{",", TOKEN_COMMA},
		{";", TOKEN_SEMICOLON},
		{"*", TOKEN_STAR},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lex := NewLexer(tt.input)
			tok := lex.NextToken()
			assert.Equal(t, tt.expected, tok.Type)
		})
	}
}

func TestLexer_IdentifiersWithUnderscores(t *testing.T) {
	lex := NewLexer("sensor_A my_var _private")
	tokens := lex.Tokenize()

	require.GreaterOrEqual(t, len(tokens), 3)
	assert.Equal(t, TOKEN_IDENT, tokens[0].Type)
	assert.Equal(t, "sensor_A", tokens[0].Literal)
	assert.Equal(t, TOKEN_IDENT, tokens[1].Type)
	assert.Equal(t, "my_var", tokens[1].Literal)
	assert.Equal(t, TOKEN_IDENT, tokens[2].Type)
	assert.Equal(t, "_private", tokens[2].Literal)
}

func TestLexer_EmptyInput(t *testing.T) {
	lex := NewLexer("")
	tok := lex.NextToken()
	assert.Equal(t, TOKEN_EOF, tok.Type)
}

func TestLexer_BooleanLiterals(t *testing.T) {
	lex := NewLexer("TRUE FALSE true false")
	tokens := lex.Tokenize()

	require.GreaterOrEqual(t, len(tokens), 4)
	assert.Equal(t, TOKEN_TRUE, tokens[0].Type)
	assert.Equal(t, TOKEN_FALSE, tokens[1].Type)
	assert.Equal(t, TOKEN_TRUE, tokens[2].Type)
	assert.Equal(t, TOKEN_FALSE, tokens[3].Type)
}

func TestLexer_IllegalCharacter(t *testing.T) {
	lex := NewLexer("@")
	tok := lex.NextToken()
	assert.Equal(t, TOKEN_ILLEGAL, tok.Type)
	assert.Equal(t, "@", tok.Literal)
}
