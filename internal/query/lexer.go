package query

import (
	"strings"
	"unicode"
)

// Lexer tokenizes a NalaQL input string into a sequence of tokens.
type Lexer struct {
	input []rune
	pos   int
	line  int
	col   int
}

// NewLexer creates a new Lexer for the given input string.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: []rune(input),
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Tokenize returns all tokens from the input, including the final EOF token.
func (l *Lexer) Tokenize() []Token {
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TOKEN_EOF {
			break
		}
		if tok.Type == TOKEN_ILLEGAL {
			break
		}
	}
	return tokens
}

func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) advance() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	ch := l.input[l.pos]
	l.pos++
	if ch == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
	return ch
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		l.advance()
	}
}

func (l *Lexer) skipLineComment() {
	for l.pos < len(l.input) && l.input[l.pos] != '\n' {
		l.advance()
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TOKEN_EOF, Line: l.line, Col: l.col}
	}

	// Skip line comments (// ...)
	if l.pos+1 < len(l.input) && l.input[l.pos] == '/' && l.input[l.pos+1] == '/' {
		l.skipLineComment()
		return l.NextToken()
	}

	line, col := l.line, l.col
	ch := l.peek()

	switch {
	case ch == '(':
		l.advance()
		return Token{Type: TOKEN_LPAREN, Literal: "(", Line: line, Col: col}
	case ch == ')':
		l.advance()
		return Token{Type: TOKEN_RPAREN, Literal: ")", Line: line, Col: col}
	case ch == '[':
		l.advance()
		return Token{Type: TOKEN_LBRACKET, Literal: "[", Line: line, Col: col}
	case ch == ']':
		l.advance()
		return Token{Type: TOKEN_RBRACKET, Literal: "]", Line: line, Col: col}
	case ch == ':':
		l.advance()
		return Token{Type: TOKEN_COLON, Literal: ":", Line: line, Col: col}
	case ch == '.':
		l.advance()
		return Token{Type: TOKEN_DOT, Literal: ".", Line: line, Col: col}
	case ch == ',':
		l.advance()
		return Token{Type: TOKEN_COMMA, Literal: ",", Line: line, Col: col}
	case ch == ';':
		l.advance()
		return Token{Type: TOKEN_SEMICOLON, Literal: ";", Line: line, Col: col}
	case ch == '*':
		l.advance()
		return Token{Type: TOKEN_STAR, Literal: "*", Line: line, Col: col}
	case ch == '+':
		l.advance()
		return Token{Type: TOKEN_PLUS, Literal: "+", Line: line, Col: col}
	case ch == '=':
		l.advance()
		return Token{Type: TOKEN_EQ, Literal: "=", Line: line, Col: col}
	case ch == '!':
		l.advance()
		if l.peek() == '=' {
			l.advance()
			return Token{Type: TOKEN_NEQ, Literal: "!=", Line: line, Col: col}
		}
		return Token{Type: TOKEN_ILLEGAL, Literal: "!", Line: line, Col: col}
	case ch == '>':
		l.advance()
		if l.peek() == '=' {
			l.advance()
			return Token{Type: TOKEN_GTE, Literal: ">=", Line: line, Col: col}
		}
		return Token{Type: TOKEN_GT, Literal: ">", Line: line, Col: col}
	case ch == '<':
		l.advance()
		if l.peek() == '=' {
			l.advance()
			return Token{Type: TOKEN_LTE, Literal: "<=", Line: line, Col: col}
		}
		if l.peek() == '-' {
			l.advance()
			return Token{Type: TOKEN_LARROW, Literal: "<-", Line: line, Col: col}
		}
		return Token{Type: TOKEN_LT, Literal: "<", Line: line, Col: col}
	case ch == '-':
		l.advance()
		if l.peek() == '>' {
			l.advance()
			return Token{Type: TOKEN_ARROW, Literal: "->", Line: line, Col: col}
		}
		return Token{Type: TOKEN_DASH, Literal: "-", Line: line, Col: col}
	case ch == '"':
		return l.readString(line, col)
	case unicode.IsDigit(ch):
		return l.readNumber(line, col)
	case isIdentStart(ch):
		return l.readIdentifier(line, col)
	default:
		l.advance()
		return Token{Type: TOKEN_ILLEGAL, Literal: string(ch), Line: line, Col: col}
	}
}

func (l *Lexer) readString(line, col int) Token {
	l.advance() // consume opening "
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.peek()
		if ch == '"' {
			l.advance() // consume closing "
			return Token{Type: TOKEN_STRING, Literal: sb.String(), Line: line, Col: col}
		}
		if ch == '\\' {
			l.advance()
			esc := l.advance()
			switch esc {
			case 'n':
				sb.WriteRune('\n')
			case 't':
				sb.WriteRune('\t')
			case '"':
				sb.WriteRune('"')
			case '\\':
				sb.WriteRune('\\')
			default:
				sb.WriteRune('\\')
				sb.WriteRune(esc)
			}
			continue
		}
		if ch == '\n' || ch == 0 {
			return Token{Type: TOKEN_ILLEGAL, Literal: "unterminated string", Line: line, Col: col}
		}
		sb.WriteRune(l.advance())
	}
	return Token{Type: TOKEN_ILLEGAL, Literal: "unterminated string", Line: line, Col: col}
}

func (l *Lexer) readNumber(line, col int) Token {
	var sb strings.Builder
	isFloat := false
	for l.pos < len(l.input) {
		ch := l.peek()
		if ch == '.' {
			if isFloat {
				break // second dot ends the number
			}
			isFloat = true
			sb.WriteRune(l.advance())
			continue
		}
		if !unicode.IsDigit(ch) {
			break
		}
		sb.WriteRune(l.advance())
	}

	lit := sb.String()
	if isFloat {
		return Token{Type: TOKEN_FLOAT, Literal: lit, Line: line, Col: col}
	}
	return Token{Type: TOKEN_INT, Literal: lit, Line: line, Col: col}
}

func (l *Lexer) readIdentifier(line, col int) Token {
	var sb strings.Builder
	for l.pos < len(l.input) && isIdentPart(l.peek()) {
		sb.WriteRune(l.advance())
	}
	lit := sb.String()
	tokType := LookupKeyword(lit)
	return Token{Type: tokType, Literal: lit, Line: line, Col: col}
}

func isIdentStart(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_'
}

func isIdentPart(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_'
}
