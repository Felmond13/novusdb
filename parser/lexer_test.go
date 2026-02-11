package parser

import (
	"testing"
)

func TestLexerSelect(t *testing.T) {
	input := `SELECT * FROM workflows WHERE retry > 3`
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	expected := []TokenType{
		TokenSelect, TokenStar, TokenFrom, TokenIdent, TokenWhere,
		TokenIdent, TokenGT, TokenInteger, TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token[%d]: expected type %d, got %d (%q)", i, expected[i], tok.Type, tok.Literal)
		}
	}
}

func TestLexerInsert(t *testing.T) {
	input := `INSERT INTO jobs VALUES (type="oracle", retry=5)`
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	expected := []TokenType{
		TokenInsert, TokenInto, TokenIdent, TokenValues, TokenLParen,
		TokenIdent, TokenEQ, TokenString, TokenComma,
		TokenIdent, TokenEQ, TokenInteger, TokenRParen, TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token[%d]: expected type %d, got %d (%q)", i, expected[i], tok.Type, tok.Literal)
		}
	}
}

func TestLexerUpdate(t *testing.T) {
	input := `UPDATE jobs SET params.timeout=60 WHERE params.timeout<30`
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	expected := []TokenType{
		TokenUpdate, TokenIdent, TokenSet,
		TokenIdent, TokenDot, TokenIdent, TokenEQ, TokenInteger,
		TokenWhere,
		TokenIdent, TokenDot, TokenIdent, TokenLT, TokenInteger,
		TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token[%d]: expected type %d, got %d (%q)", i, expected[i], tok.Type, tok.Literal)
		}
	}
}

func TestLexerDelete(t *testing.T) {
	input := `DELETE FROM jobs WHERE enabled=false`
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	expected := []TokenType{
		TokenDelete, TokenFrom, TokenIdent, TokenWhere,
		TokenIdent, TokenEQ, TokenFalse, TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token[%d]: expected type %d, got %d (%q)", i, expected[i], tok.Type, tok.Literal)
		}
	}
}

func TestLexerOperators(t *testing.T) {
	input := `= != <> < > <= >=`
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	expected := []TokenType{
		TokenEQ, TokenNEQ, TokenNEQ, TokenLT, TokenGT, TokenLTE, TokenGTE, TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("token[%d]: expected type %d, got %d (%q)", i, expected[i], tok.Type, tok.Literal)
		}
	}
}

func TestLexerFloat(t *testing.T) {
	input := `3.14 -2.5`
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()

	if tokens[0].Type != TokenFloat || tokens[0].Literal != "3.14" {
		t.Errorf("expected float 3.14, got %v", tokens[0])
	}
	// '-' is now a separate operator token; 2.5 is the float
	if tokens[1].Type != TokenMinus {
		t.Errorf("expected TokenMinus, got %v", tokens[1])
	}
	if tokens[2].Type != TokenFloat || tokens[2].Literal != "2.5" {
		t.Errorf("expected float 2.5, got %v", tokens[2])
	}
}
