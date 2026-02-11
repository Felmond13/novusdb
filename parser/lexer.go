package parser

import (
	"strings"
	"unicode"
)

// Lexer découpe une chaîne SQL-like en tokens.
type Lexer struct {
	input string
	pos   int  // position courante
	ch    byte // caractère courant (0 si fin)
}

// NewLexer crée un nouveau lexer pour l'entrée donnée.
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	if len(input) > 0 {
		l.ch = input[0]
	}
	return l
}

// advance avance d'un caractère.
func (l *Lexer) advance() {
	l.pos++
	if l.pos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.pos]
	}
}

// peek retourne le prochain caractère sans avancer.
func (l *Lexer) peek() byte {
	if l.pos+1 >= len(l.input) {
		return 0
	}
	return l.input[l.pos+1]
}

// skipWhitespace saute les espaces et tabulations.
func (l *Lexer) skipWhitespace() {
	for l.ch != 0 && (l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r') {
		l.advance()
	}
}

// NextToken retourne le prochain token.
func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	pos := l.pos

	if l.ch == 0 {
		return Token{Type: TokenEOF, Literal: "", Pos: pos}
	}

	// Chaîne entre guillemets simples ou doubles
	if l.ch == '"' || l.ch == '\'' {
		return l.readString(pos)
	}

	// Nombre (entier ou flottant)
	if isDigit(l.ch) {
		return l.readNumber(pos)
	}

	// Identifiant ou mot-clé
	if isLetter(l.ch) || l.ch == '_' {
		return l.readIdentifier(pos)
	}

	// Opérateurs et ponctuation
	switch l.ch {
	case '*':
		l.advance()
		return Token{Type: TokenStar, Literal: "*", Pos: pos}
	case ',':
		l.advance()
		return Token{Type: TokenComma, Literal: ",", Pos: pos}
	case '.':
		l.advance()
		return Token{Type: TokenDot, Literal: ".", Pos: pos}
	case '(':
		l.advance()
		return Token{Type: TokenLParen, Literal: "(", Pos: pos}
	case ')':
		l.advance()
		return Token{Type: TokenRParen, Literal: ")", Pos: pos}
	case '=':
		l.advance()
		return Token{Type: TokenEQ, Literal: "=", Pos: pos}
	case '!':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenNEQ, Literal: "!=", Pos: pos}
		}
		l.advance()
		return Token{Type: TokenIllegal, Literal: "!", Pos: pos}
	case '<':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenLTE, Literal: "<=", Pos: pos}
		}
		if l.peek() == '>' {
			l.advance()
			l.advance()
			return Token{Type: TokenNEQ, Literal: "<>", Pos: pos}
		}
		l.advance()
		return Token{Type: TokenLT, Literal: "<", Pos: pos}
	case '>':
		if l.peek() == '=' {
			l.advance()
			l.advance()
			return Token{Type: TokenGTE, Literal: ">=", Pos: pos}
		}
		l.advance()
		return Token{Type: TokenGT, Literal: ">", Pos: pos}
	case '+':
		l.advance()
		return Token{Type: TokenPlus, Literal: "+", Pos: pos}
	case '-':
		// -- commentaire sur une ligne → ignorer jusqu'à fin de ligne
		if l.peek() == '-' {
			l.advance() // skip first '-'
			l.advance() // skip second '-'
			for l.ch != 0 && l.ch != '\n' {
				l.advance()
			}
			return l.NextToken()
		}
		// Si suivi d'un chiffre et que le contexte est "début d'expression" (géré par readNumber)
		// Le cas négatif est déjà géré au-dessus (isDigit check), ici c'est l'opérateur
		l.advance()
		return Token{Type: TokenMinus, Literal: "-", Pos: pos}
	case '/':
		// /*+ ... */ = Oracle-style hint
		if l.peek() == '*' {
			return l.readHintOrComment(pos)
		}
		l.advance()
		return Token{Type: TokenSlash, Literal: "/", Pos: pos}
	case '{':
		l.advance()
		return Token{Type: TokenLBrace, Literal: "{", Pos: pos}
	case '}':
		l.advance()
		return Token{Type: TokenRBrace, Literal: "}", Pos: pos}
	case ':':
		l.advance()
		return Token{Type: TokenColon, Literal: ":", Pos: pos}
	case '[':
		l.advance()
		return Token{Type: TokenLBrack, Literal: "[", Pos: pos}
	case ']':
		l.advance()
		return Token{Type: TokenRBrack, Literal: "]", Pos: pos}
	case '?':
		l.advance()
		return Token{Type: TokenParam, Literal: "?", Pos: pos}
	}

	// Caractère inconnu
	ch := l.ch
	l.advance()
	return Token{Type: TokenIllegal, Literal: string(ch), Pos: pos}
}

// Tokenize retourne tous les tokens de l'entrée.
func (l *Lexer) Tokenize() []Token {
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}
	return tokens
}

func (l *Lexer) readString(startPos int) Token {
	quote := l.ch
	l.advance() // skip opening quote
	start := l.pos
	for l.ch != 0 && l.ch != quote {
		l.advance()
	}
	literal := l.input[start:l.pos]
	if l.ch == quote {
		l.advance() // skip closing quote
	}
	return Token{Type: TokenString, Literal: literal, Pos: startPos}
}

func (l *Lexer) readNumber(startPos int) Token {
	start := l.pos
	isFloat := false

	if l.ch == '-' {
		l.advance()
	}
	for isDigit(l.ch) {
		l.advance()
	}
	if l.ch == '.' && isDigit(l.peek()) {
		isFloat = true
		l.advance() // skip '.'
		for isDigit(l.ch) {
			l.advance()
		}
	}

	literal := l.input[start:l.pos]
	if isFloat {
		return Token{Type: TokenFloat, Literal: literal, Pos: startPos}
	}
	return Token{Type: TokenInteger, Literal: literal, Pos: startPos}
}

func (l *Lexer) readIdentifier(startPos int) Token {
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.advance()
	}
	literal := l.input[start:l.pos]
	tokType := LookupIdent(strings.ToLower(literal))
	return Token{Type: tokType, Literal: literal, Pos: startPos}
}

// readHintOrComment lit un commentaire /* ... */ ou un hint /*+ ... */.
func (l *Lexer) readHintOrComment(startPos int) Token {
	l.advance() // skip '/'
	l.advance() // skip '*'

	isHint := l.ch == '+'
	if isHint {
		l.advance() // skip '+'
	}

	// Lire le contenu jusqu'à */
	start := l.pos
	for l.ch != 0 {
		if l.ch == '*' && l.peek() == '/' {
			content := strings.TrimSpace(l.input[start:l.pos])
			l.advance() // skip '*'
			l.advance() // skip '/'
			if isHint {
				return Token{Type: TokenHint, Literal: content, Pos: startPos}
			}
			// Commentaire normal → ignorer, retourner le prochain token
			return l.NextToken()
		}
		l.advance()
	}
	// Pas de fermeture → traiter comme illégal
	return Token{Type: TokenIllegal, Literal: l.input[startPos:l.pos], Pos: startPos}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}
