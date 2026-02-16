package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser analyse une séquence de tokens et produit un AST.
type Parser struct {
	lexer      *Lexer
	current    Token
	peek       Token
	paramIndex int // auto-incrementing index for ? placeholders
}

// NewParser crée un parser pour l'entrée SQL-like donnée.
func NewParser(input string) *Parser {
	p := &Parser{lexer: NewLexer(input)}
	// Charger les deux premiers tokens
	p.current = p.lexer.NextToken()
	p.peek = p.lexer.NextToken()
	return p
}

// advance avance d'un token.
func (p *Parser) advance() {
	p.current = p.peek
	p.peek = p.lexer.NextToken()
}

// parserState sauvegarde l'état complet du parser pour le lookahead.
type parserState struct {
	current Token
	peek    Token
	lexPos  int
	lexCh   byte
}

func (p *Parser) saveState() parserState {
	return parserState{
		current: p.current,
		peek:    p.peek,
		lexPos:  p.lexer.pos,
		lexCh:   p.lexer.ch,
	}
}

// captureRemaining retourne le texte brut restant depuis le token courant.
// Utilisé pour CREATE VIEW ... AS <remaining>.
func (p *Parser) captureRemaining() string {
	if p.current.Type == TokenEOF {
		return ""
	}
	// Le token courant commence à p.current.Pos dans l'input du lexer
	remaining := strings.TrimSpace(p.lexer.input[p.current.Pos:])
	// Avancer jusqu'à EOF
	for p.current.Type != TokenEOF {
		p.advance()
	}
	return remaining
}

func (p *Parser) restoreState(s parserState) {
	p.current = s.current
	p.peek = s.peek
	p.lexer.pos = s.lexPos
	p.lexer.ch = s.lexCh
}

// expectNumber vérifie que le token courant est un entier ou un flottant et avance.
func (p *Parser) expectNumber() (Token, error) {
	if p.current.Type != TokenInteger && p.current.Type != TokenFloat {
		return Token{}, fmt.Errorf("parser: expected number, got %d (%q) at pos %d",
			p.current.Type, p.current.Literal, p.current.Pos)
	}
	tok := p.current
	p.advance()
	return tok, nil
}

// expect vérifie que le token courant est du type attendu et avance.
func (p *Parser) expect(t TokenType) (Token, error) {
	if p.current.Type != t {
		return Token{}, fmt.Errorf("parser: expected token %d, got %d (%q) at pos %d",
			t, p.current.Type, p.current.Literal, p.current.Pos)
	}
	tok := p.current
	p.advance()
	return tok, nil
}

// parseOptionalAlias parse un alias optionnel après un nom de table.
// Accepte : "AS alias" ou juste "alias" (si c'est un ident simple non-keyword).
func (p *Parser) parseOptionalAlias() string {
	if p.current.Type == TokenAs {
		p.advance()
		if p.current.Type == TokenIdent {
			alias := p.current.Literal
			p.advance()
			return alias
		}
		return ""
	}
	// Bare alias : seulement si c'est un ident qui n'est pas un mot-clé structurel
	if p.current.Type == TokenIdent && !isStructuralKeyword(p.current.Literal) {
		alias := p.current.Literal
		p.advance()
		return alias
	}
	return ""
}

// isStructuralKeyword retourne true si le mot pourrait être confondu avec une clause SQL.
func isStructuralKeyword(s string) bool {
	switch strings.ToLower(s) {
	case "where", "join", "left", "right", "inner", "on", "order", "group",
		"having", "limit", "offset", "set", "values", "and", "or", "not",
		"in", "is", "as", "asc", "desc", "into", "from", "select",
		"insert", "update", "delete", "create", "drop", "index",
		"like", "distinct", "table", "between", "if", "exists",
		"sequence":
		return true
	}
	return false
}

// Parse analyse l'entrée et retourne un Statement.
func (p *Parser) Parse() (Statement, error) {
	switch p.current.Type {
	case TokenSelect:
		left, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		// Vérifier UNION [ALL] après le SELECT
		if p.current.Type == TokenUnion {
			return p.parseUnion(left)
		}
		return left, nil
	case TokenInsert:
		return p.parseInsert()
	case TokenUpdate:
		return p.parseUpdate()
	case TokenDelete:
		return p.parseDelete()
	case TokenCreate:
		return p.parseCreate()
	case TokenDrop:
		return p.parseDrop()
	case TokenExplain:
		return p.parseExplain()
	case TokenTruncate:
		return p.parseTruncate()
	case TokenAnalyze:
		return p.parseAnalyze()
	case TokenAlter:
		return p.parseAlterTable()
	default:
		return nil, fmt.Errorf("parser: unexpected token %q at pos %d", p.current.Literal, p.current.Pos)
	}
}

// ---------- Hints ----------

// parseHints parse les hints Oracle-style /*+ ... */ après un mot-clé SQL.
// Format supporté : /*+ HINT1 HINT2(param) HINT3 */
func (p *Parser) parseHints() []QueryHint {
	if p.current.Type != TokenHint {
		return nil
	}
	raw := p.current.Literal
	p.advance()
	return parseHintString(raw)
}

// parseHintString parse le contenu textuel d'un hint (sans les délimiteurs /*+ */).
func parseHintString(raw string) []QueryHint {
	var hints []QueryHint
	raw = strings.TrimSpace(raw)
	i := 0
	for i < len(raw) {
		// Sauter les espaces
		for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t') {
			i++
		}
		if i >= len(raw) {
			break
		}
		// Lire le nom du hint
		start := i
		for i < len(raw) && raw[i] != '(' && raw[i] != ' ' && raw[i] != '\t' {
			i++
		}
		name := strings.ToUpper(raw[start:i])
		// Lire le paramètre optionnel entre parenthèses
		param := ""
		if i < len(raw) && raw[i] == '(' {
			i++ // skip '('
			pStart := i
			for i < len(raw) && raw[i] != ')' {
				i++
			}
			param = strings.TrimSpace(raw[pStart:i])
			if i < len(raw) {
				i++ // skip ')'
			}
		}
		switch name {
		case "PARALLEL":
			hints = append(hints, QueryHint{Type: HintParallel, Param: param})
		case "NO_CACHE":
			hints = append(hints, QueryHint{Type: HintNoCache})
		case "FULL_SCAN":
			hints = append(hints, QueryHint{Type: HintFullScan})
		case "FORCE_INDEX":
			hints = append(hints, QueryHint{Type: HintForceIndex, Param: param})
		case "HASH_JOIN":
			hints = append(hints, QueryHint{Type: HintHashJoin})
		case "NESTED_LOOP":
			hints = append(hints, QueryHint{Type: HintNestedLoop})
		}
	}
	return hints
}

// ---------- SELECT ----------

func (p *Parser) parseSelect() (*SelectStatement, error) {
	p.advance() // skip SELECT

	stmt := &SelectStatement{Limit: -1}

	// Hints optionnels
	stmt.Hints = p.parseHints()

	// DISTINCT optionnel
	if p.current.Type == TokenDistinct {
		stmt.Distinct = true
		p.advance()
	}

	// Colonnes
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if _, err := p.expect(TokenFrom); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	stmt.From = tableTok.Literal
	stmt.FromAlias = p.parseOptionalAlias()

	// JOINs optionnels
	for p.current.Type == TokenJoin || p.current.Type == TokenLeft ||
		p.current.Type == TokenRight || p.current.Type == TokenInner {
		join, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		stmt.Joins = append(stmt.Joins, join)
	}

	// WHERE optionnel
	if p.current.Type == TokenWhere {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// GROUP BY optionnel
	if p.current.Type == TokenGroupBy {
		p.advance()
		// Consommer "BY" s'il est présent (le lexer renvoie TokenGroupBy pour "group")
		if p.current.Type == TokenIdent && strings.ToLower(p.current.Literal) == "by" {
			p.advance()
		}
		gb, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = gb

		// HAVING optionnel
		if p.current.Type == TokenHaving {
			p.advance()
			having, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			stmt.Having = having
		}
	}

	// ORDER BY optionnel
	if p.current.Type == TokenOrderBy {
		p.advance()
		if p.current.Type == TokenIdent && strings.ToLower(p.current.Literal) == "by" {
			p.advance()
		}
		ob, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = ob
	}

	// LIMIT optionnel
	if p.current.Type == TokenLimit {
		p.advance()
		tok, err := p.expect(TokenInteger)
		if err != nil {
			return nil, err
		}
		stmt.Limit, _ = strconv.Atoi(tok.Literal)
	}

	// OFFSET optionnel
	if p.current.Type == TokenOffset {
		p.advance()
		tok, err := p.expect(TokenInteger)
		if err != nil {
			return nil, err
		}
		stmt.Offset, _ = strconv.Atoi(tok.Literal)
	}

	return stmt, nil
}

// ---------- UNION ----------

func (p *Parser) parseUnion(left *SelectStatement) (*UnionStatement, error) {
	p.advance() // skip UNION
	all := false
	if p.current.Type == TokenAll {
		all = true
		p.advance()
	}
	if p.current.Type != TokenSelect {
		return nil, fmt.Errorf("parser: expected SELECT after UNION at pos %d", p.current.Pos)
	}
	right, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	return &UnionStatement{Left: left, Right: right, All: all}, nil
}

func (p *Parser) parseSelectColumns() ([]Expr, error) {
	var cols []Expr
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		// Alias optionnel
		if p.current.Type == TokenAs {
			p.advance()
			aliasTok, err := p.expect(TokenIdent)
			if err != nil {
				return nil, err
			}
			col = &AliasExpr{Expr: col, Alias: aliasTok.Literal}
		}
		cols = append(cols, col)
		if p.current.Type != TokenComma {
			break
		}
		p.advance() // skip comma
	}
	return cols, nil
}

func (p *Parser) parseSelectColumn() (Expr, error) {
	if p.current.Type == TokenStar {
		p.advance()
		return &StarExpr{}, nil
	}
	// Fonctions d'agrégation
	if isAggregateFunc(p.current.Type) {
		return p.parseFuncCall()
	}
	// Vérifier A.* (qualified star) avec lookahead fiable
	if p.current.Type == TokenIdent && p.peek.Type == TokenDot {
		state := p.saveState()
		qualifier := p.current.Literal
		p.advance() // skip ident → current=dot
		if p.peek.Type == TokenStar {
			p.advance() // skip dot → current=*
			p.advance() // skip *
			return &QualifiedStarExpr{Qualifier: qualifier}, nil
		}
		// Pas un qualified star → restaurer l'état complet
		p.restoreState(state)
	}
	// Expressions générales : littéraux, arithmétique, champs
	return p.parseAddSub()
}

func (p *Parser) parseFuncCall() (Expr, error) {
	name := strings.ToUpper(p.current.Literal)
	p.advance()
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}
	distinct := false
	// COUNT(DISTINCT field)
	if p.current.Type == TokenDistinct {
		distinct = true
		p.advance()
	}
	var args []Expr
	if p.current.Type != TokenRParen {
		for {
			if p.current.Type == TokenStar {
				args = append(args, &StarExpr{})
				p.advance()
			} else {
				arg, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
			}
			if p.current.Type != TokenComma {
				break
			}
			p.advance()
		}
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}
	return &FuncCallExpr{Name: name, Args: args, Distinct: distinct}, nil
}

func isAggregateFunc(t TokenType) bool {
	return t == TokenCount || t == TokenSum || t == TokenAvg || t == TokenMin || t == TokenMax
}

// isScalarFunc vérifie si un nom (en majuscules) est une fonction scalaire connue.
func isScalarFunc(name string) bool {
	switch name {
	case "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM",
		"LENGTH", "SUBSTR", "SUBSTRING", "CONCAT", "REPLACE",
		"ABS", "ROUND", "CEIL", "FLOOR",
		"COALESCE", "TYPEOF", "IFNULL", "NULLIF",
		"INSTR", "REPEAT", "REVERSE",
		"CAST", "PRINTF", "HEX":
		return true
	}
	return false
}

// ---------- JOIN ----------

func (p *Parser) parseJoin() (*JoinClause, error) {
	joinType := "INNER"
	switch p.current.Type {
	case TokenLeft:
		joinType = "LEFT"
		p.advance()
	case TokenRight:
		joinType = "RIGHT"
		p.advance()
	case TokenInner:
		joinType = "INNER"
		p.advance()
	}
	if _, err := p.expect(TokenJoin); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	alias := p.parseOptionalAlias()
	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}
	cond, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	return &JoinClause{Type: joinType, Table: tableTok.Literal, Alias: alias, Condition: cond}, nil
}

// ---------- ORDER BY ----------

func (p *Parser) parseOrderBy() ([]*OrderByExpr, error) {
	var result []*OrderByExpr
	for {
		expr, err := p.parseFieldRef()
		if err != nil {
			return nil, err
		}
		desc := false
		switch p.current.Type {
		case TokenAsc:
			p.advance()
		case TokenDesc:
			desc = true
			p.advance()
		}
		result = append(result, &OrderByExpr{Expr: expr, Desc: desc})
		if p.current.Type != TokenComma {
			break
		}
		p.advance()
	}
	return result, nil
}

// ---------- INSERT ----------

func (p *Parser) parseInsert() (*InsertStatement, error) {
	p.advance() // skip INSERT

	// INSERT OR REPLACE INTO ...
	orReplace := false
	if p.current.Type == TokenOr {
		p.advance()
		if _, err := p.expect(TokenReplace); err != nil {
			return nil, err
		}
		orReplace = true
	}

	if _, err := p.expect(TokenInto); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}

	// INSERT INTO table SELECT ... (insert from select)
	if p.current.Type == TokenSelect {
		selectStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		return &InsertStatement{Table: tableTok.Literal, Source: selectStmt, OrReplace: orReplace}, nil
	}

	// INSERT INTO table VALUES (field=value, ...) [, (field=value, ...) ...]
	// INSERT INTO table VALUES ({"key": val, ...})  — JSON inside parens
	// INSERT INTO table VALUES {"key": val, ...}    — bare JSON object
	if _, err := p.expect(TokenValues); err != nil {
		return nil, err
	}

	var rows [][]FieldAssignment
	for {
		if p.current.Type == TokenLBrace {
			// Bare JSON object : VALUES {"key": val}
			docLit, err := p.parseDocumentLiteral()
			if err != nil {
				return nil, err
			}
			rows = append(rows, docLit.Fields)
		} else {
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, err
			}
			if p.current.Type == TokenLBrace {
				// JSON inside parens : VALUES ({"key": val})
				docLit, err := p.parseDocumentLiteral()
				if err != nil {
					return nil, err
				}
				if _, err := p.expect(TokenRParen); err != nil {
					return nil, err
				}
				rows = append(rows, docLit.Fields)
			} else {
				fields, err := p.parseFieldAssignments()
				if err != nil {
					return nil, err
				}
				if _, err := p.expect(TokenRParen); err != nil {
					return nil, err
				}
				rows = append(rows, fields)
			}
		}
		if p.current.Type != TokenComma {
			break
		}
		p.advance() // skip comma between value groups
	}

	return &InsertStatement{
		Table:     tableTok.Literal,
		Fields:    rows[0],
		Rows:      rows,
		OrReplace: orReplace,
	}, nil
}

// ---------- UPDATE ----------

func (p *Parser) parseUpdate() (*UpdateStatement, error) {
	p.advance() // skip UPDATE
	hints := p.parseHints()
	tableTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenSet); err != nil {
		return nil, err
	}
	assignments, err := p.parseUpdateAssignments()
	if err != nil {
		return nil, err
	}
	var where Expr
	if p.current.Type == TokenWhere {
		p.advance()
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	return &UpdateStatement{Hints: hints, Table: tableTok.Literal, Assignments: assignments, Where: where}, nil
}

// ---------- DELETE ----------

func (p *Parser) parseDelete() (*DeleteStatement, error) {
	p.advance() // skip DELETE
	hints := p.parseHints()
	if _, err := p.expect(TokenFrom); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	var where Expr
	if p.current.Type == TokenWhere {
		p.advance()
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}
	return &DeleteStatement{Hints: hints, Table: tableTok.Literal, Where: where}, nil
}

// ---------- CREATE INDEX / CREATE VIEW / DROP ----------

func (p *Parser) parseCreate() (Statement, error) {
	p.advance() // skip CREATE
	if p.current.Type == TokenView {
		return p.parseCreateView()
	}
	if p.current.Type == TokenSequence {
		return p.parseCreateSequence()
	}
	return p.parseCreateIndex()
}

func (p *Parser) parseCreateView() (*CreateViewStatement, error) {
	p.advance() // skip VIEW
	nameTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenAs); err != nil {
		return nil, fmt.Errorf("parser: expected AS after view name: %w", err)
	}
	// Capturer tout le reste comme la requête SQL source
	query := p.captureRemaining()
	return &CreateViewStatement{Name: nameTok.Literal, Query: query}, nil
}

// ---------- CREATE SEQUENCE ----------

func (p *Parser) parseCreateSequence() (*CreateSequenceStatement, error) {
	p.advance() // skip SEQUENCE
	nameTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	stmt := &CreateSequenceStatement{
		Name:        nameTok.Literal,
		StartWith:   1,
		IncrementBy: 1,
		MinValue:    1,
		MaxValue:    9999999999,
		Cycle:       false,
	}
	// Options optionnelles (ordre libre)
	for p.current.Type == TokenIdent || p.current.Type == TokenMin || p.current.Type == TokenMax {
		kw := strings.ToUpper(p.current.Literal)
		switch kw {
		case "START":
			p.advance()
			// "WITH" optionnel
			if p.current.Type == TokenIdent && strings.ToUpper(p.current.Literal) == "WITH" {
				p.advance()
			}
			tok, err := p.expectNumber()
			if err != nil {
				return nil, fmt.Errorf("CREATE SEQUENCE: expected number after START WITH: %w", err)
			}
			stmt.StartWith, _ = strconv.ParseFloat(tok.Literal, 64)
		case "INCREMENT":
			p.advance()
			// "BY" optionnel
			if p.current.Type == TokenIdent && strings.ToUpper(p.current.Literal) == "BY" {
				p.advance()
			}
			tok, err := p.expectNumber()
			if err != nil {
				return nil, fmt.Errorf("CREATE SEQUENCE: expected number after INCREMENT BY: %w", err)
			}
			stmt.IncrementBy, _ = strconv.ParseFloat(tok.Literal, 64)
		case "MINVALUE":
			p.advance()
			tok, err := p.expectNumber()
			if err != nil {
				return nil, fmt.Errorf("CREATE SEQUENCE: expected number after MINVALUE: %w", err)
			}
			stmt.MinValue, _ = strconv.ParseFloat(tok.Literal, 64)
		case "MAXVALUE":
			p.advance()
			tok, err := p.expectNumber()
			if err != nil {
				return nil, fmt.Errorf("CREATE SEQUENCE: expected number after MAXVALUE: %w", err)
			}
			stmt.MaxValue, _ = strconv.ParseFloat(tok.Literal, 64)
		case "CYCLE":
			p.advance()
			stmt.Cycle = true
		case "NOCYCLE":
			p.advance()
			stmt.Cycle = false
		default:
			// Mot-clé inconnu → fin des options
			goto done
		}
	}
done:
	return stmt, nil
}

func (p *Parser) parseCreateIndex() (*CreateIndexStatement, error) {
	if _, err := p.expect(TokenIndex); err != nil {
		return nil, err
	}

	// Nom optionnel de l'index : CREATE INDEX [name] ...
	var indexName string

	// IF NOT EXISTS
	ifNotExists := false
	if p.current.Type == TokenIf {
		p.advance()
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		ifNotExists = true
	} else if p.current.Type == TokenIdent {
		// C'est un nom d'index : CREATE INDEX idx_name ON ...
		indexName = p.current.Literal
		p.advance()
		// Vérifier IF NOT EXISTS après le nom
		if p.current.Type == TokenIf {
			p.advance()
			if _, err := p.expect(TokenNot); err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenExists); err != nil {
				return nil, err
			}
			ifNotExists = true
		}
	}

	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}
	tableTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}
	fieldTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	fieldName := fieldTok.Literal
	for p.current.Type == TokenDot {
		p.advance() // skip '.'
		next, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		fieldName += "." + next.Literal
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	// Auto-générer le nom si absent : idx_<table>_<field>
	if indexName == "" {
		indexName = "idx_" + tableTok.Literal + "_" + fieldTok.Literal
	}

	return &CreateIndexStatement{Name: indexName, Table: tableTok.Literal, Field: fieldName, IfNotExists: ifNotExists}, nil
}

func (p *Parser) parseDrop() (Statement, error) {
	p.advance() // skip DROP

	// DROP SEQUENCE [IF EXISTS] <name>
	if p.current.Type == TokenSequence {
		p.advance()
		ifExists := false
		if p.current.Type == TokenIf {
			p.advance()
			if _, err := p.expect(TokenExists); err != nil {
				return nil, err
			}
			ifExists = true
		}
		nameTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		return &DropSequenceStatement{Name: nameTok.Literal, IfExists: ifExists}, nil
	}

	// DROP VIEW [IF EXISTS] <name>
	if p.current.Type == TokenView {
		p.advance()
		ifExists := false
		if p.current.Type == TokenIf {
			p.advance()
			if _, err := p.expect(TokenExists); err != nil {
				return nil, err
			}
			ifExists = true
		}
		nameTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		return &DropViewStatement{Name: nameTok.Literal, IfExists: ifExists}, nil
	}

	// DROP TABLE [IF EXISTS] <name>
	if p.current.Type == TokenTable {
		p.advance()
		ifExists := false
		if p.current.Type == TokenIf {
			p.advance()
			if _, err := p.expect(TokenExists); err != nil {
				return nil, err
			}
			ifExists = true
		}
		tableTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		return &DropTableStatement{Table: tableTok.Literal, IfExists: ifExists}, nil
	}

	// DROP INDEX [IF EXISTS] <name>
	// DROP INDEX [IF EXISTS] ON <table> (<field>)
	if _, err := p.expect(TokenIndex); err != nil {
		return nil, err
	}
	ifExists := false
	if p.current.Type == TokenIf {
		p.advance()
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		ifExists = true
	}

	// Si le token suivant est ON → ancienne syntaxe DROP INDEX ON table(field)
	if p.current.Type == TokenOn {
		p.advance() // skip ON
		tableTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		fieldTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, err
		}
		fieldName := fieldTok.Literal
		for p.current.Type == TokenDot {
			p.advance()
			next, err := p.expect(TokenIdent)
			if err != nil {
				return nil, err
			}
			fieldName += "." + next.Literal
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return &DropIndexStatement{Table: tableTok.Literal, Field: fieldName, IfExists: ifExists}, nil
	}

	// Sinon → nouvelle syntaxe : DROP INDEX <name>
	nameTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	return &DropIndexStatement{Name: nameTok.Literal, IfExists: ifExists}, nil
}

// ---------- Expressions ----------

// ---------- EXPLAIN ----------

func (p *Parser) parseExplain() (*ExplainStatement, error) {
	p.advance() // skip EXPLAIN
	inner, err := p.Parse()
	if err != nil {
		return nil, err
	}
	return &ExplainStatement{Inner: inner}, nil
}

// ---------- TRUNCATE ----------

func (p *Parser) parseTruncate() (*TruncateTableStatement, error) {
	p.advance() // skip TRUNCATE
	// TABLE est optionnel
	if p.current.Type == TokenTable {
		p.advance()
	}
	tableTok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	return &TruncateTableStatement{Table: tableTok.Literal}, nil
}

// ---------- ANALYZE ----------

func (p *Parser) parseAnalyze() (*AnalyzeStatement, error) {
	p.advance() // skip ANALYZE
	stmt := &AnalyzeStatement{}
	// TABLE est optionnel
	if p.current.Type == TokenTable {
		p.advance()
	}
	// Nom de table optionnel (si absent → toutes les collections)
	if p.current.Type == TokenIdent {
		stmt.Table = p.current.Literal
		p.advance()
	}
	return stmt, nil
}

// parseExpr analyse une expression avec priorité (OR < AND < comparaison).
func (p *Parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.current.Type == TokenOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: TokenOr, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.current.Type == TokenAnd {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: TokenAnd, Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.current.Type == TokenNot &&
		p.peek.Type != TokenLike &&
		p.peek.Type != TokenBetween &&
		p.peek.Type != TokenIn {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Expr: expr}, nil
	}
	return p.parseComparison()
}

// parseAddSub parse les expressions d'addition et soustraction.
func (p *Parser) parseAddSub() (Expr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.current.Type == TokenPlus || p.current.Type == TokenMinus {
		op := p.current.Type
		p.advance()
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

// parseMulDiv parse les expressions de multiplication et division.
func (p *Parser) parseMulDiv() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.current.Type == TokenStar || p.current.Type == TokenSlash {
		op := p.current.Type
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}
	return left, nil
}

// parseUnary parse le moins unaire.
func (p *Parser) parseUnary() (Expr, error) {
	if p.current.Type == TokenMinus {
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		// Optimiser : si c'est un literal entier ou float, on le rend négatif directement
		if lit, ok := expr.(*LiteralExpr); ok {
			if lit.Token.Type == TokenInteger {
				lit.Token.Literal = "-" + lit.Token.Literal
				return lit, nil
			}
			if lit.Token.Type == TokenFloat {
				lit.Token.Literal = "-" + lit.Token.Literal
				return lit, nil
			}
		}
		// Sinon, 0 - expr
		return &BinaryExpr{
			Left:  &LiteralExpr{Token: Token{Type: TokenInteger, Literal: "0"}},
			Op:    TokenMinus,
			Right: expr,
		}, nil
	}
	return p.parsePrimary()
}

func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}

	// IS NULL / IS NOT NULL
	if p.current.Type == TokenIs {
		p.advance()
		negate := false
		if p.current.Type == TokenNot {
			negate = true
			p.advance()
		}
		if _, err := p.expect(TokenNull); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: left, Negate: negate}, nil
	}

	// LIKE / NOT LIKE
	if p.current.Type == TokenLike {
		p.advance()
		patTok, err := p.expect(TokenString)
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Expr: left, Pattern: patTok.Literal, Negate: false}, nil
	}
	if p.current.Type == TokenNot && p.peek.Type == TokenLike {
		p.advance() // skip NOT
		p.advance() // skip LIKE
		patTok, err := p.expect(TokenString)
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Expr: left, Pattern: patTok.Literal, Negate: true}, nil
	}

	// BETWEEN / NOT BETWEEN
	if p.current.Type == TokenBetween {
		p.advance()
		low, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenAnd); err != nil {
			return nil, fmt.Errorf("BETWEEN requires AND: %w", err)
		}
		high, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Low: low, High: high, Negate: false}, nil
	}
	if p.current.Type == TokenNot && p.peek.Type == TokenBetween {
		p.advance() // skip NOT
		p.advance() // skip BETWEEN
		low, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenAnd); err != nil {
			return nil, fmt.Errorf("NOT BETWEEN requires AND: %w", err)
		}
		high, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Low: low, High: high, Negate: true}, nil
	}

	// IN / NOT IN operator
	if p.current.Type == TokenIn || (p.current.Type == TokenNot && p.peek.Type == TokenIn) {
		negate := false
		if p.current.Type == TokenNot {
			negate = true
			p.advance() // skip NOT
		}
		p.advance() // skip IN
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		// IN (SELECT ...) — sous-requête comme source de valeurs
		if p.current.Type == TokenSelect {
			subQ, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
			return &InExpr{Expr: left, Values: []Expr{&SubqueryExpr{Query: subQ}}, Negate: negate}, nil
		}
		values, err := p.parseExprListUntilRParen()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return &InExpr{Expr: left, Values: values, Negate: negate}, nil
	}

	switch p.current.Type {
	case TokenEQ, TokenNEQ, TokenLT, TokenGT, TokenLTE, TokenGTE:
		op := p.current.Type
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil
	}
	return left, nil
}

func (p *Parser) parsePrimary() (Expr, error) {
	switch p.current.Type {
	case TokenLBrace:
		return p.parseDocumentLiteral()

	case TokenNot:
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Expr: expr}, nil

	case TokenLParen:
		p.advance()
		// Sous-requête ?
		if p.current.Type == TokenSelect {
			subQ, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
			return &SubqueryExpr{Query: subQ}, nil
		}
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return expr, nil

	case TokenInteger:
		tok := p.current
		p.advance()
		return &LiteralExpr{Token: tok}, nil

	case TokenFloat:
		tok := p.current
		p.advance()
		return &LiteralExpr{Token: tok}, nil

	case TokenString:
		tok := p.current
		p.advance()
		return &LiteralExpr{Token: tok}, nil

	case TokenTrue, TokenFalse:
		tok := p.current
		p.advance()
		return &LiteralExpr{Token: tok}, nil

	case TokenNull:
		tok := p.current
		p.advance()
		return &LiteralExpr{Token: tok}, nil

	case TokenIdent:
		// SYSDATE / CURRENT_DATE / CURRENT_TIMESTAMP (sans parenthèses)
		upper := strings.ToUpper(p.current.Literal)
		if upper == "SYSDATE" || upper == "CURRENT_DATE" || upper == "CURRENT_TIMESTAMP" {
			p.advance()
			return &SysdateExpr{Variant: upper}, nil
		}
		// NOW() → SysdateExpr avec Variant "SYSDATE"
		if upper == "NOW" && p.peek.Type == TokenLParen {
			p.advance() // skip NOW
			p.advance() // skip (
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("parser: expected ) after NOW(: %w", err)
			}
			return &SysdateExpr{Variant: "SYSDATE"}, nil
		}
		// Fonction d'agrégation ou référence de champ
		if isAggregateFunc(LookupIdent(strings.ToLower(p.current.Literal))) {
			return p.parseFuncCall()
		}
		// Fonction scalaire : IDENT suivi de '(' et nom connu
		if p.peek.Type == TokenLParen && isScalarFunc(upper) {
			return p.parseFuncCall()
		}
		return p.parseFieldRef()

	case TokenCount, TokenSum, TokenAvg, TokenMin, TokenMax:
		return p.parseFuncCall()

	case TokenReplace:
		// REPLACE() comme fonction scalaire
		if p.peek.Type == TokenLParen {
			return p.parseFuncCall()
		}
		return nil, fmt.Errorf("parser: unexpected REPLACE at pos %d", p.current.Pos)

	case TokenCase:
		return p.parseCaseExpr()

	case TokenParam:
		idx := p.paramIndex
		p.paramIndex++
		p.advance()
		return &ParamExpr{Index: idx}, nil

	default:
		return nil, fmt.Errorf("parser: unexpected token %q (type %d) at pos %d",
			p.current.Literal, p.current.Type, p.current.Pos)
	}
}

// parseFieldRef parse un identifiant pouvant contenir des points (a.b.c),
// des wildcards (* = enfants directs, ** = récursif profond),
// et des références de séquences (seq_name.NEXTVAL / seq_name.CURRVAL).
func (p *Parser) parseFieldRef() (Expr, error) {
	tok, err := p.expect(TokenIdent)
	if err != nil {
		return nil, err
	}
	parts := []string{tok.Literal}
	for p.current.Type == TokenDot {
		p.advance()
		if p.current.Type == TokenStar {
			p.advance() // consommer le premier *
			if p.current.Type == TokenStar {
				p.advance() // consommer le second * → **
				parts = append(parts, "**")
			} else {
				parts = append(parts, "*")
			}
		} else {
			next, err := p.expect(TokenIdent)
			if err != nil {
				return nil, err
			}
			// Détecter seq_name.NEXTVAL / seq_name.CURRVAL
			upper := strings.ToUpper(next.Literal)
			if len(parts) == 1 && (upper == "NEXTVAL" || upper == "CURRVAL") {
				return &SequenceExpr{SeqName: parts[0], Op: upper}, nil
			}
			parts = append(parts, next.Literal)
		}
	}
	if len(parts) == 1 {
		return &IdentExpr{Name: parts[0]}, nil
	}
	return &DotExpr{Parts: parts}, nil
}

// parseUpdateAssignments parse les assignments pour UPDATE SET, supportant les expressions comme valeurs.
func (p *Parser) parseUpdateAssignments() ([]FieldAssignment, error) {
	var assignments []FieldAssignment
	for {
		field, err := p.parseFieldRef()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenEQ); err != nil {
			return nil, err
		}
		value, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, FieldAssignment{Field: field, Value: value})
		if p.current.Type != TokenComma {
			break
		}
		p.advance()
	}
	return assignments, nil
}

// parseDocumentLiteral parse un sous-document littéral.
// Supporte deux syntaxes :
//
//	NovusDB : {key=val, key2=val2}
//	JSON    : {"key": val, "key2": val2}
//
// Les valeurs peuvent être des expressions, sous-documents imbriqués, ou tableaux JSON [].
func (p *Parser) parseDocumentLiteral() (*DocumentLiteralExpr, error) {
	if _, err := p.expect(TokenLBrace); err != nil {
		return nil, err
	}
	var fields []FieldAssignment
	for p.current.Type != TokenRBrace && p.current.Type != TokenEOF {
		// Clé : soit un ident (NovusDB), soit une chaîne (JSON)
		var field Expr
		if p.current.Type == TokenString {
			// JSON-style : "key"
			keyName := p.current.Literal
			p.advance()
			field = &IdentExpr{Name: keyName}
		} else {
			var err error
			field, err = p.parseFieldRef()
			if err != nil {
				return nil, err
			}
		}
		// Séparateur : = (NovusDB) ou : (JSON)
		if p.current.Type == TokenEQ {
			p.advance()
		} else if p.current.Type == TokenColon {
			p.advance()
		} else {
			return nil, fmt.Errorf("parser: expected '=' or ':' after key, got %q at pos %d", p.current.Literal, p.current.Pos)
		}
		// Valeur
		value, err := p.parseJSONValue()
		if err != nil {
			return nil, err
		}
		fields = append(fields, FieldAssignment{Field: field, Value: value})
		if p.current.Type != TokenComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(TokenRBrace); err != nil {
		return nil, err
	}
	return &DocumentLiteralExpr{Fields: fields}, nil
}

// parseJSONValue parse une valeur qui peut être un primaire classique ou un tableau JSON [].
// Utilise parseUnary pour supporter les nombres négatifs (-1200.5).
func (p *Parser) parseJSONValue() (Expr, error) {
	if p.current.Type == TokenLBrack {
		return p.parseArrayLiteral()
	}
	return p.parseUnary()
}

// parseArrayLiteral parse un tableau JSON [val1, val2, ...].
func (p *Parser) parseArrayLiteral() (*ArrayLiteralExpr, error) {
	p.advance() // skip [
	var elems []Expr
	for p.current.Type != TokenRBrack && p.current.Type != TokenEOF {
		elem, err := p.parseJSONValue()
		if err != nil {
			return nil, err
		}
		elems = append(elems, elem)
		if p.current.Type != TokenComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(TokenRBrack); err != nil {
		return nil, err
	}
	return &ArrayLiteralExpr{Elements: elems}, nil
}

// parseCaseExpr parse CASE WHEN cond THEN val [WHEN ...] [ELSE val] END.
func (p *Parser) parseCaseExpr() (Expr, error) {
	p.advance() // skip CASE
	var whens []WhenClause
	for p.current.Type == TokenWhen {
		p.advance() // skip WHEN
		cond, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenThen); err != nil {
			return nil, err
		}
		result, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		whens = append(whens, WhenClause{Condition: cond, Result: result})
	}
	if len(whens) == 0 {
		return nil, fmt.Errorf("parser: CASE requires at least one WHEN clause")
	}
	var elseExpr Expr
	if p.current.Type == TokenElse {
		p.advance()
		var err error
		elseExpr, err = p.parseAddSub()
		if err != nil {
			return nil, err
		}
	}
	if _, err := p.expect(TokenEnd); err != nil {
		return nil, fmt.Errorf("parser: expected END to close CASE expression: %w", err)
	}
	return &CaseExpr{Whens: whens, Else: elseExpr}, nil
}

// parseFieldAssignments parse une liste de champ=valeur séparés par des virgules.
func (p *Parser) parseFieldAssignments() ([]FieldAssignment, error) {
	var assignments []FieldAssignment
	for {
		field, err := p.parseFieldRef()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenEQ); err != nil {
			return nil, err
		}
		value, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, FieldAssignment{Field: field, Value: value})
		if p.current.Type != TokenComma {
			break
		}
		p.advance()
	}
	return assignments, nil
}

// parseExprList parse une liste d'expressions séparées par des virgules.
func (p *Parser) parseExprList() ([]Expr, error) {
	var exprs []Expr
	for {
		expr, err := p.parseFieldRef()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.current.Type != TokenComma {
			break
		}
		p.advance()
	}
	return exprs, nil
}

// isKeyword checks if the current token matches a contextual keyword by literal (case-insensitive).
// This handles words like "key" that are not in the keywords map to avoid conflicts with field names.
func (p *Parser) isKeyword(word string) bool {
	return (p.current.Type == TokenIdent || p.current.Type == TokenKey) &&
		strings.EqualFold(p.current.Literal, word)
}

// ---------- ALTER TABLE ----------

// parseAlterTable parse ALTER TABLE table ADD [CONSTRAINT name] PRIMARY KEY(col) |
//
//	FOREIGN KEY(col) REFERENCES ref(refcol) [ON DELETE CASCADE|RESTRICT|SET NULL|NO ACTION] |
//	UNIQUE(col[, col2, ...])
func (p *Parser) parseAlterTable() (*AlterTableStatement, error) {
	p.advance() // skip ALTER
	if p.current.Type != TokenTable {
		return nil, fmt.Errorf("parser: expected TABLE after ALTER at pos %d", p.current.Pos)
	}
	p.advance() // skip TABLE
	if p.current.Type != TokenIdent {
		return nil, fmt.Errorf("parser: expected table name after ALTER TABLE at pos %d", p.current.Pos)
	}
	tableName := p.current.Literal
	p.advance()

	if p.current.Type != TokenAdd {
		return nil, fmt.Errorf("parser: expected ADD after ALTER TABLE %s at pos %d", tableName, p.current.Pos)
	}
	p.advance() // skip ADD

	stmt := &AlterTableStatement{Table: tableName}
	cdef := &ConstraintDef{}

	// Nom de contrainte optionnel : CONSTRAINT name
	if p.current.Type == TokenConstraint {
		p.advance()
		if p.current.Type != TokenIdent {
			return nil, fmt.Errorf("parser: expected constraint name at pos %d", p.current.Pos)
		}
		cdef.Name = p.current.Literal
		p.advance()
	}

	switch p.current.Type {
	case TokenPrimary:
		// PRIMARY KEY (col [, col2, ...])
		p.advance() // skip PRIMARY
		if !p.isKeyword("key") {
			return nil, fmt.Errorf("parser: expected KEY after PRIMARY at pos %d", p.current.Pos)
		}
		p.advance() // skip KEY
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		cdef.Type = "PRIMARY_KEY"
		cdef.Columns = cols

	case TokenForeign:
		// FOREIGN KEY (col) REFERENCES ref_table(ref_col) [ON DELETE ...]
		p.advance() // skip FOREIGN
		if !p.isKeyword("key") {
			return nil, fmt.Errorf("parser: expected KEY after FOREIGN at pos %d", p.current.Pos)
		}
		p.advance() // skip KEY
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		if p.current.Type != TokenReferences {
			return nil, fmt.Errorf("parser: expected REFERENCES at pos %d", p.current.Pos)
		}
		p.advance() // skip REFERENCES
		if p.current.Type != TokenIdent {
			return nil, fmt.Errorf("parser: expected reference table name at pos %d", p.current.Pos)
		}
		refTable := p.current.Literal
		p.advance()
		refCols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		cdef.Type = "FOREIGN_KEY"
		cdef.Columns = cols
		cdef.RefTable = refTable
		if len(refCols) > 0 {
			cdef.RefColumn = refCols[0]
		}
		cdef.OnDelete = OnDeleteRestrict // défaut
		// ON DELETE action optionnel
		if p.current.Type == TokenOn {
			p.advance() // skip ON
			if p.current.Type != TokenDelete {
				return nil, fmt.Errorf("parser: expected DELETE after ON at pos %d", p.current.Pos)
			}
			p.advance() // skip DELETE
			switch p.current.Type {
			case TokenCascade:
				cdef.OnDelete = OnDeleteCascade
				p.advance()
			case TokenRestrict:
				cdef.OnDelete = OnDeleteRestrict
				p.advance()
			case TokenNo:
				p.advance() // skip NO
				if p.current.Type == TokenAction {
					p.advance()
				}
				cdef.OnDelete = OnDeleteNoAction
			default:
				// SET NULL
				if p.current.Type == TokenSet {
					p.advance()
					if p.current.Type == TokenNull {
						p.advance()
					}
					cdef.OnDelete = OnDeleteSetNull
				} else {
					return nil, fmt.Errorf("parser: unexpected ON DELETE action %q at pos %d", p.current.Literal, p.current.Pos)
				}
			}
		}

	case TokenUnique:
		// UNIQUE (col [, col2, ...])
		p.advance() // skip UNIQUE
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		cdef.Type = "UNIQUE"
		cdef.Columns = cols

	default:
		return nil, fmt.Errorf("parser: expected PRIMARY, FOREIGN, or UNIQUE after ADD at pos %d", p.current.Pos)
	}

	stmt.Constraint = cdef
	return stmt, nil
}

// parseColumnList parse (col1, col2, ...) et retourne les noms.
func (p *Parser) parseColumnList() ([]string, error) {
	if p.current.Type != TokenLParen {
		return nil, fmt.Errorf("parser: expected '(' at pos %d", p.current.Pos)
	}
	p.advance() // skip (
	var cols []string
	for p.current.Type != TokenRParen && p.current.Type != TokenEOF {
		if p.current.Type != TokenIdent {
			return nil, fmt.Errorf("parser: expected column name at pos %d, got %q", p.current.Pos, p.current.Literal)
		}
		cols = append(cols, p.current.Literal)
		p.advance()
		if p.current.Type == TokenComma {
			p.advance()
		}
	}
	if p.current.Type == TokenRParen {
		p.advance()
	}
	return cols, nil
}

// parseExprListUntilRParen parse des expressions jusqu'à ')'.
func (p *Parser) parseExprListUntilRParen() ([]Expr, error) {
	var exprs []Expr
	for p.current.Type != TokenRParen && p.current.Type != TokenEOF {
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.current.Type == TokenComma {
			p.advance()
		}
	}
	return exprs, nil
}
