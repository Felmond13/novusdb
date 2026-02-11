package parser

// ---------- AST : Arbre de syntaxe abstraite pour le langage SQL-like ----------

// Statement est l'interface commune à toutes les instructions.
type Statement interface {
	statementNode()
}

// ---------- Expressions ----------

// Expr est l'interface commune à toutes les expressions.
type Expr interface {
	exprNode()
}

// IdentExpr représente un identifiant simple (ex: "retry").
type IdentExpr struct {
	Name string
}

func (e *IdentExpr) exprNode() {}

// DotExpr représente un accès imbriqué (ex: "params.timeout").
type DotExpr struct {
	Parts []string // ["params", "timeout"]
}

func (e *DotExpr) exprNode() {}

// LiteralExpr représente un littéral (string, int, float, bool, null).
type LiteralExpr struct {
	Token Token // le token original
}

func (e *LiteralExpr) exprNode() {}

// ParamExpr représente un placeholder ? dans une requête paramétrée.
type ParamExpr struct {
	Index int // 0-based index in the parameter list
}

func (e *ParamExpr) exprNode() {}

// BinaryExpr représente une expression binaire (comparaison ou logique).
type BinaryExpr struct {
	Left  Expr
	Op    TokenType // TokenEQ, TokenLT, TokenAnd, TokenOr, etc.
	Right Expr
}

func (e *BinaryExpr) exprNode() {}

// NotExpr représente une négation logique.
type NotExpr struct {
	Expr Expr
}

func (e *NotExpr) exprNode() {}

// FuncCallExpr représente un appel de fonction (COUNT, SUM, AVG, MIN, MAX).
type FuncCallExpr struct {
	Name     string
	Args     []Expr
	Distinct bool // true pour COUNT(DISTINCT field)
}

func (e *FuncCallExpr) exprNode() {}

// StarExpr représente le joker *.
type StarExpr struct{}

func (e *StarExpr) exprNode() {}

// QualifiedStarExpr représente un joker qualifié (ex: A.*).
type QualifiedStarExpr struct {
	Qualifier string // nom de table ou alias (ex: "A")
}

func (e *QualifiedStarExpr) exprNode() {}

// DocumentLiteralExpr représente un sous-document littéral {key=val, key2=val2}.
type DocumentLiteralExpr struct {
	Fields []FieldAssignment
}

func (e *DocumentLiteralExpr) exprNode() {}

// SubqueryExpr représente une sous-requête entre parenthèses.
type SubqueryExpr struct {
	Query *SelectStatement
}

func (e *SubqueryExpr) exprNode() {}

// IsNullExpr représente l'opérateur IS NULL / IS NOT NULL.
type IsNullExpr struct {
	Expr   Expr
	Negate bool // true = IS NOT NULL
}

func (e *IsNullExpr) exprNode() {}

// LikeExpr représente field LIKE "pattern%" ou field NOT LIKE "pattern%".
type LikeExpr struct {
	Expr    Expr
	Pattern string
	Negate  bool // true = NOT LIKE
}

func (e *LikeExpr) exprNode() {}

// BetweenExpr représente field BETWEEN low AND high.
type BetweenExpr struct {
	Expr   Expr
	Low    Expr
	High   Expr
	Negate bool // true = NOT BETWEEN
}

func (e *BetweenExpr) exprNode() {}

// InExpr représente l'opérateur IN / NOT IN (expr [NOT] IN (values...)).
type InExpr struct {
	Expr   Expr
	Values []Expr
	Negate bool // true = NOT IN
}

func (e *InExpr) exprNode() {}

// AliasExpr représente une expression avec un alias (expr AS alias).
type AliasExpr struct {
	Expr  Expr
	Alias string
}

func (e *AliasExpr) exprNode() {}

// ---------- Query Hints (Oracle-style /*+ HINT */) ----------

// HintType identifie le type de hint.
type HintType int

const (
	HintParallel   HintType = iota // /*+ PARALLEL(n) */
	HintNoCache                    // /*+ NO_CACHE */
	HintFullScan                   // /*+ FULL_SCAN */
	HintForceIndex                 // /*+ FORCE_INDEX(field) */
	HintHashJoin                   // /*+ HASH_JOIN */
	HintNestedLoop                 // /*+ NESTED_LOOP */
)

// QueryHint représente un hint de requête.
type QueryHint struct {
	Type  HintType
	Param string // paramètre optionnel (ex: "4" pour PARALLEL(4), "age" pour FORCE_INDEX(age))
}

// ---------- Instructions ----------

// SelectStatement représente SELECT ... FROM ... WHERE ... GROUP BY ... ORDER BY ... LIMIT ...
type SelectStatement struct {
	Hints     []QueryHint    // hints Oracle-style /*+ ... */
	Distinct  bool           // true si SELECT DISTINCT
	Columns   []Expr         // colonnes sélectionnées
	From      string         // table principale
	FromAlias string         // alias optionnel de la table principale
	Joins     []*JoinClause  // clauses JOIN
	Where     Expr           // condition WHERE (peut être nil)
	GroupBy   []Expr         // colonnes GROUP BY
	Having    Expr           // condition HAVING (peut être nil)
	OrderBy   []*OrderByExpr // colonnes ORDER BY
	Limit     int            // -1 si pas de LIMIT
	Offset    int            // 0 si pas d'OFFSET
}

func (s *SelectStatement) statementNode() {}

// JoinClause représente une clause JOIN.
type JoinClause struct {
	Type      string // "INNER", "LEFT", "RIGHT"
	Table     string
	Alias     string // alias optionnel
	Condition Expr
}

// OrderByExpr représente une expression ORDER BY.
type OrderByExpr struct {
	Expr Expr
	Desc bool // true si DESC
}

// InsertStatement représente INSERT INTO table VALUES (...) ou INSERT INTO table SELECT ...
type InsertStatement struct {
	Table     string
	Fields    []FieldAssignment   // premier groupe VALUES (rétro-compat)
	Rows      [][]FieldAssignment // tous les groupes VALUES (batch)
	Source    *SelectStatement    // pour INSERT INTO ... SELECT ... (nil si VALUES)
	OrReplace bool                // INSERT OR REPLACE INTO ...
}

func (s *InsertStatement) statementNode() {}

// FieldAssignment représente une affectation champ=valeur.
type FieldAssignment struct {
	Field Expr // IdentExpr ou DotExpr
	Value Expr
}

// UpdateStatement représente UPDATE table SET field=value, ... WHERE ...
type UpdateStatement struct {
	Hints       []QueryHint
	Table       string
	Assignments []FieldAssignment
	Where       Expr
}

func (s *UpdateStatement) statementNode() {}

// DeleteStatement représente DELETE FROM table WHERE ...
type DeleteStatement struct {
	Hints []QueryHint
	Table string
	Where Expr
}

func (s *DeleteStatement) statementNode() {}

// CreateIndexStatement représente CREATE INDEX ON table (field).
type CreateIndexStatement struct {
	Table       string
	Field       string
	IfNotExists bool
}

func (s *CreateIndexStatement) statementNode() {}

// DropIndexStatement représente DROP INDEX ON table (field).
type DropIndexStatement struct {
	Table    string
	Field    string
	IfExists bool
}

func (s *DropIndexStatement) statementNode() {}

// DropTableStatement représente DROP TABLE <collection>.
type DropTableStatement struct {
	Table    string
	IfExists bool
}

func (s *DropTableStatement) statementNode() {}

// TruncateTableStatement représente TRUNCATE TABLE <collection>.
type TruncateTableStatement struct {
	Table string
}

func (s *TruncateTableStatement) statementNode() {}

// CreateViewStatement représente CREATE VIEW name AS SELECT ...
type CreateViewStatement struct {
	Name  string
	Query string // requête SQL source brute
}

func (s *CreateViewStatement) statementNode() {}

// DropViewStatement représente DROP VIEW name.
type DropViewStatement struct {
	Name     string
	IfExists bool
}

func (s *DropViewStatement) statementNode() {}

// UnionStatement représente SELECT ... UNION [ALL] SELECT ...
type UnionStatement struct {
	Left  *SelectStatement
	Right *SelectStatement
	All   bool // true = UNION ALL (garde les doublons)
}

func (s *UnionStatement) statementNode() {}

// ArrayLiteralExpr représente un tableau JSON [val1, val2, ...].
type ArrayLiteralExpr struct {
	Elements []Expr
}

func (e *ArrayLiteralExpr) exprNode() {}

// CaseExpr représente CASE WHEN cond THEN val [WHEN ...] [ELSE val] END.
type CaseExpr struct {
	Whens []WhenClause
	Else  Expr // peut être nil
}

func (e *CaseExpr) exprNode() {}

// WhenClause représente une clause WHEN cond THEN val.
type WhenClause struct {
	Condition Expr
	Result    Expr
}

// CreateSequenceStatement représente CREATE SEQUENCE name [START WITH n] [INCREMENT BY n] [MINVALUE n] [MAXVALUE n] [CYCLE|NOCYCLE].
type CreateSequenceStatement struct {
	Name        string
	StartWith   float64
	IncrementBy float64
	MinValue    float64
	MaxValue    float64
	Cycle       bool
}

func (s *CreateSequenceStatement) statementNode() {}

// DropSequenceStatement représente DROP SEQUENCE [IF EXISTS] name.
type DropSequenceStatement struct {
	Name     string
	IfExists bool
}

func (s *DropSequenceStatement) statementNode() {}

// SequenceExpr représente seq_name.NEXTVAL ou seq_name.CURRVAL dans une expression.
type SequenceExpr struct {
	SeqName string
	Op      string // "NEXTVAL" ou "CURRVAL"
}

func (e *SequenceExpr) exprNode() {}

// SysdateExpr représente SYSDATE, CURRENT_DATE ou CURRENT_TIMESTAMP.
type SysdateExpr struct {
	Variant string // "SYSDATE", "CURRENT_DATE", "CURRENT_TIMESTAMP"
}

func (e *SysdateExpr) exprNode() {}

// ExplainStatement encapsule un statement pour afficher son plan d'exécution.
type ExplainStatement struct {
	Inner Statement
}

func (s *ExplainStatement) statementNode() {}
