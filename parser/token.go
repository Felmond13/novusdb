// Package parser implémente le lexer et le parser SQL-like de NovusDB.
package parser

// TokenType identifie le type d'un token lexical.
type TokenType int

const (
	// Tokens spéciaux
	TokenEOF     TokenType = iota
	TokenIllegal           // caractère non reconnu

	// Littéraux et identifiants
	TokenIdent   // nom de champ, table, etc.
	TokenInteger // littéral entier
	TokenFloat   // littéral flottant
	TokenString  // littéral chaîne entre guillemets

	// Mots-clés SQL
	TokenSelect
	TokenInsert
	TokenInto
	TokenUpdate
	TokenDelete
	TokenFrom
	TokenWhere
	TokenSet
	TokenValues
	TokenAnd
	TokenOr
	TokenNot
	TokenTrue
	TokenFalse
	TokenNull
	TokenJoin
	TokenOn
	TokenLeft
	TokenRight
	TokenInner
	TokenGroupBy
	TokenHaving
	TokenOrderBy
	TokenAsc
	TokenDesc
	TokenLimit
	TokenOffset
	TokenAs
	TokenIn
	TokenCount
	TokenSum
	TokenAvg
	TokenMin
	TokenMax
	TokenCreate
	TokenIndex
	TokenDrop
	TokenIs         // IS (pour IS NULL / IS NOT NULL)
	TokenLike       // LIKE
	TokenDistinct   // DISTINCT
	TokenTable      // TABLE
	TokenBetween    // BETWEEN
	TokenExplain    // EXPLAIN
	TokenIf         // IF
	TokenExists     // EXISTS
	TokenReplace    // REPLACE
	TokenTruncate   // TRUNCATE
	TokenUnion      // UNION
	TokenAll        // ALL
	TokenCase       // CASE
	TokenWhen       // WHEN
	TokenThen       // THEN
	TokenElse       // ELSE
	TokenEnd        // END
	TokenView       // VIEW
	TokenSequence   // SEQUENCE
	TokenAnalyze    // ANALYZE
	TokenAlter      // ALTER
	TokenAdd        // ADD
	TokenPrimary    // PRIMARY
	TokenKey        // KEY
	TokenForeign    // FOREIGN
	TokenReferences // REFERENCES
	TokenCascade    // CASCADE
	TokenRestrict   // RESTRICT
	TokenConstraint // CONSTRAINT
	TokenUnique     // UNIQUE
	TokenAction     // ACTION
	TokenNo         // NO
	TokenHint       // /*+ ... */ (Oracle-style hint)

	// Opérateurs et ponctuation
	TokenStar   // *
	TokenComma  // ,
	TokenDot    // .
	TokenLParen // (
	TokenRParen // )
	TokenEQ     // =
	TokenNEQ    // != ou <>
	TokenLT     // <
	TokenGT     // >
	TokenLTE    // <=
	TokenGTE    // >=
	TokenPlus   // +
	TokenMinus  // -
	TokenSlash  // /
	TokenColon  // :
	TokenLBrace // {
	TokenRBrace // }
	TokenLBrack // [
	TokenRBrack // ]
	TokenParam  // ? (parameterized query placeholder)
)

// Token représente un token lexical.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int // position dans la chaîne d'entrée
}

// keywords mappe les mots-clés SQL (en minuscules) vers leur TokenType.
var keywords = map[string]TokenType{
	"select":   TokenSelect,
	"insert":   TokenInsert,
	"into":     TokenInto,
	"update":   TokenUpdate,
	"delete":   TokenDelete,
	"from":     TokenFrom,
	"where":    TokenWhere,
	"set":      TokenSet,
	"values":   TokenValues,
	"and":      TokenAnd,
	"or":       TokenOr,
	"not":      TokenNot,
	"true":     TokenTrue,
	"false":    TokenFalse,
	"null":     TokenNull,
	"join":     TokenJoin,
	"on":       TokenOn,
	"left":     TokenLeft,
	"right":    TokenRight,
	"inner":    TokenInner,
	"group":    TokenGroupBy, // "group" seul, "by" consommé par le parser
	"having":   TokenHaving,
	"order":    TokenOrderBy, // idem pour "by"
	"asc":      TokenAsc,
	"desc":     TokenDesc,
	"limit":    TokenLimit,
	"offset":   TokenOffset,
	"as":       TokenAs,
	"in":       TokenIn,
	"count":    TokenCount,
	"sum":      TokenSum,
	"avg":      TokenAvg,
	"min":      TokenMin,
	"max":      TokenMax,
	"create":   TokenCreate,
	"index":    TokenIndex,
	"drop":     TokenDrop,
	"is":       TokenIs,
	"like":     TokenLike,
	"distinct": TokenDistinct,
	"table":    TokenTable,
	"between":  TokenBetween,
	"explain":  TokenExplain,
	"if":       TokenIf,
	"exists":   TokenExists,
	"replace":  TokenReplace,
	"truncate": TokenTruncate,
	"union":    TokenUnion,
	"all":      TokenAll,
	"case":     TokenCase,
	"when":     TokenWhen,
	"then":     TokenThen,
	"else":     TokenElse,
	"end":      TokenEnd,
	"view":     TokenView,
	"sequence": TokenSequence,
	"analyze":  TokenAnalyze,
	"alter":    TokenAlter,
	"add":      TokenAdd,
	"primary":  TokenPrimary,
	// "key" not a keyword — too common as field name; handled contextually in ALTER TABLE
	"foreign":    TokenForeign,
	"references": TokenReferences,
	"cascade":    TokenCascade,
	"restrict":   TokenRestrict,
	"constraint": TokenConstraint,
	"unique":     TokenUnique,
	"action":     TokenAction,
	"no":         TokenNo,
}

// LookupIdent retourne le TokenType d'un identifiant (mot-clé ou ident).
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TokenIdent
}
