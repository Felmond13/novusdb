// Package engine implémente le moteur d'exécution CRUD de NovusDB.
package engine

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// EvalExpr évalue une expression WHERE sur un document.
// Retourne true si le document satisfait la condition.
func EvalExpr(expr parser.Expr, doc *storage.Document) (bool, error) {
	if expr == nil {
		return true, nil
	}
	result, err := evalValue(expr, doc)
	if err != nil {
		return false, err
	}
	return toBool(result), nil
}

// wildcardValues encapsule plusieurs valeurs résolues par un wildcard path (* ou **).
// Sémantique : la condition est vraie si AU MOINS UNE valeur satisfait le test.
type wildcardValues struct {
	values []interface{}
}

// hasWildcard vérifie si un chemin contient un wildcard (* ou **).
func hasWildcard(parts []string) bool {
	for _, p := range parts {
		if p == "*" || p == "**" {
			return true
		}
	}
	return false
}

// resolveWildcard résout un chemin avec wildcards contre un document.
// Retourne toutes les valeurs scalaires matchées.
func resolveWildcard(doc *storage.Document, parts []string) []interface{} {
	if doc == nil || len(parts) == 0 {
		return nil
	}
	return resolveWildcardRec(doc, parts)
}

func resolveWildcardRec(doc *storage.Document, parts []string) []interface{} {
	if len(parts) == 0 {
		return nil
	}

	head := parts[0]
	rest := parts[1:]

	switch head {
	case "*":
		// Enfants directs : itérer toutes les valeurs du document
		var results []interface{}
		for _, f := range doc.Fields {
			if len(rest) == 0 {
				// Fin du chemin : collecter les valeurs scalaires
				results = append(results, f.Value)
			} else {
				// Continuer la résolution sur les sous-documents
				if sub, ok := f.Value.(*storage.Document); ok {
					results = append(results, resolveWildcardRec(sub, rest)...)
				}
			}
		}
		return results

	case "**":
		// Récursif profond : collecter ici + descendre dans les sous-documents
		var results []interface{}
		for _, f := range doc.Fields {
			if len(rest) == 0 {
				// Collecter la valeur
				results = append(results, f.Value)
				// Si c'est un sous-document, descendre aussi
				if sub, ok := f.Value.(*storage.Document); ok {
					results = append(results, resolveWildcardRec(sub, parts)...) // même parts = continuer **
				}
			} else {
				// On a encore un suffixe après ** : vérifier si le champ matche le suffixe
				if f.Name == rest[0] {
					if len(rest) == 1 {
						results = append(results, f.Value)
					} else if sub, ok := f.Value.(*storage.Document); ok {
						results = append(results, resolveWildcardRec(sub, rest[1:])...)
					}
				}
				// Descendre dans les sous-documents pour continuer la recherche **
				if sub, ok := f.Value.(*storage.Document); ok {
					results = append(results, resolveWildcardRec(sub, parts)...) // même parts = continuer **
				}
			}
		}
		return results

	default:
		// Champ nommé : résolution classique
		val, ok := doc.Get(head)
		if !ok {
			return nil
		}
		if len(rest) == 0 {
			return []interface{}{val}
		}
		sub, ok := val.(*storage.Document)
		if !ok {
			return nil
		}
		return resolveWildcardRec(sub, rest)
	}
}

// evalValue évalue une expression et retourne sa valeur.
func evalValue(expr parser.Expr, doc *storage.Document) (interface{}, error) {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return literalToValue(e.Token), nil

	case *parser.IdentExpr:
		val, _ := doc.Get(e.Name)
		return val, nil

	case *parser.DotExpr:
		if hasWildcard(e.Parts) {
			vals := resolveWildcard(doc, e.Parts)
			return &wildcardValues{values: vals}, nil
		}
		val, _ := doc.GetNested(e.Parts)
		return val, nil

	case *parser.BinaryExpr:
		return evalBinary(e, doc)

	case *parser.NotExpr:
		val, err := evalValue(e.Expr, doc)
		if err != nil {
			return nil, err
		}
		return !toBool(val), nil

	case *parser.InExpr:
		return evalIn(e, doc)

	case *parser.IsNullExpr:
		val, err := evalValue(e.Expr, doc)
		if err != nil {
			return nil, err
		}
		// Wildcard IS [NOT] NULL : au moins une valeur est/n'est pas null
		if wv, ok := val.(*wildcardValues); ok {
			for _, v := range wv.values {
				isNull := v == nil
				if e.Negate && !isNull {
					return true, nil // IS NOT NULL : au moins une non-null
				}
				if !e.Negate && isNull {
					return true, nil // IS NULL : au moins une null
				}
			}
			return false, nil
		}
		isNull := val == nil
		if e.Negate {
			return !isNull, nil // IS NOT NULL
		}
		return isNull, nil // IS NULL

	case *parser.FuncCallExpr:
		// Fonctions scalaires
		if isScalarFuncName(e.Name) {
			return evalScalarFunc(e, doc)
		}
		// Dans le contexte HAVING, les agrégats sont déjà calculés et stockés
		// dans le document sous le nom de la fonction (ex: "COUNT", "SUM", etc.)
		val, _ := doc.Get(e.Name)
		return val, nil

	case *parser.LikeExpr:
		return evalLike(e, doc)

	case *parser.BetweenExpr:
		return evalBetween(e, doc)

	case *parser.ArrayLiteralExpr:
		arr := make([]interface{}, len(e.Elements))
		for i, elem := range e.Elements {
			v, err := evalValue(elem, doc)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil

	case *parser.CaseExpr:
		for _, w := range e.Whens {
			cond, err := evalValue(w.Condition, doc)
			if err != nil {
				return nil, err
			}
			if toBool(cond) {
				return evalValue(w.Result, doc)
			}
		}
		if e.Else != nil {
			return evalValue(e.Else, doc)
		}
		return nil, nil

	case *parser.SequenceExpr:
		return nil, fmt.Errorf("eval: sequence %s.%s must be resolved before evaluation (use Executor)", e.SeqName, e.Op)

	case *parser.SysdateExpr:
		now := time.Now()
		switch e.Variant {
		case "CURRENT_DATE":
			return now.Format("2006-01-02"), nil
		case "CURRENT_TIMESTAMP":
			return now.Format(time.RFC3339Nano), nil
		default: // SYSDATE
			return now.Format("2006-01-02 15:04:05"), nil
		}

	default:
		return nil, fmt.Errorf("eval: unsupported expression type %T", expr)
	}
}

func evalBinary(e *parser.BinaryExpr, doc *storage.Document) (interface{}, error) {
	// Opérateurs logiques
	if e.Op == parser.TokenAnd {
		left, err := evalValue(e.Left, doc)
		if err != nil {
			return nil, err
		}
		if !toBool(left) {
			return false, nil // short-circuit
		}
		right, err := evalValue(e.Right, doc)
		if err != nil {
			return nil, err
		}
		return toBool(right), nil
	}
	if e.Op == parser.TokenOr {
		left, err := evalValue(e.Left, doc)
		if err != nil {
			return nil, err
		}
		if toBool(left) {
			return true, nil // short-circuit
		}
		right, err := evalValue(e.Right, doc)
		if err != nil {
			return nil, err
		}
		return toBool(right), nil
	}

	// Évaluer les deux côtés
	left, err := evalValue(e.Left, doc)
	if err != nil {
		return nil, err
	}
	right, err := evalValue(e.Right, doc)
	if err != nil {
		return nil, err
	}

	// Wildcard : si l'un des côtés est wildcardValues, tester chaque valeur
	if wv, ok := left.(*wildcardValues); ok {
		for _, v := range wv.values {
			// Ignorer les sous-documents pour les comparaisons scalaires
			if _, isDoc := v.(*storage.Document); isDoc {
				continue
			}
			r, err := compareSingle(v, right, e.Op)
			if err != nil {
				continue // type incompatible → skip
			}
			if toBool(r) {
				return true, nil
			}
		}
		return false, nil
	}
	if wv, ok := right.(*wildcardValues); ok {
		for _, v := range wv.values {
			if _, isDoc := v.(*storage.Document); isDoc {
				continue
			}
			r, err := compareSingle(left, v, e.Op)
			if err != nil {
				continue
			}
			if toBool(r) {
				return true, nil
			}
		}
		return false, nil
	}

	// Opérateurs arithmétiques
	switch e.Op {
	case parser.TokenPlus, parser.TokenMinus, parser.TokenStar, parser.TokenSlash:
		return evalArithmetic(left, right, e.Op)
	}

	// Opérateurs de comparaison
	return compare(left, right, e.Op)
}

// compareSingle effectue une comparaison ou une opération arithmétique entre deux valeurs scalaires.
func compareSingle(left, right interface{}, op parser.TokenType) (interface{}, error) {
	switch op {
	case parser.TokenPlus, parser.TokenMinus, parser.TokenStar, parser.TokenSlash:
		return evalArithmetic(left, right, op)
	default:
		return compare(left, right, op)
	}
}

// evalArithmetic effectue une opération arithmétique entre deux valeurs numériques.
func evalArithmetic(left, right interface{}, op parser.TokenType) (interface{}, error) {
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if !lok || !rok {
		return nil, fmt.Errorf("arithmetic: non-numeric operands: %v %v", left, right)
	}

	var result float64
	switch op {
	case parser.TokenPlus:
		result = lf + rf
	case parser.TokenMinus:
		result = lf - rf
	case parser.TokenStar:
		result = lf * rf
	case parser.TokenSlash:
		if rf == 0 {
			return nil, fmt.Errorf("arithmetic: division by zero")
		}
		result = lf / rf
	}

	// Si les deux opérandes sont entiers et le résultat est entier, retourner int64
	if isIntVal(left) && isIntVal(right) && result == float64(int64(result)) && op != parser.TokenSlash {
		return int64(result), nil
	}
	return result, nil
}

func isIntVal(v interface{}) bool {
	switch v.(type) {
	case int64, int:
		return true
	}
	return false
}

func evalIn(e *parser.InExpr, doc *storage.Document) (interface{}, error) {
	val, err := evalValue(e.Expr, doc)
	if err != nil {
		return nil, err
	}

	// Wildcard IN : au moins une valeur résolue est dans la liste
	if wv, ok := val.(*wildcardValues); ok {
		for _, wval := range wv.values {
			if _, isDoc := wval.(*storage.Document); isDoc {
				continue
			}
			found := false
			for _, v := range e.Values {
				candidate, err := evalValue(v, doc)
				if err != nil {
					continue
				}
				eq, _ := compare(wval, candidate, parser.TokenEQ)
				if toBool(eq) {
					found = true
					break
				}
			}
			if found && !e.Negate {
				return true, nil
			}
			if found && e.Negate {
				return false, nil
			}
		}
		if e.Negate {
			return true, nil
		}
		return false, nil
	}

	for _, v := range e.Values {
		candidate, err := evalValue(v, doc)
		if err != nil {
			return nil, err
		}
		eq, err := compare(val, candidate, parser.TokenEQ)
		if err != nil {
			return nil, err
		}
		if toBool(eq) {
			if e.Negate {
				return false, nil
			}
			return true, nil
		}
	}
	if e.Negate {
		return true, nil
	}
	return false, nil
}

// compare effectue une comparaison entre deux valeurs.
func compare(left, right interface{}, op parser.TokenType) (interface{}, error) {
	// nil handling
	if left == nil && right == nil {
		switch op {
		case parser.TokenEQ:
			return true, nil
		case parser.TokenNEQ:
			return false, nil
		default:
			return false, nil
		}
	}
	if left == nil || right == nil {
		switch op {
		case parser.TokenEQ:
			return false, nil
		case parser.TokenNEQ:
			return true, nil
		default:
			return false, nil
		}
	}

	// Promouvoir en types comparables
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)

	if lok && rok {
		return compareNumbers(lf, rf, op), nil
	}

	// Comparaison de strings
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return compareStrings(ls, rs, op), nil
	}

	// Comparaison de bools
	lb, lok := left.(bool)
	rb, rok := right.(bool)
	if lok && rok {
		switch op {
		case parser.TokenEQ:
			return lb == rb, nil
		case parser.TokenNEQ:
			return lb != rb, nil
		}
	}

	// Types incompatibles
	switch op {
	case parser.TokenEQ:
		return false, nil
	case parser.TokenNEQ:
		return true, nil
	default:
		return false, nil
	}
}

func compareNumbers(l, r float64, op parser.TokenType) bool {
	switch op {
	case parser.TokenEQ:
		return l == r
	case parser.TokenNEQ:
		return l != r
	case parser.TokenLT:
		return l < r
	case parser.TokenGT:
		return l > r
	case parser.TokenLTE:
		return l <= r
	case parser.TokenGTE:
		return l >= r
	default:
		return false
	}
}

func compareStrings(l, r string, op parser.TokenType) bool {
	switch op {
	case parser.TokenEQ:
		return l == r
	case parser.TokenNEQ:
		return l != r
	case parser.TokenLT:
		return l < r
	case parser.TokenGT:
		return l > r
	case parser.TokenLTE:
		return l <= r
	case parser.TokenGTE:
		return l >= r
	default:
		return false
	}
}

// ---------- Fonctions utilitaires ----------

// literalToValue convertit un token littéral en valeur Go.
func literalToValue(tok parser.Token) interface{} {
	switch tok.Type {
	case parser.TokenInteger:
		v, _ := strconv.ParseInt(tok.Literal, 10, 64)
		return v
	case parser.TokenFloat:
		v, _ := strconv.ParseFloat(tok.Literal, 64)
		return v
	case parser.TokenString:
		return tok.Literal
	case parser.TokenTrue:
		return true
	case parser.TokenFalse:
		return false
	case parser.TokenNull:
		return nil
	default:
		return tok.Literal
	}
}

// toBool convertit une valeur en booléen.
func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != ""
	default:
		return true
	}
}

// toFloat64 tente de convertir une valeur en float64.
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case float64:
		return val, true
	case int:
		return float64(val), true
	case bool:
		if val {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// evalLike évalue une expression LIKE avec pattern matching SQL.
// % = zéro ou plusieurs caractères, _ = un seul caractère.
func evalLike(e *parser.LikeExpr, doc *storage.Document) (interface{}, error) {
	val, err := evalValue(e.Expr, doc)
	if err != nil {
		return nil, err
	}

	// Wildcard LIKE : au moins une valeur matche le pattern
	if wv, ok := val.(*wildcardValues); ok {
		for _, v := range wv.values {
			if v == nil {
				continue
			}
			s, ok := v.(string)
			if !ok {
				continue // LIKE ne s'applique qu'aux strings
			}
			matched := matchLikePattern(strings.ToLower(s), strings.ToLower(e.Pattern))
			if matched && !e.Negate {
				return true, nil
			}
			if matched && e.Negate {
				return false, nil
			}
		}
		if e.Negate {
			return true, nil
		}
		return false, nil
	}

	if val == nil {
		return false, nil
	}
	s, ok := val.(string)
	if !ok {
		s = fmt.Sprintf("%v", val)
	}

	matched := matchLikePattern(strings.ToLower(s), strings.ToLower(e.Pattern))
	if e.Negate {
		return !matched, nil
	}
	return matched, nil
}

// matchLikePattern implémente le pattern matching SQL LIKE.
// % matche zéro ou plusieurs caractères, _ matche exactement un caractère.
func matchLikePattern(s, pattern string) bool {
	si, pi := 0, 0
	starSi, starPi := -1, -1

	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == s[si]) {
			si++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			starSi = si
			starPi = pi
			pi++
		} else if starPi >= 0 {
			starSi++
			si = starSi
			pi = starPi + 1
		} else {
			return false
		}
	}

	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}
	return pi == len(pattern)
}

// evalBetween évalue expr BETWEEN low AND high (ou NOT BETWEEN).
func evalBetween(e *parser.BetweenExpr, doc *storage.Document) (interface{}, error) {
	val, err := evalValue(e.Expr, doc)
	if err != nil {
		return nil, err
	}
	low, err := evalValue(e.Low, doc)
	if err != nil {
		return nil, err
	}
	high, err := evalValue(e.High, doc)
	if err != nil {
		return nil, err
	}

	// Wildcard BETWEEN : au moins une valeur dans l'intervalle
	if wv, ok := val.(*wildcardValues); ok {
		for _, v := range wv.values {
			if v == nil {
				continue
			}
			if _, isDoc := v.(*storage.Document); isDoc {
				continue
			}
			cmpLow := compareValuesForBetween(v, low)
			cmpHigh := compareValuesForBetween(v, high)
			inRange := cmpLow >= 0 && cmpHigh <= 0
			if inRange && !e.Negate {
				return true, nil
			}
			if inRange && e.Negate {
				return false, nil
			}
		}
		if e.Negate {
			return true, nil
		}
		return false, nil
	}

	if val == nil || low == nil || high == nil {
		if e.Negate {
			return true, nil
		}
		return false, nil
	}

	cmpLow := compareValuesForBetween(val, low)
	cmpHigh := compareValuesForBetween(val, high)

	inRange := cmpLow >= 0 && cmpHigh <= 0
	if e.Negate {
		return !inRange, nil
	}
	return inRange, nil
}

// compareValuesForBetween compare deux valeurs. Retourne -1, 0 ou 1.
func compareValuesForBetween(a, b interface{}) int {
	fa, oka := toFloat(a)
	fb, okb := toFloat(b)
	if oka && okb {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

// toFloat tente de convertir une valeur en float64.
func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// ExprToFieldPath extrait le chemin de champ depuis une expression (Ident ou Dot).
func ExprToFieldPath(expr parser.Expr) []string {
	switch e := expr.(type) {
	case *parser.IdentExpr:
		return []string{e.Name}
	case *parser.DotExpr:
		return e.Parts
	default:
		return nil
	}
}

// ExprToFieldName retourne le nom de champ à plat (avec points) depuis une expression.
func ExprToFieldName(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.IdentExpr:
		return e.Name
	case *parser.DotExpr:
		return strings.Join(e.Parts, ".")
	default:
		return ""
	}
}
