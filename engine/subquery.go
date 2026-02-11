package engine

import (
	"fmt"

	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// materializeSubqueries parcourt un arbre d'expressions et exécute les
// sous-requêtes non corrélées (SubqueryExpr), remplaçant chacune par ses valeurs.
// Les sous-requêtes corrélées (qui référencent outerAlias) sont laissées en place.
func (ex *Executor) materializeSubqueries(expr parser.Expr, outerAlias string) (parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		if isCorrelatedSubquery(e, outerAlias) {
			return expr, nil // laisser pour exécution per-row
		}
		return ex.execSubqueryScalar(e.Query)

	case *parser.BinaryExpr:
		left, err := ex.materializeSubqueries(e.Left, outerAlias)
		if err != nil {
			return nil, err
		}
		right, err := ex.materializeSubqueries(e.Right, outerAlias)
		if err != nil {
			return nil, err
		}
		return &parser.BinaryExpr{Left: left, Op: e.Op, Right: right}, nil

	case *parser.NotExpr:
		inner, err := ex.materializeSubqueries(e.Expr, outerAlias)
		if err != nil {
			return nil, err
		}
		return &parser.NotExpr{Expr: inner}, nil

	case *parser.InExpr:
		left, err := ex.materializeSubqueries(e.Expr, outerAlias)
		if err != nil {
			return nil, err
		}
		var newValues []parser.Expr
		for _, v := range e.Values {
			if sub, ok := v.(*parser.SubqueryExpr); ok {
				if isCorrelatedSubquery(sub, outerAlias) {
					newValues = append(newValues, v) // laisser pour per-row
					continue
				}
				expanded, err := ex.execSubqueryValues(sub.Query)
				if err != nil {
					return nil, err
				}
				newValues = append(newValues, expanded...)
			} else {
				mat, err := ex.materializeSubqueries(v, outerAlias)
				if err != nil {
					return nil, err
				}
				newValues = append(newValues, mat)
			}
		}
		return &parser.InExpr{Expr: left, Values: newValues, Negate: e.Negate}, nil

	case *parser.IsNullExpr:
		inner, err := ex.materializeSubqueries(e.Expr, outerAlias)
		if err != nil {
			return nil, err
		}
		return &parser.IsNullExpr{Expr: inner, Negate: e.Negate}, nil

	case *parser.LikeExpr:
		inner, err := ex.materializeSubqueries(e.Expr, outerAlias)
		if err != nil {
			return nil, err
		}
		return &parser.LikeExpr{Expr: inner, Pattern: e.Pattern, Negate: e.Negate}, nil

	case *parser.BetweenExpr:
		inner, err := ex.materializeSubqueries(e.Expr, outerAlias)
		if err != nil {
			return nil, err
		}
		low, err := ex.materializeSubqueries(e.Low, outerAlias)
		if err != nil {
			return nil, err
		}
		high, err := ex.materializeSubqueries(e.High, outerAlias)
		if err != nil {
			return nil, err
		}
		return &parser.BetweenExpr{Expr: inner, Low: low, High: high, Negate: e.Negate}, nil

	case *parser.AliasExpr:
		inner, err := ex.materializeSubqueries(e.Expr, outerAlias)
		if err != nil {
			return nil, err
		}
		return &parser.AliasExpr{Expr: inner, Alias: e.Alias}, nil

	case *parser.CaseExpr:
		newWhens := make([]parser.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			cond, errC := ex.materializeSubqueries(w.Condition, outerAlias)
			if errC != nil {
				return nil, errC
			}
			result, errR := ex.materializeSubqueries(w.Result, outerAlias)
			if errR != nil {
				return nil, errR
			}
			newWhens[i] = parser.WhenClause{Condition: cond, Result: result}
		}
		var elseExpr parser.Expr
		if e.Else != nil {
			var errE error
			elseExpr, errE = ex.materializeSubqueries(e.Else, outerAlias)
			if errE != nil {
				return nil, errE
			}
		}
		return &parser.CaseExpr{Whens: newWhens, Else: elseExpr}, nil

	default:
		return expr, nil
	}
}

// isCorrelatedSubquery vérifie si une sous-requête référence l'alias externe.
func isCorrelatedSubquery(sub *parser.SubqueryExpr, outerAlias string) bool {
	if outerAlias == "" {
		return false
	}
	return referencesAlias(sub.Query.Where, outerAlias) ||
		referencesAliasInExprs(sub.Query.Columns, outerAlias)
}

// referencesAlias vérifie si un arbre d'expressions contient des DotExpr commençant par alias.
func referencesAlias(expr parser.Expr, alias string) bool {
	if expr == nil || alias == "" {
		return false
	}
	switch e := expr.(type) {
	case *parser.DotExpr:
		return len(e.Parts) >= 2 && e.Parts[0] == alias
	case *parser.BinaryExpr:
		return referencesAlias(e.Left, alias) || referencesAlias(e.Right, alias)
	case *parser.InExpr:
		if referencesAlias(e.Expr, alias) {
			return true
		}
		for _, v := range e.Values {
			if referencesAlias(v, alias) {
				return true
			}
		}
		return false
	case *parser.NotExpr:
		return referencesAlias(e.Expr, alias)
	case *parser.IsNullExpr:
		return referencesAlias(e.Expr, alias)
	case *parser.LikeExpr:
		return referencesAlias(e.Expr, alias)
	case *parser.BetweenExpr:
		return referencesAlias(e.Expr, alias) || referencesAlias(e.Low, alias) || referencesAlias(e.High, alias)
	case *parser.AliasExpr:
		return referencesAlias(e.Expr, alias)
	default:
		return false
	}
}

func referencesAliasInExprs(exprs []parser.Expr, alias string) bool {
	for _, e := range exprs {
		if referencesAlias(e, alias) {
			return true
		}
	}
	return false
}

// containsSubqueryExpr vérifie si l'arbre contient des SubqueryExpr non matérialisés.
func containsSubqueryExpr(expr parser.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		return true
	case *parser.BinaryExpr:
		return containsSubqueryExpr(e.Left) || containsSubqueryExpr(e.Right)
	case *parser.InExpr:
		if containsSubqueryExpr(e.Expr) {
			return true
		}
		for _, v := range e.Values {
			if containsSubqueryExpr(v) {
				return true
			}
		}
		return false
	case *parser.NotExpr:
		return containsSubqueryExpr(e.Expr)
	case *parser.IsNullExpr:
		return containsSubqueryExpr(e.Expr)
	case *parser.LikeExpr:
		return containsSubqueryExpr(e.Expr)
	case *parser.BetweenExpr:
		return containsSubqueryExpr(e.Expr) || containsSubqueryExpr(e.Low) || containsSubqueryExpr(e.High)
	case *parser.AliasExpr:
		return containsSubqueryExpr(e.Expr)
	default:
		return false
	}
}

// stripTableAlias supprime le préfixe d'alias des DotExpr dans l'arbre d'expressions.
// DotExpr(["A","prenom"]) → IdentExpr("prenom")
// DotExpr(["A","notes","math"]) → DotExpr(["notes","math"])
// Ne descend PAS dans les SubqueryExpr (qui ont leurs propres alias).
func stripTableAlias(expr parser.Expr, alias string) parser.Expr {
	if expr == nil || alias == "" {
		return expr
	}
	switch e := expr.(type) {
	case *parser.DotExpr:
		if len(e.Parts) >= 2 && e.Parts[0] == alias {
			newParts := e.Parts[1:]
			if len(newParts) == 1 {
				return &parser.IdentExpr{Name: newParts[0]}
			}
			return &parser.DotExpr{Parts: newParts}
		}
		return expr
	case *parser.BinaryExpr:
		return &parser.BinaryExpr{
			Left:  stripTableAlias(e.Left, alias),
			Op:    e.Op,
			Right: stripTableAlias(e.Right, alias),
		}
	case *parser.InExpr:
		newValues := make([]parser.Expr, len(e.Values))
		for i, v := range e.Values {
			newValues[i] = stripTableAlias(v, alias)
		}
		return &parser.InExpr{Expr: stripTableAlias(e.Expr, alias), Values: newValues, Negate: e.Negate}
	case *parser.NotExpr:
		return &parser.NotExpr{Expr: stripTableAlias(e.Expr, alias)}
	case *parser.IsNullExpr:
		return &parser.IsNullExpr{Expr: stripTableAlias(e.Expr, alias), Negate: e.Negate}
	case *parser.LikeExpr:
		return &parser.LikeExpr{Expr: stripTableAlias(e.Expr, alias), Pattern: e.Pattern, Negate: e.Negate}
	case *parser.BetweenExpr:
		return &parser.BetweenExpr{
			Expr: stripTableAlias(e.Expr, alias), Low: stripTableAlias(e.Low, alias),
			High: stripTableAlias(e.High, alias), Negate: e.Negate,
		}
	case *parser.AliasExpr:
		return &parser.AliasExpr{Expr: stripTableAlias(e.Expr, alias), Alias: e.Alias}
	case *parser.FuncCallExpr:
		newArgs := make([]parser.Expr, len(e.Args))
		for i, a := range e.Args {
			newArgs[i] = stripTableAlias(a, alias)
		}
		return &parser.FuncCallExpr{Name: e.Name, Args: newArgs}
	case *parser.SubqueryExpr:
		return expr // ne PAS entrer dans les sous-requêtes
	default:
		return expr
	}
}

// substituteOuterRefs remplace les références à outerAlias par des valeurs littérales
// extraites du document externe. A.prenom → LiteralExpr("Anouar").
func substituteOuterRefs(expr parser.Expr, outerAlias string, outerDoc *storage.Document) parser.Expr {
	if expr == nil || outerAlias == "" {
		return expr
	}
	switch e := expr.(type) {
	case *parser.DotExpr:
		if len(e.Parts) >= 2 && e.Parts[0] == outerAlias {
			fieldParts := e.Parts[1:]
			var val interface{}
			if len(fieldParts) == 1 {
				val, _ = outerDoc.Get(fieldParts[0])
			} else {
				val, _ = outerDoc.GetNested(fieldParts)
			}
			return valueToLiteralExpr(val)
		}
		return expr
	case *parser.BinaryExpr:
		return &parser.BinaryExpr{
			Left:  substituteOuterRefs(e.Left, outerAlias, outerDoc),
			Op:    e.Op,
			Right: substituteOuterRefs(e.Right, outerAlias, outerDoc),
		}
	case *parser.InExpr:
		newValues := make([]parser.Expr, len(e.Values))
		for i, v := range e.Values {
			newValues[i] = substituteOuterRefs(v, outerAlias, outerDoc)
		}
		return &parser.InExpr{
			Expr:   substituteOuterRefs(e.Expr, outerAlias, outerDoc),
			Values: newValues, Negate: e.Negate,
		}
	case *parser.NotExpr:
		return &parser.NotExpr{Expr: substituteOuterRefs(e.Expr, outerAlias, outerDoc)}
	case *parser.IsNullExpr:
		return &parser.IsNullExpr{Expr: substituteOuterRefs(e.Expr, outerAlias, outerDoc), Negate: e.Negate}
	case *parser.LikeExpr:
		return &parser.LikeExpr{Expr: substituteOuterRefs(e.Expr, outerAlias, outerDoc), Pattern: e.Pattern, Negate: e.Negate}
	case *parser.BetweenExpr:
		return &parser.BetweenExpr{
			Expr: substituteOuterRefs(e.Expr, outerAlias, outerDoc),
			Low:  substituteOuterRefs(e.Low, outerAlias, outerDoc),
			High: substituteOuterRefs(e.High, outerAlias, outerDoc), Negate: e.Negate,
		}
	default:
		return expr
	}
}

// materializeForRow matérialise les sous-requêtes corrélées pour une ligne externe donnée.
// Substitue les références à outerAlias dans les sous-requêtes avec les valeurs du doc,
// puis exécute les sous-requêtes.
func (ex *Executor) materializeForRow(expr parser.Expr, outerAlias string, outerDoc *storage.Document) (parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		resolvedQuery := &parser.SelectStatement{
			Distinct:  e.Query.Distinct,
			Columns:   e.Query.Columns,
			From:      e.Query.From,
			FromAlias: e.Query.FromAlias,
			Joins:     e.Query.Joins,
			Where:     substituteOuterRefs(e.Query.Where, outerAlias, outerDoc),
			GroupBy:   e.Query.GroupBy,
			Having:    e.Query.Having,
			OrderBy:   e.Query.OrderBy,
			Limit:     e.Query.Limit,
			Offset:    e.Query.Offset,
		}
		return ex.execSubqueryScalar(resolvedQuery)
	case *parser.BinaryExpr:
		left, err := ex.materializeForRow(e.Left, outerAlias, outerDoc)
		if err != nil {
			return nil, err
		}
		right, err := ex.materializeForRow(e.Right, outerAlias, outerDoc)
		if err != nil {
			return nil, err
		}
		return &parser.BinaryExpr{Left: left, Op: e.Op, Right: right}, nil
	case *parser.InExpr:
		left, err := ex.materializeForRow(e.Expr, outerAlias, outerDoc)
		if err != nil {
			return nil, err
		}
		var newValues []parser.Expr
		for _, v := range e.Values {
			if sub, ok := v.(*parser.SubqueryExpr); ok {
				resolvedQuery := &parser.SelectStatement{
					Distinct:  sub.Query.Distinct,
					Columns:   sub.Query.Columns,
					From:      sub.Query.From,
					FromAlias: sub.Query.FromAlias,
					Joins:     sub.Query.Joins,
					Where:     substituteOuterRefs(sub.Query.Where, outerAlias, outerDoc),
					GroupBy:   sub.Query.GroupBy,
					Having:    sub.Query.Having,
					OrderBy:   sub.Query.OrderBy,
					Limit:     sub.Query.Limit,
					Offset:    sub.Query.Offset,
				}
				expanded, err := ex.execSubqueryValues(resolvedQuery)
				if err != nil {
					return nil, err
				}
				newValues = append(newValues, expanded...)
			} else {
				mat, err := ex.materializeForRow(v, outerAlias, outerDoc)
				if err != nil {
					return nil, err
				}
				newValues = append(newValues, mat)
			}
		}
		return &parser.InExpr{Expr: left, Values: newValues, Negate: e.Negate}, nil
	case *parser.NotExpr:
		inner, err := ex.materializeForRow(e.Expr, outerAlias, outerDoc)
		if err != nil {
			return nil, err
		}
		return &parser.NotExpr{Expr: inner}, nil
	case *parser.AliasExpr:
		inner, err := ex.materializeForRow(e.Expr, outerAlias, outerDoc)
		if err != nil {
			return nil, err
		}
		return &parser.AliasExpr{Expr: inner, Alias: e.Alias}, nil
	default:
		return expr, nil
	}
}

// execSubqueryScalar exécute un SELECT et retourne un LiteralExpr scalaire.
// Si le résultat contient plus d'une ligne ou colonne, prend la première valeur.
func (ex *Executor) execSubqueryScalar(stmt *parser.SelectStatement) (parser.Expr, error) {
	result, err := ex.execSelect(stmt)
	if err != nil {
		return nil, fmt.Errorf("subquery: %w", err)
	}

	if len(result.Docs) == 0 {
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenNull, Literal: "NULL"}}, nil
	}

	// Prendre le premier champ du premier document
	doc := result.Docs[0].Doc
	if len(doc.Fields) == 0 {
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenNull, Literal: "NULL"}}, nil
	}

	return valueToLiteralExpr(doc.Fields[0].Value), nil
}

// execSubqueryValues exécute un SELECT et retourne une liste de LiteralExpr
// (un par ligne, prenant le premier champ de chaque ligne).
func (ex *Executor) execSubqueryValues(stmt *parser.SelectStatement) ([]parser.Expr, error) {
	result, err := ex.execSelect(stmt)
	if err != nil {
		return nil, fmt.Errorf("subquery: %w", err)
	}

	var exprs []parser.Expr
	for _, rd := range result.Docs {
		if len(rd.Doc.Fields) == 0 {
			continue
		}
		exprs = append(exprs, valueToLiteralExpr(rd.Doc.Fields[0].Value))
	}
	return exprs, nil
}

// valueToLiteralExpr convertit une valeur Go en LiteralExpr du parser.
func valueToLiteralExpr(val interface{}) parser.Expr {
	switch v := val.(type) {
	case string:
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenString, Literal: v}}
	case int64:
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenInteger, Literal: fmt.Sprintf("%d", v)}}
	case float64:
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenFloat, Literal: fmt.Sprintf("%g", v)}}
	case bool:
		if v {
			return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenTrue, Literal: "true"}}
		}
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenFalse, Literal: "false"}}
	case *storage.Document:
		// Sous-document → pas convertible en scalaire, retourner null
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenNull, Literal: "NULL"}}
	default:
		return &parser.LiteralExpr{Token: parser.Token{Type: parser.TokenNull, Literal: "NULL"}}
	}
}
