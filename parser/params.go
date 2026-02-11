package parser

import (
	"fmt"
	"strconv"
)

// ResolveParams walks the AST and replaces all ParamExpr nodes with LiteralExpr
// nodes using the provided parameter values. This is the core of parameterized
// query support, preventing SQL injection by keeping data separate from the query.
func ResolveParams(stmt Statement, params []interface{}) error {
	count := countParams(stmt)
	if count != len(params) {
		return fmt.Errorf("expected %d parameters, got %d", count, len(params))
	}
	if count == 0 {
		return nil
	}
	return resolveInStatement(stmt, params)
}

// paramToLiteral converts a Go value to a LiteralExpr token.
func paramToLiteral(val interface{}) (*LiteralExpr, error) {
	switch v := val.(type) {
	case string:
		return &LiteralExpr{Token: Token{Type: TokenString, Literal: v}}, nil
	case int:
		return &LiteralExpr{Token: Token{Type: TokenInteger, Literal: strconv.Itoa(v)}}, nil
	case int64:
		return &LiteralExpr{Token: Token{Type: TokenInteger, Literal: strconv.FormatInt(v, 10)}}, nil
	case float64:
		return &LiteralExpr{Token: Token{Type: TokenFloat, Literal: strconv.FormatFloat(v, 'f', -1, 64)}}, nil
	case bool:
		if v {
			return &LiteralExpr{Token: Token{Type: TokenTrue, Literal: "true"}}, nil
		}
		return &LiteralExpr{Token: Token{Type: TokenFalse, Literal: "false"}}, nil
	case nil:
		return &LiteralExpr{Token: Token{Type: TokenNull, Literal: "null"}}, nil
	default:
		return &LiteralExpr{Token: Token{Type: TokenString, Literal: fmt.Sprintf("%v", v)}}, nil
	}
}

// resolveExpr replaces ParamExpr in an expression tree with LiteralExpr.
// Returns the (possibly replaced) expression.
func resolveExpr(expr Expr, params []interface{}) (Expr, error) {
	if expr == nil {
		return nil, nil
	}
	switch e := expr.(type) {
	case *ParamExpr:
		if e.Index < 0 || e.Index >= len(params) {
			return nil, fmt.Errorf("parameter index %d out of range (have %d params)", e.Index, len(params))
		}
		return paramToLiteral(params[e.Index])

	case *BinaryExpr:
		left, err := resolveExpr(e.Left, params)
		if err != nil {
			return nil, err
		}
		right, err := resolveExpr(e.Right, params)
		if err != nil {
			return nil, err
		}
		e.Left = left
		e.Right = right
		return e, nil

	case *NotExpr:
		inner, err := resolveExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		e.Expr = inner
		return e, nil

	case *IsNullExpr:
		inner, err := resolveExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		e.Expr = inner
		return e, nil

	case *InExpr:
		exprResolved, err := resolveExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		e.Expr = exprResolved
		for i, v := range e.Values {
			resolved, err := resolveExpr(v, params)
			if err != nil {
				return nil, err
			}
			e.Values[i] = resolved
		}
		return e, nil

	case *BetweenExpr:
		expr, err := resolveExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		low, err := resolveExpr(e.Low, params)
		if err != nil {
			return nil, err
		}
		high, err := resolveExpr(e.High, params)
		if err != nil {
			return nil, err
		}
		e.Expr = expr
		e.Low = low
		e.High = high
		return e, nil

	case *CaseExpr:
		for i, w := range e.Whens {
			cond, err := resolveExpr(w.Condition, params)
			if err != nil {
				return nil, err
			}
			result, err := resolveExpr(w.Result, params)
			if err != nil {
				return nil, err
			}
			e.Whens[i].Condition = cond
			e.Whens[i].Result = result
		}
		if e.Else != nil {
			el, err := resolveExpr(e.Else, params)
			if err != nil {
				return nil, err
			}
			e.Else = el
		}
		return e, nil

	case *FuncCallExpr:
		for i, arg := range e.Args {
			resolved, err := resolveExpr(arg, params)
			if err != nil {
				return nil, err
			}
			e.Args[i] = resolved
		}
		return e, nil

	case *AliasExpr:
		inner, err := resolveExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		e.Expr = inner
		return e, nil

	case *SubqueryExpr:
		return e, resolveInStatement(e.Query, params)

	default:
		// LiteralExpr, IdentExpr, DotExpr, StarExpr, etc. — no params to resolve
		return e, nil
	}
}

// resolveExprList resolves params in a slice of expressions.
func resolveExprList(exprs []Expr, params []interface{}) error {
	for i, expr := range exprs {
		resolved, err := resolveExpr(expr, params)
		if err != nil {
			return err
		}
		exprs[i] = resolved
	}
	return nil
}

// resolveInStatement walks a statement and resolves all ParamExpr nodes.
func resolveInStatement(stmt Statement, params []interface{}) error {
	switch s := stmt.(type) {
	case *SelectStatement:
		if err := resolveExprList(s.Columns, params); err != nil {
			return err
		}
		if s.Where != nil {
			w, err := resolveExpr(s.Where, params)
			if err != nil {
				return err
			}
			s.Where = w
		}
		if s.Having != nil {
			h, err := resolveExpr(s.Having, params)
			if err != nil {
				return err
			}
			s.Having = h
		}
		if err := resolveExprList(s.GroupBy, params); err != nil {
			return err
		}
		for i, ob := range s.OrderBy {
			resolved, err := resolveExpr(ob.Expr, params)
			if err != nil {
				return err
			}
			s.OrderBy[i].Expr = resolved
		}
		for _, j := range s.Joins {
			if j.Condition != nil {
				cond, err := resolveExpr(j.Condition, params)
				if err != nil {
					return err
				}
				j.Condition = cond
			}
		}

	case *InsertStatement:
		for i, fa := range s.Fields {
			resolved, err := resolveExpr(fa.Value, params)
			if err != nil {
				return err
			}
			s.Fields[i].Value = resolved
		}

	case *UpdateStatement:
		for i, fa := range s.Assignments {
			resolved, err := resolveExpr(fa.Value, params)
			if err != nil {
				return err
			}
			s.Assignments[i].Value = resolved
		}
		if s.Where != nil {
			w, err := resolveExpr(s.Where, params)
			if err != nil {
				return err
			}
			s.Where = w
		}

	case *DeleteStatement:
		if s.Where != nil {
			w, err := resolveExpr(s.Where, params)
			if err != nil {
				return err
			}
			s.Where = w
		}

	case *ExplainStatement:
		return resolveInStatement(s.Inner, params)

	case *UnionStatement:
		if err := resolveInStatement(s.Left, params); err != nil {
			return err
		}
		return resolveInStatement(s.Right, params)
	}
	return nil
}

// countParams counts the total number of ParamExpr nodes in a statement.
func countParams(stmt Statement) int {
	count := 0
	countInExpr(stmt, &count)
	return count
}

func countInExpr(node interface{}, count *int) {
	switch n := node.(type) {
	case *ParamExpr:
		*count++
	case *BinaryExpr:
		countInExpr(n.Left, count)
		countInExpr(n.Right, count)
	case *NotExpr:
		countInExpr(n.Expr, count)
	case *IsNullExpr:
		countInExpr(n.Expr, count)
	case *InExpr:
		countInExpr(n.Expr, count)
		for _, v := range n.Values {
			countInExpr(v, count)
		}
	case *BetweenExpr:
		countInExpr(n.Expr, count)
		countInExpr(n.Low, count)
		countInExpr(n.High, count)
	case *CaseExpr:
		for _, w := range n.Whens {
			countInExpr(w.Condition, count)
			countInExpr(w.Result, count)
		}
		if n.Else != nil {
			countInExpr(n.Else, count)
		}
	case *FuncCallExpr:
		for _, arg := range n.Args {
			countInExpr(arg, count)
		}
	case *AliasExpr:
		countInExpr(n.Expr, count)
	case *SubqueryExpr:
		countInExpr(n.Query, count)
	case *SelectStatement:
		for _, c := range n.Columns {
			countInExpr(c, count)
		}
		if n.Where != nil {
			countInExpr(n.Where, count)
		}
		if n.Having != nil {
			countInExpr(n.Having, count)
		}
		for _, ob := range n.OrderBy {
			countInExpr(ob.Expr, count)
		}
		for _, j := range n.Joins {
			if j.Condition != nil {
				countInExpr(j.Condition, count)
			}
		}
	case *InsertStatement:
		for _, fa := range n.Fields {
			countInExpr(fa.Value, count)
		}
	case *UpdateStatement:
		for _, fa := range n.Assignments {
			countInExpr(fa.Value, count)
		}
		if n.Where != nil {
			countInExpr(n.Where, count)
		}
	case *DeleteStatement:
		if n.Where != nil {
			countInExpr(n.Where, count)
		}
	case *ExplainStatement:
		countInExpr(n.Inner, count)
	case *UnionStatement:
		countInExpr(n.Left, count)
		countInExpr(n.Right, count)
	}
}
