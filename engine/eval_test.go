package engine

import (
	"testing"

	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// helper : crée un document de test
func testDoc() *storage.Document {
	doc := storage.NewDocument()
	doc.Set("name", "oracle")
	doc.Set("retry", int64(5))
	doc.Set("enabled", true)
	doc.Set("rate", 3.14)
	doc.Set("empty", nil)

	sub := storage.NewDocument()
	sub.Set("timeout", int64(30))
	doc.Set("params", sub)
	return doc
}

func evalWhere(t *testing.T, query string, doc *storage.Document) bool {
	t.Helper()
	// Parse "SELECT * FROM x WHERE <expr>" et extraire le WHERE
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	sel := stmt.(*parser.SelectStatement)
	result, err := EvalExpr(sel.Where, doc)
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}
	return result
}

func TestEvalEQ(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE name="oracle"`, doc) {
		t.Error("name=oracle should match")
	}
	if evalWhere(t, `SELECT * FROM x WHERE name="mysql"`, doc) {
		t.Error("name=mysql should not match")
	}
}

func TestEvalNumericComparison(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE retry > 3`, doc) {
		t.Error("retry>3 should match (retry=5)")
	}
	if evalWhere(t, `SELECT * FROM x WHERE retry > 10`, doc) {
		t.Error("retry>10 should not match")
	}
	if !evalWhere(t, `SELECT * FROM x WHERE retry >= 5`, doc) {
		t.Error("retry>=5 should match")
	}
	if !evalWhere(t, `SELECT * FROM x WHERE retry <= 5`, doc) {
		t.Error("retry<=5 should match")
	}
	if evalWhere(t, `SELECT * FROM x WHERE retry < 5`, doc) {
		t.Error("retry<5 should not match")
	}
	if !evalWhere(t, `SELECT * FROM x WHERE retry != 3`, doc) {
		t.Error("retry!=3 should match")
	}
}

func TestEvalBool(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE enabled=true`, doc) {
		t.Error("enabled=true should match")
	}
	if evalWhere(t, `SELECT * FROM x WHERE enabled=false`, doc) {
		t.Error("enabled=false should not match")
	}
}

func TestEvalNull(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE empty=null`, doc) {
		t.Error("empty=null should match")
	}
	if evalWhere(t, `SELECT * FROM x WHERE name=null`, doc) {
		t.Error("name=null should not match")
	}
}

func TestEvalAnd(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE name="oracle" AND retry=5`, doc) {
		t.Error("AND both true should match")
	}
	if evalWhere(t, `SELECT * FROM x WHERE name="oracle" AND retry=99`, doc) {
		t.Error("AND one false should not match")
	}
}

func TestEvalOr(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE name="mysql" OR retry=5`, doc) {
		t.Error("OR one true should match")
	}
	if evalWhere(t, `SELECT * FROM x WHERE name="mysql" OR retry=99`, doc) {
		t.Error("OR both false should not match")
	}
}

func TestEvalNested(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE params.timeout=30`, doc) {
		t.Error("params.timeout=30 should match")
	}
	if evalWhere(t, `SELECT * FROM x WHERE params.timeout=99`, doc) {
		t.Error("params.timeout=99 should not match")
	}
}

func TestEvalFloatComparison(t *testing.T) {
	doc := testDoc()
	if !evalWhere(t, `SELECT * FROM x WHERE rate > 3.0`, doc) {
		t.Error("rate>3.0 should match (rate=3.14)")
	}
	if evalWhere(t, `SELECT * FROM x WHERE rate > 4.0`, doc) {
		t.Error("rate>4.0 should not match")
	}
}

func TestEvalNilWhere(t *testing.T) {
	doc := testDoc()
	result, err := EvalExpr(nil, doc)
	if err != nil {
		t.Fatalf("eval nil: %v", err)
	}
	if !result {
		t.Error("nil WHERE should match all")
	}
}

func TestEvalMissingField(t *testing.T) {
	doc := testDoc()
	// Champ inexistant comparé à une valeur → ne doit pas matcher
	if evalWhere(t, `SELECT * FROM x WHERE nonexistent=5`, doc) {
		t.Error("nonexistent=5 should not match")
	}
	// Champ inexistant comparé à null → doit matcher (les deux sont nil)
	if !evalWhere(t, `SELECT * FROM x WHERE nonexistent=null`, doc) {
		t.Error("nonexistent=null should match (both nil)")
	}
}

func TestEvalIn(t *testing.T) {
	doc := testDoc()
	p := parser.NewParser(`SELECT * FROM x WHERE name IN ("oracle", "mysql")`)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(*parser.SelectStatement)
	result, err := EvalExpr(sel.Where, doc)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !result {
		t.Error("name IN ('oracle','mysql') should match")
	}
}

func TestEvalIsNull(t *testing.T) {
	doc := testDoc()
	// "empty" field is nil → IS NULL should match
	if !evalWhere(t, `SELECT * FROM x WHERE empty IS NULL`, doc) {
		t.Error("empty IS NULL should match (field is nil)")
	}
	// "name" field is "oracle" → IS NULL should NOT match
	if evalWhere(t, `SELECT * FROM x WHERE name IS NULL`, doc) {
		t.Error("name IS NULL should not match (name=oracle)")
	}
	// non-existent field → IS NULL should match
	if !evalWhere(t, `SELECT * FROM x WHERE nonexistent IS NULL`, doc) {
		t.Error("nonexistent IS NULL should match (missing field is nil)")
	}
}

func TestEvalIsNotNull(t *testing.T) {
	doc := testDoc()
	// "name" field exists → IS NOT NULL should match
	if !evalWhere(t, `SELECT * FROM x WHERE name IS NOT NULL`, doc) {
		t.Error("name IS NOT NULL should match")
	}
	// "empty" is nil → IS NOT NULL should NOT match
	if evalWhere(t, `SELECT * FROM x WHERE empty IS NOT NULL`, doc) {
		t.Error("empty IS NOT NULL should not match")
	}
	// non-existent → IS NOT NULL should NOT match
	if evalWhere(t, `SELECT * FROM x WHERE nonexistent IS NOT NULL`, doc) {
		t.Error("nonexistent IS NOT NULL should not match")
	}
}

func TestExprToFieldPath(t *testing.T) {
	path := ExprToFieldPath(&parser.IdentExpr{Name: "retry"})
	if len(path) != 1 || path[0] != "retry" {
		t.Errorf("expected [retry], got %v", path)
	}

	path = ExprToFieldPath(&parser.DotExpr{Parts: []string{"params", "timeout"}})
	if len(path) != 2 || path[0] != "params" || path[1] != "timeout" {
		t.Errorf("expected [params timeout], got %v", path)
	}

	path = ExprToFieldPath(&parser.StarExpr{})
	if path != nil {
		t.Errorf("expected nil for StarExpr, got %v", path)
	}
}

func TestExprToFieldName(t *testing.T) {
	name := ExprToFieldName(&parser.IdentExpr{Name: "retry"})
	if name != "retry" {
		t.Errorf("expected retry, got %s", name)
	}
	name = ExprToFieldName(&parser.DotExpr{Parts: []string{"params", "timeout"}})
	if name != "params.timeout" {
		t.Errorf("expected params.timeout, got %s", name)
	}
}
