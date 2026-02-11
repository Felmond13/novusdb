package parser

import (
	"testing"
)

func TestParseSelect(t *testing.T) {
	input := `SELECT * FROM workflows WHERE retry > 3`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if sel.From != "workflows" {
		t.Errorf("expected FROM workflows, got %s", sel.From)
	}
	if len(sel.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(sel.Columns))
	}
	if _, ok := sel.Columns[0].(*StarExpr); !ok {
		t.Errorf("expected StarExpr, got %T", sel.Columns[0])
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseInsert(t *testing.T) {
	input := `INSERT INTO jobs VALUES (type="oracle", retry=5)`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ins, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	if ins.Table != "jobs" {
		t.Errorf("expected table jobs, got %s", ins.Table)
	}
	if len(ins.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(ins.Fields))
	}
}

func TestParseInsertFromSelect(t *testing.T) {
	input := `INSERT INTO backup SELECT * FROM jobs WHERE retry > 0`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ins, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	if ins.Table != "backup" {
		t.Errorf("expected table backup, got %s", ins.Table)
	}
	if ins.Source == nil {
		t.Fatal("expected Source SelectStatement, got nil")
	}
	if ins.Source.From != "jobs" {
		t.Errorf("expected source from jobs, got %s", ins.Source.From)
	}
	if ins.Source.Where == nil {
		t.Error("expected WHERE clause in source select")
	}
	if len(ins.Fields) != 0 {
		t.Errorf("expected 0 fields for insert-select, got %d", len(ins.Fields))
	}
}

func TestParseUpdate(t *testing.T) {
	input := `UPDATE jobs SET params.timeout=60 WHERE params.timeout<30`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	upd, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected UpdateStatement, got %T", stmt)
	}
	if upd.Table != "jobs" {
		t.Errorf("expected table jobs, got %s", upd.Table)
	}
	if len(upd.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(upd.Assignments))
	}
	if upd.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseDelete(t *testing.T) {
	input := `DELETE FROM jobs WHERE enabled=false`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	del, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected DeleteStatement, got %T", stmt)
	}
	if del.Table != "jobs" {
		t.Errorf("expected table jobs, got %s", del.Table)
	}
	if del.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseSelectWithGroupBy(t *testing.T) {
	input := `SELECT type, COUNT(*) FROM jobs GROUP BY type`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if len(sel.GroupBy) != 1 {
		t.Errorf("expected 1 GROUP BY column, got %d", len(sel.GroupBy))
	}
}

func TestParseSelectWithJoin(t *testing.T) {
	input := `SELECT * FROM jobs JOIN results ON jobs.id = results.job_id`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if len(sel.Joins) != 1 {
		t.Errorf("expected 1 JOIN, got %d", len(sel.Joins))
	}
	if sel.Joins[0].Type != "INNER" {
		t.Errorf("expected INNER join, got %s", sel.Joins[0].Type)
	}
	if sel.Joins[0].Table != "results" {
		t.Errorf("expected join table results, got %s", sel.Joins[0].Table)
	}
}

func TestParseSelectWithOrderByLimit(t *testing.T) {
	input := `SELECT * FROM jobs ORDER BY retry DESC LIMIT 10 OFFSET 5`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if len(sel.OrderBy) != 1 {
		t.Errorf("expected 1 ORDER BY, got %d", len(sel.OrderBy))
	}
	if !sel.OrderBy[0].Desc {
		t.Error("expected DESC order")
	}
	if sel.Limit != 10 {
		t.Errorf("expected LIMIT 10, got %d", sel.Limit)
	}
	if sel.Offset != 5 {
		t.Errorf("expected OFFSET 5, got %d", sel.Offset)
	}
}

func TestParseCreateIndex(t *testing.T) {
	input := `CREATE INDEX ON jobs (type)`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ci, ok := stmt.(*CreateIndexStatement)
	if !ok {
		t.Fatalf("expected CreateIndexStatement, got %T", stmt)
	}
	if ci.Table != "jobs" {
		t.Errorf("expected table jobs, got %s", ci.Table)
	}
	if ci.Field != "type" {
		t.Errorf("expected field type, got %s", ci.Field)
	}
}

func TestParseSelectWithAndOr(t *testing.T) {
	input := `SELECT * FROM jobs WHERE retry > 3 AND enabled = true OR type = "oracle"`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	// The root should be an OR expression (AND binds tighter)
	or, ok := sel.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", sel.Where)
	}
	if or.Op != TokenOr {
		t.Errorf("expected OR at root, got %d", or.Op)
	}
}

func TestParseSubquery(t *testing.T) {
	input := `SELECT * FROM jobs WHERE type IN ("oracle", "mysql")`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	inExpr, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", sel.Where)
	}
	if len(inExpr.Values) != 2 {
		t.Errorf("expected 2 IN values, got %d", len(inExpr.Values))
	}
}

func TestParseCreateSequence(t *testing.T) {
	input := `CREATE SEQUENCE user_seq START WITH 100 INCREMENT BY 5 MAXVALUE 9999 CYCLE`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	cs, ok := stmt.(*CreateSequenceStatement)
	if !ok {
		t.Fatalf("expected CreateSequenceStatement, got %T", stmt)
	}
	if cs.Name != "user_seq" {
		t.Errorf("expected name user_seq, got %s", cs.Name)
	}
	if cs.StartWith != 100 {
		t.Errorf("expected START WITH 100, got %g", cs.StartWith)
	}
	if cs.IncrementBy != 5 {
		t.Errorf("expected INCREMENT BY 5, got %g", cs.IncrementBy)
	}
	if cs.MaxValue != 9999 {
		t.Errorf("expected MAXVALUE 9999, got %g", cs.MaxValue)
	}
	if !cs.Cycle {
		t.Error("expected CYCLE to be true")
	}
}

func TestParseCreateSequenceDefaults(t *testing.T) {
	input := `CREATE SEQUENCE my_seq`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	cs, ok := stmt.(*CreateSequenceStatement)
	if !ok {
		t.Fatalf("expected CreateSequenceStatement, got %T", stmt)
	}
	if cs.Name != "my_seq" {
		t.Errorf("expected name my_seq, got %s", cs.Name)
	}
	if cs.StartWith != 1 {
		t.Errorf("expected default START WITH 1, got %g", cs.StartWith)
	}
	if cs.IncrementBy != 1 {
		t.Errorf("expected default INCREMENT BY 1, got %g", cs.IncrementBy)
	}
	if cs.Cycle {
		t.Error("expected CYCLE to be false by default")
	}
}

func TestParseDropSequence(t *testing.T) {
	input := `DROP SEQUENCE IF EXISTS user_seq`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ds, ok := stmt.(*DropSequenceStatement)
	if !ok {
		t.Fatalf("expected DropSequenceStatement, got %T", stmt)
	}
	if ds.Name != "user_seq" {
		t.Errorf("expected name user_seq, got %s", ds.Name)
	}
	if !ds.IfExists {
		t.Error("expected IfExists to be true")
	}
}

func TestParseSequenceExprNextval(t *testing.T) {
	input := `INSERT INTO users VALUES (id=user_seq.NEXTVAL, name="Alice")`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ins, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	if len(ins.Fields) < 1 {
		t.Fatal("expected at least 1 field")
	}
	seq, ok := ins.Fields[0].Value.(*SequenceExpr)
	if !ok {
		t.Fatalf("expected SequenceExpr for id value, got %T", ins.Fields[0].Value)
	}
	if seq.SeqName != "user_seq" {
		t.Errorf("expected SeqName user_seq, got %s", seq.SeqName)
	}
	if seq.Op != "NEXTVAL" {
		t.Errorf("expected Op NEXTVAL, got %s", seq.Op)
	}
}

func TestParseSequenceExprCurrval(t *testing.T) {
	input := `SELECT user_seq.CURRVAL FROM users`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	seq, ok := sel.Columns[0].(*SequenceExpr)
	if !ok {
		t.Fatalf("expected SequenceExpr, got %T", sel.Columns[0])
	}
	if seq.SeqName != "user_seq" {
		t.Errorf("expected SeqName user_seq, got %s", seq.SeqName)
	}
	if seq.Op != "CURRVAL" {
		t.Errorf("expected Op CURRVAL, got %s", seq.Op)
	}
}

func TestParseSysdate(t *testing.T) {
	input := `INSERT INTO logs VALUES (msg="hello", created=SYSDATE)`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ins, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	if len(ins.Fields) < 2 {
		t.Fatal("expected at least 2 fields")
	}
	sd, ok := ins.Fields[1].Value.(*SysdateExpr)
	if !ok {
		t.Fatalf("expected SysdateExpr for created value, got %T", ins.Fields[1].Value)
	}
	if sd.Variant != "SYSDATE" {
		t.Errorf("expected Variant SYSDATE, got %s", sd.Variant)
	}
}

func TestParseCurrentDate(t *testing.T) {
	input := `SELECT CURRENT_DATE FROM users`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	sd, ok := sel.Columns[0].(*SysdateExpr)
	if !ok {
		t.Fatalf("expected SysdateExpr, got %T", sel.Columns[0])
	}
	if sd.Variant != "CURRENT_DATE" {
		t.Errorf("expected Variant CURRENT_DATE, got %s", sd.Variant)
	}
}

func TestParseNow(t *testing.T) {
	input := `INSERT INTO logs VALUES (ts=NOW())`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	ins, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}
	sd, ok := ins.Fields[0].Value.(*SysdateExpr)
	if !ok {
		t.Fatalf("expected SysdateExpr for ts value, got %T", ins.Fields[0].Value)
	}
	if sd.Variant != "SYSDATE" {
		t.Errorf("expected Variant SYSDATE (NOW alias), got %s", sd.Variant)
	}
}

func TestParseSysdateInWhere(t *testing.T) {
	input := `DELETE FROM logs WHERE created < SYSDATE`
	p := NewParser(input)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	del, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected DeleteStatement, got %T", stmt)
	}
	if del.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	bin, ok := del.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", del.Where)
	}
	_, ok = bin.Right.(*SysdateExpr)
	if !ok {
		t.Fatalf("expected SysdateExpr on right side, got %T", bin.Right)
	}
}
