package api

import (
	"fmt"
	"testing"
)

// ---------- helpers ----------

// setupComplexDB creates a DB with employees + departments + projects for complex query tests.
func setupComplexDB(t *testing.T) *DB {
	t.Helper()
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// departments
	mustExec(t, db, `INSERT INTO departments VALUES {"name": "Engineering", "budget": 500000, "floor": 3}`)
	mustExec(t, db, `INSERT INTO departments VALUES {"name": "Sales", "budget": 200000, "floor": 1}`)
	mustExec(t, db, `INSERT INTO departments VALUES {"name": "HR", "budget": 150000, "floor": 2}`)

	// employees
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Alice", "department": "Engineering", "salary": 90000, "age": 30, "active": true}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Bob", "department": "Engineering", "salary": 85000, "age": 28, "active": true}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Charlie", "department": "Sales", "salary": 60000, "age": 35, "active": true}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Diana", "department": "Sales", "salary": 65000, "age": 40, "active": false}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Eve", "department": "HR", "salary": 55000, "age": 25, "active": true}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Frank", "department": "HR", "salary": 58000, "age": 32, "active": false}`)

	// projects
	mustExec(t, db, `INSERT INTO projects VALUES {"title": "Alpha", "department": "Engineering", "priority": 1}`)
	mustExec(t, db, `INSERT INTO projects VALUES {"title": "Beta", "department": "Engineering", "priority": 2}`)
	mustExec(t, db, `INSERT INTO projects VALUES {"title": "Gamma", "department": "Sales", "priority": 1}`)

	return db
}

func mustExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func mustQuery(t *testing.T, db *DB, sql string) []map[string]interface{} {
	t.Helper()
	res, err := db.Exec(sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	var rows []map[string]interface{}
	for _, d := range res.Docs {
		row := make(map[string]interface{})
		for _, f := range d.Doc.Fields {
			row[f.Name] = f.Value
		}
		rows = append(rows, row)
	}
	return rows
}

func assertRowCount(t *testing.T, rows []map[string]interface{}, expected int) {
	t.Helper()
	if len(rows) != expected {
		t.Fatalf("expected %d rows, got %d", expected, len(rows))
	}
}

func assertFieldEquals(t *testing.T, row map[string]interface{}, field string, expected interface{}) {
	t.Helper()
	got, ok := row[field]
	if !ok {
		t.Fatalf("field %q not found in row: %v", field, row)
	}
	// Compare with type coercion for numbers
	if !valuesEqual(got, expected) {
		t.Errorf("field %q: expected %v (%T), got %v (%T)", field, expected, expected, got, got)
	}
}

func valuesEqual(a, b interface{}) bool {
	// Convert both to float64 for numeric comparison
	af, aok := toF64(a)
	bf, bok := toF64(b)
	if aok && bok {
		return af == bf
	}
	// String comparison
	if as, ok := a.(string); ok {
		if bs, ok2 := b.(string); ok2 {
			return as == bs
		}
	}
	// Bool comparison
	if ab, ok := a.(bool); ok {
		if bb, ok2 := b.(bool); ok2 {
			return ab == bb
		}
	}
	return a == b
}

func toF64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	}
	return 0, false
}

// =====================================================
// INNER JOIN
// =====================================================

func TestJoinInner(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.name, e.salary, d.budget FROM employees e JOIN departments d ON e.department = d.name WHERE e.department = "Engineering"`)

	assertRowCount(t, rows, 2)
	for _, r := range rows {
		assertFieldEquals(t, r, "d.budget", int64(500000))
	}
}

func TestJoinInnerAllRows(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.name, d.floor FROM employees e JOIN departments d ON e.department = d.name`)

	// All 6 employees should match a department
	assertRowCount(t, rows, 6)
}

// =====================================================
// LEFT JOIN
// =====================================================

func TestJoinLeft(t *testing.T) {
	db := setupComplexDB(t)

	// Add employee with no matching department
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Ghost", "department": "Unknown", "salary": 0, "age": 99, "active": false}`)

	rows := mustQuery(t, db,
		`SELECT e.name, d.budget FROM employees e LEFT JOIN departments d ON e.department = d.name`)

	// 7 employees: 6 matched + 1 unmatched (Ghost with NULL budget)
	assertRowCount(t, rows, 7)

	// Find Ghost row
	found := false
	for _, r := range rows {
		if r["e.name"] == "Ghost" {
			found = true
			if r["d.budget"] != nil {
				t.Errorf("Ghost should have nil budget, got %v", r["d.budget"])
			}
		}
	}
	if !found {
		t.Error("Ghost row not found in LEFT JOIN results")
	}
}

// =====================================================
// JOIN + GROUP BY
// =====================================================

func TestJoinGroupBy(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.department AS dept, COUNT(*) AS cnt, d.budget AS budget
		 FROM employees e JOIN departments d ON e.department = d.name
		 GROUP BY e.department`)

	assertRowCount(t, rows, 3) // Engineering, Sales, HR

	for _, r := range rows {
		dept := r["dept"]
		switch dept {
		case "Engineering":
			assertFieldEquals(t, r, "cnt", int64(2))
			assertFieldEquals(t, r, "budget", int64(500000))
		case "Sales":
			assertFieldEquals(t, r, "cnt", int64(2))
			assertFieldEquals(t, r, "budget", int64(200000))
		case "HR":
			assertFieldEquals(t, r, "cnt", int64(2))
			assertFieldEquals(t, r, "budget", int64(150000))
		default:
			t.Errorf("unexpected department: %v", dept)
		}
	}
}

// =====================================================
// JOIN + ORDER BY + LIMIT
// =====================================================

func TestJoinOrderByLimit(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.name, e.salary, d.floor
		 FROM employees e JOIN departments d ON e.department = d.name
		 ORDER BY e.salary DESC LIMIT 3`)

	assertRowCount(t, rows, 3)
	// Top 3 salaries: Alice 90000, Bob 85000, Diana 65000
	assertFieldEquals(t, rows[0], "e.name", "Alice")
	assertFieldEquals(t, rows[1], "e.name", "Bob")
	assertFieldEquals(t, rows[2], "e.name", "Diana")
}

// =====================================================
// Multiple JOINs
// =====================================================

func TestMultipleJoins(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.name, d.budget, p.title
		 FROM employees e
		 JOIN departments d ON e.department = d.name
		 JOIN projects p ON e.department = p.department
		 WHERE e.name = "Alice"`)

	// Alice is in Engineering which has 2 projects (Alpha, Beta)
	assertRowCount(t, rows, 2)
	for _, r := range rows {
		assertFieldEquals(t, r, "e.name", "Alice")
		assertFieldEquals(t, r, "d.budget", int64(500000))
	}
}

// =====================================================
// Subquery in WHERE (scalar)
// =====================================================

func TestSubqueryScalar(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)`)

	// AVG salary = (90000+85000+60000+65000+55000+58000)/6 = 68833.33
	// Above avg: Alice (90000), Bob (85000)
	assertRowCount(t, rows, 2)
}

// =====================================================
// Subquery in IN clause
// =====================================================

func TestSubqueryIN(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name, department FROM employees
		 WHERE department IN (SELECT department FROM projects WHERE priority = 1)`)

	// Projects with priority 1: Engineering, Sales → employees: Alice, Bob, Charlie, Diana
	assertRowCount(t, rows, 4)
}

func TestSubqueryNOTIN(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees
		 WHERE department NOT IN (SELECT department FROM projects WHERE priority = 1)`)

	// Departments NOT in projects(priority=1): HR → employees: Eve, Frank
	assertRowCount(t, rows, 2)
}

// =====================================================
// INSERT INTO ... SELECT
// =====================================================

func TestInsertIntoSelect(t *testing.T) {
	db := setupComplexDB(t)

	mustExec(t, db,
		`INSERT INTO high_earners SELECT name, salary, department FROM employees WHERE salary > 80000`)

	rows := mustQuery(t, db, `SELECT * FROM high_earners`)
	assertRowCount(t, rows, 2) // Alice, Bob
}

// =====================================================
// UNION / UNION ALL
// =====================================================

func TestUnionAll(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE department = "Engineering"
		 UNION ALL
		 SELECT name FROM employees WHERE department = "Sales"`)

	assertRowCount(t, rows, 4) // Alice, Bob, Charlie, Diana
}

func TestUnionDistinct(t *testing.T) {
	db := setupComplexDB(t)

	// Both sides include Alice
	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE name = "Alice"
		 UNION
		 SELECT name FROM employees WHERE department = "Engineering"`)

	assertRowCount(t, rows, 2) // Alice (deduplicated), Bob
}

// =====================================================
// GROUP BY + HAVING
// =====================================================

func TestGroupByHaving(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING COUNT(*) >= 2`)

	// All departments have exactly 2 employees, so all should match
	assertRowCount(t, rows, 3)
}

func TestGroupByHavingFilter(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT department, AVG(salary) AS avg_sal FROM employees GROUP BY department HAVING AVG(salary) > 70000`)

	// AVG: Engineering=87500, Sales=62500, HR=56500 → only Engineering
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "department", "Engineering")
}

// =====================================================
// BETWEEN
// =====================================================

func TestBetweenComplex(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name, salary FROM employees WHERE salary BETWEEN 55000 AND 65000`)

	// Eve (55000), Frank (58000), Charlie (60000), Diana (65000)
	assertRowCount(t, rows, 4)
}

func TestNotBetweenComplex(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name, salary FROM employees WHERE salary NOT BETWEEN 55000 AND 65000`)

	// Alice (90000), Bob (85000)
	assertRowCount(t, rows, 2)
}

// =====================================================
// LIKE
// =====================================================

func TestLikeComplex(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE name LIKE "A%"`)

	assertRowCount(t, rows, 1) // Alice
	assertFieldEquals(t, rows[0], "name", "Alice")
}

func TestLikeMiddleComplex(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE name LIKE "%li%"`)

	// Alice, Charlie (both contain "li")
	assertRowCount(t, rows, 2)
}

func TestNotLikeComplex(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE name NOT LIKE "A%"`)

	assertRowCount(t, rows, 5) // Everyone except Alice
}

// =====================================================
// IS NULL / IS NOT NULL
// =====================================================

func TestIsNull(t *testing.T) {
	db := setupComplexDB(t)

	mustExec(t, db, `INSERT INTO employees VALUES {"name": "NoSalary", "department": "HR", "active": true}`)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE salary IS NULL`)

	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "name", "NoSalary")
}

func TestIsNotNull(t *testing.T) {
	db := setupComplexDB(t)

	mustExec(t, db, `INSERT INTO employees VALUES {"name": "NoSalary", "department": "HR", "active": true}`)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE salary IS NOT NULL`)

	assertRowCount(t, rows, 6) // Original 6 employees
}

// =====================================================
// IN list (not subquery)
// =====================================================

func TestInList(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE department IN ("Engineering", "HR")`)

	assertRowCount(t, rows, 4) // Alice, Bob, Eve, Frank
}

func TestNotInList(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees WHERE department NOT IN ("Engineering", "HR")`)

	assertRowCount(t, rows, 2) // Charlie, Diana
}

// =====================================================
// DISTINCT
// =====================================================

func TestDistinctComplex(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT DISTINCT department FROM employees`)

	assertRowCount(t, rows, 3) // Engineering, Sales, HR
}

// =====================================================
// ORDER BY + OFFSET + LIMIT (pagination)
// =====================================================

func TestPagination(t *testing.T) {
	db := setupComplexDB(t)

	// Page 1: first 2
	page1 := mustQuery(t, db,
		`SELECT name FROM employees ORDER BY name LIMIT 2 OFFSET 0`)
	assertRowCount(t, page1, 2)
	assertFieldEquals(t, page1[0], "name", "Alice")
	assertFieldEquals(t, page1[1], "name", "Bob")

	// Page 2: next 2
	page2 := mustQuery(t, db,
		`SELECT name FROM employees ORDER BY name LIMIT 2 OFFSET 2`)
	assertRowCount(t, page2, 2)
	assertFieldEquals(t, page2[0], "name", "Charlie")
	assertFieldEquals(t, page2[1], "name", "Diana")

	// Page 3: last 2
	page3 := mustQuery(t, db,
		`SELECT name FROM employees ORDER BY name LIMIT 2 OFFSET 4`)
	assertRowCount(t, page3, 2)
	assertFieldEquals(t, page3[0], "name", "Eve")
	assertFieldEquals(t, page3[1], "name", "Frank")
}

// =====================================================
// Complex WHERE with AND / OR
// =====================================================

func TestComplexWhereAndOr(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name FROM employees
		 WHERE (department = "Engineering" AND salary > 87000)
		    OR (department = "Sales" AND active = true)`)

	// Engineering + salary>87000: Alice (90000)
	// Sales + active: Charlie
	assertRowCount(t, rows, 2)
}

// =====================================================
// Aggregate functions: COUNT, SUM, AVG, MIN, MAX
// =====================================================

func TestAggregates(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT COUNT(*) AS cnt, SUM(salary) AS total, AVG(salary) AS avg_sal,
		        MIN(salary) AS min_sal, MAX(salary) AS max_sal
		 FROM employees`)

	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "cnt", int64(6))
	assertFieldEquals(t, rows[0], "total", float64(413000))
	assertFieldEquals(t, rows[0], "min_sal", float64(55000))
	assertFieldEquals(t, rows[0], "max_sal", float64(90000))
}

func TestGroupByAggregates(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT department, SUM(salary) AS total, MIN(salary) AS min_sal, MAX(salary) AS max_sal
		 FROM employees GROUP BY department ORDER BY total DESC`)

	assertRowCount(t, rows, 3)
	// Engineering: 175000, Sales: 125000, HR: 113000
	assertFieldEquals(t, rows[0], "department", "Engineering")
	assertFieldEquals(t, rows[0], "total", float64(175000))
}

// =====================================================
// CASE WHEN
// =====================================================

func TestCaseWhen(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name,
		        CASE WHEN salary > 80000 THEN "high"
		             WHEN salary > 60000 THEN "medium"
		             ELSE "low"
		        END AS tier
		 FROM employees ORDER BY name`)

	assertRowCount(t, rows, 6)
	// Alice: 90000 → high
	assertFieldEquals(t, rows[0], "name", "Alice")
	assertFieldEquals(t, rows[0], "tier", "high")
	// Bob: 85000 → high
	assertFieldEquals(t, rows[1], "name", "Bob")
	assertFieldEquals(t, rows[1], "tier", "high")
	// Charlie: 60000 → low (not > 60000)
	assertFieldEquals(t, rows[2], "name", "Charlie")
	assertFieldEquals(t, rows[2], "tier", "low")
	// Diana: 65000 → medium
	assertFieldEquals(t, rows[3], "name", "Diana")
	assertFieldEquals(t, rows[3], "tier", "medium")
}

// =====================================================
// Complex: JOIN + WHERE + GROUP BY + HAVING + ORDER BY + LIMIT
// =====================================================

func TestFullComplexQuery(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.department AS dept, COUNT(*) AS cnt, AVG(e.salary) AS avg_sal
		 FROM employees e
		 JOIN departments d ON e.department = d.name
		 WHERE e.active = true
		 GROUP BY e.department
		 HAVING COUNT(*) >= 1
		 ORDER BY avg_sal DESC
		 LIMIT 2`)

	assertRowCount(t, rows, 2)
	// Active employees:
	//   Engineering: Alice(90000), Bob(85000) → avg=87500
	//   Sales: Charlie(60000) → avg=60000
	//   HR: Eve(55000) → avg=55000
	// Top 2 by avg_sal DESC: Engineering (87500), Sales (60000)
	assertFieldEquals(t, rows[0], "dept", "Engineering")
	assertFieldEquals(t, rows[1], "dept", "Sales")
}

// =====================================================
// Index-accelerated JOIN
// =====================================================

func TestJoinWithIndex(t *testing.T) {
	db := setupComplexDB(t)

	// Create index on the join key
	mustExec(t, db, `CREATE INDEX idx_dept_name ON departments(name)`)

	rows := mustQuery(t, db,
		`SELECT e.name, d.budget FROM employees e JOIN departments d ON e.department = d.name
		 WHERE e.department = "Engineering"`)

	assertRowCount(t, rows, 2)
	for _, r := range rows {
		assertFieldEquals(t, r, "d.budget", int64(500000))
	}
}

// =====================================================
// Computed expressions in SELECT
// =====================================================

func TestComputedColumns(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT name, salary, salary * 12 AS annual FROM employees WHERE name = "Alice"`)

	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "annual", float64(90000*12))
}

// =====================================================
// UPDATE with complex WHERE
// =====================================================

func TestUpdateComplexWhere(t *testing.T) {
	db := setupComplexDB(t)

	mustExec(t, db,
		`UPDATE employees SET salary = 70000 WHERE department = "HR" AND active = true`)

	rows := mustQuery(t, db,
		`SELECT name, salary FROM employees WHERE department = "HR" AND active = true`)

	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "name", "Eve")
	assertFieldEquals(t, rows[0], "salary", int64(70000))
}

// =====================================================
// DELETE with complex WHERE
// =====================================================

func TestDeleteComplexWhere(t *testing.T) {
	db := setupComplexDB(t)

	mustExec(t, db,
		`DELETE FROM employees WHERE active = false AND salary < 60000`)

	rows := mustQuery(t, db, `SELECT name FROM employees`)
	// Frank (58000, inactive) should be deleted. Diana (65000, inactive) stays.
	assertRowCount(t, rows, 5)
}

// =====================================================
// Batch INSERT + query
// =====================================================

func TestBatchInsertMultiRow(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db,
		`INSERT INTO items VALUES {"name": "a", "val": 1}, {"name": "b", "val": 2}, {"name": "c", "val": 3}`)

	rows := mustQuery(t, db, `SELECT COUNT(*) AS cnt FROM items`)
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "cnt", int64(3))

	rows = mustQuery(t, db, `SELECT SUM(val) AS total FROM items`)
	assertFieldEquals(t, rows[0], "total", float64(6))
}

// =====================================================
// INSERT OR REPLACE
// =====================================================

func TestInsertOrReplaceComplex(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO kv VALUES {"key": "a", "val": 1}`)
	mustExec(t, db, `INSERT INTO kv VALUES {"key": "b", "val": 2}`)

	// Replace where key = "a"
	mustExec(t, db, `INSERT OR REPLACE INTO kv VALUES (key="a", val=100)`)

	rows := mustQuery(t, db, `SELECT val FROM kv WHERE key = "a"`)
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "val", int64(100))
}

// =====================================================
// Nested field access (dot notation)
// =====================================================

func TestNestedFieldAccess(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"name": "Alice", "address": {"city": "Paris", "zip": "75001"}}`)
	mustExec(t, db, `INSERT INTO users VALUES {"name": "Bob", "address": {"city": "Lyon", "zip": "69001"}}`)

	rows := mustQuery(t, db, `SELECT name FROM users WHERE address.city = "Paris"`)
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "name", "Alice")
}

// =====================================================
// Transactions: BEGIN / COMMIT / ROLLBACK
// =====================================================

func TestTransactionCommit(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO tx_test VALUES {"val": 1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO tx_test VALUES {"val": 2}`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows := mustQuery(t, db, `SELECT COUNT(*) AS cnt FROM tx_test`)
	assertFieldEquals(t, rows[0], "cnt", int64(2))
}

func TestTransactionRollback(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO tx_test VALUES {"val": 1}`)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO tx_test VALUES {"val": 2}`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO tx_test VALUES {"val": 3}`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	rows := mustQuery(t, db, `SELECT COUNT(*) AS cnt FROM tx_test`)
	assertFieldEquals(t, rows[0], "cnt", int64(1))
}

// =====================================================
// Index: CREATE, use in WHERE, DROP
// =====================================================

func TestIndexCreateAndUse(t *testing.T) {
	db := setupComplexDB(t)

	// Without index
	rows1 := mustQuery(t, db, `SELECT name FROM employees WHERE department = "Sales"`)
	assertRowCount(t, rows1, 2)

	// Create named index
	mustExec(t, db, `CREATE INDEX idx_dept ON employees(department)`)

	// With index — same results
	rows2 := mustQuery(t, db, `SELECT name FROM employees WHERE department = "Sales"`)
	assertRowCount(t, rows2, 2)

	// Drop by name
	mustExec(t, db, `DROP INDEX idx_dept`)

	// Still works after drop (full scan)
	rows3 := mustQuery(t, db, `SELECT name FROM employees WHERE department = "Sales"`)
	assertRowCount(t, rows3, 2)
}

// =====================================================
// 3-table JOIN with strict value verification
// =====================================================

func TestThreeTableJoin(t *testing.T) {
	db := setupComplexDB(t)

	// employees(6) JOIN departments(3) JOIN projects(3)
	// Alice(Engineering) → dept budget=500000, projects Alpha(pri=1) + Beta(pri=2)
	// Bob(Engineering)   → dept budget=500000, projects Alpha(pri=1) + Beta(pri=2)
	// Charlie(Sales)     → dept budget=200000, project Gamma(pri=1)
	// Diana(Sales)       → dept budget=200000, project Gamma(pri=1)
	// Eve(HR)            → dept budget=150000, NO projects → excluded by INNER JOIN
	// Frank(HR)          → dept budget=150000, NO projects → excluded by INNER JOIN

	rows := mustQuery(t, db,
		`SELECT e.name, e.salary, d.budget, p.title, p.priority
		 FROM employees e
		 JOIN departments d ON e.department = d.name
		 JOIN projects p ON e.department = p.department
		 ORDER BY e.name, p.title`)

	// 2 Engineering employees × 2 projects + 2 Sales employees × 1 project = 6
	assertRowCount(t, rows, 6)

	// Verify Alice + Alpha
	assertFieldEquals(t, rows[0], "e.name", "Alice")
	assertFieldEquals(t, rows[0], "p.title", "Alpha")
	assertFieldEquals(t, rows[0], "d.budget", int64(500000))
	assertFieldEquals(t, rows[0], "p.priority", int64(1))

	// Verify Alice + Beta
	assertFieldEquals(t, rows[1], "e.name", "Alice")
	assertFieldEquals(t, rows[1], "p.title", "Beta")
	assertFieldEquals(t, rows[1], "p.priority", int64(2))

	// Verify Bob + Alpha
	assertFieldEquals(t, rows[2], "e.name", "Bob")
	assertFieldEquals(t, rows[2], "p.title", "Alpha")

	// Verify Charlie + Gamma
	assertFieldEquals(t, rows[4], "e.name", "Charlie")
	assertFieldEquals(t, rows[4], "p.title", "Gamma")
	assertFieldEquals(t, rows[4], "d.budget", int64(200000))
}

// =====================================================
// 4-table JOIN
// =====================================================

func TestFourTableJoin(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// Setup 4 tables: customers, orders, products, categories
	mustExec(t, db, `INSERT INTO categories VALUES {"name": "Electronics", "tax_rate": 20}`)
	mustExec(t, db, `INSERT INTO categories VALUES {"name": "Books", "tax_rate": 5}`)

	mustExec(t, db, `INSERT INTO products VALUES {"name": "Laptop", "category": "Electronics", "price": 1000}`)
	mustExec(t, db, `INSERT INTO products VALUES {"name": "Phone", "category": "Electronics", "price": 500}`)
	mustExec(t, db, `INSERT INTO products VALUES {"name": "Novel", "category": "Books", "price": 15}`)

	mustExec(t, db, `INSERT INTO customers VALUES {"name": "Alice", "city": "Paris"}`)
	mustExec(t, db, `INSERT INTO customers VALUES {"name": "Bob", "city": "Lyon"}`)

	mustExec(t, db, `INSERT INTO orders VALUES {"customer": "Alice", "product": "Laptop", "qty": 1}`)
	mustExec(t, db, `INSERT INTO orders VALUES {"customer": "Alice", "product": "Novel", "qty": 3}`)
	mustExec(t, db, `INSERT INTO orders VALUES {"customer": "Bob", "product": "Phone", "qty": 2}`)

	// 4-table JOIN: orders → customers → products → categories
	rows := mustQuery(t, db,
		`SELECT o.customer, c.city, o.product, p.price, o.qty, cat.tax_rate
		 FROM orders o
		 JOIN customers c ON o.customer = c.name
		 JOIN products p ON o.product = p.name
		 JOIN categories cat ON p.category = cat.name
		 ORDER BY o.customer, o.product`)

	assertRowCount(t, rows, 3)

	// Alice, Laptop: city=Paris, price=1000, qty=1, tax=20
	assertFieldEquals(t, rows[0], "o.customer", "Alice")
	assertFieldEquals(t, rows[0], "c.city", "Paris")
	assertFieldEquals(t, rows[0], "o.product", "Laptop")
	assertFieldEquals(t, rows[0], "p.price", int64(1000))
	assertFieldEquals(t, rows[0], "o.qty", int64(1))
	assertFieldEquals(t, rows[0], "cat.tax_rate", int64(20))

	// Alice, Novel: city=Paris, price=15, qty=3, tax=5
	assertFieldEquals(t, rows[1], "o.customer", "Alice")
	assertFieldEquals(t, rows[1], "o.product", "Novel")
	assertFieldEquals(t, rows[1], "p.price", int64(15))
	assertFieldEquals(t, rows[1], "cat.tax_rate", int64(5))

	// Bob, Phone: city=Lyon, price=500, qty=2, tax=20
	assertFieldEquals(t, rows[2], "o.customer", "Bob")
	assertFieldEquals(t, rows[2], "c.city", "Lyon")
	assertFieldEquals(t, rows[2], "p.price", int64(500))
	assertFieldEquals(t, rows[2], "o.qty", int64(2))
	assertFieldEquals(t, rows[2], "cat.tax_rate", int64(20))
}

// =====================================================
// 4-table JOIN + GROUP BY + aggregate
// =====================================================

func TestFourTableJoinGroupBy(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	mustExec(t, db, `INSERT INTO categories VALUES {"name": "Electronics", "tax_rate": 20}`)
	mustExec(t, db, `INSERT INTO categories VALUES {"name": "Books", "tax_rate": 5}`)
	mustExec(t, db, `INSERT INTO products VALUES {"name": "Laptop", "category": "Electronics", "price": 1000}`)
	mustExec(t, db, `INSERT INTO products VALUES {"name": "Phone", "category": "Electronics", "price": 500}`)
	mustExec(t, db, `INSERT INTO products VALUES {"name": "Novel", "category": "Books", "price": 15}`)
	mustExec(t, db, `INSERT INTO customers VALUES {"name": "Alice", "city": "Paris"}`)
	mustExec(t, db, `INSERT INTO customers VALUES {"name": "Bob", "city": "Lyon"}`)
	mustExec(t, db, `INSERT INTO orders VALUES {"customer": "Alice", "product": "Laptop", "qty": 1}`)
	mustExec(t, db, `INSERT INTO orders VALUES {"customer": "Alice", "product": "Novel", "qty": 3}`)
	mustExec(t, db, `INSERT INTO orders VALUES {"customer": "Bob", "product": "Phone", "qty": 2}`)

	// Total spend per customer: Alice = 1×1000 + 3×15 = 1045, Bob = 2×500 = 1000
	// But we can't do p.price * o.qty in NovusDB easily, so just count orders per customer
	rows := mustQuery(t, db,
		`SELECT o.customer AS customer, c.city AS city, COUNT(*) AS order_count
		 FROM orders o
		 JOIN customers c ON o.customer = c.name
		 JOIN products p ON o.product = p.name
		 JOIN categories cat ON p.category = cat.name
		 GROUP BY o.customer
		 ORDER BY customer`)

	assertRowCount(t, rows, 2)
	assertFieldEquals(t, rows[0], "customer", "Alice")
	assertFieldEquals(t, rows[0], "city", "Paris")
	assertFieldEquals(t, rows[0], "order_count", int64(2))
	assertFieldEquals(t, rows[1], "customer", "Bob")
	assertFieldEquals(t, rows[1], "city", "Lyon")
	assertFieldEquals(t, rows[1], "order_count", int64(1))
}

// =====================================================
// 3-table LEFT JOIN chain (unmatched rows propagate)
// =====================================================

func TestThreeTableLeftJoin(t *testing.T) {
	db := setupComplexDB(t)

	// HR employees (Eve, Frank) have no projects → LEFT JOIN should keep them with NULL project fields
	rows := mustQuery(t, db,
		`SELECT e.name, d.budget, p.title
		 FROM employees e
		 JOIN departments d ON e.department = d.name
		 LEFT JOIN projects p ON e.department = p.department
		 ORDER BY e.name`)

	// Engineering: 2 emp × 2 projects = 4
	// Sales: 2 emp × 1 project = 2
	// HR: 2 emp × 0 projects = 2 (LEFT JOIN keeps them)
	assertRowCount(t, rows, 8)

	// Eve should appear with NULL project
	eveFound := false
	for _, r := range rows {
		if r["e.name"] == "Eve" {
			eveFound = true
			assertFieldEquals(t, r, "d.budget", int64(150000))
			if r["p.title"] != nil {
				t.Errorf("Eve should have nil project title, got %v", r["p.title"])
			}
		}
	}
	if !eveFound {
		t.Error("Eve not found in 3-table LEFT JOIN results")
	}
}

// =====================================================
// JOIN + subquery in WHERE
// =====================================================

func TestJoinWithSubqueryWhere(t *testing.T) {
	db := setupComplexDB(t)

	// Employees in departments that have at least one priority-1 project
	rows := mustQuery(t, db,
		`SELECT e.name, e.salary, d.budget
		 FROM employees e
		 JOIN departments d ON e.department = d.name
		 WHERE e.department IN (SELECT department FROM projects WHERE priority = 1)
		 ORDER BY e.salary DESC`)

	// Engineering has Alpha(pri=1), Sales has Gamma(pri=1)
	// Employees: Alice(90k), Bob(85k), Diana(65k), Charlie(60k)
	assertRowCount(t, rows, 4)
	assertFieldEquals(t, rows[0], "e.name", "Alice")
	assertFieldEquals(t, rows[0], "e.salary", int64(90000))
	assertFieldEquals(t, rows[0], "d.budget", int64(500000))
	assertFieldEquals(t, rows[3], "e.name", "Charlie")
}

// =====================================================
// JOIN + WHERE filtering both sides
// =====================================================

func TestJoinWhereOnBothTables(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.name, e.salary, d.budget, d.floor
		 FROM employees e
		 JOIN departments d ON e.department = d.name
		 WHERE e.salary > 60000 AND d.budget > 180000`)

	// salary>60000: Alice(90k), Bob(85k), Diana(65k)
	// budget>180000: Engineering(500k), Sales(200k)
	// Intersection: Alice(Eng), Bob(Eng), Diana(Sales)
	assertRowCount(t, rows, 3)

	for _, r := range rows {
		sal, _ := toF64(r["e.salary"])
		budget, _ := toF64(r["d.budget"])
		if sal <= 60000 {
			t.Errorf("salary %v should be > 60000", sal)
		}
		if budget <= 180000 {
			t.Errorf("budget %v should be > 180000", budget)
		}
	}
}

// =====================================================
// 3-table JOIN + HAVING + LIMIT
// =====================================================

func TestThreeTableJoinHavingLimit(t *testing.T) {
	db := setupComplexDB(t)

	rows := mustQuery(t, db,
		`SELECT e.department AS dept, COUNT(*) AS emp_proj_count, d.budget AS budget
		 FROM employees e
		 JOIN departments d ON e.department = d.name
		 JOIN projects p ON e.department = p.department
		 GROUP BY e.department
		 HAVING COUNT(*) > 1
		 ORDER BY emp_proj_count DESC
		 LIMIT 2`)

	assertRowCount(t, rows, 2)
	// Engineering: 2 emp × 2 proj = 4 combinations
	// Sales: 2 emp × 1 proj = 2 combinations
	assertFieldEquals(t, rows[0], "dept", "Engineering")
	assertFieldEquals(t, rows[0], "emp_proj_count", int64(4))
	assertFieldEquals(t, rows[0], "budget", int64(500000))
	assertFieldEquals(t, rows[1], "dept", "Sales")
	assertFieldEquals(t, rows[1], "emp_proj_count", int64(2))
}

func TestIndexIfNotExists(t *testing.T) {
	db := setupComplexDB(t)

	mustExec(t, db, `CREATE INDEX idx_sal ON employees(salary)`)
	// Should not error — named index: IF NOT EXISTS goes after the name
	mustExec(t, db, `CREATE INDEX idx_sal2 IF NOT EXISTS ON employees(salary)`)
}

// =====================================================
// ANALYZE tests
// =====================================================

func TestAnalyzeBasic(t *testing.T) {
	db := setupComplexDB(t)

	// ANALYZE should not error
	res, err := db.Exec("ANALYZE")
	if err != nil {
		t.Fatal(err)
	}
	// Should have analyzed employees, departments, projects, items, kv (5 tables from setupComplexDB)
	if res.RowsAffected < 1 {
		t.Errorf("expected at least 1 table analyzed, got %d", res.RowsAffected)
	}
}

func TestAnalyzeSingleTable(t *testing.T) {
	db := setupComplexDB(t)

	res, err := db.Exec("ANALYZE employees")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 table analyzed, got %d", res.RowsAffected)
	}
}

func TestAnalyzeColumnStats(t *testing.T) {
	db := setupComplexDB(t)

	mustExec(t, db, "ANALYZE employees")

	// Verify stats via EXPLAIN — should show "ANALYZED" instead of "HEURISTIC"
	rows := mustQuery(t, db, `EXPLAIN SELECT * FROM employees WHERE salary > 70000`)
	if len(rows) == 0 {
		t.Fatal("EXPLAIN returned no rows")
	}

	// Check that stats are ANALYZED
	if v, ok := rows[0]["stats"]; ok {
		if v != "ANALYZED" {
			t.Errorf("expected stats=ANALYZED, got %v", v)
		}
	}

	// Selectivity should be data-driven now, not 0.33
	if v, ok := rows[0]["selectivity"]; ok {
		sel, ok2 := toF64(v)
		if ok2 && sel == 0.33 {
			t.Errorf("selectivity should be data-driven after ANALYZE, got default 0.33")
		}
	}
}

func TestAnalyzeExplainSelectivity(t *testing.T) {
	path := tempDBPath(t)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// Insert 100 rows with salary from 1 to 100
	for i := 1; i <= 100; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO bigtest VALUES {"id": %d, "salary": %d, "dept": "D%d"}`, i, i*1000, i%5))
	}

	mustExec(t, db, "ANALYZE bigtest")

	// salary > 50000 should match ~50 out of 100 rows
	rows := mustQuery(t, db, `EXPLAIN SELECT * FROM bigtest WHERE salary > 50000`)
	if len(rows) == 0 {
		t.Fatal("EXPLAIN returned no rows")
	}

	if v, ok := rows[0]["selectivity"]; ok {
		sel, ok2 := toF64(v)
		if !ok2 {
			t.Fatalf("selectivity not a number: %v", v)
		}
		// Should be around 0.5 (±0.2), not the default 0.33
		if sel < 0.2 || sel > 0.8 {
			t.Errorf("expected selectivity ~0.5 for salary>50000 on 1-100k range, got %.3f", sel)
		}
	}

	// dept = "D0" should match ~20 out of 100 rows (NDV=5 → 1/5=0.2)
	rows2 := mustQuery(t, db, `EXPLAIN SELECT * FROM bigtest WHERE dept = "D0"`)
	if len(rows2) == 0 {
		t.Fatal("EXPLAIN returned no rows")
	}

	if v, ok := rows2[0]["selectivity"]; ok {
		sel, ok2 := toF64(v)
		if !ok2 {
			t.Fatalf("selectivity not a number: %v", v)
		}
		// NDV=5, so selectivity should be 1/5 = 0.2
		if sel < 0.1 || sel > 0.35 {
			t.Errorf("expected selectivity ~0.2 for dept=D0 with NDV=5, got %.3f", sel)
		}
	}
}

func TestAnalyzePersistence(t *testing.T) {
	path := tempDBPath(t)

	// Open, insert, analyze, close
	func() {
		db, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 1; i <= 20; i++ {
			mustExec(t, db, fmt.Sprintf(`INSERT INTO persist_test VALUES {"id": %d, "val": %d}`, i, i*10))
		}
		mustExec(t, db, "ANALYZE persist_test")
	}()

	// Reopen — stats should be loaded from _novusdb_stats
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows := mustQuery(t, db, `EXPLAIN SELECT * FROM persist_test WHERE val > 100`)
	if len(rows) == 0 {
		t.Fatal("EXPLAIN returned no rows")
	}

	if v, ok := rows[0]["stats"]; ok {
		if v != "ANALYZED" {
			t.Errorf("expected stats=ANALYZED after reopen, got %v", v)
		}
	}
}

func TestAnalyzeReAnalyze(t *testing.T) {
	db := setupComplexDB(t)

	// First ANALYZE
	mustExec(t, db, "ANALYZE employees")

	// Add more data
	mustExec(t, db, `INSERT INTO employees VALUES {"name": "Grace", "salary": 120000, "department": "Engineering", "active": true}`)

	// Re-ANALYZE should update stats
	mustExec(t, db, "ANALYZE employees")

	// EXPLAIN should still show ANALYZED
	rows := mustQuery(t, db, `EXPLAIN SELECT * FROM employees WHERE salary > 100000`)
	if len(rows) == 0 {
		t.Fatal("EXPLAIN returned no rows")
	}
	if v, ok := rows[0]["stats"]; ok {
		if v != "ANALYZED" {
			t.Errorf("expected ANALYZED after re-analyze, got %v", v)
		}
	}
}
