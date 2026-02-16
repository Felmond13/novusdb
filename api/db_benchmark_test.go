package api

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

// ============================================================
// NovusDB Grand Benchmark
// ============================================================
// Ce fichier contient des benchmarks massifs couvrant :
//   - INSERT en masse (1k, 5k, 10k lignes)
//   - SELECT sur tables volumineuses (full scan, filtré, index)
//   - UPDATE en masse (bulk, ciblé, conditionnel)
//   - DELETE en masse (partiel, total)
//   - Transactions mixtes (INSERT+UPDATE, UPDATE+SELECT, DELETE+INSERT)
//   - JOINs sur grosses tables
//   - Agrégats (COUNT, SUM, AVG, MIN, MAX, GROUP BY)
//   - Sous-requêtes sur grosses tables
//   - ANALYZE + stats-driven queries
//   - Concurrent reads/writes

const (
	benchSmall  = 1000
	benchMedium = 5000
	benchLarge  = 10000
)

// ---------- helpers ----------

func benchDB(b *testing.B) (*DB, func()) {
	b.Helper()
	path := tempDBPathB(b)
	db, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	return db, func() { db.Close(); os.Remove(path) }
}

func seedUsers(b *testing.B, db *DB, n int) {
	b.Helper()
	departments := []string{"Engineering", "Sales", "HR", "Marketing", "Finance", "Legal", "Support", "Ops"}
	cities := []string{"Paris", "London", "Berlin", "NYC", "Tokyo", "Sydney", "Toronto", "Dubai"}
	for i := 1; i <= n; i++ {
		dept := departments[i%len(departments)]
		city := cities[i%len(cities)]
		salary := 30000 + (i%70)*1000
		age := 20 + (i % 45)
		active := i%3 != 0
		sql := fmt.Sprintf(
			`INSERT INTO users VALUES {"id": %d, "name": "user_%d", "dept": "%s", "city": "%s", "salary": %d, "age": %d, "active": %v}`,
			i, i, dept, city, salary, age, active,
		)
		if _, err := db.Exec(sql); err != nil {
			b.Fatalf("seed user %d: %v", i, err)
		}
	}
}

func seedOrders(b *testing.B, db *DB, n int) {
	b.Helper()
	statuses := []string{"pending", "shipped", "delivered", "cancelled"}
	for i := 1; i <= n; i++ {
		userID := (i % 1000) + 1
		amount := 10 + (i%990)*5
		status := statuses[i%len(statuses)]
		sql := fmt.Sprintf(
			`INSERT INTO orders VALUES {"id": %d, "user_id": %d, "amount": %d, "status": "%s", "qty": %d}`,
			i, userID, amount, status, 1+(i%20),
		)
		if _, err := db.Exec(sql); err != nil {
			b.Fatalf("seed order %d: %v", i, err)
		}
	}
}

func seedProducts(b *testing.B, db *DB, n int) {
	b.Helper()
	categories := []string{"Electronics", "Books", "Clothing", "Food", "Sports", "Home", "Toys", "Auto"}
	for i := 1; i <= n; i++ {
		cat := categories[i%len(categories)]
		price := 5 + (i%500)*2
		stock := i % 200
		sql := fmt.Sprintf(
			`INSERT INTO products VALUES {"id": %d, "name": "prod_%d", "category": "%s", "price": %d, "stock": %d}`,
			i, i, cat, price, stock,
		)
		if _, err := db.Exec(sql); err != nil {
			b.Fatalf("seed product %d: %v", i, err)
		}
	}
}

// ============================================================
// 1. INSERT Benchmarks
// ============================================================

func BenchmarkInsert1K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, cleanup := benchDB(b)
		seedUsers(b, db, benchSmall)
		cleanup()
	}
}

func BenchmarkInsert5K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, cleanup := benchDB(b)
		seedUsers(b, db, benchMedium)
		cleanup()
	}
}

func BenchmarkInsert10K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, cleanup := benchDB(b)
		seedUsers(b, db, benchLarge)
		cleanup()
	}
}

// ============================================================
// 2. SELECT Benchmarks
// ============================================================

func BenchmarkSelectFullScan1K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectFullScan10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectWhereRange1K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users WHERE salary > 60000 AND salary < 80000`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectWhereRange10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users WHERE salary > 60000 AND salary < 80000`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectWhereEQ10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users WHERE dept = "Engineering"`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectWithIndex10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	db.Exec(`CREATE INDEX idx_dept ON users(dept)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users WHERE dept = "Engineering"`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectLike10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users WHERE name LIKE "user_1%"`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectIN10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users WHERE dept IN ("Engineering", "Sales", "HR")`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectOrderByLimit10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users ORDER BY salary DESC LIMIT 100`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectDistinct10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT DISTINCT dept FROM users`); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// 3. UPDATE Benchmarks
// ============================================================

func BenchmarkUpdateBulk1K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`UPDATE users SET salary = salary + 1`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdateBulk10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`UPDATE users SET salary = salary + 1`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdateConditional10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`UPDATE users SET active = true WHERE dept = "Sales" AND salary > 50000`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdateWithIndex10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	db.Exec(`CREATE INDEX idx_dept ON users(dept)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`UPDATE users SET salary = 99999 WHERE dept = "HR"`); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// 4. DELETE Benchmarks
// ============================================================

func BenchmarkDeletePartial5K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, cleanup := benchDB(b)
		seedUsers(b, db, benchMedium)
		b.StartTimer()
		if _, err := db.Exec(`DELETE FROM users WHERE active = false`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		cleanup()
	}
}

func BenchmarkDeleteRange10K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, cleanup := benchDB(b)
		seedUsers(b, db, benchLarge)
		b.StartTimer()
		if _, err := db.Exec(`DELETE FROM users WHERE salary < 50000`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		cleanup()
	}
}

func BenchmarkDeleteAll10K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, cleanup := benchDB(b)
		seedUsers(b, db, benchLarge)
		b.StartTimer()
		if _, err := db.Exec(`DELETE FROM users WHERE id > 0`); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		cleanup()
	}
}

// ============================================================
// 5. AGGREGATE Benchmarks
// ============================================================

func BenchmarkCountAll10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT COUNT(*) FROM users`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSumGroupBy10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT dept, SUM(salary) AS total FROM users GROUP BY dept`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAvgGroupByHaving10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT dept, AVG(salary) AS avg_sal, COUNT(*) AS cnt FROM users GROUP BY dept HAVING COUNT(*) > 500`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMinMaxGroupBy10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT dept, MIN(salary) AS min_sal, MAX(salary) AS max_sal FROM users GROUP BY dept`); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// 6. JOIN Benchmarks
// ============================================================

func BenchmarkHashJoin1Kx1K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	seedOrders(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashJoin5Kx5K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	seedOrders(b, db, benchMedium)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIndexJoin5Kx5K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	seedOrders(b, db, benchMedium)
	db.Exec(`CREATE INDEX idx_order_uid ON orders(user_id)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinWithFilter5K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	seedOrders(b, db, benchMedium)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 2000`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinGroupBy5K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	seedOrders(b, db, benchMedium)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT u.dept, COUNT(*) AS order_count, SUM(o.amount) AS total
			 FROM users u JOIN orders o ON u.id = o.user_id
			 GROUP BY u.dept`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkThreeTableJoin(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	seedOrders(b, db, benchSmall)
	seedProducts(b, db, 500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT u.name, o.amount, p.name AS product
			 FROM users u
			 JOIN orders o ON u.id = o.user_id
			 JOIN products p ON o.qty = p.id`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// 7. TRANSACTION Benchmarks
// ============================================================

func BenchmarkTxInsertBatch1K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < benchSmall; j++ {
			id := i*benchSmall + j + 1
			sql := fmt.Sprintf(`INSERT INTO tx_batch VALUES {"id": %d, "val": %d}`, id, j)
			if _, err := tx.Exec(sql); err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxUpdateThenSelect(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		// UPDATE en masse
		if _, err := tx.Exec(`UPDATE users SET salary = salary + 100 WHERE dept = "Engineering"`); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		// SELECT immédiatement après
		if _, err := tx.Exec(`SELECT name, salary FROM users WHERE dept = "Engineering" ORDER BY salary DESC`); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxDeleteThenInsert(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		// DELETE les inactifs
		if _, err := tx.Exec(`DELETE FROM users WHERE active = false`); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		// Re-insérer des remplaçants
		for j := 0; j < 100; j++ {
			id := 100000 + i*100 + j
			sql := fmt.Sprintf(`INSERT INTO users VALUES {"id": %d, "name": "new_%d", "dept": "Support", "city": "Remote", "salary": 45000, "age": 25, "active": true}`, id, id)
			if _, err := tx.Exec(sql); err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxMixedOperations(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	seedOrders(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		// 1. INSERT 50 nouvelles commandes
		for j := 0; j < 50; j++ {
			id := 900000 + i*50 + j
			sql := fmt.Sprintf(`INSERT INTO orders VALUES {"id": %d, "user_id": %d, "amount": %d, "status": "pending", "qty": 1}`, id, j+1, 100+j)
			if _, err := tx.Exec(sql); err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}
		// 2. UPDATE salaires Engineering
		if _, err := tx.Exec(`UPDATE users SET salary = salary + 500 WHERE dept = "Engineering"`); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		// 3. DELETE commandes annulées
		if _, err := tx.Exec(`DELETE FROM orders WHERE status = "cancelled"`); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		// 4. SELECT agrégat
		if _, err := tx.Exec(`SELECT dept, AVG(salary) AS avg_sal FROM users GROUP BY dept`); err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxRollback(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		// Faire plein de modifications
		tx.Exec(`UPDATE users SET salary = 0`)
		tx.Exec(`DELETE FROM users WHERE dept = "HR"`)
		for j := 0; j < 200; j++ {
			tx.Exec(fmt.Sprintf(`INSERT INTO users VALUES {"id": %d, "name": "ghost_%d", "dept": "Ghost", "city": "Nowhere", "salary": 0, "age": 0, "active": false}`, 500000+j, j))
		}
		// Tout annuler
		tx.Rollback()
	}
}

// ============================================================
// 8. SUBQUERY Benchmarks
// ============================================================

func BenchmarkSubqueryIN5K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	seedOrders(b, db, benchMedium)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 3000)`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubqueryScalar5K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT * FROM users WHERE salary > (SELECT AVG(salary) FROM users)`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// 9. ANALYZE + Stats-Driven Benchmarks
// ============================================================

func BenchmarkAnalyze10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`ANALYZE users`); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectAfterAnalyze10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	db.Exec(`ANALYZE users`)
	db.Exec(`CREATE INDEX idx_dept ON users(dept)`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(`SELECT * FROM users WHERE dept = "Engineering" AND salary > 60000`); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// 10. CONCURRENT Benchmarks
// ============================================================

func BenchmarkConcurrentReads(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			dept := []string{"Engineering", "Sales", "HR", "Marketing"}[rand.Intn(4)]
			if _, err := db.Exec(fmt.Sprintf(`SELECT * FROM users WHERE dept = "%s"`, dept)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkConcurrentWrites(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			id := 200000 + rand.Intn(1000000)
			if _, err := db.Exec(fmt.Sprintf(`INSERT INTO users VALUES {"id": %d, "name": "par_%d", "dept": "Concurrent", "city": "Test", "salary": %d, "age": 30, "active": true}`, id, i, 40000+i)); err != nil {
				// Ignore duplicate ID errors
				continue
			}
		}
	})
}

func BenchmarkConcurrentMixed(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			switch i % 4 {
			case 0:
				db.Exec(`SELECT COUNT(*) FROM users`)
			case 1:
				db.Exec(`SELECT * FROM users WHERE dept = "Sales" LIMIT 10`)
			case 2:
				db.Exec(fmt.Sprintf(`INSERT INTO users VALUES {"id": %d, "name": "mix_%d", "dept": "Mixed", "city": "X", "salary": 50000, "age": 30, "active": true}`, 300000+rand.Intn(1000000), i))
			case 3:
				db.Exec(`UPDATE users SET age = age + 1 WHERE dept = "HR" LIMIT 10`)
			}
		}
	})
}

// ============================================================
// 11. COMPLEX QUERY Benchmarks
// ============================================================

func BenchmarkComplexFilterChain10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT name, salary, dept FROM users
			 WHERE (dept = "Engineering" OR dept = "Sales")
			 AND salary > 50000
			 AND age >= 25
			 AND active = true
			 ORDER BY salary DESC
			 LIMIT 50`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJoinAggregateOrderBy(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchSmall)
	seedOrders(b, db, benchSmall)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT u.dept, COUNT(*) AS cnt, SUM(o.amount) AS total_amount
			 FROM users u
			 JOIN orders o ON u.id = o.user_id
			 GROUP BY u.dept
			 HAVING SUM(o.amount) > 10000
			 ORDER BY total_amount DESC`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCaseWhen10K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchLarge)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT name,
				CASE WHEN salary > 80000 THEN "high"
				     WHEN salary > 50000 THEN "mid"
				     ELSE "low" END AS tier
			 FROM users`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnion5K(b *testing.B) {
	db, cleanup := benchDB(b)
	defer cleanup()
	seedUsers(b, db, benchMedium)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(
			`SELECT name, salary FROM users WHERE dept = "Engineering"
			 UNION ALL
			 SELECT name, salary FROM users WHERE dept = "Sales"`,
		); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// 12. STRESS TEST — Full Pipeline
// ============================================================

func BenchmarkStressFullPipeline(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, cleanup := benchDB(b)

		// Phase 1: Seed 2K users + 2K orders
		seedUsers(b, db, 2000)
		seedOrders(b, db, 2000)

		// Phase 2: Create indexes
		db.Exec(`CREATE INDEX idx_dept ON users(dept)`)
		db.Exec(`CREATE INDEX idx_uid ON orders(user_id)`)

		// Phase 3: ANALYZE
		db.Exec(`ANALYZE`)

		// Phase 4: Complex queries
		db.Exec(`SELECT u.dept, COUNT(*) AS cnt, SUM(o.amount) AS total
		         FROM users u JOIN orders o ON u.id = o.user_id
		         GROUP BY u.dept ORDER BY total DESC`)

		db.Exec(`SELECT * FROM users WHERE salary > (SELECT AVG(salary) FROM users)`)

		// Phase 5: Transaction — bulk update + verify
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		tx.Exec(`UPDATE users SET salary = salary * 2 WHERE dept = "Engineering"`)
		tx.Exec(`DELETE FROM orders WHERE status = "cancelled"`)
		tx.Exec(`SELECT dept, AVG(salary) AS avg FROM users GROUP BY dept`)
		tx.Commit()

		// Phase 6: More inserts after modifications
		for j := 0; j < 500; j++ {
			db.Exec(fmt.Sprintf(`INSERT INTO users VALUES {"id": %d, "name": "late_%d", "dept": "New", "city": "X", "salary": 60000, "age": 28, "active": true}`, 50000+j, j))
		}

		// Phase 7: Final aggregation
		db.Exec(`SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg, MIN(salary) AS lo, MAX(salary) AS hi FROM users GROUP BY dept ORDER BY cnt DESC`)

		cleanup()
	}
}
