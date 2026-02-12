package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/engine"
)

const (
	reset   = "\033[0m"
	bold    = "\033[1m"
	cyan    = "\033[36m"
	green   = "\033[32m"
	yellow  = "\033[33m"
	red     = "\033[31m"
	magenta = "\033[35m"
	dim     = "\033[2m"
)

var (
	firstNames = []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hector", "Ivy", "Jack",
		"Karen", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina",
		"Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe", "Adam", "Bella", "Carl", "Donna"}
	lastNames = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
		"Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"}
	cityList   = []string{"Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"}
	deptList   = []string{"Engineering", "Marketing", "Sales", "HR", "Finance", "Support", "Product", "Design", "Legal", "Operations"}
	skillsList = []string{"Go", "Python", "JavaScript", "SQL", "React", "Docker", "Kubernetes", "AWS", "TypeScript", "Rust", "Java", "C++", "GraphQL", "Redis", "PostgreSQL"}
)

func main() {
	dbPath := "novusdb_bench.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	printBanner()

	db, err := api.Open(dbPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer db.Close()
	printDone("Database ready")
	fmt.Println()

	// ================================================================
	// 1) INSERT 300K employees
	// ================================================================
	totalDocs := 300_000
	batchSize := 1000    // tx commit interval
	rowsPerInsert := 100 // rows per INSERT statement (reduces parsing 300x)
	printSection("Phase 1 — INSERT 300K employees")
	fmt.Println()
	printStep(fmt.Sprintf("Inserting %s employees (batches of %d, %d rows/INSERT)...", formatNum(totalDocs), batchSize, rowsPerInsert))
	fmt.Println()

	start := time.Now()
	for i := 0; i < totalDocs; i += rowsPerInsert {
		if i%batchSize == 0 {
			db.Exec("BEGIN")
		}
		// Build batch INSERT: INSERT INTO employees VALUES {...}, {...}, ...
		count := rowsPerInsert
		if i+count > totalDocs {
			count = totalDocs - i
		}
		var sb strings.Builder
		sb.WriteString("INSERT INTO employees VALUES ")
		for j := 0; j < count; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(generateEmployeeJSON())
		}
		_, err := db.Exec(sb.String())
		if err != nil {
			fmt.Printf("  Error at doc %d: %v\n", i, err)
			return
		}
		done := i + count
		if done%batchSize == 0 || done >= totalDocs {
			db.Exec("COMMIT")
			if done%(batchSize*10) == 0 || done >= totalDocs {
				elapsed := time.Since(start)
				rate := float64(done) / elapsed.Seconds()
				bar := progressBar(done, totalDocs, 40)
				fmt.Printf("\r  %s %s %s%.0f docs/sec%s",
					bar, formatNum(done), dim, rate, reset)
			}
		}
	}
	insertTime := time.Since(start)
	fmt.Printf("\r  %s %s                                    \n", progressBar(totalDocs, totalDocs, 40), formatNum(totalDocs))
	printDone(fmt.Sprintf("%s employees in %s (%s%.0f docs/sec%s)",
		formatNum(totalDocs), insertTime.Round(time.Millisecond), bold, float64(totalDocs)/insertTime.Seconds(), reset))
	fmt.Println()

	// ================================================================
	// 2) INSERT departments collection (for JOINs)
	// ================================================================
	printSection("Phase 2 — INSERT departments collection")
	fmt.Println()
	for i, dept := range deptList {
		budget := 500000 + rand.Intn(2000000)
		headcount := 50 + rand.Intn(200)
		q := fmt.Sprintf(`INSERT INTO departments VALUES {"name": "%s", "budget": %d, "headcount": %d, "floor": %d}`,
			dept, budget, headcount, i+1)
		db.Exec(q)
	}
	printDone(fmt.Sprintf("%d departments inserted", len(deptList)))
	fmt.Println()

	// ================================================================
	// 3) QUERIES SANS INDEX (full scan sur 300K)
	// ================================================================
	printSection("Phase 3 — Queries WITHOUT index (full scan on 300K)")
	fmt.Println()

	tNoIdx1 := runQuery(db, "Full scan: COUNT city = Paris (~30K results)",
		`SELECT COUNT(*) AS cnt FROM employees WHERE city = "Paris"`)
	tNoIdx2 := runQuery(db, "Full scan: department = Engineering (~30K results)",
		`SELECT COUNT(*) AS cnt FROM employees WHERE department = "Engineering"`)
	tNoIdx3 := runQuery(db, "Full scan: city = Nice LIMIT 3 (selective)",
		`SELECT first_name, last_name, city, salary FROM employees WHERE city = "Nice" LIMIT 3`)
	tNoIdx4 := runQuery(db, "Full scan: department = Legal LIMIT 1 (single row)",
		`SELECT first_name, last_name, department FROM employees WHERE department = "Legal" LIMIT 1`)

	// ================================================================
	// 4) CREATE INDEXES
	// ================================================================
	printSection("Phase 4 — CREATE INDEX (building B-Tree on 300K rows)")
	fmt.Println()

	idxStart := time.Now()
	printStep("CREATE INDEX idx_city ON employees(city)...")
	db.Exec(`CREATE INDEX idx_city ON employees(city)`)
	tCity := time.Since(idxStart)
	printDone(fmt.Sprintf("city index built in %s", tCity.Round(time.Millisecond)))

	idxStart = time.Now()
	printStep("CREATE INDEX idx_dept ON employees(department)...")
	db.Exec(`CREATE INDEX idx_dept ON employees(department)`)
	tDept := time.Since(idxStart)
	printDone(fmt.Sprintf("department index built in %s", tDept.Round(time.Millisecond)))

	idxStart = time.Now()
	printStep("CREATE INDEX idx_dept_name ON departments(name)...")
	db.Exec(`CREATE INDEX idx_dept_name ON departments(name)`)
	tDeptName := time.Since(idxStart)
	printDone(fmt.Sprintf("departments.name index built in %s", tDeptName.Round(time.Millisecond)))
	fmt.Println()

	// ================================================================
	// 5) QUERIES AVEC INDEX (readByLocs direct O(1))
	// ================================================================
	printSection("Phase 5 — Queries WITH index (direct page lookup)")
	fmt.Println()

	tIdx1 := runQuery(db, "Index: COUNT city = Paris (~30K results)",
		`SELECT COUNT(*) AS cnt FROM employees WHERE city = "Paris"`)
	tIdx2 := runQuery(db, "Index: department = Engineering (~30K results)",
		`SELECT COUNT(*) AS cnt FROM employees WHERE department = "Engineering"`)
	tIdx3 := runQuery(db, "Index: city = Nice LIMIT 3 (selective)",
		`SELECT first_name, last_name, city, salary FROM employees WHERE city = "Nice" LIMIT 3`)
	tIdx4 := runQuery(db, "Index: department = Legal LIMIT 1 (single row)",
		`SELECT first_name, last_name, department FROM employees WHERE department = "Legal" LIMIT 1`)

	// Force full scan on indexed field for comparison
	tFS := runQuery(db, "FORCE full scan: city = Paris (/*+ FULL_SCAN */)",
		`SELECT /*+ FULL_SCAN */ COUNT(*) AS cnt FROM employees WHERE city = "Paris"`)
	_ = tFS
	_ = tDeptName

	// ================================================================
	// 6) SPEED COMPARISON
	// ================================================================
	printSection("Phase 6 — Speed Comparison: Full Scan vs Index")
	fmt.Println()

	printSpeedup("COUNT city=Paris (30K rows)", tNoIdx1, tIdx1)
	printSpeedup("COUNT dept=Engineering (30K)", tNoIdx2, tIdx2)
	printSpeedup("city=Nice LIMIT 3 (selective)", tNoIdx3, tIdx3)
	printSpeedup("dept=Legal LIMIT 1 (1 row)", tNoIdx4, tIdx4)
	fmt.Println()

	// ================================================================
	// 7) JOIN QUERIES
	// ================================================================
	printSection("Phase 7 — JOIN (employees x departments)")
	fmt.Println()

	runQuery(db, "JOIN: employee details with department budget",
		`SELECT e.first_name, e.last_name, e.department, d.budget FROM employees e JOIN departments d ON e.department = d.name WHERE e.city = "Paris" LIMIT 10`)

	runQuery(db, "JOIN + GROUP BY: avg salary per department with budget",
		`SELECT e.department, COUNT(*) AS emp_count, AVG(e.salary) AS avg_sal, d.budget FROM employees e JOIN departments d ON e.department = d.name GROUP BY e.department`)

	runQuery(db, "JOIN: high earners with department info",
		`SELECT e.first_name, e.salary, e.department, d.floor FROM employees e JOIN departments d ON e.department = d.name WHERE e.salary > 105000 LIMIT 10`)

	// ================================================================
	// 8) ADVANCED QUERIES
	// ================================================================
	printSection("Phase 8 — Advanced queries on 300K rows")
	fmt.Println()

	runQuery(db, "GROUP BY city + ORDER",
		`SELECT city, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY city`)

	runQuery(db, "TOP 5 highest salaries",
		`SELECT first_name, last_name, salary, city FROM employees ORDER BY salary DESC LIMIT 5`)

	runQuery(db, "COUNT active by department",
		`SELECT department, COUNT(*) AS cnt FROM employees WHERE active = true GROUP BY department`)

	// ================================================================
	// SUMMARY
	// ================================================================
	fmt.Println()
	printSection("Summary")
	fmt.Println()
	info, _ := os.Stat(dbPath)
	fileMB := float64(info.Size()) / (1024 * 1024)
	fmt.Printf("  %s•%s Documents:    %s%s employees + %d departments%s\n", cyan, reset, bold, formatNum(totalDocs), len(deptList), reset)
	fmt.Printf("  %s•%s Insert speed: %s%.0f docs/sec%s (%s)\n", cyan, reset, bold, float64(totalDocs)/insertTime.Seconds(), reset, insertTime.Round(time.Millisecond))
	fmt.Printf("  %s•%s File size:    %s%.2f MB%s (snappy compression)\n", cyan, reset, bold, fileMB, reset)
	fmt.Printf("  %s•%s Index build:  %scity %s + dept %s%s\n", cyan, reset, bold, tCity.Round(time.Millisecond), tDept.Round(time.Millisecond), reset)
	fmt.Printf("  %s•%s COUNT(*):    full scan %s%s%s (aggregates skip index — correct)\n", cyan, reset, dim, tIdx1.Round(time.Microsecond), reset)
	fmt.Printf("  %s•%s LIMIT 3:     full scan %s%s%s → index %s%s%s\n", cyan, reset, red, tNoIdx3.Round(time.Microsecond), reset, green, tIdx3.Round(time.Microsecond), reset)
	fmt.Printf("  %s•%s LIMIT 1:     full scan %s%s%s → index %s%s%s\n", cyan, reset, red, tNoIdx4.Round(time.Microsecond), reset, green, tIdx4.Round(time.Microsecond), reset)
	if tNoIdx4 > 0 && tIdx4 > 0 {
		fmt.Printf("  %s•%s Speedup:      %s%.0fx faster with index (LIMIT 1)%s\n", cyan, reset, bold, float64(tNoIdx4)/float64(tIdx4), reset)
	}
	fmt.Println()
	fmt.Printf("  %s%sNovusDB — A database that fits inside your app.%s\n", bold, cyan, reset)
	fmt.Println()
}

// -------- Generators --------

func generateEmployeeJSON() string {
	fn := firstNames[rand.Intn(len(firstNames))]
	ln := lastNames[rand.Intn(len(lastNames))]
	city := cityList[rand.Intn(len(cityList))]
	dept := deptList[rand.Intn(len(deptList))]
	age := 22 + rand.Intn(40)
	salary := 30000 + rand.Intn(80000)
	active := "true"
	if rand.Intn(10) == 0 {
		active = "false"
	}
	numSkills := 2 + rand.Intn(3)
	picked := make([]string, numSkills)
	for j := 0; j < numSkills; j++ {
		picked[j] = `"` + skillsList[rand.Intn(len(skillsList))] + `"`
	}
	skillsJSON := "[" + strings.Join(picked, ", ") + "]"

	return fmt.Sprintf(`{"first_name": "%s", "last_name": "%s", "city": "%s", "department": "%s", "age": %d, "salary": %d, "active": %s, "skills": %s}`,
		fn, ln, city, dept, age, salary, active, skillsJSON)
}

// -------- Query runner --------

func runQuery(db *api.DB, label, sql string) time.Duration {
	fmt.Printf("  %s▸ %s%s\n", yellow, label, reset)
	fmt.Printf("    %s%s%s\n", dim, sql, reset)

	start := time.Now()
	res, err := db.Exec(sql)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("    %sError: %v%s\n\n", red, err, reset)
		return elapsed
	}

	fmt.Printf("    %s✓ %d rows in %s%s\n", green, len(res.Docs), elapsed.Round(time.Microsecond), reset)
	limit := 3
	if len(res.Docs) < limit {
		limit = len(res.Docs)
	}
	for i := 0; i < limit; i++ {
		fmt.Printf("    %s→ %s%s\n", magenta, docToString(res.Docs[i]), reset)
	}
	if len(res.Docs) > limit {
		fmt.Printf("    %s  ... and %d more%s\n", dim, len(res.Docs)-limit, reset)
	}
	fmt.Println()
	return elapsed
}

func printSpeedup(label string, fullScan, indexed time.Duration) {
	speedup := float64(fullScan) / float64(indexed)
	color := green
	if speedup < 1.5 {
		color = yellow
	}
	fmt.Printf("  %s%-35s%s  full scan: %s%-12s%s  index: %s%-12s%s  %s%s%.1fx%s\n",
		bold, label, reset,
		dim, fullScan.Round(time.Microsecond), reset,
		dim, indexed.Round(time.Microsecond), reset,
		color, bold, speedup, reset)
}

// -------- Helpers --------

func docToString(rd *engine.ResultDoc) string {
	var parts []string
	for _, f := range rd.Doc.Fields {
		parts = append(parts, fmt.Sprintf("%s: %v", f.Name, f.Value))
	}
	return strings.Join(parts, ", ")
}

func printBanner() {
	fmt.Println()
	fmt.Printf("  %s%s╔══════════════════════════════════════════════════════╗%s\n", bold, cyan, reset)
	fmt.Printf("  %s%s║     NovusDB — 300K Benchmark + Index + JOIN          ║%s\n", bold, cyan, reset)
	fmt.Printf("  %s%s║     Embedded JSON DB with full SQL support            ║%s\n", bold, cyan, reset)
	fmt.Printf("  %s%s╚══════════════════════════════════════════════════════╝%s\n", bold, cyan, reset)
	fmt.Println()
}

func printStep(msg string) { fmt.Printf("  %s▸%s %s\n", cyan, reset, msg) }
func printDone(msg string) { fmt.Printf("  %s✓%s %s\n", green, reset, msg) }
func printSection(title string) {
	fmt.Println()
	fmt.Printf("  %s%s══ %s ══%s\n", bold, cyan, title, reset)
}

func progressBar(current, total, width int) string {
	pct := float64(current) / float64(total)
	filled := int(pct * float64(width))
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	return fmt.Sprintf("%s%s%s %s%3.0f%%%s", green, bar, reset, bold, pct*100, reset)
}

func formatNum(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []string
	for i := len(s); i > 0; i -= 3 {
		start := i - 3
		if start < 0 {
			start = 0
		}
		result = append([]string{s[start:i]}, result...)
	}
	return strings.Join(result, ",")
}
