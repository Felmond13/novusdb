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

// ANSI colors
const (
	reset   = "\033[0m"
	bold    = "\033[1m"
	cyan    = "\033[36m"
	green   = "\033[32m"
	yellow  = "\033[33m"
	magenta = "\033[35m"
	dim     = "\033[2m"
)

var (
	firstNames = []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hector", "Ivy", "Jack",
		"Karen", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina",
		"Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe", "Adam", "Bella", "Carl", "Donna"}
	lastNames = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
		"Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"}
	cities      = []string{"Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"}
	departments = []string{"Engineering", "Marketing", "Sales", "HR", "Finance", "Support", "Product", "Design", "Legal", "Operations"}
	skills      = []string{"Go", "Python", "JavaScript", "SQL", "React", "Docker", "Kubernetes", "AWS", "TypeScript", "Rust", "Java", "C++", "GraphQL", "Redis", "PostgreSQL"}
)

func main() {
	dbPath := "novusdb_demo.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	printBanner()

	// Open database
	printStep("Opening NovusDB database...")
	db, err := api.Open(dbPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer db.Close()
	printDone("Database ready (single file, zero config)")
	fmt.Println()

	// === INSERT BENCHMARK ===
	totalDocs := 10_000
	batchSize := 500
	printStep(fmt.Sprintf("Inserting %s employees (batches of %d with transactions)...", formatNum(totalDocs), batchSize))
	fmt.Println()

	start := time.Now()
	for i := 0; i < totalDocs; i++ {
		if i%batchSize == 0 {
			db.Exec("BEGIN")
		}
		query := generateInsert(i)
		_, err := db.Exec(query)
		if err != nil {
			fmt.Printf("  Error at doc %d: %v\n", i, err)
			return
		}
		if (i+1)%batchSize == 0 || i == totalDocs-1 {
			db.Exec("COMMIT")
			elapsed := time.Since(start)
			rate := float64(i+1) / elapsed.Seconds()
			bar := progressBar(i+1, totalDocs, 40)
			fmt.Printf("\r  %s %s %s%.0f docs/sec%s",
				bar, formatNum(i+1), dim, rate, reset)
		}
	}
	insertTime := time.Since(start)
	fmt.Printf("\r  %s %s                              \n", progressBar(totalDocs, totalDocs, 40), formatNum(totalDocs))
	printDone(fmt.Sprintf("%s documents inserted in %s (%.0f docs/sec)",
		formatNum(totalDocs), insertTime.Round(time.Millisecond), float64(totalDocs)/insertTime.Seconds()))
	fmt.Println()

	// === FILE SIZE ===
	info, _ := os.Stat(dbPath)
	printStep(fmt.Sprintf("Database file size: %s%.2f MB%s (with snappy compression)",
		green, float64(info.Size())/(1024*1024), reset))
	fmt.Println()
	fmt.Println()

	// === QUERIES ===
	printSection("SQL Query Benchmark")
	fmt.Println()

	queries := []struct {
		label string
		sql   string
	}{
		{"COUNT all employees", "SELECT COUNT(*) AS total FROM employees"},
		{"Filter by city (Paris)", `SELECT COUNT(*) AS paris_count FROM employees WHERE city = "Paris"`},
		{"AVG salary by department", `SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department`},
		{"TOP 5 highest salaries", `SELECT first_name, last_name, salary FROM employees ORDER BY salary DESC LIMIT 5`},
		{"String functions", `SELECT UPPER(first_name) AS name, LENGTH(last_name) AS len FROM employees WHERE city = "Lyon" LIMIT 3`},
		{"Nested field access", `SELECT first_name, skills FROM employees WHERE age > 55 LIMIT 5`},
		{"Complex filter + sort", `SELECT first_name, last_name, city, salary FROM employees WHERE salary > 90000 AND department = "Engineering" ORDER BY salary DESC LIMIT 10`},
		{"Aggregation + HAVING", `SELECT city, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY city`},
	}

	for _, q := range queries {
		runQuery(db, q.label, q.sql)
	}

	// === FINAL ===
	fmt.Println()
	printSection("Summary")
	fmt.Println()
	fmt.Printf("  %s•%s Documents:  %s%s%s\n", cyan, reset, bold, formatNum(totalDocs), reset)
	fmt.Printf("  %s•%s Insert:     %s%s%s (%.0f docs/sec)\n", cyan, reset, bold, insertTime.Round(time.Millisecond), reset, float64(totalDocs)/insertTime.Seconds())
	fmt.Printf("  %s•%s File size:  %s%.2f MB%s\n", cyan, reset, bold, float64(info.Size())/(1024*1024), reset)
	fmt.Printf("  %s•%s Queries:    %ssub-millisecond to milliseconds%s\n", cyan, reset, bold, reset)
	fmt.Printf("  %s•%s Setup:      %szero — just Open() and query%s\n", cyan, reset, bold, reset)
	fmt.Println()
	fmt.Printf("  %s%sNovusDB — A database that fits inside your app.%s\n", bold, cyan, reset)
	fmt.Printf("  %shttps://novusdb.dev%s\n", dim, reset)
	fmt.Println()
}

func generateInsert(i int) string {
	fn := firstNames[rand.Intn(len(firstNames))]
	ln := lastNames[rand.Intn(len(lastNames))]
	city := cities[rand.Intn(len(cities))]
	dept := departments[rand.Intn(len(departments))]
	age := 22 + rand.Intn(40)
	salary := 30000 + rand.Intn(80000)
	active := "true"
	if rand.Intn(10) == 0 {
		active = "false"
	}
	// Pick 2-4 random skills
	numSkills := 2 + rand.Intn(3)
	picked := make([]string, numSkills)
	for j := 0; j < numSkills; j++ {
		picked[j] = `"` + skills[rand.Intn(len(skills))] + `"`
	}
	skillsJSON := "[" + strings.Join(picked, ", ") + "]"

	return fmt.Sprintf(`INSERT INTO employees VALUES {"first_name": "%s", "last_name": "%s", "city": "%s", "department": "%s", "age": %d, "salary": %d, "active": %s, "skills": %s}`,
		fn, ln, city, dept, age, salary, active, skillsJSON)
}

func runQuery(db *api.DB, label, sql string) {
	fmt.Printf("  %s▸ %s%s\n", yellow, label, reset)
	fmt.Printf("    %s%s%s\n", dim, sql, reset)

	start := time.Now()
	res, err := db.Exec(sql)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("    %sError: %v%s\n\n", "\033[31m", err, reset)
		return
	}

	fmt.Printf("    %s✓ %d rows in %s%s\n", green, len(res.Docs), elapsed.Round(time.Microsecond), reset)
	// Show first 3 results
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
}

func docToString(rd *engine.ResultDoc) string {
	var parts []string
	for _, f := range rd.Doc.Fields {
		parts = append(parts, fmt.Sprintf("%s: %v", f.Name, f.Value))
	}
	return strings.Join(parts, ", ")
}

func printBanner() {
	fmt.Println()
	fmt.Printf("  %s%s╔══════════════════════════════════════════════╗%s\n", bold, cyan, reset)
	fmt.Printf("  %s%s║          NovusDB — Speed Demo                ║%s\n", bold, cyan, reset)
	fmt.Printf("  %s%s║   Embedded JSON DB with full SQL support     ║%s\n", bold, cyan, reset)
	fmt.Printf("  %s%s╚══════════════════════════════════════════════╝%s\n", bold, cyan, reset)
	fmt.Println()
}

func printStep(msg string) {
	fmt.Printf("  %s▸%s %s\n", cyan, reset, msg)
}

func printDone(msg string) {
	fmt.Printf("  %s✓%s %s\n", green, reset, msg)
}

func printSection(title string) {
	fmt.Printf("  %s%s── %s ──%s\n", bold, cyan, title, reset)
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
