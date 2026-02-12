package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/engine"
)

//go:embed static
var staticFiles embed.FS

var db *api.DB

func main() {
	var err error
	db, err = api.Open("demo_employees.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	seedIfEmpty()

	// Serve embedded static files
	staticFS, _ := fs.Sub(staticFiles, "static")
	http.Handle("/", http.FileServer(http.FS(staticFS)))

	// API routes
	http.HandleFunc("/api/employees", cors(handleEmployees))
	http.HandleFunc("/api/employees/search", cors(handleSearch))
	http.HandleFunc("/api/employees/add", cors(handleAdd))
	http.HandleFunc("/api/employees/delete", cors(handleDelete))
	http.HandleFunc("/api/stats", cors(handleStats))
	http.HandleFunc("/api/query", cors(handleQuery))

	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║     NovusDB Demo — http://localhost:3000     ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	log.Fatal(http.ListenAndServe(":3000", nil))
}

func cors(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			return
		}
		next(w, r)
	}
}

// GET /api/employees?limit=50&sort=last_name&order=ASC
func handleEmployees(w http.ResponseWriter, r *http.Request) {
	limit := qp(r, "limit", "50")
	offset := qp(r, "offset", "0")
	sort := qp(r, "sort", "last_name")
	order := qp(r, "order", "ASC")
	sql := fmt.Sprintf(`SELECT * FROM employees ORDER BY %s %s LIMIT %s OFFSET %s`, sort, order, limit, offset)
	queryToJSON(w, sql)
}

// GET /api/employees/search?q=Paris&field=city
func handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	field := qp(r, "field", "city")
	if q == "" {
		jerr(w, "missing q", 400)
		return
	}
	sql := fmt.Sprintf(`SELECT * FROM employees WHERE %s = "%s" ORDER BY last_name ASC LIMIT 100`, field, q)
	queryToJSON(w, sql)
}

// POST /api/employees/add
func handleAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		jerr(w, "POST required", 405)
		return
	}
	var b struct {
		FirstName  string `json:"first_name"`
		LastName   string `json:"last_name"`
		City       string `json:"city"`
		Department string `json:"department"`
		Age        int    `json:"age"`
		Salary     int    `json:"salary"`
	}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		jerr(w, "invalid JSON", 400)
		return
	}
	sql := fmt.Sprintf(`INSERT INTO employees VALUES {"first_name": "%s", "last_name": "%s", "city": "%s", "department": "%s", "age": %d, "salary": %d, "active": true}`,
		b.FirstName, b.LastName, b.City, b.Department, b.Age, b.Salary)
	start := time.Now()
	_, err := db.Exec(sql)
	if err != nil {
		jerr(w, err.Error(), 500)
		return
	}
	jok(w, map[string]interface{}{"ok": true, "time_ms": ms(start)})
}

// DELETE /api/employees/delete?first_name=X&last_name=Y
func handleDelete(w http.ResponseWriter, r *http.Request) {
	fn := r.URL.Query().Get("first_name")
	ln := r.URL.Query().Get("last_name")
	if fn == "" || ln == "" {
		jerr(w, "missing params", 400)
		return
	}
	sql := fmt.Sprintf(`DELETE FROM employees WHERE first_name = "%s" AND last_name = "%s" LIMIT 1`, fn, ln)
	start := time.Now()
	res, err := db.Exec(sql)
	if err != nil {
		jerr(w, err.Error(), 500)
		return
	}
	jok(w, map[string]interface{}{"ok": true, "deleted": res.RowsAffected, "time_ms": ms(start)})
}

// GET /api/stats
func handleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{}

	if res, err := db.Exec(`SELECT COUNT(*) AS count FROM employees`); err == nil && len(res.Docs) > 0 {
		stats["total"] = toMap(res.Docs[0])
	}
	if res, err := db.Exec(`SELECT AVG(salary) AS value FROM employees`); err == nil && len(res.Docs) > 0 {
		stats["avg_salary"] = toMap(res.Docs[0])
	}
	if res, err := db.Exec(`SELECT city, COUNT(*) AS count FROM employees GROUP BY city`); err == nil {
		var arr []map[string]interface{}
		for _, d := range res.Docs {
			arr = append(arr, toMap(d))
		}
		stats["by_city"] = arr
	}
	if res, err := db.Exec(`SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary FROM employees GROUP BY department`); err == nil {
		var arr []map[string]interface{}
		for _, d := range res.Docs {
			arr = append(arr, toMap(d))
		}
		stats["by_department"] = arr
	}
	if res, err := db.Exec(`SELECT first_name, last_name, salary, department FROM employees ORDER BY salary DESC LIMIT 5`); err == nil {
		var arr []map[string]interface{}
		for _, d := range res.Docs {
			arr = append(arr, toMap(d))
		}
		stats["top_earners"] = arr
	}
	jok(w, stats)
}

// POST /api/query {sql: "..."}
func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		jerr(w, "POST required", 405)
		return
	}
	var b struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		jerr(w, "invalid JSON", 400)
		return
	}
	start := time.Now()
	res, err := db.Exec(b.SQL)
	elapsed := ms(start)
	if err != nil {
		jerr(w, err.Error(), 500)
		return
	}
	var rows []map[string]interface{}
	for _, d := range res.Docs {
		rows = append(rows, toMap(d))
	}
	jok(w, map[string]interface{}{"rows": rows, "count": len(rows), "time_ms": elapsed, "affected": res.RowsAffected})
}

// ---------- Helpers ----------

func queryToJSON(w http.ResponseWriter, sql string) {
	start := time.Now()
	res, err := db.Exec(sql)
	elapsed := ms(start)
	if err != nil {
		jerr(w, err.Error(), 500)
		return
	}
	var rows []map[string]interface{}
	for _, d := range res.Docs {
		rows = append(rows, toMap(d))
	}
	jok(w, map[string]interface{}{"rows": rows, "count": len(rows), "time_ms": elapsed})
}

func toMap(rd *engine.ResultDoc) map[string]interface{} {
	m := map[string]interface{}{}
	for _, f := range rd.Doc.Fields {
		m[f.Name] = f.Value
	}
	return m
}

func jok(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jerr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func qp(r *http.Request, key, def string) string {
	if v := r.URL.Query().Get(key); v != "" {
		return v
	}
	return def
}

func ms(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}

// ---------- Seed ----------

var (
	firstNames  = []string{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hector", "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina"}
	lastNames   = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	cities      = []string{"Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"}
	departments = []string{"Engineering", "Marketing", "Sales", "HR", "Finance", "Support", "Product", "Design"}
)

func seedIfEmpty() {
	res, _ := db.Exec(`SELECT COUNT(*) AS c FROM employees`)
	if res != nil && len(res.Docs) > 0 {
		for _, f := range res.Docs[0].Doc.Fields {
			if f.Name == "c" {
				if count, ok := f.Value.(int64); ok && count > 0 {
					fmt.Printf("Database: %d employees ready\n", count)
					return
				}
			}
		}
	}

	fmt.Print("Seeding 1,000 employees... ")
	start := time.Now()
	db.Exec("BEGIN")
	for i := 0; i < 1000; i++ {
		fn := firstNames[rand.Intn(len(firstNames))]
		ln := lastNames[rand.Intn(len(lastNames))]
		city := cities[rand.Intn(len(cities))]
		dept := departments[rand.Intn(len(departments))]
		age := 22 + rand.Intn(40)
		salary := 30000 + rand.Intn(80000)
		db.Exec(fmt.Sprintf(`INSERT INTO employees VALUES {"first_name": "%s", "last_name": "%s", "city": "%s", "department": "%s", "age": %d, "salary": %d, "active": true}`,
			fn, ln, city, dept, age, salary))
	}
	db.Exec("COMMIT")
	fmt.Printf("done in %s\n", time.Since(start).Round(time.Millisecond))
}
