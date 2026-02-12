package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/engine"
)

var db *api.DB

func main() {
	var err error
	db, err = api.Open("demo_employees.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Seed data if empty
	seedIfEmpty()

	// Routes
	http.HandleFunc("/api/employees", corsMiddleware(handleEmployees))
	http.HandleFunc("/api/employees/search", corsMiddleware(handleSearch))
	http.HandleFunc("/api/employees/add", corsMiddleware(handleAdd))
	http.HandleFunc("/api/employees/delete", corsMiddleware(handleDelete))
	http.HandleFunc("/api/stats", corsMiddleware(handleStats))
	http.HandleFunc("/api/query", corsMiddleware(handleQuery))

	fmt.Println("NovusDB Demo API running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		next(w, r)
	}
}

// GET /api/employees?limit=50&offset=0&sort=salary&order=desc
func handleEmployees(w http.ResponseWriter, r *http.Request) {
	limit := queryParam(r, "limit", "50")
	offset := queryParam(r, "offset", "0")
	sort := queryParam(r, "sort", "first_name")
	order := queryParam(r, "order", "ASC")

	sql := fmt.Sprintf(`SELECT * FROM employees ORDER BY %s %s LIMIT %s OFFSET %s`, sort, order, limit, offset)
	writeQueryResult(w, sql)
}

// GET /api/employees/search?q=Paris&field=city
func handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	field := queryParam(r, "field", "city")
	if q == "" {
		jsonError(w, "missing q parameter", 400)
		return
	}
	sql := fmt.Sprintf(`SELECT * FROM employees WHERE %s = "%s" ORDER BY last_name ASC LIMIT 100`, field, q)
	writeQueryResult(w, sql)
}

// POST /api/employees/add
func handleAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		jsonError(w, "POST required", 405)
		return
	}
	var body struct {
		FirstName  string `json:"first_name"`
		LastName   string `json:"last_name"`
		City       string `json:"city"`
		Department string `json:"department"`
		Age        int    `json:"age"`
		Salary     int    `json:"salary"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid JSON", 400)
		return
	}
	sql := fmt.Sprintf(`INSERT INTO employees VALUES {"first_name": "%s", "last_name": "%s", "city": "%s", "department": "%s", "age": %d, "salary": %d, "active": true}`,
		body.FirstName, body.LastName, body.City, body.Department, body.Age, body.Salary)

	start := time.Now()
	_, err := db.Exec(sql)
	elapsed := time.Since(start)
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "time_ms": float64(elapsed.Microseconds()) / 1000})
}

// DELETE /api/employees/delete?first_name=X&last_name=Y
func handleDelete(w http.ResponseWriter, r *http.Request) {
	fn := r.URL.Query().Get("first_name")
	ln := r.URL.Query().Get("last_name")
	if fn == "" || ln == "" {
		jsonError(w, "missing first_name or last_name", 400)
		return
	}
	sql := fmt.Sprintf(`DELETE FROM employees WHERE first_name = "%s" AND last_name = "%s" LIMIT 1`, fn, ln)
	start := time.Now()
	res, err := db.Exec(sql)
	elapsed := time.Since(start)
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}
	jsonOK(w, map[string]interface{}{"ok": true, "deleted": res.RowsAffected, "time_ms": float64(elapsed.Microseconds()) / 1000})
}

// GET /api/stats
func handleStats(w http.ResponseWriter, r *http.Request) {
	stats := make(map[string]interface{})

	run := func(key, sql string) {
		start := time.Now()
		res, err := db.Exec(sql)
		elapsed := time.Since(start)
		if err != nil || len(res.Docs) == 0 {
			return
		}
		row := docToMap(res.Docs[0])
		row["_time_ms"] = float64(elapsed.Microseconds()) / 1000
		stats[key] = row
	}

	run("total", `SELECT COUNT(*) AS count FROM employees`)
	run("avg_salary", `SELECT AVG(salary) AS value FROM employees`)
	run("by_city", `SELECT city, COUNT(*) AS count FROM employees GROUP BY city`)
	run("by_department", `SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary FROM employees GROUP BY department`)
	run("top_earners", `SELECT first_name, last_name, salary, department FROM employees ORDER BY salary DESC LIMIT 5`)

	// by_city and by_department should return arrays
	cityRes, _ := db.Exec(`SELECT city, COUNT(*) AS count FROM employees GROUP BY city`)
	if cityRes != nil {
		var arr []map[string]interface{}
		for _, d := range cityRes.Docs {
			arr = append(arr, docToMap(d))
		}
		stats["by_city"] = arr
	}
	deptRes, _ := db.Exec(`SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary FROM employees GROUP BY department`)
	if deptRes != nil {
		var arr []map[string]interface{}
		for _, d := range deptRes.Docs {
			arr = append(arr, docToMap(d))
		}
		stats["by_department"] = arr
	}
	topRes, _ := db.Exec(`SELECT first_name, last_name, salary, department FROM employees ORDER BY salary DESC LIMIT 5`)
	if topRes != nil {
		var arr []map[string]interface{}
		for _, d := range topRes.Docs {
			arr = append(arr, docToMap(d))
		}
		stats["top_earners"] = arr
	}

	jsonOK(w, stats)
}

// POST /api/query  {sql: "SELECT ..."}
func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		jsonError(w, "POST required", 405)
		return
	}
	var body struct {
		SQL string `json:"sql"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid JSON", 400)
		return
	}
	start := time.Now()
	res, err := db.Exec(body.SQL)
	elapsed := time.Since(start)
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}
	var rows []map[string]interface{}
	for _, d := range res.Docs {
		rows = append(rows, docToMap(d))
	}
	jsonOK(w, map[string]interface{}{
		"rows":     rows,
		"count":    len(rows),
		"time_ms":  float64(elapsed.Microseconds()) / 1000,
		"affected": res.RowsAffected,
	})
}

// ---------- Helpers ----------

func writeQueryResult(w http.ResponseWriter, sql string) {
	start := time.Now()
	res, err := db.Exec(sql)
	elapsed := time.Since(start)
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}
	var rows []map[string]interface{}
	for _, d := range res.Docs {
		rows = append(rows, docToMap(d))
	}
	jsonOK(w, map[string]interface{}{
		"rows":    rows,
		"count":   len(rows),
		"time_ms": float64(elapsed.Microseconds()) / 1000,
	})
}

func docToMap(rd *engine.ResultDoc) map[string]interface{} {
	m := make(map[string]interface{})
	for _, f := range rd.Doc.Fields {
		m[f.Name] = f.Value
	}
	return m
}

func jsonOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func queryParam(r *http.Request, key, def string) string {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	return v
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
					fmt.Printf("Database already has %d employees\n", count)
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
		sql := fmt.Sprintf(`INSERT INTO employees VALUES {"first_name": "%s", "last_name": "%s", "city": "%s", "department": "%s", "age": %d, "salary": %d, "active": true}`,
			fn, ln, city, dept, age, salary)
		db.Exec(sql)
	}
	db.Exec("COMMIT")
	fmt.Printf("done in %s\n", time.Since(start).Round(time.Millisecond))
}
