// Exemple d'utilisation de NovusDB v1.
// Démontre INSERT, SELECT, UPDATE, DELETE, index et champs imbriqués.
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/storage"
)

func main() {
	const dbPath = "example.dlite"
	defer os.Remove(dbPath)

	db, err := api.Open(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== NovusDB v1 — Exemple d'utilisation ===")
	fmt.Println()

	// -------------------------------------------------------
	// 1. INSERT
	// -------------------------------------------------------
	fmt.Println("--- INSERT ---")
	queries := []string{
		`INSERT INTO jobs VALUES (type="oracle", retry=5, enabled=true)`,
		`INSERT INTO jobs VALUES (type="mysql", retry=2, enabled=true)`,
		`INSERT INTO jobs VALUES (type="postgres", retry=0, enabled=false)`,
		`INSERT INTO jobs VALUES (type="oracle", retry=8, enabled=true, params.timeout=30)`,
		`INSERT INTO jobs VALUES (type="mysql", retry=1, enabled=false, params.timeout=60)`,
	}
	for _, q := range queries {
		res, err := db.Exec(q)
		if err != nil {
			log.Fatalf("INSERT error: %v", err)
		}
		fmt.Printf("  Inserted record #%d\n", res.LastInsertID)
	}
	fmt.Println()

	// -------------------------------------------------------
	// 2. SELECT *
	// -------------------------------------------------------
	fmt.Println("--- SELECT * FROM jobs ---")
	printQuery(db, `SELECT * FROM jobs`)

	// -------------------------------------------------------
	// 3. SELECT avec WHERE
	// -------------------------------------------------------
	fmt.Println("--- SELECT * FROM jobs WHERE retry > 3 ---")
	printQuery(db, `SELECT * FROM jobs WHERE retry > 3`)

	fmt.Println("--- SELECT * FROM jobs WHERE type=\"oracle\" AND enabled=true ---")
	printQuery(db, `SELECT * FROM jobs WHERE type="oracle" AND enabled=true`)

	// -------------------------------------------------------
	// 4. Champs imbriqués
	// -------------------------------------------------------
	fmt.Println("--- SELECT * FROM jobs WHERE params.timeout=30 ---")
	printQuery(db, `SELECT * FROM jobs WHERE params.timeout=30`)

	// -------------------------------------------------------
	// 5. UPDATE
	// -------------------------------------------------------
	fmt.Println("--- UPDATE jobs SET retry=99 WHERE type=\"postgres\" ---")
	res, err := db.Exec(`UPDATE jobs SET retry=99 WHERE type="postgres"`)
	if err != nil {
		log.Fatalf("UPDATE error: %v", err)
	}
	fmt.Printf("  Rows updated: %d\n\n", res.RowsAffected)

	fmt.Println("--- After UPDATE: SELECT * FROM jobs WHERE type=\"postgres\" ---")
	printQuery(db, `SELECT * FROM jobs WHERE type="postgres"`)

	// -------------------------------------------------------
	// 6. UPDATE champ imbriqué
	// -------------------------------------------------------
	fmt.Println("--- UPDATE jobs SET params.timeout=120 WHERE params.timeout=30 ---")
	res, err = db.Exec(`UPDATE jobs SET params.timeout=120 WHERE params.timeout=30`)
	if err != nil {
		log.Fatalf("UPDATE nested error: %v", err)
	}
	fmt.Printf("  Rows updated: %d\n\n", res.RowsAffected)

	// -------------------------------------------------------
	// 7. DELETE
	// -------------------------------------------------------
	fmt.Println("--- DELETE FROM jobs WHERE enabled=false ---")
	res, err = db.Exec(`DELETE FROM jobs WHERE enabled=false`)
	if err != nil {
		log.Fatalf("DELETE error: %v", err)
	}
	fmt.Printf("  Rows deleted: %d\n\n", res.RowsAffected)

	fmt.Println("--- After DELETE: SELECT * FROM jobs ---")
	printQuery(db, `SELECT * FROM jobs`)

	// -------------------------------------------------------
	// 8. CREATE INDEX + requête indexée
	// -------------------------------------------------------
	fmt.Println("--- CREATE INDEX ON jobs (type) ---")
	_, err = db.Exec(`CREATE INDEX ON jobs (type)`)
	if err != nil {
		log.Fatalf("CREATE INDEX error: %v", err)
	}
	fmt.Println("  Index created.")
	fmt.Println()

	fmt.Println("--- SELECT with index: WHERE type=\"oracle\" ---")
	printQuery(db, `SELECT * FROM jobs WHERE type="oracle"`)

	// -------------------------------------------------------
	// 9. ORDER BY + LIMIT
	// -------------------------------------------------------
	fmt.Println("--- SELECT * FROM jobs ORDER BY retry DESC LIMIT 2 ---")
	printQuery(db, `SELECT * FROM jobs ORDER BY retry DESC LIMIT 2`)

	// -------------------------------------------------------
	// 10. GROUP BY + COUNT
	// -------------------------------------------------------
	// Insérer plus de données pour le GROUP BY
	db.Exec(`INSERT INTO logs VALUES (level="INFO", msg="started")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR", msg="failed")`)
	db.Exec(`INSERT INTO logs VALUES (level="INFO", msg="processing")`)
	db.Exec(`INSERT INTO logs VALUES (level="WARN", msg="slow query")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR", msg="timeout")`)

	fmt.Println("--- SELECT level, COUNT(*) FROM logs GROUP BY level ---")
	printQuery(db, `SELECT level, COUNT(*) FROM logs GROUP BY level`)

	// -------------------------------------------------------
	// 11. Multi-collection
	// -------------------------------------------------------
	fmt.Println("--- Collections ---")
	for _, c := range db.Collections() {
		fmt.Printf("  - %s\n", c)
	}
	fmt.Println()

	// -------------------------------------------------------
	// 12. Concurrence : inserts parallèles
	// -------------------------------------------------------
	fmt.Println("--- Concurrent inserts (10 goroutines x 100 docs) ---")
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				q := fmt.Sprintf(`INSERT INTO bench VALUES (gid=%d, idx=%d)`, gid, i)
				if _, err := db.Exec(q); err != nil {
					log.Printf("  concurrent insert error: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()

	res, err = db.Exec(`SELECT * FROM bench`)
	if err != nil {
		log.Fatalf("SELECT bench error: %v", err)
	}
	fmt.Printf("  Total docs in bench: %d (expected 1000)\n\n", len(res.Docs))

	fmt.Println("=== Done ===")
}

// printQuery exécute un SELECT et affiche les résultats formatés.
func printQuery(db *api.DB, query string) {
	res, err := db.Exec(query)
	if err != nil {
		log.Fatalf("query error: %v\n  query: %s", err, query)
	}
	if len(res.Docs) == 0 {
		fmt.Println("  (no results)")
	}
	for _, doc := range res.Docs {
		parts := make([]string, 0, len(doc.Doc.Fields))
		for _, f := range doc.Doc.Fields {
			parts = append(parts, fmt.Sprintf("%s=%v", f.Name, formatValue(f.Value)))
		}
		fmt.Printf("  [#%d] %s\n", doc.RecordID, strings.Join(parts, ", "))
	}
	fmt.Println()
}

func formatValue(v interface{}) string {
	if v == nil {
		return "null"
	}
	// Affichage lisible des sous-documents
	switch doc := v.(type) {
	case *storage.Document:
		parts := make([]string, 0, len(doc.Fields))
		for _, f := range doc.Fields {
			parts = append(parts, fmt.Sprintf("%s=%v", f.Name, formatValue(f.Value)))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprintf("%v", v)
	}
}
