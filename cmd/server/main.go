// Package main implements a minimal HTTP REST server for NovusDB.
// Usage: NovusDB-server [-addr :8080] [-db data.db]
//
// Endpoints:
//
//	POST /query               — Execute SQL, body = {"sql": "SELECT ..."}
//	POST /insert/{collection} — Insert JSON document, body = {"name": "Alice", ...}
//	GET  /collections         — List collections
//	GET  /views               — List views
//	GET  /schema              — Schema of all collections
//	GET  /dump                — Export database as SQL
//	GET  /cache               — Cache statistics
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/storage"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "novusdb.db", "database file path")
	flag.Parse()

	db, err := api.Open(*dbPath)
	if err != nil {
		log.Fatalf("Cannot open database: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/query", queryHandler(db))
	mux.HandleFunc("/insert/", insertHandler(db))
	mux.HandleFunc("/collections", collectionsHandler(db))
	mux.HandleFunc("/views", viewsHandler(db))
	mux.HandleFunc("/schema", schemaHandler(db))
	mux.HandleFunc("/dump", dumpHandler(db))
	mux.HandleFunc("/cache", cacheHandler(db))

	// CORS wrapper pour le développement (Lumen)
	handler := corsMiddleware(mux)

	log.Printf("NovusDB HTTP server listening on %s (db: %s)", *addr, *dbPath)
	log.Fatal(http.ListenAndServe(*addr, handler))
}

type queryRequest struct {
	SQL string `json:"sql"`
}

type queryResponse struct {
	Docs         []map[string]interface{} `json:"docs,omitempty"`
	RowsAffected int64                    `json:"rows_affected,omitempty"`
	Error        string                   `json:"error,omitempty"`
}

func queryHandler(db *api.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req queryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, queryResponse{Error: "invalid JSON: " + err.Error()})
			return
		}
		if req.SQL == "" {
			writeJSON(w, http.StatusBadRequest, queryResponse{Error: "missing 'sql' field"})
			return
		}

		result, err := db.Exec(req.SQL)
		if err != nil {
			writeJSON(w, http.StatusOK, queryResponse{Error: err.Error()})
			return
		}

		resp := queryResponse{RowsAffected: result.RowsAffected}
		if result.Docs != nil {
			resp.Docs = make([]map[string]interface{}, len(result.Docs))
			for i, rd := range result.Docs {
				resp.Docs[i] = docToMap(rd.Doc)
			}
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

type insertResponse struct {
	ID    uint64 `json:"id"`
	Error string `json:"error,omitempty"`
}

func insertHandler(db *api.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		// Extract collection name from path: /insert/{collection}
		collection := strings.TrimPrefix(r.URL.Path, "/insert/")
		if collection == "" {
			writeJSON(w, http.StatusBadRequest, insertResponse{Error: "missing collection name in URL: /insert/{collection}"})
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, insertResponse{Error: "cannot read body: " + err.Error()})
			return
		}
		id, err := db.InsertJSON(collection, string(body))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, insertResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, insertResponse{ID: id})
	}
}

func collectionsHandler(db *api.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, db.Collections())
	}
}

func viewsHandler(db *api.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, db.Views())
	}
}

func schemaHandler(db *api.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, db.Schema())
	}
}

func dumpHandler(db *api.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte(db.Dump()))
	}
}

func cacheHandler(db *api.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits, misses, size, capacity := db.CacheStats()
		rate := db.CacheHitRate()
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"hits":     hits,
			"misses":   misses,
			"size":     size,
			"capacity": capacity,
			"hit_rate": fmt.Sprintf("%.1f%%", rate*100),
		})
	}
}

func docToMap(doc *storage.Document) map[string]interface{} {
	m := make(map[string]interface{})
	for _, f := range doc.Fields {
		if sub, ok := f.Value.(*storage.Document); ok {
			m[f.Name] = docToMap(sub)
		} else {
			m[f.Name] = f.Value
		}
	}
	return m
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// corsMiddleware ajoute les headers CORS pour le développement (Lumen UI).
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
