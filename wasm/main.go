//go:build js && wasm

package main

import (
	"fmt"
	"strings"
	"syscall/js"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/storage"
)

var db *api.DB

func main() {
	var err error
	db, err = api.OpenMemory()
	if err != nil {
		js.Global().Get("console").Call("error", "NovusDB init failed: "+err.Error())
		return
	}

	// Register JS functions
	js.Global().Set("NovusDBExec", js.FuncOf(execQuery))
	js.Global().Set("NovusDBCollections", js.FuncOf(getCollections))
	js.Global().Set("NovusDBSchema", js.FuncOf(getSchema))

	// Signal ready
	js.Global().Set("NovusDBReady", true)
	cb := js.Global().Get("onNovusDBReady")
	if cb.Truthy() {
		cb.Invoke()
	}

	// Block forever
	select {}
}

// execQuery is called from JS: NovusDBExec(sql) -> string
func execQuery(this js.Value, args []js.Value) interface{} {
	if len(args) == 0 {
		return "error: no query provided"
	}
	query := strings.TrimSpace(args[0].String())
	if query == "" {
		return ""
	}

	// Handle dot-commands
	if strings.HasPrefix(query, ".") {
		return handleDotCommand(query)
	}

	res, err := db.Exec(query)
	if err != nil {
		return fmt.Sprintf("Erreur : %v", err)
	}

	var sb strings.Builder
	if res.Docs != nil {
		if len(res.Docs) == 0 {
			sb.WriteString("(aucun résultat)")
		} else {
			for _, doc := range res.Docs {
				sb.WriteString(fmt.Sprintf("[#%d] %s\n", doc.RecordID, formatDoc(doc.Doc)))
			}
			sb.WriteString(fmt.Sprintf("--- %d document(s)", len(res.Docs)))
		}
	} else {
		if res.LastInsertID > 0 {
			sb.WriteString(fmt.Sprintf("OK — %d ligne(s) affectée(s), dernier ID : %d", res.RowsAffected, res.LastInsertID))
		} else {
			sb.WriteString(fmt.Sprintf("OK — %d ligne(s) affectée(s)", res.RowsAffected))
		}
	}
	return sb.String()
}

func getCollections(this js.Value, args []js.Value) interface{} {
	cols := db.Collections()
	if len(cols) == 0 {
		return "(aucune collection)"
	}
	return strings.Join(cols, "\n")
}

func getSchema(this js.Value, args []js.Value) interface{} {
	schemas := db.Schema()
	if len(schemas) == 0 {
		return "(aucune collection)"
	}
	var sb strings.Builder
	for _, s := range schemas {
		sb.WriteString(fmt.Sprintf("Collection: %s (%d docs)\n", s.Name, s.DocCount))
		for _, f := range s.Fields {
			sb.WriteString(fmt.Sprintf("  ├─ %-25s %s\n", f.Name, strings.Join(f.Types, ",")))
		}
	}
	return sb.String()
}

func handleDotCommand(cmd string) string {
	switch {
	case cmd == ".tables":
		cols := db.Collections()
		if len(cols) == 0 {
			return "(aucune collection)"
		}
		return strings.Join(cols, "\n")
	case cmd == ".schema":
		return getSchema(js.Null(), nil).(string)
	case cmd == ".indexes":
		defs := db.IndexDefs()
		if len(defs) == 0 {
			return "(aucun index)"
		}
		var sb strings.Builder
		for _, d := range defs {
			sb.WriteString(fmt.Sprintf("%s(%s)\n", d.Collection, d.Field))
		}
		return sb.String()
	case cmd == ".sequences":
		seqs := db.Sequences()
		if len(seqs) == 0 {
			return "(aucune séquence)"
		}
		var sb strings.Builder
		for _, s := range seqs {
			sb.WriteString(fmt.Sprintf("%s  val=%g  incr=%g  min=%g  max=%g  cycle=%v\n",
				s.Name, s.CurrentVal, s.IncrementBy, s.MinValue, s.MaxValue, s.Cycle))
		}
		return sb.String()
	case cmd == ".help":
		return `.tables          Liste des collections
.schema          Schéma des collections
.indexes         Liste des index
.sequences       Liste des séquences
.dump            Export SQL complet
.help            Cette aide

SQL supporté : SELECT, INSERT INTO, UPDATE, DELETE,
CREATE INDEX, DROP INDEX, CREATE VIEW, EXPLAIN,
CREATE/DROP SEQUENCE, seq.NEXTVAL, seq.CURRVAL,
JOIN, GROUP BY, ORDER BY, LIMIT, DISTINCT,
CASE WHEN, sous-requêtes, dot-notation JSON`
	case cmd == ".dump":
		return db.Dump()
	default:
		return fmt.Sprintf("Commande inconnue : %s (tapez .help)", cmd)
	}
}

func formatDoc(doc *storage.Document) string {
	parts := make([]string, 0, len(doc.Fields))
	for _, f := range doc.Fields {
		parts = append(parts, fmt.Sprintf("%s=%s", f.Name, formatValue(f.Value)))
	}
	return strings.Join(parts, ", ")
}

func formatValue(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch doc := v.(type) {
	case *storage.Document:
		return "{" + formatDoc(doc) + "}"
	case string:
		return `"` + doc + `"`
	case bool:
		if doc {
			return "true"
		}
		return "false"
	case []interface{}:
		parts := make([]string, len(doc))
		for i, elem := range doc {
			parts[i] = formatValue(elem)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return fmt.Sprintf("%v", v)
	}
}
