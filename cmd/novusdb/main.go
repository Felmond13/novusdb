// NovusDB CLI — Interface en ligne de commande interactive pour NovusDB.
//
// Usage :
//
//	NovusDB <fichier.dlite>
//	NovusDB                     (base en mémoire temporaire)
//
// Commandes spéciales (préfixées par .) :
//
//	.help       Affiche l'aide
//	.tables     Liste les collections
//	.quit       Quitte le REPL
//	.exit       Quitte le REPL
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Felmond13/novusdb/api"
	"github.com/Felmond13/novusdb/storage"
)

const version = "1.0.0"

func main() {
	fmt.Printf("NovusDB v%s — Mini SGBD embarqué orienté documents\n", version)
	fmt.Println("Tapez .help pour l'aide, .quit pour quitter.")
	fmt.Println()

	// Déterminer le chemin du fichier
	dbPath := ""
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	} else {
		dbPath = ":memory:"
	}

	// Ouvrir la base
	var actualPath string
	if dbPath == ":memory:" {
		// Fichier temporaire pour le mode mémoire
		f, err := os.CreateTemp("", "NovusDB_*.db")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Erreur: %v\n", err)
			os.Exit(1)
		}
		actualPath = f.Name()
		f.Close()
		defer os.Remove(actualPath)
		fmt.Println("Mode mémoire (fichier temporaire)")
	} else {
		actualPath = dbPath
		fmt.Printf("Base : %s\n", actualPath)
	}

	db, err := api.Open(actualPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Erreur d'ouverture : %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println()

	// REPL avec support multi-lignes (accumule jusqu'à ';')
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // buffer 1 MB pour gros JSON
	var accum strings.Builder
	for {
		if accum.Len() == 0 {
			fmt.Print("NovusDB> ")
		} else {
			fmt.Print("    ...> ")
		}
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" && accum.Len() == 0 {
			continue
		}

		// Commentaires SQL -- (ignorer la ligne entière)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Commandes spéciales (seulement en début de saisie)
		if accum.Len() == 0 && strings.HasPrefix(trimmed, ".") {
			if handleCommand(db, trimmed) {
				break // .quit ou .exit
			}
			continue
		}

		// Accumuler la ligne
		if accum.Len() > 0 {
			accum.WriteByte(' ')
		}
		accum.WriteString(trimmed)

		// Exécuter quand on voit un ';' en fin de ligne (ou ligne sans ';' si c'est une requête simple)
		text := accum.String()
		if strings.HasSuffix(trimmed, ";") {
			// Retirer le ';' final
			text = strings.TrimSuffix(strings.TrimSpace(text), ";")
			accum.Reset()
			executeQuery(db, text)
		} else if !strings.ContainsAny(trimmed, "{}[]") && accum.Len() > 0 && !strings.Contains(text, "{") {
			// Requête simple sur une ligne (pas de JSON) → exécuter directement
			accum.Reset()
			executeQuery(db, text)
		}
		// Sinon on continue d'accumuler (JSON multi-lignes)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Erreur de lecture : %v\n", err)
	}
}

// handleCommand gère les commandes spéciales (.help, .tables, etc.).
// Retourne true si on doit quitter.
func handleCommand(db *api.DB, cmd string) bool {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return false
	}

	switch strings.ToLower(parts[0]) {
	case ".quit", ".exit":
		fmt.Println("Au revoir.")
		return true

	case ".help":
		printHelp()

	case ".tables", ".collections":
		collections := db.Collections()
		if len(collections) == 0 {
			fmt.Println("  (aucune collection)")
		} else {
			for _, c := range collections {
				fmt.Printf("  %s\n", c)
			}
		}

	case ".schema":
		printSchema(db)

	case ".vacuum":
		n, err := db.Vacuum()
		if err != nil {
			fmt.Printf("  Erreur vacuum : %v\n", err)
		} else {
			fmt.Printf("  Vacuum terminé — %d record(s) récupéré(s)\n", n)
		}

	case ".indexes":
		defs := db.IndexDefs()
		if len(defs) == 0 {
			fmt.Println("  (aucun index)")
		} else {
			for _, d := range defs {
				fmt.Printf("  %s (%s)\n", d.Collection, d.Field)
			}
		}

	case ".cache":
		hits, misses, size, capacity := db.CacheStats()
		rate := db.CacheHitRate()
		fmt.Printf("  LRU Page Cache:\n")
		fmt.Printf("    Capacity : %d pages (%d KB)\n", capacity, capacity*4)
		fmt.Printf("    Size     : %d pages\n", size)
		fmt.Printf("    Hits     : %d\n", hits)
		fmt.Printf("    Misses   : %d\n", misses)
		fmt.Printf("    Hit rate : %.1f%%\n", rate*100)

	case ".dump":
		fmt.Print(db.Dump())

	case ".import":
		// .import <collection> <fichier.json>
		if len(parts) < 3 {
			fmt.Println("  Usage : .import <collection> <fichier.json>")
			break
		}
		importJSON(db, parts[1], parts[2])

	case ".views":
		views := db.Views()
		if len(views) == 0 {
			fmt.Println("  (aucune vue)")
		} else {
			for _, v := range views {
				fmt.Printf("  %s\n", v)
			}
		}

	case ".clear":
		// Compatibilité : ignorer silencieusement
		fmt.Print("\033[H\033[2J")

	case ".version":
		fmt.Printf("  NovusDB v%s\n", version)

	default:
		fmt.Printf("  Commande inconnue : %s (tapez .help)\n", parts[0])
	}

	return false
}

func printHelp() {
	fmt.Println(`Commandes SQL-like :
  SELECT [DISTINCT] * FROM <collection> [WHERE ...]
  SELECT <champs> FROM <collection> [WHERE ...] [ORDER BY ... [ASC|DESC]] [LIMIT n] [OFFSET n]
  SELECT <champ>, COUNT(*) FROM <collection> GROUP BY <champ> [HAVING ...]
  SELECT COUNT(*) | COUNT(field) | SUM(f) | MIN(f) | MAX(f) FROM <collection>
  SELECT * FROM <c1> [LEFT] JOIN <c2> ON <c1>.champ = <c2>.champ
  INSERT INTO <collection> VALUES (...) [, (...) ...]   Batch
  INSERT OR REPLACE INTO <collection> VALUES (...)     UPSERT
  INSERT INTO <dest> SELECT ... FROM <source> [WHERE ...]
  UPDATE <collection> SET champ=val [WHERE ...]
  DELETE FROM <collection> [WHERE ...]
  CREATE INDEX [IF NOT EXISTS] ON <collection> (champ)
  DROP INDEX [IF EXISTS] ON <collection> (champ)
  DROP TABLE [IF EXISTS] <collection>
  TRUNCATE TABLE <collection>
  EXPLAIN <requête>             Plan d'exécution

Opérateurs WHERE :
  =, !=, <, >, <=, >=        Comparaison
  AND, OR, NOT                Logique
  IN (val1, val2, ...)        Appartenance
  IS NULL / IS NOT NULL       Nullité
  LIKE "pattern%"             Pattern matching (% = *, _ = ?)
  NOT LIKE "pattern%"         Pattern matching inversé
  BETWEEN a AND b             Intervalle (inclusif)
  NOT BETWEEN a AND b         Hors intervalle

Commandes spéciales :
  .tables     Liste les collections
  .schema     Structure de chaque collection
  .vacuum     Compacte (récupère l'espace des records supprimés)
  .indexes    Liste les index persistés
  .cache      Statistiques du cache LRU (hits, misses, hit rate)
  .dump       Exporte toute la base en SQL (backup)
  .import     Importe un fichier JSON : .import <collection> <fichier.json>
  .views      Liste les vues
  .clear      Efface l'écran
  .version    Affiche la version
  .help       Affiche cette aide
  .quit       Quitte`)
}

// printSchema affiche la structure maximaliste de toutes les collections.
func printSchema(db *api.DB) {
	schemas := db.Schema()
	if len(schemas) == 0 {
		fmt.Println("  (aucune collection)")
		return
	}
	for i, s := range schemas {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("  %s (%d document(s))\n", s.Name, s.DocCount)
		if len(s.Fields) == 0 {
			fmt.Println("    (vide)")
			continue
		}
		for _, f := range s.Fields {
			types := strings.Join(f.Types, "|")
			pct := ""
			if s.DocCount > 0 {
				p := float64(f.Count) / float64(s.DocCount) * 100
				pct = fmt.Sprintf(" (%d/%d = %.0f%%)", f.Count, s.DocCount, p)
			}
			fmt.Printf("    ├─ %-25s %s%s\n", f.Name, types, pct)
		}
	}
}

// executeQuery exécute une requête et affiche le résultat.
func executeQuery(db *api.DB, query string) {
	res, err := db.Exec(query)
	if err != nil {
		fmt.Printf("  Erreur : %v\n", err)
		return
	}

	// Affichage selon le type de résultat
	if res.Docs != nil {
		// SELECT
		if len(res.Docs) == 0 {
			fmt.Println("  (aucun résultat)")
			return
		}
		for _, doc := range res.Docs {
			fmt.Printf("  [#%d] %s\n", doc.RecordID, formatDoc(doc.Doc))
		}
		fmt.Printf("  --- %d document(s)\n", len(res.Docs))
	} else {
		// INSERT / UPDATE / DELETE / CREATE INDEX
		if res.LastInsertID > 0 {
			fmt.Printf("  OK — %d ligne(s) affectée(s), dernier ID : %d\n", res.RowsAffected, res.LastInsertID)
		} else {
			fmt.Printf("  OK — %d ligne(s) affectée(s)\n", res.RowsAffected)
		}
	}
}

// formatDoc formate un document pour l'affichage.
func formatDoc(doc *storage.Document) string {
	parts := make([]string, 0, len(doc.Fields))
	for _, f := range doc.Fields {
		parts = append(parts, fmt.Sprintf("%s=%s", f.Name, formatValue(f.Value)))
	}
	return strings.Join(parts, ", ")
}

// formatValue formate une valeur pour l'affichage, y compris les sous-documents.
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

// importJSON importe un fichier JSON (objet ou tableau d'objets) dans une collection.
func importJSON(db *api.DB, collection, filepath string) {
	f, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("  Erreur : %v\n", err)
		return
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		fmt.Printf("  Erreur lecture : %v\n", err)
		return
	}

	// Try array of objects first, then single object
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err == nil {
		count := 0
		for _, raw := range arr {
			if _, err := db.InsertJSON(collection, string(raw)); err != nil {
				fmt.Printf("  Erreur insert #%d : %v\n", count+1, err)
				continue
			}
			count++
		}
		fmt.Printf("  %d document(s) importé(s) dans %s\n", count, collection)
		return
	}

	// Single object
	if _, err := db.InsertJSON(collection, string(data)); err != nil {
		fmt.Printf("  Erreur : %v\n", err)
		return
	}
	fmt.Printf("  1 document importé dans %s\n", collection)
}
