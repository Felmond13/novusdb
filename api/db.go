// Package api fournit l'interface utilisateur de NovusDB.
// C'est le point d'entrée principal pour ouvrir une base, exécuter des requêtes
// et manipuler des documents.
package api

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Felmond13/novusdb/concurrency"
	"github.com/Felmond13/novusdb/engine"
	"github.com/Felmond13/novusdb/index"
	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// DB représente une instance de base de données NovusDB.
type DB struct {
	pager    *storage.Pager
	executor *engine.Executor
	lockMgr  *concurrency.LockManager
	indexMgr *index.Manager
}

// Open ouvre ou crée une base de données NovusDB sur le fichier donné.
func Open(path string) (*DB, error) {
	pager, err := storage.OpenPager(path)
	if err != nil {
		return nil, fmt.Errorf("NovusDB: %w", err)
	}

	lockMgr := concurrency.NewLockManager(concurrency.LockPolicyWait)
	indexMgr := index.NewManager(pager)
	executor := engine.NewExecutor(pager, lockMgr, indexMgr)

	db := &DB{
		pager:    pager,
		executor: executor,
		lockMgr:  lockMgr,
		indexMgr: indexMgr,
	}

	// Ouvrir les B-Trees persistés (pas de rebuild — lecture directe depuis le disque)
	db.openPersistentIndexes()

	// Charger les stats ANALYZE persistées
	executor.LoadStats()

	return db, nil
}

// OpenReadOnly ouvre une base de données en mode lecture seule.
// Toute tentative d'écriture (INSERT, UPDATE, DELETE, CREATE, DROP, BEGIN) retournera une erreur.
func OpenReadOnly(path string) (*DB, error) {
	pager, err := storage.OpenPagerReadOnly(path)
	if err != nil {
		return nil, fmt.Errorf("NovusDB: %w", err)
	}

	lockMgr := concurrency.NewLockManager(concurrency.LockPolicyWait)
	indexMgr := index.NewManager(pager)
	executor := engine.NewExecutor(pager, lockMgr, indexMgr)

	db := &DB{
		pager:    pager,
		executor: executor,
		lockMgr:  lockMgr,
		indexMgr: indexMgr,
	}
	db.openPersistentIndexes()
	executor.LoadStats()
	return db, nil
}

// OpenMemory crée une base de données entièrement en mémoire (sans fichier ni WAL).
// Utilisé pour le mode WASM / playground.
func OpenMemory() (*DB, error) {
	pager, err := storage.OpenPagerMemory()
	if err != nil {
		return nil, fmt.Errorf("NovusDB: %w", err)
	}

	lockMgr := concurrency.NewLockManager(concurrency.LockPolicyWait)
	indexMgr := index.NewManager(pager)
	executor := engine.NewExecutor(pager, lockMgr, indexMgr)

	return &DB{
		pager:    pager,
		executor: executor,
		lockMgr:  lockMgr,
		indexMgr: indexMgr,
	}, nil
}

// openPersistentIndexes ouvre les B-Trees existants à partir des pages racines persistées.
func (db *DB) openPersistentIndexes() {
	for _, def := range db.pager.IndexDefs() {
		if def.RootPageID != 0 {
			db.indexMgr.OpenIndex(def.Collection, def.Field, def.RootPageID)
		}
	}
}

// Close ferme la base de données proprement.
func (db *DB) Close() error {
	return db.pager.Close()
}

// Exec exécute une requête SQL-like et retourne le résultat.
func (db *DB) Exec(query string) (*engine.Result, error) {
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("NovusDB: parse error: %w", err)
	}
	result, err := db.executor.Execute(stmt)
	if err != nil {
		return nil, fmt.Errorf("NovusDB: exec error: %w", err)
	}
	return result, nil
}

// ExecParams exécute une requête SQL-like avec des paramètres positionnels (? placeholders).
// Cela protège contre l'injection SQL en séparant la requête des données.
//
// Exemple :
//
//	db.ExecParams(`SELECT * FROM users WHERE name = ? AND age > ?`, "Alice", 25)
func (db *DB) ExecParams(query string, params ...interface{}) (*engine.Result, error) {
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("NovusDB: parse error: %w", err)
	}
	// Resolve parameter placeholders in the AST
	if err := parser.ResolveParams(stmt, params); err != nil {
		return nil, fmt.Errorf("NovusDB: param error: %w", err)
	}
	result, err := db.executor.Execute(stmt)
	if err != nil {
		return nil, fmt.Errorf("NovusDB: exec error: %w", err)
	}
	return result, nil
}

// ---------- Transactions ----------

// Tx représente une transaction explicite.
type Tx struct {
	db     *DB
	active bool
}

// Begin démarre une transaction explicite.
// Les écritures sont atomiques : Commit() les rend permanentes, Rollback() les annule.
func (db *DB) Begin() (*Tx, error) {
	if err := db.pager.BeginTx(); err != nil {
		return nil, fmt.Errorf("NovusDB: %w", err)
	}
	return &Tx{db: db, active: true}, nil
}

// Exec exécute une requête dans la transaction.
func (tx *Tx) Exec(query string) (*engine.Result, error) {
	if !tx.active {
		return nil, fmt.Errorf("NovusDB: transaction is no longer active")
	}
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("NovusDB: parse error: %w", err)
	}
	result, err := tx.db.executor.Execute(stmt)
	if err != nil {
		return nil, fmt.Errorf("NovusDB: exec error: %w", err)
	}
	return result, nil
}

// Commit valide la transaction. Toutes les écritures deviennent permanentes.
func (tx *Tx) Commit() error {
	if !tx.active {
		return fmt.Errorf("NovusDB: transaction is no longer active")
	}
	tx.active = false
	if err := tx.db.pager.CommitTx(); err != nil {
		return fmt.Errorf("NovusDB: commit: %w", err)
	}
	return nil
}

// Rollback annule la transaction. Toutes les écritures sont défaites.
func (tx *Tx) Rollback() error {
	if !tx.active {
		return fmt.Errorf("NovusDB: transaction is no longer active")
	}
	tx.active = false
	if err := tx.db.pager.RollbackTx(); err != nil {
		return fmt.Errorf("NovusDB: rollback: %w", err)
	}
	return nil
}

// Collections retourne la liste des collections existantes.
func (db *DB) Collections() []string {
	return db.pager.ListCollections()
}

// IndexDefs retourne la liste des définitions d'index persistées.
func (db *DB) IndexDefs() []storage.IndexDef {
	return db.pager.IndexDefs()
}

// CacheStats retourne les statistiques du cache LRU de pages.
func (db *DB) CacheStats() (hits, misses uint64, size, capacity int) {
	return db.pager.CacheStats()
}

// CacheHitRate retourne le taux de hit du cache (0.0 à 1.0).
func (db *DB) CacheHitRate() float64 {
	return db.pager.CacheHitRate()
}

// InsertDoc insère un document programmatiquement (sans passer par le parser).
func (db *DB) InsertDoc(collection string, doc *storage.Document) (uint64, error) {
	coll, err := db.pager.GetOrCreateCollection(collection)
	if err != nil {
		return 0, err
	}

	recordID, err := db.pager.NextRecordID(collection)
	if err != nil {
		return 0, err
	}

	encoded, err := doc.Encode()
	if err != nil {
		return 0, err
	}

	// Insertion atomique dans les pages de la collection
	insPageID, insSlotOff, insErr := db.pager.InsertRecordAtomic(coll, recordID, encoded)
	if insErr != nil {
		return 0, insErr
	}

	// Mettre à jour les index
	db.updateIndexes(collection, recordID, doc, insPageID, insSlotOff)

	if err := db.pager.FlushMeta(); err != nil {
		return 0, err
	}

	// WAL commit : garantir la durabilité
	if err := db.pager.CommitWAL(); err != nil {
		return 0, err
	}

	return recordID, nil
}

// updateIndexes met à jour tous les index après un insert programmatique.
func (db *DB) updateIndexes(collection string, recordID uint64, doc *storage.Document, pageID uint32, slotOff uint16) {
	db.lockMgr.IndexMu.Lock()
	defer db.lockMgr.IndexMu.Unlock()

	for _, idx := range db.indexMgr.GetIndexesForCollection(collection) {
		val, ok := doc.Get(idx.Field)
		if ok {
			idx.Add(index.ValueToKey(val), recordID, pageID, slotOff)
		}
	}
}

// FieldInfo décrit un champ observé dans une collection.
type FieldInfo struct {
	Name  string   // chemin complet (ex: "params.timeout")
	Types []string // types observés (ex: ["int64", "string"])
	Count int      // nombre de documents contenant ce champ
}

// CollectionSchema décrit la structure maximaliste d'une collection.
type CollectionSchema struct {
	Name     string
	DocCount int
	Fields   []FieldInfo
}

// Schema retourne la structure maximaliste de chaque collection.
// Scanne tous les documents pour extraire l'union de tous les champs et types observés.
func (db *DB) Schema() []CollectionSchema {
	var schemas []CollectionSchema

	for _, collName := range db.pager.ListCollections() {
		res, err := db.Exec("SELECT * FROM " + collName)
		if err != nil {
			continue
		}

		// Map champ → types observés + count
		fieldTypes := make(map[string]map[string]bool)
		fieldCount := make(map[string]int)

		for _, rd := range res.Docs {
			collectFields(rd.Doc, "", fieldTypes, fieldCount)
		}

		// Construire la liste
		var fields []FieldInfo
		for name, types := range fieldTypes {
			var typeList []string
			for t := range types {
				typeList = append(typeList, t)
			}
			fields = append(fields, FieldInfo{
				Name:  name,
				Types: typeList,
				Count: fieldCount[name],
			})
		}

		schemas = append(schemas, CollectionSchema{
			Name:     collName,
			DocCount: len(res.Docs),
			Fields:   fields,
		})
	}

	return schemas
}

// collectFields parcourt récursivement un document pour extraire les champs et leurs types.
func collectFields(doc *storage.Document, prefix string, fieldTypes map[string]map[string]bool, fieldCount map[string]int) {
	for _, f := range doc.Fields {
		fullName := f.Name
		if prefix != "" {
			fullName = prefix + "." + f.Name
		}

		typeName := fieldTypeName(f.Type)

		if f.Type == storage.FieldDocument {
			// Récurser dans les sous-documents
			if sub, ok := f.Value.(*storage.Document); ok {
				collectFields(sub, fullName, fieldTypes, fieldCount)
			}
			continue
		}

		if fieldTypes[fullName] == nil {
			fieldTypes[fullName] = make(map[string]bool)
		}
		fieldTypes[fullName][typeName] = true
		fieldCount[fullName]++
	}
}

func fieldTypeName(ft storage.FieldType) string {
	switch ft {
	case storage.FieldNull:
		return "null"
	case storage.FieldString:
		return "string"
	case storage.FieldInt64:
		return "int64"
	case storage.FieldFloat64:
		return "float64"
	case storage.FieldBool:
		return "bool"
	case storage.FieldDocument:
		return "document"
	default:
		return "unknown"
	}
}

// Vacuum compacte toutes les collections en supprimant les records marqués comme supprimés.
// Retourne le nombre total de records récupérés.
func (db *DB) Vacuum() (int, error) {
	total := 0
	for _, collName := range db.pager.ListCollections() {
		n, err := db.pager.VacuumCollection(collName)
		if err != nil {
			return total, fmt.Errorf("vacuum %s: %w", collName, err)
		}
		total += n
	}
	if err := db.pager.CommitWAL(); err != nil {
		return total, err
	}
	return total, nil
}

// Dump exporte toute la base de données sous forme de commandes SQL reproductibles.
// Inclut : CREATE INDEX, CREATE VIEW, INSERT INTO pour chaque collection.
func (db *DB) Dump() string {
	var sb strings.Builder

	// Index definitions
	for _, def := range db.pager.IndexDefs() {
		sb.WriteString(fmt.Sprintf("CREATE INDEX %s ON %s (%s);\n", def.Name, def.Collection, def.Field))
	}

	// Views
	for _, name := range db.pager.ListViews() {
		query, ok := db.pager.GetView(name)
		if ok {
			sb.WriteString(fmt.Sprintf("CREATE VIEW %s AS %s;\n", name, query))
		}
	}

	// Collections data
	for _, collName := range db.pager.ListCollections() {
		res, err := db.Exec("SELECT * FROM " + collName)
		if err != nil || len(res.Docs) == 0 {
			continue
		}
		for _, rd := range res.Docs {
			sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES (", collName))
			for i, f := range rd.Doc.Fields {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(f.Name)
				sb.WriteString("=")
				sb.WriteString(dumpValue(f.Value))
			}
			sb.WriteString(");\n")
		}
	}

	return sb.String()
}

// dumpValue sérialise une valeur en format SQL NovusDB.
func dumpValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("%q", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case *storage.Document:
		var sb strings.Builder
		sb.WriteByte('{')
		for i, f := range val.Fields {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(f.Name)
			sb.WriteByte('=')
			sb.WriteString(dumpValue(f.Value))
		}
		sb.WriteByte('}')
		return sb.String()
	case []interface{}:
		var sb strings.Builder
		sb.WriteByte('[')
		for i, elem := range val {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(dumpValue(elem))
		}
		sb.WriteByte(']')
		return sb.String()
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// InsertJSON insère un document JSON brut dans une collection.
// Accepte un objet JSON : {"name": "Alice", "age": 30, "tags": ["admin", "user"]}
func (db *DB) InsertJSON(collection string, jsonStr string) (uint64, error) {
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return 0, fmt.Errorf("NovusDB: invalid JSON: %w", err)
	}
	doc := storage.NewDocument()
	jsonMapToDoc(raw, doc)
	return db.InsertDoc(collection, doc)
}

// jsonMapToDoc convertit une map JSON en Document récursivement.
func jsonMapToDoc(m map[string]interface{}, doc *storage.Document) {
	for k, v := range m {
		switch val := v.(type) {
		case map[string]interface{}:
			sub := storage.NewDocument()
			jsonMapToDoc(val, sub)
			doc.Set(k, sub)
		case []interface{}:
			// Convertir les sous-objets récursivement
			arr := make([]interface{}, len(val))
			for i, elem := range val {
				if obj, ok := elem.(map[string]interface{}); ok {
					sub := storage.NewDocument()
					jsonMapToDoc(obj, sub)
					arr[i] = sub
				} else {
					arr[i] = normalizeJSONValue(elem)
				}
			}
			doc.Set(k, arr)
		default:
			doc.Set(k, normalizeJSONValue(v))
		}
	}
}

// normalizeJSONValue convertit les types JSON Go (float64 pour les nombres) en types NovusDB.
func normalizeJSONValue(v interface{}) interface{} {
	switch val := v.(type) {
	case float64:
		// JSON parse tous les nombres comme float64 ; convertir en int64 si entier
		if val == float64(int64(val)) {
			return int64(val)
		}
		return val
	default:
		return v
	}
}

// Views retourne la liste des noms de vues.
func (db *DB) Views() []string {
	return db.pager.ListViews()
}

// Sequences retourne la map des séquences définies.
func (db *DB) Sequences() map[string]*engine.Sequence {
	return db.executor.GetSequences()
}

// SetLockPolicy définit la politique de verrouillage (Wait ou Fail).
func (db *DB) SetLockPolicy(policy concurrency.LockPolicy) {
	db.lockMgr = concurrency.NewLockManager(policy)
}
