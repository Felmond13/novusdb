package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Felmond13/novusdb/concurrency"
	"github.com/Felmond13/novusdb/index"
	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// Result représente le résultat d'une requête.
type Result struct {
	Docs         []*ResultDoc // documents retournés (SELECT)
	RowsAffected int64        // nombre de lignes affectées (INSERT/UPDATE/DELETE)
	LastInsertID uint64       // dernier record_id inséré
}

// ResultDoc est un document avec son record_id.
type ResultDoc struct {
	RecordID uint64
	Doc      *storage.Document
}

// Sequence représente une séquence Oracle-style (compteur auto-incrémenté).
type Sequence struct {
	Name        string
	CurrentVal  float64
	IncrementBy float64
	MinValue    float64
	MaxValue    float64
	Cycle       bool
	Started     bool // false tant que NEXTVAL n'a pas été appelé
}

// Executor orchestre l'exécution des requêtes sur le stockage.
type Executor struct {
	pager       *storage.Pager
	lockMgr     *concurrency.LockManager
	indexMgr    *index.Manager
	seqs        map[string]*Sequence
	statsCache  map[string]*TableStats   // stats ANALYZE en cache
	constraints map[string][]*Constraint // contraintes PK/FK/UNIQUE par table
}

// NewExecutor crée un nouvel exécuteur.
func NewExecutor(pager *storage.Pager, lockMgr *concurrency.LockManager, indexMgr *index.Manager) *Executor {
	return &Executor{
		pager:    pager,
		lockMgr:  lockMgr,
		indexMgr: indexMgr,
		seqs:     make(map[string]*Sequence),
	}
}

// GetSequences retourne la map des séquences (pour les dot-commands).
func (ex *Executor) GetSequences() map[string]*Sequence {
	return ex.seqs
}

// Execute exécute un Statement parsé et retourne un Result.
func (ex *Executor) Execute(stmt parser.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *parser.SelectStatement:
		return ex.execSelect(s)
	case *parser.InsertStatement:
		return ex.execInsert(s)
	case *parser.UpdateStatement:
		return ex.execUpdate(s)
	case *parser.DeleteStatement:
		return ex.execDelete(s)
	case *parser.CreateIndexStatement:
		return ex.execCreateIndex(s)
	case *parser.DropIndexStatement:
		return ex.execDropIndex(s)
	case *parser.DropTableStatement:
		return ex.execDropTable(s)
	case *parser.ExplainStatement:
		return ex.execExplain(s)
	case *parser.TruncateTableStatement:
		return ex.execTruncate(s)
	case *parser.UnionStatement:
		return ex.execUnion(s)
	case *parser.CreateViewStatement:
		return ex.execCreateView(s)
	case *parser.DropViewStatement:
		return ex.execDropView(s)
	case *parser.CreateSequenceStatement:
		return ex.execCreateSequence(s)
	case *parser.DropSequenceStatement:
		return ex.execDropSequence(s)
	case *parser.AnalyzeStatement:
		return ex.execAnalyze(s)
	case *parser.AlterTableStatement:
		return ex.execAlterTable(s)
	default:
		return nil, fmt.Errorf("executor: unsupported statement type %T", stmt)
	}
}

// ---------- SELECT ----------

func (ex *Executor) execSelect(stmt *parser.SelectStatement) (*Result, error) {
	// Résoudre les vues : si FROM est une vue, exécuter la requête sous-jacente
	if viewResult, ok := ex.resolveView(stmt.From); ok {
		return ex.applyViewProjection(viewResult, stmt)
	}

	var docs []*ResultDoc
	var err error

	outerAlias := stmt.FromAlias

	// Matérialiser les sous-requêtes non corrélées dans le WHERE
	if stmt.Where != nil {
		stmt.Where, err = ex.materializeSubqueries(stmt.Where, outerAlias)
		if err != nil {
			return nil, err
		}
	}

	// Matérialiser les sous-requêtes non corrélées dans les colonnes SELECT
	for i, col := range stmt.Columns {
		stmt.Columns[i], err = ex.materializeSubqueries(col, outerAlias)
		if err != nil {
			return nil, err
		}
	}

	// Matérialiser les sous-requêtes dans HAVING
	if stmt.Having != nil {
		stmt.Having, err = ex.materializeSubqueries(stmt.Having, outerAlias)
		if err != nil {
			return nil, err
		}
	}

	// Strip FROM alias pour les requêtes non-JOIN (A.prenom → prenom)
	if len(stmt.Joins) == 0 && outerAlias != "" {
		if stmt.Where != nil {
			stmt.Where = stripTableAlias(stmt.Where, outerAlias)
		}
		for i, col := range stmt.Columns {
			stmt.Columns[i] = stripTableAlias(col, outerAlias)
		}
		if stmt.Having != nil {
			stmt.Having = stripTableAlias(stmt.Having, outerAlias)
		}
		for i, gb := range stmt.GroupBy {
			stmt.GroupBy[i] = stripTableAlias(gb, outerAlias)
		}
		for _, ob := range stmt.OrderBy {
			ob.Expr = stripTableAlias(ob.Expr, outerAlias)
		}
	}

	// Appliquer le hint NO_CACHE : vider le cache avant le scan
	if hasHint(stmt.Hints, parser.HintNoCache) {
		ex.pager.ClearCache()
	}

	if len(stmt.Joins) > 0 {
		// JOIN path
		docs, err = ex.execJoin(stmt)
	} else if containsSubqueryExpr(stmt.Where) {
		// Correlated subquery in WHERE — scan all, filter per-row
		allDocs, scanErr := ex.scanCollection(stmt.From, nil)
		if scanErr != nil {
			return nil, scanErr
		}
		for _, rd := range allDocs {
			rowWhere, matErr := ex.materializeForRow(stmt.Where, outerAlias, rd.Doc)
			if matErr != nil {
				return nil, matErr
			}
			match, evalErr := EvalExpr(rowWhere, rd.Doc)
			if evalErr != nil {
				return nil, evalErr
			}
			if match {
				docs = append(docs, rd)
			}
		}
	} else if hasHint(stmt.Hints, parser.HintParallel) {
		// PARALLEL hint — scan parallèle
		degree := parallelDegree(stmt.Hints)
		docs, err = ex.parallelScan(stmt.From, stmt.Where, degree)
	} else {
		// Simple scan path
		forceFullScan := hasHint(stmt.Hints, parser.HintFullScan)
		var candidateLocs []index.RecordLoc
		hasAgg := hasAggregateColumns(stmt.Columns) || len(stmt.GroupBy) > 0

		// Calculer earlyLimit : si pas de ORDER BY, GROUP BY ou aggrégat, on peut arrêter tôt
		earlyLimit := -1
		if stmt.Limit >= 0 && len(stmt.OrderBy) == 0 && !hasAgg {
			earlyLimit = stmt.Limit + stmt.Offset
		}

		if !forceFullScan {
			forceField := getHintParam(stmt.Hints, parser.HintForceIndex)
			if forceField != "" {
				candidateLocs = ex.resolveForceIndex(stmt.From, forceField, stmt.Where)
			} else if !hasAgg {
				candidateLocs = ex.resolveIndexLookup(stmt.From, stmt.Where, earlyLimit)
				// CBO : vérifier si l'index est rentable vs full scan
				if candidateLocs != nil && !ex.shouldUseIndex(stmt.From, stmt.Where, candidateLocs) {
					candidateLocs = nil // full scan plus efficace
				}
			}
		}
		if candidateLocs != nil {
			docs, err = ex.readByLocs(stmt.From, candidateLocs, stmt.Where, earlyLimit)
		} else {
			docs, err = ex.scanCollection(stmt.From, stmt.Where)
		}
	}
	if err != nil {
		return nil, err
	}

	// GROUP BY ou agrégat standalone (COUNT(*) sans GROUP BY)
	if len(stmt.GroupBy) > 0 {
		docs, err = ex.applyGroupBy(docs, stmt)
		if err != nil {
			return nil, err
		}
	} else if hasAggregateColumns(stmt.Columns) {
		docs, err = ex.applyStandaloneAggregate(docs, stmt)
		if err != nil {
			return nil, err
		}
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		ex.applyOrderBy(docs, stmt.OrderBy)
	}

	// OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(docs) {
		docs = docs[stmt.Offset:]
	} else if stmt.Offset >= len(docs) {
		docs = nil
	}

	// LIMIT
	if stmt.Limit >= 0 && stmt.Limit < len(docs) {
		docs = docs[:stmt.Limit]
	}

	// Projection des colonnes (avec support correlated subqueries per-row)
	if !isSelectAll(stmt.Columns) {
		docs, err = ex.projectColumns(docs, stmt.Columns, outerAlias)
		if err != nil {
			return nil, err
		}
	}

	// DISTINCT : dédupliquer les documents
	if stmt.Distinct {
		docs = deduplicateDocs(docs)
	}

	return &Result{Docs: docs}, nil
}

// ---------- JOIN ----------

// joinStrategy identifie la stratégie de jointure utilisée.
type joinStrategy int

const (
	strategyNestedLoop  joinStrategy = iota // O(n×m) — fallback
	strategyHashJoin                        // O(n+m) — equi-join sans index
	strategyIndexLookup                     // O(n × log m) — equi-join avec index B+ Tree
)

func (s joinStrategy) String() string {
	switch s {
	case strategyHashJoin:
		return "HASH JOIN"
	case strategyIndexLookup:
		return "INDEX LOOKUP JOIN"
	default:
		return "NESTED LOOP"
	}
}

// extractEquiJoinKeys extrait les champs gauche et droit d'une condition d'equi-join.
// Retourne ("", "", false) si la condition n'est pas un simple A.x = B.y.
func extractEquiJoinKeys(cond parser.Expr) (leftField, rightField string, ok bool) {
	be, isBinary := cond.(*parser.BinaryExpr)
	if !isBinary || be.Op != parser.TokenEQ {
		return "", "", false
	}
	leftField = ExprToFieldName(be.Left)
	rightField = ExprToFieldName(be.Right)
	if leftField == "" || rightField == "" {
		return "", "", false
	}
	return leftField, rightField, true
}

// chooseJoinStrategy choisit la meilleure stratégie de jointure.
// Les hints HASH_JOIN et NESTED_LOOP permettent de forcer la stratégie.
func (ex *Executor) chooseJoinStrategy(
	rightTable string,
	cond parser.Expr,
	leftName, rightName string,
	hints []parser.QueryHint,
) (joinStrategy, string, string) {
	leftField, rightField, isEqui := extractEquiJoinKeys(cond)
	if !isEqui {
		return strategyNestedLoop, "", ""
	}

	lf, rf := normalizeJoinFields(leftField, rightField, leftName, rightName)

	// Hints de stratégie de jointure : forcer si présent
	if hasHint(hints, parser.HintNestedLoop) {
		return strategyNestedLoop, lf, rf
	}
	if hasHint(hints, parser.HintHashJoin) {
		return strategyHashJoin, lf, rf
	}

	// Essayer Index Lookup Join : chercher un index sur le champ de la table droite
	rightFieldBare := stripPrefix(rf, rightName)
	idx := ex.indexMgr.GetIndex(rightTable, rightFieldBare)
	if idx != nil {
		return strategyIndexLookup, lf, rf
	}

	// Sinon Hash Join pour toute equi-join
	return strategyHashJoin, lf, rf
}

// normalizeJoinFields s'assure que lf correspond au côté gauche et rf au côté droit.
func normalizeJoinFields(leftField, rightField, leftName, rightName string) (string, string) {
	// Si rightField commence par le préfixe de la table gauche, inverser
	if rightName != "" && strings.HasPrefix(leftField, rightName+".") {
		return rightField, leftField
	}
	if leftName != "" && strings.HasPrefix(rightField, leftName+".") {
		return rightField, leftField
	}
	return leftField, rightField
}

// stripPrefix supprime le préfixe "alias." d'un nom de champ.
func stripPrefix(field, prefix string) string {
	if prefix != "" && strings.HasPrefix(field, prefix+".") {
		return field[len(prefix)+1:]
	}
	return field
}

// execJoin exécute un join entre la table FROM et les tables JOINées.
// Choisit automatiquement la stratégie optimale :
//   - INDEX LOOKUP JOIN : O(n × log m) si un index B+ Tree existe sur le champ de jointure
//   - HASH JOIN : O(n+m) pour les equi-joins sans index
//   - NESTED LOOP : O(n×m) fallback pour les conditions non-equi
func (ex *Executor) execJoin(stmt *parser.SelectStatement) ([]*ResultDoc, error) {
	// Scanner la table principale (FROM)
	leftDocs, err := ex.scanCollection(stmt.From, nil) // pas de WHERE ici, appliqué après merge
	if err != nil {
		return nil, err
	}

	leftName := stmt.From
	if stmt.FromAlias != "" {
		leftName = stmt.FromAlias
	}

	// Appliquer chaque JOIN séquentiellement
	currentDocs := leftDocs
	currentName := leftName

	for _, join := range stmt.Joins {
		rightName := join.Table
		if join.Alias != "" {
			rightName = join.Alias
		}

		isFirstJoin := (currentName == leftName && len(stmt.Joins) > 0)
		isLeftJoin := join.Type == "LEFT"
		isRightJoin := join.Type == "RIGHT"

		// RIGHT JOIN = LEFT JOIN avec les tables inversées
		effectiveLeftDocs := currentDocs
		effectiveLeftName := currentName
		effectiveRightName := rightName
		effectiveIsFirst := isFirstJoin
		outerJoin := isLeftJoin || isRightJoin

		if isRightJoin {
			// Scanner la table droite qui devient la table "gauche"
			swappedLeft, scanErr := ex.scanCollection(join.Table, nil)
			if scanErr != nil {
				return nil, scanErr
			}
			effectiveLeftDocs = swappedLeft
			effectiveLeftName = rightName
			effectiveRightName = currentName
			effectiveIsFirst = true // les docs gauche (ex-droite) sont des docs simples
		}

		// Choisir la stratégie (CBO : coût basé sur les stats)
		strategy, leftField, rightField := ex.chooseJoinStrategyCBO(
			stmt.From, join.Table, join.Condition,
			effectiveLeftName, effectiveRightName,
			int64(len(effectiveLeftDocs)),
			stmt.Hints,
		)

		var joinedDocs []*ResultDoc

		switch strategy {
		case strategyIndexLookup:
			if isRightJoin {
				// Pour RIGHT JOIN avec index lookup, utiliser la table gauche originale
				joinedDocs, err = ex.indexLookupJoin(
					effectiveLeftDocs, stmt.From,
					effectiveLeftName, effectiveRightName,
					leftField, rightField,
					join.Condition,
					effectiveIsFirst, outerJoin,
				)
			} else {
				joinedDocs, err = ex.indexLookupJoin(
					effectiveLeftDocs, join.Table,
					effectiveLeftName, effectiveRightName,
					leftField, rightField,
					join.Condition,
					effectiveIsFirst, outerJoin,
				)
			}

		case strategyHashJoin:
			var rightDocs []*ResultDoc
			if isRightJoin {
				rightDocs = currentDocs // la table gauche originale devient la droite
			} else {
				rightDocs, err = ex.scanCollection(join.Table, nil)
				if err != nil {
					return nil, err
				}
			}
			joinedDocs, err = ex.hashJoin(
				effectiveLeftDocs, rightDocs,
				effectiveLeftName, effectiveRightName,
				leftField, rightField,
				join.Condition,
				effectiveIsFirst, outerJoin,
			)

		default: // strategyNestedLoop
			var rightDocs []*ResultDoc
			if isRightJoin {
				rightDocs = currentDocs
			} else {
				rightDocs, err = ex.scanCollection(join.Table, nil)
				if err != nil {
					return nil, err
				}
			}
			joinedDocs, err = ex.nestedLoopJoin(
				effectiveLeftDocs, rightDocs,
				effectiveLeftName, effectiveRightName,
				join.Condition,
				effectiveIsFirst, outerJoin,
			)
		}

		if err != nil {
			return nil, err
		}

		currentDocs = joinedDocs
		currentName = "" // après le premier join, les docs sont déjà mergés
	}

	// Appliquer le WHERE global sur les documents mergés
	if stmt.Where != nil {
		var filtered []*ResultDoc
		for _, rd := range currentDocs {
			match, err := EvalExpr(stmt.Where, rd.Doc)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, rd)
			}
		}
		currentDocs = filtered
	}

	return currentDocs, nil
}

// JoinStrategy retourne la stratégie de jointure qui serait choisie pour un statement.
// Utilisé par EXPLAIN.
func (ex *Executor) JoinStrategy(stmt *parser.SelectStatement) []string {
	var strategies []string
	leftName := stmt.From
	if stmt.FromAlias != "" {
		leftName = stmt.FromAlias
	}
	leftStats := ex.collectStats(stmt.From)
	leftRows := leftStats.RowCount
	for _, join := range stmt.Joins {
		rightName := join.Table
		if join.Alias != "" {
			rightName = join.Alias
		}
		strategy, _, _ := ex.chooseJoinStrategyCBO(
			stmt.From, join.Table, join.Condition,
			leftName, rightName, leftRows, stmt.Hints,
		)
		strategies = append(strategies, strategy.String())
		leftName = ""
	}
	return strategies
}

// nestedLoopJoin effectue un nested loop join entre left et right.
// Si isFirstJoin, les docs left sont des documents simples (non encore mergés).
func (ex *Executor) nestedLoopJoin(
	leftDocs, rightDocs []*ResultDoc,
	leftName, rightName string,
	condition parser.Expr,
	isFirstJoin bool,
	leftJoin bool,
) ([]*ResultDoc, error) {
	var results []*ResultDoc

	for _, ld := range leftDocs {
		matched := false

		for _, rd := range rightDocs {
			merged := ex.mergeJoinDocs(ld.Doc, rd.Doc, leftName, rightName, isFirstJoin)

			if condition != nil {
				ok, err := EvalExpr(condition, merged)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}

			matched = true
			results = append(results, &ResultDoc{Doc: merged})
		}

		// LEFT JOIN : garder la ligne gauche même sans correspondance
		if leftJoin && !matched {
			merged := ex.mergeJoinDocs(ld.Doc, nil, leftName, rightName, isFirstJoin)
			results = append(results, &ResultDoc{Doc: merged})
		}
	}

	return results, nil
}

// mergeJoinDocs fusionne deux documents en un seul pour le résultat du JOIN.
// Chaque table est accessible via son nom/alias comme sous-document (ex: jobs.type).
// Les champs sont aussi copiés au niveau racine.
func (ex *Executor) mergeJoinDocs(
	leftDoc *storage.Document,
	rightDoc *storage.Document,
	leftName, rightName string,
	isFirstJoin bool,
) *storage.Document {
	merged := storage.NewDocument()

	if isFirstJoin && leftName != "" {
		// Premier join : copier les champs du doc gauche comme sous-document
		leftSub := cloneDocument(leftDoc)
		merged.Set(leftName, leftSub)
		// Copier aussi au niveau racine
		for _, f := range leftDoc.Fields {
			merged.Set(f.Name, f.Value)
		}
	} else {
		// Joins chaînés : le doc gauche est déjà un doc mergé, copier tel quel
		for _, f := range leftDoc.Fields {
			merged.Set(f.Name, f.Value)
		}
	}

	if rightDoc != nil && rightName != "" {
		// Ajouter les champs du doc droit comme sous-document
		rightSub := cloneDocument(rightDoc)
		merged.Set(rightName, rightSub)
		// Copier aussi au niveau racine (écrase en cas de conflit)
		for _, f := range rightDoc.Fields {
			merged.Set(f.Name, f.Value)
		}
	}

	return merged
}

// resolveFieldValue extrait la valeur d'un champ depuis un document joiné.
// Le champ peut être qualifié ("A.id") ou non ("id").
func resolveFieldValue(doc *storage.Document, field string) (interface{}, bool) {
	// Essayer le chemin direct (qualifié ou non)
	parts := strings.Split(field, ".")
	if len(parts) > 1 {
		val, ok := doc.GetNested(parts)
		if ok {
			return val, true
		}
	}
	// Fallback : champ simple au niveau racine
	return doc.Get(parts[len(parts)-1])
}

// hashJoin effectue un hash join O(n+m) pour les equi-joins.
// Phase 1 (Build) : construire une hash map sur la table droite indexée par la clé de jointure.
// Phase 2 (Probe) : pour chaque doc gauche, chercher dans la hash map.
func (ex *Executor) hashJoin(
	leftDocs, rightDocs []*ResultDoc,
	leftName, rightName string,
	leftField, rightField string,
	_ parser.Expr,
	isFirstJoin bool,
	leftJoin bool,
) ([]*ResultDoc, error) {
	// Champ nu (sans préfixe alias) pour extraction des valeurs
	rightBare := stripPrefix(rightField, rightName)
	leftBare := stripPrefix(leftField, leftName)

	// Phase 1 — Build : indexer la table droite par clé de jointure
	hashTable := make(map[string][]*ResultDoc)
	for _, rd := range rightDocs {
		val, ok := rd.Doc.Get(rightBare)
		if !ok {
			val, ok = rd.Doc.GetNested(strings.Split(rightBare, "."))
		}
		if !ok {
			continue
		}
		key := index.ValueToKey(val)
		hashTable[key] = append(hashTable[key], rd)
	}

	// Phase 2 — Probe : parcourir la table gauche
	var results []*ResultDoc
	for _, ld := range leftDocs {
		// Extraire la valeur de la clé côté gauche
		var val interface{}
		var ok bool
		if isFirstJoin {
			val, ok = ld.Doc.Get(leftBare)
			if !ok {
				val, ok = ld.Doc.GetNested(strings.Split(leftBare, "."))
			}
		} else {
			val, ok = resolveFieldValue(ld.Doc, leftField)
			if !ok {
				val, ok = resolveFieldValue(ld.Doc, leftBare)
			}
		}

		matched := false
		if ok {
			key := index.ValueToKey(val)
			if bucket, found := hashTable[key]; found {
				for _, rd := range bucket {
					merged := ex.mergeJoinDocs(ld.Doc, rd.Doc, leftName, rightName, isFirstJoin)
					results = append(results, &ResultDoc{Doc: merged})
					matched = true
				}
			}
		}

		if leftJoin && !matched {
			merged := ex.mergeJoinDocs(ld.Doc, nil, leftName, rightName, isFirstJoin)
			results = append(results, &ResultDoc{Doc: merged})
		}
	}

	return results, nil
}

// indexLookupJoin effectue un index lookup join O(n × log m).
// Pour chaque doc de la table gauche, on fait un B+ Tree lookup sur la table droite.
// Pas besoin de charger toute la table droite en mémoire.
func (ex *Executor) indexLookupJoin(
	leftDocs []*ResultDoc,
	rightTable string,
	leftName, rightName string,
	leftField, rightField string,
	_ parser.Expr,
	isFirstJoin bool,
	leftJoin bool,
) ([]*ResultDoc, error) {
	rightBare := stripPrefix(rightField, rightName)
	leftBare := stripPrefix(leftField, leftName)

	// Récupérer l'index B+ Tree sur la table droite
	idx := ex.indexMgr.GetIndex(rightTable, rightBare)
	if idx == nil {
		return nil, fmt.Errorf("index lookup join: no index on %s.%s", rightTable, rightBare)
	}

	var results []*ResultDoc

	for _, ld := range leftDocs {
		// Extraire la valeur de la clé côté gauche
		var val interface{}
		var ok bool
		if isFirstJoin {
			val, ok = ld.Doc.Get(leftBare)
			if !ok {
				val, ok = ld.Doc.GetNested(strings.Split(leftBare, "."))
			}
		} else {
			val, ok = resolveFieldValue(ld.Doc, leftField)
			if !ok {
				val, ok = resolveFieldValue(ld.Doc, leftBare)
			}
		}

		matched := false
		if ok {
			key := index.ValueToKey(val)
			locs, err := idx.Lookup(key)
			if err != nil {
				return nil, err
			}

			if len(locs) > 0 {
				// Charger les documents droits par leurs localisations
				rightDocs, err := ex.readByLocs(rightTable, locs, nil, -1)
				if err != nil {
					return nil, err
				}
				for _, rd := range rightDocs {
					merged := ex.mergeJoinDocs(ld.Doc, rd.Doc, leftName, rightName, isFirstJoin)
					results = append(results, &ResultDoc{Doc: merged})
					matched = true
				}
			}
		}

		if leftJoin && !matched {
			merged := ex.mergeJoinDocs(ld.Doc, nil, leftName, rightName, isFirstJoin)
			results = append(results, &ResultDoc{Doc: merged})
		}
	}

	return results, nil
}

// ---------- INSERT ----------

func (ex *Executor) execInsert(stmt *parser.InsertStatement) (*Result, error) {
	// INSERT INTO ... SELECT ...
	if stmt.Source != nil {
		return ex.execInsertFromSelect(stmt)
	}

	// INSERT OR REPLACE (single row only)
	if stmt.OrReplace && len(stmt.Fields) > 0 {
		doc := ex.buildDocFromFields(stmt.Fields)
		return ex.execInsertOrReplace(stmt, doc)
	}

	// Batch INSERT : itérer sur tous les groupes VALUES
	rows := stmt.Rows
	if len(rows) == 0 {
		rows = [][]parser.FieldAssignment{stmt.Fields}
	}

	coll, err := ex.pager.GetOrCreateCollection(stmt.Table)
	if err != nil {
		return nil, err
	}

	var lastID uint64
	for _, fields := range rows {
		// Résoudre les séquences (NEXTVAL/CURRVAL) avant de construire le document
		if err := ex.resolveSequencesInFields(fields); err != nil {
			return nil, fmt.Errorf("insert: %w", err)
		}
		doc := ex.buildDocFromFields(fields)

		recordID, err := ex.pager.NextRecordID(stmt.Table)
		if err != nil {
			return nil, err
		}

		// Auto-ID : ajouter _id si absent
		ensureAutoID(doc, recordID)

		// Vérifier les contraintes PK/UNIQUE/FK
		if err := ex.checkInsertConstraints(stmt.Table, doc); err != nil {
			return nil, err
		}

		encoded, err := doc.Encode()
		if err != nil {
			return nil, err
		}

		insPageID, insSlotOff, insErr := ex.pager.InsertRecordAtomic(coll, recordID, encoded)
		if insErr != nil {
			return nil, insErr
		}

		ex.updateIndexesAfterInsert(stmt.Table, recordID, doc, insPageID, insSlotOff)
		lastID = recordID
	}

	if err := ex.pager.FlushMeta(); err != nil {
		return nil, err
	}

	if err := ex.pager.CommitWAL(); err != nil {
		return nil, err
	}

	return &Result{RowsAffected: int64(len(rows)), LastInsertID: lastID}, nil
}

// buildDocFromFields construit un Document à partir d'une liste de FieldAssignment.
func (ex *Executor) buildDocFromFields(fields []parser.FieldAssignment) *storage.Document {
	doc := storage.NewDocument()
	for _, fa := range fields {
		path := ExprToFieldPath(fa.Field)
		value := fieldAssignmentValue(fa.Value)
		if len(path) == 1 {
			doc.Set(path[0], value)
		} else {
			doc.SetNested(path, value)
		}
	}
	return doc
}

// fieldAssignmentValue extrait la valeur Go d'une expression de champ.
// Gère les littéraux simples et les sous-documents imbriqués {key=val, ...}.
func fieldAssignmentValue(expr parser.Expr) interface{} {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return literalToValue(e.Token)
	case *parser.DocumentLiteralExpr:
		sub := storage.NewDocument()
		for _, fa := range e.Fields {
			path := ExprToFieldPath(fa.Field)
			val := fieldAssignmentValue(fa.Value)
			if len(path) == 1 {
				sub.Set(path[0], val)
			} else {
				sub.SetNested(path, val)
			}
		}
		return sub
	case *parser.ArrayLiteralExpr:
		arr := make([]interface{}, len(e.Elements))
		for i, elem := range e.Elements {
			arr[i] = fieldAssignmentValue(elem)
		}
		return arr
	case *parser.SysdateExpr:
		now := time.Now()
		switch e.Variant {
		case "CURRENT_DATE":
			return now.Format("2006-01-02")
		case "CURRENT_TIMESTAMP":
			return now.Format(time.RFC3339Nano)
		default:
			return now.Format("2006-01-02 15:04:05")
		}
	default:
		return nil
	}
}

// execInsertOrReplace implémente INSERT OR REPLACE.
// Cherche un doc existant dont le premier champ correspond, et le met à jour.
// Sinon, insère normalement.
func (ex *Executor) execInsertOrReplace(stmt *parser.InsertStatement, doc *storage.Document) (*Result, error) {
	// Le champ clé est le premier champ de la liste
	keyPath := ExprToFieldPath(stmt.Fields[0].Field)
	keyValue := literalToValue(stmt.Fields[0].Value.(*parser.LiteralExpr).Token)

	// Construire un WHERE pour trouver le doc existant
	var whereExpr parser.Expr
	if len(keyPath) == 1 {
		whereExpr = &parser.BinaryExpr{
			Left:  &parser.IdentExpr{Name: keyPath[0]},
			Op:    parser.TokenEQ,
			Right: &parser.LiteralExpr{Token: stmt.Fields[0].Value.(*parser.LiteralExpr).Token},
		}
	} else {
		whereExpr = &parser.BinaryExpr{
			Left:  &parser.DotExpr{Parts: keyPath},
			Op:    parser.TokenEQ,
			Right: &parser.LiteralExpr{Token: stmt.Fields[0].Value.(*parser.LiteralExpr).Token},
		}
	}

	existing, err := ex.scanCollectionRaw(stmt.Table, whereExpr)
	if err != nil {
		return nil, err
	}

	if len(existing) > 0 {
		// Mettre à jour le premier doc trouvé
		rec := existing[0]
		oldDoc := rec.doc

		// Appliquer tous les champs du nouveau doc
		for _, fa := range stmt.Fields {
			path := ExprToFieldPath(fa.Field)
			value := literalToValue(fa.Value.(*parser.LiteralExpr).Token)
			if len(path) == 1 {
				oldDoc.Set(path[0], value)
			} else {
				oldDoc.SetNested(path, value)
			}
		}

		encoded, err := oldDoc.Encode()
		if err != nil {
			return nil, err
		}

		coll := ex.pager.GetCollection(stmt.Table)
		newPID, newSOff, updErr := ex.pager.UpdateRecordAtomic(coll, rec.pageID, rec.slotOffset, rec.recordID, encoded)
		if updErr != nil {
			return nil, updErr
		}

		// Mettre à jour les index
		ex.updateIndexesAfterUpdate(stmt.Table, rec.recordID, rec.doc, oldDoc, newPID, newSOff)

		if err := ex.pager.CommitWAL(); err != nil {
			return nil, err
		}

		return &Result{RowsAffected: 1, LastInsertID: rec.recordID}, nil
	}

	// Pas de doc existant → insert normal
	_ = keyValue // utilisé via whereExpr
	coll, err := ex.pager.GetOrCreateCollection(stmt.Table)
	if err != nil {
		return nil, err
	}

	recordID, err := ex.pager.NextRecordID(stmt.Table)
	if err != nil {
		return nil, err
	}

	encoded, err := doc.Encode()
	if err != nil {
		return nil, err
	}

	insPageID, insSlotOff, insErr := ex.pager.InsertRecordAtomic(coll, recordID, encoded)
	if insErr != nil {
		return nil, insErr
	}

	ex.updateIndexesAfterInsert(stmt.Table, recordID, doc, insPageID, insSlotOff)

	if err := ex.pager.FlushMeta(); err != nil {
		return nil, err
	}

	if err := ex.pager.CommitWAL(); err != nil {
		return nil, err
	}

	return &Result{RowsAffected: 1, LastInsertID: recordID}, nil
}

// execInsertFromSelect exécute un INSERT INTO ... SELECT ...
func (ex *Executor) execInsertFromSelect(stmt *parser.InsertStatement) (*Result, error) {
	// Exécuter le SELECT source
	selectResult, err := ex.execSelect(stmt.Source)
	if err != nil {
		return nil, fmt.Errorf("insert-select: %w", err)
	}

	if len(selectResult.Docs) == 0 {
		return &Result{RowsAffected: 0}, nil
	}

	coll, err := ex.pager.GetOrCreateCollection(stmt.Table)
	if err != nil {
		return nil, err
	}

	var affected int64
	var lastID uint64

	for _, rd := range selectResult.Docs {
		recordID, err := ex.pager.NextRecordID(stmt.Table)
		if err != nil {
			return nil, err
		}

		encoded, err := rd.Doc.Encode()
		if err != nil {
			return nil, err
		}

		insPageID, insSlotOff, insErr := ex.pager.InsertRecordAtomic(coll, recordID, encoded)
		if insErr != nil {
			return nil, insErr
		}

		ex.updateIndexesAfterInsert(stmt.Table, recordID, rd.Doc, insPageID, insSlotOff)
		lastID = recordID
		affected++
	}

	// Flush meta une seule fois
	if err := ex.pager.FlushMeta(); err != nil {
		return nil, err
	}

	// WAL commit : garantir la durabilité
	if err := ex.pager.CommitWAL(); err != nil {
		return nil, err
	}

	return &Result{RowsAffected: affected, LastInsertID: lastID}, nil
}

// ---------- UPDATE ----------

func (ex *Executor) execUpdate(stmt *parser.UpdateStatement) (*Result, error) {
	// Matérialiser les sous-requêtes dans le WHERE
	if stmt.Where != nil {
		var err error
		stmt.Where, err = ex.materializeSubqueries(stmt.Where, "")
		if err != nil {
			return nil, err
		}
	}
	// Scanner pour trouver les documents correspondants
	candidateLocs := ex.resolveIndexLookup(stmt.Table, stmt.Where, -1)

	var targets []*scanResult
	var err error

	if candidateLocs != nil {
		targets, err = ex.readByLocsRaw(candidateLocs, stmt.Where, -1)
	} else {
		targets, err = ex.scanCollectionRaw(stmt.Table, stmt.Where)
	}
	if err != nil {
		return nil, err
	}

	// Résoudre les séquences dans les assignments
	for i, fa := range stmt.Assignments {
		resolved, err := ex.resolveSequenceExpr(fa.Value)
		if err != nil {
			return nil, fmt.Errorf("update: %w", err)
		}
		stmt.Assignments[i].Value = resolved
	}

	var affected int64
	for _, t := range targets {
		// Acquérir le lock sur le record
		if err := ex.lockMgr.AcquireRecord(stmt.Table, t.recordID); err != nil {
			return nil, fmt.Errorf("update: %w", err)
		}

		// Appliquer les modifications
		oldDoc := t.doc
		newDoc := cloneDocument(oldDoc)
		for _, fa := range stmt.Assignments {
			path := ExprToFieldPath(fa.Field)
			// Évaluer l'expression de la valeur contre le document courant
			value, evalErr := evalValue(fa.Value, newDoc)
			if evalErr != nil {
				ex.lockMgr.ReleaseRecord(stmt.Table, t.recordID)
				return nil, fmt.Errorf("update eval: %w", evalErr)
			}
			if len(path) == 1 {
				newDoc.Set(path[0], value)
			} else {
				newDoc.SetNested(path, value)
			}
		}

		// Encoder le nouveau document
		newEncoded, err := newDoc.Encode()
		if err != nil {
			ex.lockMgr.ReleaseRecord(stmt.Table, t.recordID)
			return nil, err
		}

		// Mettre à jour de manière atomique (read-modify-write sous lock pager)
		coll := ex.pager.GetCollection(stmt.Table)
		newPID, newSOff, updErr := ex.pager.UpdateRecordAtomic(coll, t.pageID, t.slotOffset, t.recordID, newEncoded)
		if updErr != nil {
			ex.lockMgr.ReleaseRecord(stmt.Table, t.recordID)
			return nil, updErr
		}

		// Mettre à jour les index
		ex.updateIndexesAfterUpdate(stmt.Table, t.recordID, oldDoc, newDoc, newPID, newSOff)

		ex.lockMgr.ReleaseRecord(stmt.Table, t.recordID)
		affected++
	}

	// WAL commit : garantir la durabilité
	if affected > 0 {
		if err := ex.pager.CommitWAL(); err != nil {
			return nil, err
		}
	}

	return &Result{RowsAffected: affected}, nil
}

// ---------- DELETE ----------

func (ex *Executor) execDelete(stmt *parser.DeleteStatement) (*Result, error) {
	// Matérialiser les sous-requêtes dans le WHERE
	if stmt.Where != nil {
		var err error
		stmt.Where, err = ex.materializeSubqueries(stmt.Where, "")
		if err != nil {
			return nil, err
		}
	}
	candidateLocs := ex.resolveIndexLookup(stmt.Table, stmt.Where, -1)

	var targets []*scanResult
	var err error

	if candidateLocs != nil {
		targets, err = ex.readByLocsRaw(candidateLocs, stmt.Where, -1)
	} else {
		targets, err = ex.scanCollectionRaw(stmt.Table, stmt.Where)
	}
	if err != nil {
		return nil, err
	}

	var affected int64
	for _, t := range targets {
		if err := ex.lockMgr.AcquireRecord(stmt.Table, t.recordID); err != nil {
			return nil, fmt.Errorf("delete: %w", err)
		}

		// Vérifier les contraintes FK (CASCADE / RESTRICT / SET NULL)
		if err := ex.checkDeleteConstraints(stmt.Table, t.doc); err != nil {
			ex.lockMgr.ReleaseRecord(stmt.Table, t.recordID)
			return nil, err
		}

		if err := ex.pager.MarkDeletedAtomic(t.pageID, t.slotOffset); err != nil {
			ex.lockMgr.ReleaseRecord(stmt.Table, t.recordID)
			return nil, err
		}

		// Supprimer des index
		ex.updateIndexesAfterDelete(stmt.Table, t.recordID, t.doc)

		ex.lockMgr.ReleaseRecord(stmt.Table, t.recordID)
		affected++
	}

	// WAL commit : garantir la durabilité
	if affected > 0 {
		if err := ex.pager.CommitWAL(); err != nil {
			return nil, err
		}
	}

	return &Result{RowsAffected: affected}, nil
}

// ---------- CREATE/DROP INDEX ----------

func (ex *Executor) execCreateIndex(stmt *parser.CreateIndexStatement) (*Result, error) {
	idx, err := ex.indexMgr.CreateIndex(stmt.Table, stmt.Field)
	if err != nil {
		if stmt.IfNotExists {
			return &Result{}, nil
		}
		return nil, err
	}

	// Construire l'index via bulk load : collecter → trier → construire O(N)
	coll := ex.pager.GetCollection(stmt.Table)
	if coll == nil {
		return &Result{}, nil
	}

	fieldPath := strings.Split(stmt.Field, ".")
	pageID := coll.FirstPageID

	var entries []index.BulkEntry
	for pageID != 0 {
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		slots := page.ReadRecords()
		for _, slot := range slots {
			if slot.Deleted {
				continue
			}
			data := slot.Data
			if slot.Overflow {
				totalLen, firstPage := slot.OverflowInfo()
				var err2 error
				data, err2 = ex.pager.ReadOverflowData(totalLen, firstPage)
				if err2 != nil {
					continue
				}
			}
			if slot.Compressed && !slot.Overflow {
				var err2 error
				data, err2 = storage.DecompressRecord(&slot)
				if err2 != nil {
					continue
				}
			}
			doc, err := storage.Decode(data)
			if err != nil {
				continue
			}
			val, ok := doc.GetNested(fieldPath)
			if ok {
				entries = append(entries, index.BulkEntry{
					Key:      index.ValueToKey(val),
					RecordID: slot.RecordID,
					PageID:   pageID,
					SlotOff:  slot.Offset,
				})
			}
		}
		pageID = page.NextPageID()
	}

	// Trier par clé puis par recordID
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key != entries[j].Key {
			return entries[i].Key < entries[j].Key
		}
		return entries[i].RecordID < entries[j].RecordID
	})

	// Bulk load O(N) au lieu de N inserts O(N log N)
	if err := idx.BulkLoad(entries); err != nil {
		return nil, err
	}

	// Persister la définition de l'index avec la page racine du B-Tree
	if err := ex.pager.AddIndexDef(stmt.Name, stmt.Table, stmt.Field, idx.RootPageID()); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

func (ex *Executor) execDropIndex(stmt *parser.DropIndexStatement) (*Result, error) {
	if stmt.Name != "" {
		// DROP INDEX <name> : résoudre le nom vers collection+field
		def := ex.pager.FindIndexDefByName(stmt.Name)
		if def == nil {
			if stmt.IfExists {
				return &Result{}, nil
			}
			return nil, fmt.Errorf("index %q not found", stmt.Name)
		}
		if err := ex.indexMgr.DropIndex(def.Collection, def.Field); err != nil {
			if !stmt.IfExists {
				return nil, err
			}
		}
		if err := ex.pager.RemoveIndexDefByName(stmt.Name); err != nil {
			return nil, err
		}
		return &Result{}, nil
	}

	// DROP INDEX ON table(field) — ancienne syntaxe
	if err := ex.indexMgr.DropIndex(stmt.Table, stmt.Field); err != nil {
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, err
	}
	if err := ex.pager.RemoveIndexDef(stmt.Table, stmt.Field); err != nil {
		return nil, err
	}
	return &Result{}, nil
}

// ---------- EXPLAIN ----------

func (ex *Executor) execExplain(stmt *parser.ExplainStatement) (*Result, error) {
	doc := storage.NewDocument()

	switch s := stmt.Inner.(type) {
	case *parser.SelectStatement:
		doc = ex.buildExplainPlan(s)

	case *parser.InsertStatement:
		doc.Set("type", "INSERT")
		doc.Set("collection", s.Table)
		if s.Source != nil {
			doc.Set("source", "SELECT (INSERT...SELECT)")
		} else {
			doc.Set("source", "VALUES")
		}

	case *parser.UpdateStatement:
		doc.Set("type", "UPDATE")
		doc.Set("collection", s.Table)
		doc.Set("scan", "FULL SCAN")
		if s.Where != nil {
			doc.Set("filter", "WHERE")
		}

	case *parser.DeleteStatement:
		doc.Set("type", "DELETE")
		doc.Set("collection", s.Table)
		doc.Set("scan", "FULL SCAN")
		if s.Where != nil {
			doc.Set("filter", "WHERE")
		}

	default:
		doc.Set("type", fmt.Sprintf("%T", stmt.Inner))
	}

	return &Result{
		Docs: []*ResultDoc{{Doc: doc}},
	}, nil
}

// ---------- TRUNCATE TABLE ----------

func (ex *Executor) execTruncate(stmt *parser.TruncateTableStatement) (*Result, error) {
	// Supprimer les index en mémoire pour la collection
	ex.indexMgr.DropAllForCollection(stmt.Table)

	// Drop + recréer la collection (reset rapide)
	coll := ex.pager.GetCollection(stmt.Table)
	if coll == nil {
		return nil, fmt.Errorf("truncate: collection %q does not exist", stmt.Table)
	}

	if err := ex.pager.DropCollection(stmt.Table); err != nil {
		return nil, err
	}

	// Recréer la collection vide
	if _, err := ex.pager.GetOrCreateCollection(stmt.Table); err != nil {
		return nil, err
	}

	// Recréer les index B-Tree vides (les définitions persistent)
	for _, def := range ex.pager.IndexDefs() {
		if def.Collection == stmt.Table {
			idx, err := ex.indexMgr.CreateIndex(def.Collection, def.Field)
			if err != nil {
				return nil, err
			}
			// Mettre à jour la page racine dans la définition persistée
			if err := ex.pager.AddIndexDef(def.Name, def.Collection, def.Field, idx.RootPageID()); err != nil {
				return nil, err
			}
		}
	}

	if err := ex.pager.FlushMeta(); err != nil {
		return nil, err
	}

	if err := ex.pager.CommitWAL(); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// ---------- DROP TABLE ----------

func (ex *Executor) execDropTable(stmt *parser.DropTableStatement) (*Result, error) {
	// Supprimer tous les index de la collection
	ex.indexMgr.DropAllForCollection(stmt.Table)

	// Supprimer les définitions d'index persistées
	_ = ex.pager.RemoveAllIndexDefsForCollection(stmt.Table)

	// Supprimer la collection du pager
	if err := ex.pager.DropCollection(stmt.Table); err != nil {
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, err
	}

	// WAL commit
	if err := ex.pager.CommitWAL(); err != nil {
		return nil, err
	}

	return &Result{}, nil
}

// ---------- VIEWS ----------

func (ex *Executor) execCreateView(stmt *parser.CreateViewStatement) (*Result, error) {
	if err := ex.pager.AddView(stmt.Name, stmt.Query); err != nil {
		return nil, fmt.Errorf("create view: %w", err)
	}
	if err := ex.pager.CommitWAL(); err != nil {
		return nil, err
	}
	return &Result{}, nil
}

func (ex *Executor) execDropView(stmt *parser.DropViewStatement) (*Result, error) {
	_, exists := ex.pager.GetView(stmt.Name)
	if !exists && !stmt.IfExists {
		return nil, fmt.Errorf("drop view: view %q does not exist", stmt.Name)
	}
	if err := ex.pager.RemoveView(stmt.Name); err != nil {
		return nil, fmt.Errorf("drop view: %w", err)
	}
	if err := ex.pager.CommitWAL(); err != nil {
		return nil, err
	}
	return &Result{}, nil
}

// resolveView vérifie si le FROM est une vue et exécute la requête sous-jacente.
func (ex *Executor) resolveView(tableName string) (*Result, bool) {
	query, ok := ex.pager.GetView(tableName)
	if !ok {
		return nil, false
	}
	p := parser.NewParser(query)
	stmt, err := p.Parse()
	if err != nil {
		return nil, false
	}
	result, err := ex.Execute(stmt)
	if err != nil {
		return nil, false
	}
	return result, true
}

// applyViewProjection applique WHERE, ORDER BY, LIMIT, projection sur les résultats d'une vue.
func (ex *Executor) applyViewProjection(viewResult *Result, stmt *parser.SelectStatement) (*Result, error) {
	docs := viewResult.Docs

	// Filtrer par WHERE
	if stmt.Where != nil {
		var filtered []*ResultDoc
		for _, rd := range docs {
			match, err := EvalExpr(stmt.Where, rd.Doc)
			if err != nil {
				return nil, err
			}
			if match {
				filtered = append(filtered, rd)
			}
		}
		docs = filtered
	}

	// ORDER BY
	if len(stmt.OrderBy) > 0 {
		ex.applyOrderBy(docs, stmt.OrderBy)
	}

	// LIMIT / OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(docs) {
		docs = docs[stmt.Offset:]
	} else if stmt.Offset >= len(docs) {
		docs = nil
	}
	if stmt.Limit >= 0 && stmt.Limit < len(docs) {
		docs = docs[:stmt.Limit]
	}

	// Projection
	if !isSelectStar(stmt.Columns) {
		projected, err := ex.projectColumns(docs, stmt.Columns, "")
		if err != nil {
			return nil, err
		}
		docs = projected
	}

	return &Result{Docs: docs}, nil
}

// isSelectStar vérifie si les colonnes du SELECT sont juste *.
func isSelectStar(cols []parser.Expr) bool {
	if len(cols) == 1 {
		if _, ok := cols[0].(*parser.StarExpr); ok {
			return true
		}
	}
	return false
}

// ---------- UNION ----------

func (ex *Executor) execUnion(stmt *parser.UnionStatement) (*Result, error) {
	leftResult, err := ex.execSelect(stmt.Left)
	if err != nil {
		return nil, err
	}
	rightResult, err := ex.execSelect(stmt.Right)
	if err != nil {
		return nil, err
	}

	combined := append(leftResult.Docs, rightResult.Docs...)

	if stmt.All {
		return &Result{Docs: combined}, nil
	}

	// UNION (sans ALL) : dédupliquer par contenu des champs
	seen := make(map[string]bool)
	var unique []*ResultDoc
	for _, rd := range combined {
		key := docFingerprint(rd.Doc)
		if !seen[key] {
			seen[key] = true
			unique = append(unique, rd)
		}
	}
	return &Result{Docs: unique}, nil
}

// docFingerprint génère une clé unique pour un document basée sur ses champs.
func docFingerprint(doc *storage.Document) string {
	var sb strings.Builder
	for _, f := range doc.Fields {
		sb.WriteString(f.Name)
		sb.WriteByte('=')
		sb.WriteString(fmt.Sprintf("%v", f.Value))
		sb.WriteByte(';')
	}
	return sb.String()
}

// ---------- Helpers internes ----------

// scanResult stocke les résultats bruts d'un scan (pour update/delete).
type scanResult struct {
	recordID   uint64
	doc        *storage.Document
	pageID     uint32
	slotOffset uint16
}

// scanCollection scanne séquentiellement toutes les pages d'une collection.
func (ex *Executor) scanCollection(collName string, where parser.Expr) ([]*ResultDoc, error) {
	raw, err := ex.scanCollectionRaw(collName, where)
	if err != nil {
		return nil, err
	}
	docs := make([]*ResultDoc, len(raw))
	for i, r := range raw {
		docs[i] = &ResultDoc{RecordID: r.recordID, Doc: r.doc}
	}
	return docs, nil
}

func (ex *Executor) scanCollectionRaw(collName string, where parser.Expr) ([]*scanResult, error) {
	coll := ex.pager.GetCollection(collName)
	if coll == nil {
		return nil, nil // collection vide/inexistante
	}

	var results []*scanResult
	pageID := coll.FirstPageID

	for pageID != 0 {
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}

		slots := page.ReadRecords()
		for _, slot := range slots {
			if slot.Deleted {
				continue
			}
			data := slot.Data
			if slot.Overflow {
				totalLen, firstPage := slot.OverflowInfo()
				var err2 error
				data, err2 = ex.pager.ReadOverflowData(totalLen, firstPage)
				if err2 != nil {
					continue
				}
			}
			// Décompresser si le record est compressé (snappy)
			if slot.Compressed && !slot.Overflow {
				var err2 error
				data, err2 = storage.DecompressRecord(&slot)
				if err2 != nil {
					continue
				}
			}
			doc, err := storage.Decode(data)
			if err != nil {
				continue // skip corrupted records
			}
			match, err := EvalExpr(where, doc)
			if err != nil {
				return nil, err
			}
			if match {
				results = append(results, &scanResult{
					recordID:   slot.RecordID,
					doc:        doc,
					pageID:     pageID,
					slotOffset: slot.Offset,
				})
			}
		}

		pageID = page.NextPageID()
	}
	return results, nil
}

// readByLocs lit des documents directement par leurs localisations physiques (O(1) par record).
// limit <= 0 signifie pas de limite.
func (ex *Executor) readByLocs(collName string, locs []index.RecordLoc, where parser.Expr, limit int) ([]*ResultDoc, error) {
	raw, err := ex.readByLocsRaw(locs, where, limit)
	if err != nil {
		return nil, err
	}
	docs := make([]*ResultDoc, len(raw))
	for i, r := range raw {
		docs[i] = &ResultDoc{RecordID: r.recordID, Doc: r.doc}
	}
	return docs, nil
}

// readByLocsRaw lit des records directement depuis les pages indiquées par les localisations.
// limit <= 0 signifie pas de limite.
// Fast path : si limit est petit, itère les locs directement sans grouper (évite alloc map).
// Grouped path : sinon, groupe par pageID pour ne lire chaque page qu'une fois.
func (ex *Executor) readByLocsRaw(locs []index.RecordLoc, where parser.Expr, limit int) ([]*scanResult, error) {
	// Fast path pour petits LIMITs : pas de grouping, itération directe
	if limit > 0 && limit <= 100 {
		return ex.readByLocsDirect(locs, where, limit)
	}

	// Grouped path : grouper par pageID pour minimiser les lectures
	type slotRef struct {
		slotOff  uint16
		recordID uint64
	}
	grouped := make(map[uint32][]slotRef, len(locs)/10+1)
	for _, loc := range locs {
		grouped[loc.PageID] = append(grouped[loc.PageID], slotRef{loc.SlotOff, loc.RecordID})
	}

	var results []*scanResult
	for pageID, refs := range grouped {
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			continue
		}
		for _, ref := range refs {
			slot, ok := page.ReadRecordAt(ref.slotOff)
			if !ok || slot.Deleted || slot.RecordID != ref.recordID {
				continue
			}
			data := slot.Data
			if slot.Overflow {
				totalLen, firstPage := slot.OverflowInfo()
				data, err = ex.pager.ReadOverflowData(totalLen, firstPage)
				if err != nil {
					continue
				}
			}
			if slot.Compressed && !slot.Overflow {
				dec, err2 := storage.DecompressRecord(&slot)
				if err2 != nil {
					continue
				}
				data = dec
			}
			doc, err := storage.Decode(data)
			if err != nil {
				continue
			}
			match, err := EvalExpr(where, doc)
			if err != nil {
				return nil, err
			}
			if match {
				results = append(results, &scanResult{
					recordID:   slot.RecordID,
					doc:        doc,
					pageID:     pageID,
					slotOffset: slot.Offset,
				})
				if limit > 0 && len(results) >= limit {
					return results, nil
				}
			}
		}
	}
	return results, nil
}

// readByLocsDirect itère les locs séquentiellement sans grouper par page.
// Idéal pour LIMIT petit : on lit quelques pages et on s'arrête immédiatement.
func (ex *Executor) readByLocsDirect(locs []index.RecordLoc, where parser.Expr, limit int) ([]*scanResult, error) {
	var results []*scanResult
	for _, loc := range locs {
		page, err := ex.pager.ReadPage(loc.PageID)
		if err != nil {
			continue
		}
		slot, ok := page.ReadRecordAt(loc.SlotOff)
		if !ok || slot.Deleted || slot.RecordID != loc.RecordID {
			continue
		}
		data := slot.Data
		if slot.Overflow {
			totalLen, firstPage := slot.OverflowInfo()
			data, err = ex.pager.ReadOverflowData(totalLen, firstPage)
			if err != nil {
				continue
			}
		}
		if slot.Compressed && !slot.Overflow {
			dec, err2 := storage.DecompressRecord(&slot)
			if err2 != nil {
				continue
			}
			data = dec
		}
		doc, err := storage.Decode(data)
		if err != nil {
			continue
		}
		match, err := EvalExpr(where, doc)
		if err != nil {
			return nil, err
		}
		if match {
			results = append(results, &scanResult{
				recordID:   slot.RecordID,
				doc:        doc,
				pageID:     loc.PageID,
				slotOffset: slot.Offset,
			})
			if len(results) >= limit {
				return results, nil
			}
		}
	}
	return results, nil
}

// scanByIDs lit des documents par leurs record_ids (lookup index — fallback sans localisation).
func (ex *Executor) scanByIDs(collName string, ids []uint64, where parser.Expr) ([]*ResultDoc, error) {
	raw, err := ex.scanByIDsRaw(collName, ids, where)
	if err != nil {
		return nil, err
	}
	docs := make([]*ResultDoc, len(raw))
	for i, r := range raw {
		docs[i] = &ResultDoc{RecordID: r.recordID, Doc: r.doc}
	}
	return docs, nil
}

func (ex *Executor) scanByIDsRaw(collName string, ids []uint64, where parser.Expr) ([]*scanResult, error) {
	idSet := make(map[uint64]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	coll := ex.pager.GetCollection(collName)
	if coll == nil {
		return nil, nil
	}

	var results []*scanResult
	pageID := coll.FirstPageID

	for pageID != 0 {
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}

		slots := page.ReadRecords()
		for _, slot := range slots {
			if slot.Deleted || !idSet[slot.RecordID] {
				continue
			}
			data := slot.Data
			if slot.Overflow {
				totalLen, firstPage := slot.OverflowInfo()
				var err2 error
				data, err2 = ex.pager.ReadOverflowData(totalLen, firstPage)
				if err2 != nil {
					continue
				}
			}
			// Décompresser si le record est compressé (snappy)
			if slot.Compressed && !slot.Overflow {
				var err2 error
				data, err2 = storage.DecompressRecord(&slot)
				if err2 != nil {
					continue
				}
			}
			doc, err := storage.Decode(data)
			if err != nil {
				continue
			}
			delete(idSet, slot.RecordID) // marquer comme trouvé
			match, err := EvalExpr(where, doc)
			if err != nil {
				return nil, err
			}
			if match {
				results = append(results, &scanResult{
					recordID:   slot.RecordID,
					doc:        doc,
					pageID:     pageID,
					slotOffset: slot.Offset,
				})
			}
		}
		if len(idSet) == 0 {
			break // tous les IDs trouvés, stop le scan
		}
		pageID = page.NextPageID()
	}
	return results, nil
}

// ---------- Index helpers ----------

// resolveIndexLookup essaie de résoudre un WHERE simple via un index.
// Retourne nil si aucun index n'est utilisable.
func (ex *Executor) resolveIndexLookup(collName string, where parser.Expr, limit int) []index.RecordLoc {
	if where == nil {
		return nil
	}
	be, ok := where.(*parser.BinaryExpr)
	if !ok {
		return nil
	}
	// Seulement EQ pour v1
	if be.Op != parser.TokenEQ {
		return nil
	}
	fieldName := ExprToFieldName(be.Left)
	if fieldName == "" {
		return nil
	}
	idx := ex.indexMgr.GetIndex(collName, fieldName)
	if idx == nil {
		return nil
	}
	lit, ok := be.Right.(*parser.LiteralExpr)
	if !ok {
		return nil
	}
	key := index.ValueToKey(literalToValue(lit.Token))
	locs, _ := idx.LookupLimit(key, limit)
	return locs
}

// resolveForceIndex force l'utilisation d'un index sur un champ spécifique (hint FORCE_INDEX).
func (ex *Executor) resolveForceIndex(collName, field string, where parser.Expr) []index.RecordLoc {
	idx := ex.indexMgr.GetIndex(collName, field)
	if idx == nil {
		return nil // index inexistant → fallback full scan
	}
	// Extraire la valeur de comparaison du WHERE
	be, ok := where.(*parser.BinaryExpr)
	if !ok || be.Op != parser.TokenEQ {
		return nil
	}
	fieldName := ExprToFieldName(be.Left)
	if fieldName != field {
		// Essayer l'autre côté
		fieldName = ExprToFieldName(be.Right)
		if fieldName != field {
			return nil
		}
		lit, ok := be.Left.(*parser.LiteralExpr)
		if !ok {
			return nil
		}
		key := index.ValueToKey(literalToValue(lit.Token))
		locs, _ := idx.Lookup(key)
		return locs
	}
	lit, ok := be.Right.(*parser.LiteralExpr)
	if !ok {
		return nil
	}
	key := index.ValueToKey(literalToValue(lit.Token))
	locs, _ := idx.Lookup(key)
	return locs
}

func (ex *Executor) updateIndexesAfterInsert(collName string, recordID uint64, doc *storage.Document, pageID uint32, slotOff uint16) {
	ex.lockMgr.IndexMu.Lock()
	defer ex.lockMgr.IndexMu.Unlock()

	for _, idx := range ex.indexMgr.GetIndexesForCollection(collName) {
		path := strings.Split(idx.Field, ".")
		val, ok := doc.GetNested(path)
		if ok {
			idx.Add(index.ValueToKey(val), recordID, pageID, slotOff) // best-effort
		}
	}
}

func (ex *Executor) updateIndexesAfterDelete(collName string, recordID uint64, doc *storage.Document) {
	ex.lockMgr.IndexMu.Lock()
	defer ex.lockMgr.IndexMu.Unlock()

	for _, idx := range ex.indexMgr.GetIndexesForCollection(collName) {
		path := strings.Split(idx.Field, ".")
		val, ok := doc.GetNested(path)
		if ok {
			idx.Remove(index.ValueToKey(val), recordID) // erreur ignorée (best-effort)
		}
	}
}

func (ex *Executor) updateIndexesAfterUpdate(collName string, recordID uint64, oldDoc, newDoc *storage.Document, newPageID uint32, newSlotOff uint16) {
	ex.lockMgr.IndexMu.Lock()
	defer ex.lockMgr.IndexMu.Unlock()

	for _, idx := range ex.indexMgr.GetIndexesForCollection(collName) {
		path := strings.Split(idx.Field, ".")
		oldVal, _ := oldDoc.GetNested(path)
		newVal, _ := newDoc.GetNested(path)

		oldKey := index.ValueToKey(oldVal)
		newKey := index.ValueToKey(newVal)

		if oldKey != newKey {
			idx.Remove(oldKey, recordID)                     // best-effort
			idx.Add(newKey, recordID, newPageID, newSlotOff) // best-effort
		}
	}
}

// ---------- Projection ----------

func isSelectAll(cols []parser.Expr) bool {
	if len(cols) == 1 {
		_, ok := cols[0].(*parser.StarExpr)
		return ok
	}
	return false
}

func (ex *Executor) projectColumns(docs []*ResultDoc, cols []parser.Expr, fromAlias string) ([]*ResultDoc, error) {
	result := make([]*ResultDoc, len(docs))
	for i, rd := range docs {
		projected := storage.NewDocument()
		for _, col := range cols {
			var alias string

			// Gérer les alias
			if ae, ok := col.(*parser.AliasExpr); ok {
				alias = ae.Alias
				col = ae.Expr
			}

			switch c := col.(type) {
			case *parser.IdentExpr:
				fieldName := c.Name
				val, ok := rd.Doc.Get(fieldName)
				if ok {
					if alias != "" {
						fieldName = alias
					}
					projected.Set(fieldName, val)
				}
			case *parser.DotExpr:
				fieldName := strings.Join(c.Parts, ".")
				val, ok := rd.Doc.GetNested(c.Parts)
				if ok {
					if alias != "" {
						fieldName = alias
					}
					projected.Set(fieldName, val)
				}
			case *parser.StarExpr:
				// SELECT * = copier tous les champs
				for _, f := range rd.Doc.Fields {
					projected.Set(f.Name, f.Value)
				}
			case *parser.QualifiedStarExpr:
				// SELECT A.* = copier tous les champs du sous-document A (JOIN)
				// ou tous les champs si c'est un alias de la table principale
				sub, ok := rd.Doc.Get(c.Qualifier)
				if ok {
					if subDoc, isDoc := sub.(*storage.Document); isDoc {
						for _, f := range subDoc.Fields {
							projected.Set(f.Name, f.Value)
						}
					}
				} else {
					// Pas de sous-document : c'est probablement un alias de la table unique
					// → copier tous les champs
					for _, f := range rd.Doc.Fields {
						projected.Set(f.Name, f.Value)
					}
				}
			case *parser.FuncCallExpr:
				if isScalarFuncName(c.Name) {
					// Fonction scalaire : évaluer per-row
					val, err := evalScalarFunc(c, rd.Doc)
					if err != nil {
						return nil, err
					}
					name := c.Name
					if alias != "" {
						name = alias
					}
					projected.Set(name, val)
				} else {
					// Agrégats déjà calculés dans le GroupBy
					name := c.Name
					if alias != "" {
						name = alias
					}
					val, ok := rd.Doc.Get(name)
					if ok {
						projected.Set(name, val)
					}
				}
			case *parser.SubqueryExpr:
				// Sous-requête corrélée dans SELECT — exécuter per-row
				resolvedQuery := &parser.SelectStatement{
					Distinct:  c.Query.Distinct,
					Columns:   c.Query.Columns,
					From:      c.Query.From,
					FromAlias: c.Query.FromAlias,
					Joins:     c.Query.Joins,
					Where:     substituteOuterRefs(c.Query.Where, fromAlias, rd.Doc),
					GroupBy:   c.Query.GroupBy,
					Having:    c.Query.Having,
					OrderBy:   c.Query.OrderBy,
					Limit:     c.Query.Limit,
					Offset:    c.Query.Offset,
				}
				scalarExpr, subErr := ex.execSubqueryScalar(resolvedQuery)
				if subErr != nil {
					return nil, subErr
				}
				name := alias
				if name == "" {
					name = "subquery"
				}
				scalarVal := literalToValue(scalarExpr.(*parser.LiteralExpr).Token)
				projected.Set(name, scalarVal)
			default:
				// Expression calculée (littéral, arithmétique, etc.)
				val, err := evalValue(col, rd.Doc)
				if err != nil {
					return nil, err
				}
				name := alias
				if name == "" {
					name = exprToString(col)
				}
				projected.Set(name, val)
			}
		}
		result[i] = &ResultDoc{RecordID: rd.RecordID, Doc: projected}
	}
	return result, nil
}

// exprToString génère un nom de colonne par défaut pour une expression calculée.
func exprToString(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.LiteralExpr:
		return e.Token.Literal
	case *parser.IdentExpr:
		return e.Name
	case *parser.DotExpr:
		return strings.Join(e.Parts, ".")
	case *parser.BinaryExpr:
		opStr := "?"
		switch e.Op {
		case parser.TokenPlus:
			opStr = "+"
		case parser.TokenMinus:
			opStr = "-"
		case parser.TokenStar:
			opStr = "*"
		case parser.TokenSlash:
			opStr = "/"
		}
		return exprToString(e.Left) + opStr + exprToString(e.Right)
	default:
		return "expr"
	}
}

// ---------- ORDER BY ----------

func (ex *Executor) applyOrderBy(docs []*ResultDoc, orderBy []*parser.OrderByExpr) {
	sort.SliceStable(docs, func(i, j int) bool {
		for _, ob := range orderBy {
			path := ExprToFieldPath(ob.Expr)
			var vi, vj interface{}
			if len(path) == 1 {
				vi, _ = docs[i].Doc.Get(path[0])
				vj, _ = docs[j].Doc.Get(path[0])
			} else {
				vi, _ = docs[i].Doc.GetNested(path)
				vj, _ = docs[j].Doc.GetNested(path)
			}

			cmp := compareValues(vi, vj)
			if cmp == 0 {
				continue
			}
			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

// compareValues compare deux valeurs pour le tri. Retourne -1, 0, 1.
func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)
	if aok && bok {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}

	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}

	return 0
}

// ---------- GROUP BY ----------

func (ex *Executor) applyGroupBy(docs []*ResultDoc, stmt *parser.SelectStatement) ([]*ResultDoc, error) {
	groups := make(map[string][]*ResultDoc)
	var keys []string

	for _, rd := range docs {
		key := ex.groupKey(rd.Doc, stmt.GroupBy)
		if _, exists := groups[key]; !exists {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], rd)
	}

	var result []*ResultDoc
	for _, key := range keys {
		groupDocs := groups[key]
		if len(groupDocs) == 0 {
			continue
		}

		// Le premier document comme base
		resultDoc := storage.NewDocument()

		// Copier les champs du GROUP BY
		for _, gb := range stmt.GroupBy {
			path := ExprToFieldPath(gb)
			val, ok := groupDocs[0].Doc.GetNested(path)
			if ok {
				resultDoc.Set(ExprToFieldName(gb), val)
			}
		}

		// Calculer les agrégats et copier les colonnes non-agrégat
		for _, col := range stmt.Columns {
			actualCol := col
			alias := ""
			if ae, ok := col.(*parser.AliasExpr); ok {
				alias = ae.Alias
				actualCol = ae.Expr
			}

			fc, ok := actualCol.(*parser.FuncCallExpr)
			if ok {
				aggVal := ex.computeAggregate(fc, groupDocs)
				// Toujours stocker sous le nom de la fonction (pour HAVING)
				resultDoc.Set(fc.Name, aggVal)
				if alias != "" {
					resultDoc.Set(alias, aggVal)
				}
			} else {
				// Colonne non-agrégat (ex: d.budget) → copier du premier doc du groupe
				path := ExprToFieldPath(actualCol)
				if len(path) > 0 {
					val, found := groupDocs[0].Doc.GetNested(path)
					if found {
						dottedName := ExprToFieldName(actualCol)
						// Stocker sous le nom complet (pour que projectColumns puisse résoudre d.budget)
						if _, exists := resultDoc.Get(dottedName); !exists {
							resultDoc.Set(dottedName, val)
						}
						// Stocker aussi sous l'alias si présent
						if alias != "" {
							resultDoc.Set(alias, val)
						}
					}
				}
			}
		}

		// HAVING
		if stmt.Having != nil {
			match, err := EvalExpr(stmt.Having, resultDoc)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		result = append(result, &ResultDoc{Doc: resultDoc})
	}

	return result, nil
}

func (ex *Executor) groupKey(doc *storage.Document, groupBy []parser.Expr) string {
	var parts []string
	for _, gb := range groupBy {
		path := ExprToFieldPath(gb)
		val, _ := doc.GetNested(path)
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "|")
}

func (ex *Executor) computeAggregate(fc *parser.FuncCallExpr, docs []*ResultDoc) interface{} {
	switch fc.Name {
	case "COUNT":
		// COUNT(*) = count all, COUNT(field) = count non-null, COUNT(DISTINCT field) = count distinct non-null
		if len(fc.Args) == 0 {
			return int64(len(docs))
		}
		if _, isStar := fc.Args[0].(*parser.StarExpr); isStar {
			return int64(len(docs))
		}
		if fc.Distinct {
			seen := make(map[string]bool)
			for _, rd := range docs {
				val, err := evalValue(fc.Args[0], rd.Doc)
				if err == nil && val != nil {
					key := fmt.Sprintf("%v", val)
					seen[key] = true
				}
			}
			return int64(len(seen))
		}
		var count int64
		for _, rd := range docs {
			val, err := evalValue(fc.Args[0], rd.Doc)
			if err == nil && val != nil {
				count++
			}
		}
		return count
	case "SUM":
		return ex.aggSum(fc, docs)
	case "AVG":
		sum := ex.aggSum(fc, docs)
		if sf, ok := toFloat64(sum); ok && len(docs) > 0 {
			return sf / float64(len(docs))
		}
		return float64(0)
	case "MIN":
		return ex.aggMinMax(fc, docs, false)
	case "MAX":
		return ex.aggMinMax(fc, docs, true)
	default:
		return nil
	}
}

func (ex *Executor) aggSum(fc *parser.FuncCallExpr, docs []*ResultDoc) interface{} {
	if len(fc.Args) == 0 {
		return int64(0)
	}
	var sum float64
	for _, rd := range docs {
		val, err := evalValue(fc.Args[0], rd.Doc)
		if err != nil {
			continue
		}
		if f, ok := toFloat64(val); ok {
			sum += f
		}
	}
	// Return int64 si c'est un entier
	if sum == float64(int64(sum)) {
		return int64(sum)
	}
	return sum
}

func (ex *Executor) aggMinMax(fc *parser.FuncCallExpr, docs []*ResultDoc, isMax bool) interface{} {
	if len(fc.Args) == 0 || len(docs) == 0 {
		return nil
	}
	var result interface{}
	for _, rd := range docs {
		val, err := evalValue(fc.Args[0], rd.Doc)
		if err != nil || val == nil {
			continue
		}
		if result == nil {
			result = val
			continue
		}
		cmp := compareValues(val, result)
		if (isMax && cmp > 0) || (!isMax && cmp < 0) {
			result = val
		}
	}
	return result
}

// hasAggregateColumns retourne true si les colonnes contiennent au moins une fonction d'agrégation.
func hasAggregateColumns(cols []parser.Expr) bool {
	for _, col := range cols {
		if ae, ok := col.(*parser.AliasExpr); ok {
			col = ae.Expr
		}
		if fc, ok := col.(*parser.FuncCallExpr); ok {
			if !isScalarFuncName(fc.Name) {
				return true
			}
		}
	}
	return false
}

// applyStandaloneAggregate calcule les agrégats sans GROUP BY (ex: SELECT COUNT(*) FROM table).
// Retourne un seul document avec les résultats agrégés.
func (ex *Executor) applyStandaloneAggregate(docs []*ResultDoc, stmt *parser.SelectStatement) ([]*ResultDoc, error) {
	resultDoc := storage.NewDocument()

	for _, col := range stmt.Columns {
		actualCol := col
		alias := ""
		if ae, ok := col.(*parser.AliasExpr); ok {
			alias = ae.Alias
			actualCol = ae.Expr
		}

		fc, ok := actualCol.(*parser.FuncCallExpr)
		if !ok {
			continue
		}

		aggVal := ex.computeAggregate(fc, docs)
		name := fc.Name
		if alias != "" {
			name = alias
		}
		resultDoc.Set(name, aggVal)
	}

	return []*ResultDoc{{Doc: resultDoc}}, nil
}

// deduplicateDocs supprime les documents dupliqués (pour DISTINCT).
// Utilise l'encodage binaire comme clé de déduplication.
func deduplicateDocs(docs []*ResultDoc) []*ResultDoc {
	seen := make(map[string]bool)
	var result []*ResultDoc

	for _, rd := range docs {
		encoded, err := rd.Doc.Encode()
		if err != nil {
			result = append(result, rd)
			continue
		}
		key := string(encoded)
		if !seen[key] {
			seen[key] = true
			result = append(result, rd)
		}
	}
	return result
}

// ---------- SEQUENCES ----------

func (ex *Executor) execCreateSequence(stmt *parser.CreateSequenceStatement) (*Result, error) {
	name := strings.ToUpper(stmt.Name)
	if _, exists := ex.seqs[name]; exists {
		return nil, fmt.Errorf("sequence %s already exists", name)
	}
	ex.seqs[name] = &Sequence{
		Name:        name,
		CurrentVal:  stmt.StartWith,
		IncrementBy: stmt.IncrementBy,
		MinValue:    stmt.MinValue,
		MaxValue:    stmt.MaxValue,
		Cycle:       stmt.Cycle,
		Started:     false,
	}
	return &Result{}, nil
}

func (ex *Executor) execDropSequence(stmt *parser.DropSequenceStatement) (*Result, error) {
	name := strings.ToUpper(stmt.Name)
	if _, exists := ex.seqs[name]; !exists {
		if stmt.IfExists {
			return &Result{}, nil
		}
		return nil, fmt.Errorf("sequence %s does not exist", name)
	}
	delete(ex.seqs, name)
	return &Result{}, nil
}

// nextVal incrémente et retourne la valeur suivante de la séquence.
func (ex *Executor) nextVal(name string) (float64, error) {
	seq, ok := ex.seqs[strings.ToUpper(name)]
	if !ok {
		return 0, fmt.Errorf("sequence %s does not exist", strings.ToUpper(name))
	}
	if !seq.Started {
		seq.Started = true
		return seq.CurrentVal, nil
	}
	next := seq.CurrentVal + seq.IncrementBy
	if next > seq.MaxValue {
		if !seq.Cycle {
			return 0, fmt.Errorf("sequence %s has reached MAXVALUE (%g)", seq.Name, seq.MaxValue)
		}
		next = seq.MinValue
	}
	if next < seq.MinValue {
		if !seq.Cycle {
			return 0, fmt.Errorf("sequence %s has reached MINVALUE (%g)", seq.Name, seq.MinValue)
		}
		next = seq.MaxValue
	}
	seq.CurrentVal = next
	return next, nil
}

// currVal retourne la valeur courante de la séquence (sans incrémenter).
func (ex *Executor) currVal(name string) (float64, error) {
	seq, ok := ex.seqs[strings.ToUpper(name)]
	if !ok {
		return 0, fmt.Errorf("sequence %s does not exist", strings.ToUpper(name))
	}
	if !seq.Started {
		return 0, fmt.Errorf("sequence %s: CURRVAL is not yet defined (call NEXTVAL first)", seq.Name)
	}
	return seq.CurrentVal, nil
}

// resolveSequenceExpr remplace un SequenceExpr par un LiteralExpr résolu.
func (ex *Executor) resolveSequenceExpr(expr parser.Expr) (parser.Expr, error) {
	switch e := expr.(type) {
	case *parser.SequenceExpr:
		var val float64
		var err error
		if e.Op == "NEXTVAL" {
			val, err = ex.nextVal(e.SeqName)
		} else {
			val, err = ex.currVal(e.SeqName)
		}
		if err != nil {
			return nil, err
		}
		// Entier → TokenInteger, sinon TokenFloat
		if val == float64(int64(val)) {
			return &parser.LiteralExpr{Token: parser.Token{
				Type:    parser.TokenInteger,
				Literal: fmt.Sprintf("%d", int64(val)),
			}}, nil
		}
		return &parser.LiteralExpr{Token: parser.Token{
			Type:    parser.TokenFloat,
			Literal: fmt.Sprintf("%g", val),
		}}, nil
	default:
		return expr, nil
	}
}

// resolveSequencesInFields résout les SequenceExpr dans une liste de FieldAssignment.
func (ex *Executor) resolveSequencesInFields(fields []parser.FieldAssignment) error {
	for i, fa := range fields {
		resolved, err := ex.resolveSequenceExpr(fa.Value)
		if err != nil {
			return err
		}
		fields[i].Value = resolved
	}
	return nil
}

// cloneDocument crée une copie profonde d'un document.
func cloneDocument(doc *storage.Document) *storage.Document {
	encoded, err := doc.Encode()
	if err != nil {
		return storage.NewDocument()
	}
	cloned, err := storage.Decode(encoded)
	if err != nil {
		return storage.NewDocument()
	}
	return cloned
}
