package engine

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// ---------- Structures ----------

// CollectionStats contient les statistiques d'une collection.
type CollectionStats struct {
	Name      string
	RowCount  int64
	PageCount int64
}

const histogramBuckets = 10 // nombre de buckets dans les histogrammes equi-depth

// ColumnStats contient les statistiques d'une colonne.
type ColumnStats struct {
	Field     string   // nom du champ
	RowCount  int64    // nombre de lignes ayant ce champ
	NullCount int64    // nombre de NULL / absents
	NDV       int64    // nombre de valeurs distinctes (Number of Distinct Values)
	MinVal    float64  // valeur min (pour les numériques)
	MaxVal    float64  // valeur max
	MinStr    string   // valeur min (pour les strings)
	MaxStr    string   // valeur max (pour les strings)
	IsNumeric bool     // true si la colonne est numérique
	AvgSize   float64  // taille moyenne en octets (pour les strings)
	Histogram []Bucket // histogramme equi-depth
}

// Bucket représente un bucket d'un histogramme equi-depth.
type Bucket struct {
	LowerBound float64 // borne inférieure (numérique)
	UpperBound float64 // borne supérieure
	Count      int64   // nombre de lignes dans ce bucket
	NDV        int64   // valeurs distinctes dans ce bucket
}

// TableStats contient les statistiques complètes d'une table.
type TableStats struct {
	Collection string
	RowCount   int64
	PageCount  int64
	Columns    map[string]*ColumnStats
	AnalyzedAt time.Time
}

// ---------- Stats cache ----------

// GetTableStats retourne les stats mises en cache pour une collection, ou nil.
func (ex *Executor) GetTableStats(collName string) *TableStats {
	if ex.statsCache == nil {
		return nil
	}
	return ex.statsCache[collName]
}

// getColumnStats retourne les stats d'une colonne ou nil.
func (ex *Executor) getColumnStats(collName, field string) *ColumnStats {
	ts := ex.GetTableStats(collName)
	if ts == nil {
		return nil
	}
	return ts.Columns[field]
}

// ---------- collectStats (basique, sans ANALYZE) ----------

// collectStats calcule les statistiques d'une collection (nombre de rows et pages).
func (ex *Executor) collectStats(collName string) CollectionStats {
	// Si on a des stats ANALYZE en cache, les utiliser
	if ts := ex.GetTableStats(collName); ts != nil {
		return CollectionStats{Name: collName, RowCount: ts.RowCount, PageCount: ts.PageCount}
	}
	stats := CollectionStats{Name: collName}
	coll := ex.pager.GetCollection(collName)
	if coll == nil {
		return stats
	}
	pageID := coll.FirstPageID
	for pageID != 0 {
		stats.PageCount++
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			break
		}
		for _, slot := range page.ReadRecords() {
			if !slot.Deleted {
				stats.RowCount++
			}
		}
		pageID = page.NextPageID()
	}
	return stats
}

// ---------- ANALYZE : calcul des statistiques ----------

// execAnalyze exécute ANALYZE [table].
func (ex *Executor) execAnalyze(stmt *parser.AnalyzeStatement) (*Result, error) {
	if ex.statsCache == nil {
		ex.statsCache = make(map[string]*TableStats)
	}

	var tables []string
	if stmt.Table != "" {
		tables = []string{stmt.Table}
	} else {
		tables = ex.pager.ListCollections()
	}

	totalAnalyzed := int64(0)
	for _, tbl := range tables {
		// Ignorer les tables système
		if tbl == "_novusdb_stats" {
			continue
		}
		ts, err := ex.analyzeTable(tbl)
		if err != nil {
			return nil, fmt.Errorf("ANALYZE %s: %w", tbl, err)
		}
		ex.statsCache[tbl] = ts
		ex.persistTableStats(ts)
		totalAnalyzed++
	}

	return &Result{RowsAffected: totalAnalyzed}, nil
}

// analyzeTable scanne toutes les lignes d'une collection et calcule les statistiques par colonne.
func (ex *Executor) analyzeTable(collName string) (*TableStats, error) {
	coll := ex.pager.GetCollection(collName)
	if coll == nil {
		return &TableStats{Collection: collName, Columns: make(map[string]*ColumnStats)}, nil
	}

	ts := &TableStats{
		Collection: collName,
		Columns:    make(map[string]*ColumnStats),
		AnalyzedAt: time.Now(),
	}

	// Phase 1 : collecter toutes les valeurs par colonne
	type colAccum struct {
		values  []float64
		strVals []string
		nulls   int64
		total   int64
		numeric bool
		strSize int64
	}
	accums := make(map[string]*colAccum)

	pageID := coll.FirstPageID
	for pageID != 0 {
		ts.PageCount++
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		for _, slot := range page.ReadRecords() {
			if slot.Deleted {
				continue
			}
			ts.RowCount++
			doc, err := storage.Decode(slot.Data)
			if err != nil {
				continue
			}
			// Recenser les champs présents
			seenFields := make(map[string]bool)
			for _, f := range doc.Fields {
				seenFields[f.Name] = true
				a := accums[f.Name]
				if a == nil {
					a = &colAccum{numeric: true}
					accums[f.Name] = a
				}
				a.total++
				if f.Value == nil {
					a.nulls++
					continue
				}
				switch v := f.Value.(type) {
				case int64:
					a.values = append(a.values, float64(v))
				case float64:
					a.values = append(a.values, v)
				case string:
					a.numeric = false
					a.strVals = append(a.strVals, v)
					a.strSize += int64(len(v))
				case bool:
					if v {
						a.values = append(a.values, 1)
					} else {
						a.values = append(a.values, 0)
					}
				default:
					a.numeric = false
				}
			}
			// Compter les champs absents comme NULL
			for name, a := range accums {
				if !seenFields[name] {
					a.nulls++
					a.total++
				}
			}
		}
		pageID = page.NextPageID()
	}

	// Phase 2 : construire les ColumnStats
	for name, a := range accums {
		cs := &ColumnStats{
			Field:     name,
			RowCount:  a.total,
			NullCount: a.nulls,
			IsNumeric: a.numeric,
		}

		if a.numeric && len(a.values) > 0 {
			// NDV, Min, Max pour numériques
			sort.Float64s(a.values)
			cs.MinVal = a.values[0]
			cs.MaxVal = a.values[len(a.values)-1]
			cs.NDV = countDistinctFloat(a.values)

			// Histogramme equi-depth
			cs.Histogram = buildHistogram(a.values, histogramBuckets)
		} else if len(a.strVals) > 0 {
			// NDV, Min, Max pour strings
			sort.Strings(a.strVals)
			cs.MinStr = a.strVals[0]
			cs.MaxStr = a.strVals[len(a.strVals)-1]
			cs.NDV = countDistinctStr(a.strVals)
			if a.total-a.nulls > 0 {
				cs.AvgSize = float64(a.strSize) / float64(a.total-a.nulls)
			}
		}

		ts.Columns[name] = cs
	}

	return ts, nil
}

// ---------- Histogramme ----------

// buildHistogram construit un histogramme equi-depth à partir de valeurs triées.
func buildHistogram(sorted []float64, numBuckets int) []Bucket {
	n := len(sorted)
	if n == 0 || numBuckets <= 0 {
		return nil
	}
	if numBuckets > n {
		numBuckets = n
	}

	buckets := make([]Bucket, numBuckets)
	bucketSize := n / numBuckets
	remainder := n % numBuckets

	idx := 0
	for i := 0; i < numBuckets; i++ {
		size := bucketSize
		if i < remainder {
			size++
		}
		buckets[i].LowerBound = sorted[idx]
		buckets[i].UpperBound = sorted[idx+size-1]
		buckets[i].Count = int64(size)

		// NDV dans ce bucket
		distinct := int64(1)
		for j := idx + 1; j < idx+size; j++ {
			if sorted[j] != sorted[j-1] {
				distinct++
			}
		}
		buckets[i].NDV = distinct

		idx += size
	}
	return buckets
}

func countDistinctFloat(sorted []float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	count := int64(1)
	for i := 1; i < len(sorted); i++ {
		if sorted[i] != sorted[i-1] {
			count++
		}
	}
	return count
}

func countDistinctStr(sorted []string) int64 {
	if len(sorted) == 0 {
		return 0
	}
	count := int64(1)
	for i := 1; i < len(sorted); i++ {
		if sorted[i] != sorted[i-1] {
			count++
		}
	}
	return count
}

// ---------- Persistence : sauvegarder/charger dans _novusdb_stats ----------

const statsCollection = "_novusdb_stats"

// persistTableStats persiste les stats d'une table dans _novusdb_stats.
func (ex *Executor) persistTableStats(ts *TableStats) {
	// Supprimer l'ancien enregistrement pour cette collection
	ex.deleteStatsRecord(ts.Collection)

	// Construire le document de stats
	doc := storage.NewDocument()
	doc.Set("_type", "table_stats")
	doc.Set("collection", ts.Collection)
	doc.Set("row_count", ts.RowCount)
	doc.Set("page_count", ts.PageCount)
	doc.Set("analyzed_at", ts.AnalyzedAt.Format(time.RFC3339))

	// Sérialiser les stats colonnes
	colIdx := 0
	for _, cs := range ts.Columns {
		prefix := fmt.Sprintf("col_%d_", colIdx)
		doc.Set(prefix+"field", cs.Field)
		doc.Set(prefix+"row_count", cs.RowCount)
		doc.Set(prefix+"null_count", cs.NullCount)
		doc.Set(prefix+"ndv", cs.NDV)
		doc.Set(prefix+"is_numeric", cs.IsNumeric)
		if cs.IsNumeric {
			doc.Set(prefix+"min", cs.MinVal)
			doc.Set(prefix+"max", cs.MaxVal)
		} else {
			doc.Set(prefix+"min_str", cs.MinStr)
			doc.Set(prefix+"max_str", cs.MaxStr)
			doc.Set(prefix+"avg_size", cs.AvgSize)
		}
		// Histogramme
		for j, b := range cs.Histogram {
			bp := fmt.Sprintf("%shist_%d_", prefix, j)
			doc.Set(bp+"lo", b.LowerBound)
			doc.Set(bp+"hi", b.UpperBound)
			doc.Set(bp+"cnt", b.Count)
			doc.Set(bp+"ndv", b.NDV)
		}
		doc.Set(prefix+"hist_len", int64(len(cs.Histogram)))
		colIdx++
	}
	doc.Set("col_count", int64(colIdx))

	// Insérer dans _novusdb_stats
	data, err := doc.Encode()
	if err != nil {
		return
	}
	coll, err := ex.pager.GetOrCreateCollection(statsCollection)
	if err != nil {
		return
	}
	id, err := ex.pager.NextRecordID(statsCollection)
	if err != nil {
		return
	}
	ex.pager.InsertRecordAtomic(coll, id, data)
}

// deleteStatsRecord supprime les stats existantes pour une collection.
func (ex *Executor) deleteStatsRecord(collName string) {
	// Utiliser scanCollectionRaw pour obtenir les offsets exacts
	results, err := ex.scanCollectionRaw(statsCollection, nil)
	if err != nil || len(results) == 0 {
		return
	}
	for _, r := range results {
		if v, ok := r.doc.Get("collection"); ok && v == collName {
			ex.pager.MarkDeletedAtomic(r.pageID, r.slotOffset)
		}
	}
}

// LoadStats charge les stats depuis _novusdb_stats au démarrage.
func (ex *Executor) LoadStats() {
	coll := ex.pager.GetCollection(statsCollection)
	if coll == nil {
		return
	}
	if ex.statsCache == nil {
		ex.statsCache = make(map[string]*TableStats)
	}

	pageID := coll.FirstPageID
	for pageID != 0 {
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return
		}
		for _, slot := range page.ReadRecords() {
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
			ts := decodeTableStats(doc)
			if ts != nil {
				ex.statsCache[ts.Collection] = ts
			}
		}
		pageID = page.NextPageID()
	}
}

// decodeTableStats reconstruit un TableStats depuis un document persisté.
func decodeTableStats(doc *storage.Document) *TableStats {
	collIface, ok := doc.Get("collection")
	if !ok {
		return nil
	}
	collName, ok := collIface.(string)
	if !ok {
		return nil
	}
	ts := &TableStats{
		Collection: collName,
		RowCount:   getInt64(doc, "row_count"),
		PageCount:  getInt64(doc, "page_count"),
		Columns:    make(map[string]*ColumnStats),
	}
	if atStr, ok := doc.Get("analyzed_at"); ok {
		if s, ok := atStr.(string); ok {
			ts.AnalyzedAt, _ = time.Parse(time.RFC3339, s)
		}
	}

	colCount := int(getInt64(doc, "col_count"))
	for i := 0; i < colCount; i++ {
		prefix := fmt.Sprintf("col_%d_", i)
		field := getStr(doc, prefix+"field")
		if field == "" {
			continue
		}
		cs := &ColumnStats{
			Field:     field,
			RowCount:  getInt64(doc, prefix+"row_count"),
			NullCount: getInt64(doc, prefix+"null_count"),
			NDV:       getInt64(doc, prefix+"ndv"),
			IsNumeric: getBool(doc, prefix+"is_numeric"),
		}
		if cs.IsNumeric {
			cs.MinVal = getFloat(doc, prefix+"min")
			cs.MaxVal = getFloat(doc, prefix+"max")
		} else {
			cs.MinStr = getStr(doc, prefix+"min_str")
			cs.MaxStr = getStr(doc, prefix+"max_str")
			cs.AvgSize = getFloat(doc, prefix+"avg_size")
		}
		histLen := int(getInt64(doc, prefix+"hist_len"))
		for j := 0; j < histLen; j++ {
			bp := fmt.Sprintf("%shist_%d_", prefix, j)
			cs.Histogram = append(cs.Histogram, Bucket{
				LowerBound: getFloat(doc, bp+"lo"),
				UpperBound: getFloat(doc, bp+"hi"),
				Count:      getInt64(doc, bp+"cnt"),
				NDV:        getInt64(doc, bp+"ndv"),
			})
		}
		ts.Columns[field] = cs
	}
	return ts
}

// ---------- Helpers de désérialisation ----------

func getInt64(doc *storage.Document, key string) int64 {
	v, ok := doc.Get(key)
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

func getFloat(doc *storage.Document, key string) float64 {
	v, ok := doc.Get(key)
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	}
	return 0
}

func getStr(doc *storage.Document, key string) string {
	v, ok := doc.Get(key)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func getBool(doc *storage.Document, key string) bool {
	v, ok := doc.Get(key)
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

// ---------- Estimation de sélectivité avec vraies stats ----------

// estimateSelectivity estime la sélectivité d'un filtre WHERE.
// Si des stats ANALYZE existent, utilise NDV/histogramme ; sinon heuristiques par défaut.
func (ex *Executor) estimateSelectivityWithStats(collName string, where parser.Expr) float64 {
	return ex.estSel(collName, where)
}

func (ex *Executor) estSel(collName string, expr parser.Expr) float64 {
	if expr == nil {
		return 1.0
	}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		switch e.Op {
		case parser.TokenEQ:
			return ex.estEQ(collName, e)
		case parser.TokenNEQ:
			return 1.0 - ex.estEQ(collName, e)
		case parser.TokenLT, parser.TokenGT, parser.TokenLTE, parser.TokenGTE:
			return ex.estRange(collName, e)
		case parser.TokenAnd:
			return ex.estSel(collName, e.Left) * ex.estSel(collName, e.Right)
		case parser.TokenOr:
			l := ex.estSel(collName, e.Left)
			r := ex.estSel(collName, e.Right)
			return l + r - l*r
		default:
			return 0.5
		}
	case *parser.BetweenExpr:
		return ex.estBetween(collName, e)
	case *parser.IsNullExpr:
		return ex.estIsNull(collName, e)
	case *parser.InExpr:
		return ex.estIn(collName, e)
	case *parser.LikeExpr:
		if e.Negate {
			return 0.9
		}
		return 0.25
	case *parser.NotExpr:
		return 1.0 - ex.estSel(collName, e.Expr)
	default:
		return 0.5
	}
}

// estEQ : sélectivité pour field = value.
// Avec stats : 1/NDV. Sans stats : 0.1 (heuristique).
func (ex *Executor) estEQ(collName string, e *parser.BinaryExpr) float64 {
	field := ExprToFieldName(e.Left)
	cs := ex.getColumnStats(collName, field)
	if cs != nil && cs.NDV > 0 {
		return 1.0 / float64(cs.NDV)
	}
	return 0.1
}

// estRange : sélectivité pour field < / > / <= / >= value.
// Avec histogramme : fraction des buckets couverts. Sans stats : 0.33.
func (ex *Executor) estRange(collName string, e *parser.BinaryExpr) float64 {
	field := ExprToFieldName(e.Left)
	cs := ex.getColumnStats(collName, field)
	if cs == nil || !cs.IsNumeric || len(cs.Histogram) == 0 {
		return 0.33
	}

	lit, ok := e.Right.(*parser.LiteralExpr)
	if !ok {
		return 0.33
	}
	val := literalToFloat(lit.Token)
	if math.IsNaN(val) {
		return 0.33
	}

	// Calculer la fraction de lignes selon l'histogramme
	totalRows := int64(0)
	for _, b := range cs.Histogram {
		totalRows += b.Count
	}
	if totalRows == 0 {
		return 0.33
	}

	matchRows := int64(0)
	for _, b := range cs.Histogram {
		switch e.Op {
		case parser.TokenLT:
			if val > b.UpperBound {
				matchRows += b.Count
			} else if val > b.LowerBound {
				// Interpolation linéaire
				frac := (val - b.LowerBound) / (b.UpperBound - b.LowerBound + 1e-15)
				matchRows += int64(float64(b.Count) * frac)
			}
		case parser.TokenLTE:
			if val >= b.UpperBound {
				matchRows += b.Count
			} else if val >= b.LowerBound {
				frac := (val - b.LowerBound + 1) / (b.UpperBound - b.LowerBound + 1e-15)
				matchRows += int64(float64(b.Count) * frac)
			}
		case parser.TokenGT:
			if val < b.LowerBound {
				matchRows += b.Count
			} else if val < b.UpperBound {
				frac := (b.UpperBound - val) / (b.UpperBound - b.LowerBound + 1e-15)
				matchRows += int64(float64(b.Count) * frac)
			}
		case parser.TokenGTE:
			if val <= b.LowerBound {
				matchRows += b.Count
			} else if val <= b.UpperBound {
				frac := (b.UpperBound - val + 1) / (b.UpperBound - b.LowerBound + 1e-15)
				matchRows += int64(float64(b.Count) * frac)
			}
		}
	}

	sel := float64(matchRows) / float64(totalRows)
	if sel < 0.001 {
		sel = 0.001
	}
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

// estBetween : sélectivité pour field BETWEEN low AND high.
func (ex *Executor) estBetween(collName string, e *parser.BetweenExpr) float64 {
	field := ExprToFieldName(e.Expr)
	cs := ex.getColumnStats(collName, field)
	if cs == nil || !cs.IsNumeric || (cs.MaxVal-cs.MinVal) == 0 {
		if e.Negate {
			return 0.75
		}
		return 0.25
	}

	// Extraire les bornes
	lowLit, ok1 := e.Low.(*parser.LiteralExpr)
	highLit, ok2 := e.High.(*parser.LiteralExpr)
	if !ok1 || !ok2 {
		if e.Negate {
			return 0.75
		}
		return 0.25
	}
	lo := literalToFloat(lowLit.Token)
	hi := literalToFloat(highLit.Token)
	if math.IsNaN(lo) || math.IsNaN(hi) {
		if e.Negate {
			return 0.75
		}
		return 0.25
	}

	// Fraction couverte par [lo, hi] dans [min, max]
	span := cs.MaxVal - cs.MinVal
	sel := (hi - lo) / span
	if sel < 0 {
		sel = 0
	}
	if sel > 1.0 {
		sel = 1.0
	}
	if e.Negate {
		return 1.0 - sel
	}
	return sel
}

// estIsNull : sélectivité pour IS NULL / IS NOT NULL.
func (ex *Executor) estIsNull(collName string, e *parser.IsNullExpr) float64 {
	field := ExprToFieldName(e.Expr)
	cs := ex.getColumnStats(collName, field)
	if cs != nil && cs.RowCount > 0 {
		nullFrac := float64(cs.NullCount) / float64(cs.RowCount)
		if e.Negate {
			return 1.0 - nullFrac
		}
		return nullFrac
	}
	if e.Negate {
		return 0.95
	}
	return 0.05
}

// estIn : sélectivité pour field IN (v1, v2, ...).
func (ex *Executor) estIn(collName string, e *parser.InExpr) float64 {
	field := ExprToFieldName(e.Expr)
	cs := ex.getColumnStats(collName, field)
	if cs != nil && cs.NDV > 0 {
		sel := float64(len(e.Values)) / float64(cs.NDV)
		if sel > 1.0 {
			sel = 1.0
		}
		if e.Negate {
			return 1.0 - sel
		}
		return sel
	}
	// Fallback
	n := float64(len(e.Values)) * 0.1
	if n > 0.9 {
		n = 0.9
	}
	if e.Negate {
		return 1.0 - n
	}
	return n
}

// estimateSelectivity est le fallback statique (sans stats) pour la rétro-compatibilité.
func estimateSelectivity(where parser.Expr) float64 {
	if where == nil {
		return 1.0
	}
	switch e := where.(type) {
	case *parser.BinaryExpr:
		switch e.Op {
		case parser.TokenEQ:
			return 0.1
		case parser.TokenNEQ:
			return 0.9
		case parser.TokenLT, parser.TokenGT:
			return 0.33
		case parser.TokenLTE, parser.TokenGTE:
			return 0.33
		case parser.TokenAnd:
			return estimateSelectivity(e.Left) * estimateSelectivity(e.Right)
		case parser.TokenOr:
			l := estimateSelectivity(e.Left)
			r := estimateSelectivity(e.Right)
			return l + r - l*r
		default:
			return 0.5
		}
	case *parser.LikeExpr:
		if e.Negate {
			return 0.9
		}
		return 0.25
	case *parser.InExpr:
		n := float64(len(e.Values)) * 0.1
		if n > 0.9 {
			n = 0.9
		}
		if e.Negate {
			return 1.0 - n
		}
		return n
	case *parser.BetweenExpr:
		if e.Negate {
			return 0.75
		}
		return 0.25
	case *parser.IsNullExpr:
		if e.Negate {
			return 0.95
		}
		return 0.05
	case *parser.NotExpr:
		return 1.0 - estimateSelectivity(e.Expr)
	default:
		return 0.5
	}
}

// literalToFloat convertit un token littéral en float64 (NaN si pas numérique).
func literalToFloat(tok parser.Token) float64 {
	v := literalToValue(tok)
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	}
	return math.NaN()
}

// ---------- estimateJoinCardinality (amélioré avec stats) ----------

// estimateJoinCardinality estime le nombre de lignes résultant d'un join.
func (ex *Executor) estimateJoinCardinalityWithStats(
	leftTable, rightTable, leftField, rightField string,
	leftRows, rightRows int64,
	isEqui bool,
) int64 {
	if !isEqui {
		return leftRows * rightRows
	}
	// Avec stats : leftRows × rightRows / max(NDV_left, NDV_right)
	lcs := ex.getColumnStats(leftTable, leftField)
	rcs := ex.getColumnStats(rightTable, rightField)
	if lcs != nil && rcs != nil && lcs.NDV > 0 && rcs.NDV > 0 {
		maxNDV := lcs.NDV
		if rcs.NDV > maxNDV {
			maxNDV = rcs.NDV
		}
		est := (leftRows * rightRows) / maxNDV
		if est < 1 {
			est = 1
		}
		return est
	}
	// Fallback sans stats
	if leftRows > rightRows {
		return leftRows
	}
	return rightRows
}

// estimateJoinCardinality est le fallback sans stats (rétro-compatibilité).
func estimateJoinCardinality(leftRows, rightRows int64, isEqui bool) int64 {
	if isEqui {
		if leftRows > rightRows {
			return leftRows
		}
		return rightRows
	}
	return leftRows * rightRows
}

// ---------- buildExplainPlan (utilise les stats si disponibles) ----------

// buildExplainPlan construit un plan d'exécution détaillé pour un SELECT.
func (ex *Executor) buildExplainPlan(s *parser.SelectStatement) *storage.Document {
	doc := storage.NewDocument()
	doc.Set("type", "SELECT")
	doc.Set("collection", s.From)

	stats := ex.collectStats(s.From)
	doc.Set("estimated_rows", stats.RowCount)
	doc.Set("pages", stats.PageCount)

	// Indiquer si les stats ANALYZE sont disponibles
	if ts := ex.GetTableStats(s.From); ts != nil {
		doc.Set("stats", "ANALYZED")
		doc.Set("analyzed_at", ts.AnalyzedAt.Format(time.RFC3339))
		doc.Set("columns_analyzed", int64(len(ts.Columns)))
	} else {
		doc.Set("stats", "HEURISTIC")
	}

	// Scan strategy
	candidateIDs := ex.resolveIndexLookup(s.From, s.Where, -1)
	if candidateIDs != nil {
		doc.Set("scan", "INDEX LOOKUP")
		doc.Set("index_matches", int64(len(candidateIDs)))
	} else {
		doc.Set("scan", "FULL SCAN")
	}

	// WHERE selectivity (avec stats si disponibles)
	if s.Where != nil {
		sel := ex.estimateSelectivityWithStats(s.From, s.Where)
		afterFilter := int64(float64(stats.RowCount) * sel)
		if afterFilter < 0 {
			afterFilter = 0
		}
		doc.Set("filter", "WHERE")
		doc.Set("selectivity", sel)
		doc.Set("estimated_after_filter", afterFilter)
	}

	// JOINs
	if len(s.Joins) > 0 {
		strategies := ex.JoinStrategy(s)
		currentRows := stats.RowCount

		for i, join := range s.Joins {
			label := "join_" + itoa(i+1)
			tbl := join.Table
			if join.Alias != "" {
				tbl += " " + join.Alias
			}
			strat := "NESTED LOOP"
			if i < len(strategies) {
				strat = strategies[i]
			}

			rightStats := ex.collectStats(join.Table)
			_, _, isEqui := extractEquiJoinKeys(join.Condition)
			estRows := estimateJoinCardinality(currentRows, rightStats.RowCount, isEqui)

			var cost string
			switch strat {
			case "HASH JOIN":
				cost = itoa64(currentRows+rightStats.RowCount) + " (O(n+m))"
			case "INDEX LOOKUP JOIN":
				cost = itoa64(currentRows) + " × log(" + itoa64(rightStats.RowCount) + ")"
			default:
				cost = itoa64(currentRows) + " × " + itoa64(rightStats.RowCount)
			}

			doc.Set(label, strat+" "+join.Type+" "+tbl)
			doc.Set(label+"_cost", cost)
			doc.Set(label+"_right_rows", rightStats.RowCount)
			doc.Set(label+"_estimated_output", estRows)

			currentRows = estRows
		}
	}

	if len(s.GroupBy) > 0 {
		doc.Set("groupBy", "yes")
	}
	if hasAggregateColumns(s.Columns) && len(s.GroupBy) == 0 {
		doc.Set("aggregate", "STANDALONE")
	}
	if s.Having != nil {
		doc.Set("having", "yes")
	}
	if len(s.OrderBy) > 0 {
		doc.Set("orderBy", "IN-MEMORY SORT")
	}
	if s.Distinct {
		doc.Set("distinct", "HASH DEDUP")
	}
	if s.Limit >= 0 {
		doc.Set("limit", int64(s.Limit))
	}
	if s.Offset > 0 {
		doc.Set("offset", int64(s.Offset))
	}

	if len(s.Hints) > 0 {
		hintStrs := hintsToStrings(s.Hints)
		for i, h := range hintStrs {
			doc.Set(fmt.Sprintf("hint_%d", i+1), h)
		}
	}

	hits, misses, _, _ := ex.pager.CacheStats()
	doc.Set("cache_hits", int64(hits))
	doc.Set("cache_misses", int64(misses))

	return doc
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}

func itoa64(n int64) string {
	return fmt.Sprintf("%d", n)
}
