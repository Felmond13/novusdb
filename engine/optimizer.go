package engine

import (
	"math"

	"github.com/Felmond13/novusdb/index"
	"github.com/Felmond13/novusdb/parser"
)

// ---------- Cost Model Constants ----------

const (
	// costSeqPage est le coût d'une lecture séquentielle de page (scan linéaire).
	costSeqPage = 1.0

	// costRandPage est le coût d'une lecture aléatoire (index lookup).
	// Plus cher car pas de locality (SSD: ~2-4x, HDD: ~10-100x).
	costRandPage = 4.0

	// costCPUPerRow est le coût CPU de traitement d'une ligne (filtrage, comparaison).
	costCPUPerRow = 0.01

	// costHashBuild est le coût CPU de construction d'un hash (par ligne).
	costHashBuild = 0.02

	// costHashProbe est le coût CPU de lookup dans la hash table (par ligne).
	costHashProbe = 0.01

	// selectivityThreshold est le seuil au-dessus duquel on préfère un full scan.
	// Si on estime qu'on va lire >30% de la table, un full scan séquentiel est plus efficace.
	selectivityThreshold = 0.30
)

// ---------- Scan Cost-Based Optimizer ----------

// shouldUseIndex décide si un index lookup est plus rentable qu'un full scan.
// Retourne true si l'index est préférable.
func (ex *Executor) shouldUseIndex(collName string, where parser.Expr, indexLocs []index.RecordLoc) bool {
	if indexLocs == nil {
		return false
	}

	// L'index a déjà retourné les localisations exactes — utiliser ce nombre réel
	matchCount := int64(len(indexLocs))

	stats := ex.collectStats(collName)
	if stats.RowCount == 0 || stats.PageCount == 0 {
		return true // table vide, index OK
	}

	// Petite table (≤ 2 pages) : le coût est négligeable, préférer l'index
	if stats.PageCount <= 2 {
		return true
	}

	// Si l'index retourne peu de résultats par rapport à la table, toujours utiliser l'index
	// Seuil : si on lit moins de 30% des lignes, l'index est rentable
	if matchCount > 0 && float64(matchCount)/float64(stats.RowCount) <= selectivityThreshold {
		return true
	}

	// Pour les grosses tables avec haute sélectivité, comparer les coûts
	// Coût du full scan : lire toutes les pages séquentiellement
	fullScanCost := float64(stats.PageCount)*costSeqPage + float64(stats.RowCount)*costCPUPerRow

	// Coût de l'index lookup : lectures aléatoires pour chaque match
	distinctPages := estimateDistinctPages(matchCount, stats.PageCount)
	indexCost := float64(distinctPages)*costRandPage + float64(matchCount)*costCPUPerRow

	return indexCost < fullScanCost
}

// estimateDistinctPages estime le nombre de pages distinctes touchées par N accès aléatoires
// sur un total de P pages. Utilise le modèle "birthday problem" :
// distinct ≈ P × (1 - (1 - 1/P)^N)
func estimateDistinctPages(nRows, totalPages int64) int64 {
	if totalPages <= 0 {
		return 0
	}
	if nRows >= totalPages {
		return totalPages
	}
	p := float64(totalPages)
	n := float64(nRows)
	distinct := p * (1.0 - math.Pow(1.0-1.0/p, n))
	d := int64(distinct)
	if d < 1 {
		d = 1
	}
	return d
}

// ---------- Join Cost-Based Optimizer ----------

// joinCost représente le coût estimé d'une stratégie de jointure.
type joinCost struct {
	strategy joinStrategy
	cost     float64
}

// chooseJoinStrategyCBO choisit la stratégie de jointure la moins coûteuse.
// Respecte les hints si présents, sinon utilise le modèle de coût.
func (ex *Executor) chooseJoinStrategyCBO(
	leftTable, rightTable string,
	cond parser.Expr,
	leftName, rightName string,
	leftRows int64,
	hints []parser.QueryHint,
) (joinStrategy, string, string) {
	leftField, rightField, isEqui := extractEquiJoinKeys(cond)
	if !isEqui {
		return strategyNestedLoop, "", ""
	}

	lf, rf := normalizeJoinFields(leftField, rightField, leftName, rightName)

	// Hints forcent la stratégie
	if hasHint(hints, parser.HintNestedLoop) {
		return strategyNestedLoop, lf, rf
	}
	if hasHint(hints, parser.HintHashJoin) {
		return strategyHashJoin, lf, rf
	}

	rightStats := ex.collectStats(rightTable)
	rightRows := rightStats.RowCount
	if rightRows == 0 {
		rightRows = 1
	}

	// Pour un equi-join, comparer les stratégies par coût
	rightFieldBare := stripPrefix(rf, rightName)
	idx := ex.indexMgr.GetIndex(rightTable, rightFieldBare)

	// Index Lookup Join est préféré si un index existe (O(n × log m) avec locality)
	if idx != nil {
		return strategyIndexLookup, lf, rf
	}

	// Hash Join est préféré pour les equi-joins sans index (O(n+m) vs O(n×m))
	// Nested Loop n'est utilisé que pour les non-equi joins (déjà filtré au début)
	return strategyHashJoin, lf, rf
}

// ---------- Constraint-Aware Optimizer ----------

// estimateJoinCardinality estime le nombre de lignes résultant d'un JOIN.
// Si PK/UNIQUE est impliqué, on peut donner une estimation exacte.
func (ex *Executor) estimateJoinCardinality(leftTable, rightTable, leftField, rightField string) int64 {
	leftStats := ex.collectStats(leftTable)
	rightStats := ex.collectStats(rightTable)
	leftRows := leftStats.RowCount
	rightRows := rightStats.RowCount
	if leftRows == 0 || rightRows == 0 {
		return 0
	}

	// Si le champ droit est une PK ou UNIQUE → au plus 1 match par ligne gauche
	if pk := ex.getPrimaryKey(rightTable); pk != nil {
		for _, col := range pk.Columns {
			if col == rightField {
				return leftRows // chaque ligne gauche matche au plus 1 ligne droite
			}
		}
	}
	for _, uc := range ex.getUniqueConstraints(rightTable) {
		for _, col := range uc.Columns {
			if col == rightField {
				return leftRows
			}
		}
	}

	// Si le champ gauche est une PK ou UNIQUE → au plus 1 match par ligne droite
	if pk := ex.getPrimaryKey(leftTable); pk != nil {
		for _, col := range pk.Columns {
			if col == leftField {
				return rightRows
			}
		}
	}
	for _, uc := range ex.getUniqueConstraints(leftTable) {
		for _, col := range uc.Columns {
			if col == leftField {
				return rightRows
			}
		}
	}

	// Pas de PK/UNIQUE → estimation classique : n*m / max(NDV_left, NDV_right)
	ndvLeft := int64(0)
	ndvRight := int64(0)
	if cs := ex.getColumnStats(leftTable, leftField); cs != nil {
		ndvLeft = cs.NDV
	}
	if cs := ex.getColumnStats(rightTable, rightField); cs != nil {
		ndvRight = cs.NDV
	}
	ndv := ndvLeft
	if ndvRight > ndv {
		ndv = ndvRight
	}
	if ndv <= 0 {
		ndv = 1
	}
	card := (leftRows * rightRows) / ndv
	if card < 1 {
		card = 1
	}
	return card
}

// isPointLookupOnPK vérifie si un WHERE est un point lookup sur une colonne PK/UNIQUE.
// Si oui, le résultat est garanti 0 ou 1 ligne → early termination possible.
func (ex *Executor) isPointLookupOnPK(table string, where parser.Expr) bool {
	if where == nil {
		return false
	}
	bin, ok := where.(*parser.BinaryExpr)
	if !ok || bin.Op != parser.TokenEQ {
		return false
	}
	field := ExprToFieldName(bin.Left)
	if field == "" {
		return false
	}
	// Vérifier PK
	if pk := ex.getPrimaryKey(table); pk != nil {
		for _, col := range pk.Columns {
			if col == field {
				return true
			}
		}
	}
	// Vérifier UNIQUE
	for _, uc := range ex.getUniqueConstraints(table) {
		if len(uc.Columns) == 1 && uc.Columns[0] == field {
			return true
		}
	}
	return false
}

// constraintAwareNDV retourne le NDV d'une colonne en utilisant les contraintes.
// PK/UNIQUE → NDV = RowCount (gratuit, pas besoin d'ANALYZE).
func (ex *Executor) constraintAwareNDV(table, field string) int64 {
	stats := ex.collectStats(table)
	if stats.RowCount == 0 {
		return 0
	}

	// PK → NDV = RowCount
	if pk := ex.getPrimaryKey(table); pk != nil {
		for _, col := range pk.Columns {
			if col == field {
				return stats.RowCount
			}
		}
	}
	// UNIQUE → NDV = RowCount
	for _, uc := range ex.getUniqueConstraints(table) {
		for _, col := range uc.Columns {
			if col == field {
				return stats.RowCount
			}
		}
	}

	// Fallback : ANALYZE stats
	if cs := ex.getColumnStats(table, field); cs != nil && cs.NDV > 0 {
		return cs.NDV
	}

	return 0 // inconnu
}

// strategyName retourne le nom lisible d'une stratégie de jointure.
func strategyName(s joinStrategy) string {
	switch s {
	case strategyHashJoin:
		return "HASH JOIN"
	case strategyIndexLookup:
		return "INDEX LOOKUP JOIN"
	case strategyNestedLoop:
		return "NESTED LOOP"
	default:
		return "UNKNOWN"
	}
}
