package engine

import (
	"fmt"

	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// CollectionStats contient les statistiques d'une collection.
type CollectionStats struct {
	Name      string
	RowCount  int64
	PageCount int64
}

// collectStats calcule les statistiques d'une collection (nombre de rows et pages).
func (ex *Executor) collectStats(collName string) CollectionStats {
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

// estimateSelectivity estime la sélectivité d'un filtre WHERE (fraction de lignes retournées).
// Retourne un float64 entre 0.0 et 1.0.
func estimateSelectivity(where parser.Expr) float64 {
	if where == nil {
		return 1.0
	}

	switch e := where.(type) {
	case *parser.BinaryExpr:
		switch e.Op {
		case parser.TokenEQ:
			return 0.1 // 10% des lignes pour une égalité
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
			return l + r - l*r // P(A∪B) = P(A) + P(B) - P(A∩B)
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

// estimateJoinCardinality estime le nombre de lignes résultant d'un join.
func estimateJoinCardinality(leftRows, rightRows int64, isEqui bool) int64 {
	if isEqui {
		// Pour un equi-join, on estime que chaque ligne gauche matche ~1 ligne droite
		// La cardinalité est max(leftRows, rightRows) en moyenne
		if leftRows > rightRows {
			return leftRows
		}
		return rightRows
	}
	// Cross join (worst case) : leftRows × rightRows
	return leftRows * rightRows
}

// buildExplainPlan construit un plan d'exécution détaillé pour un SELECT.
func (ex *Executor) buildExplainPlan(s *parser.SelectStatement) *storage.Document {
	doc := storage.NewDocument()
	doc.Set("type", "SELECT")
	doc.Set("collection", s.From)

	// Statistiques de la table principale
	stats := ex.collectStats(s.From)
	doc.Set("estimated_rows", stats.RowCount)
	doc.Set("pages", stats.PageCount)

	// Scan strategy
	candidateIDs := ex.resolveIndexLookup(s.From, s.Where, -1)
	if candidateIDs != nil {
		doc.Set("scan", "INDEX LOOKUP")
		doc.Set("index_matches", int64(len(candidateIDs)))
	} else {
		doc.Set("scan", "FULL SCAN")
	}

	// WHERE selectivity
	if s.Where != nil {
		sel := estimateSelectivity(s.Where)
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

			// Coût estimé
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

	// Hints
	if len(s.Hints) > 0 {
		hintStrs := hintsToStrings(s.Hints)
		for i, h := range hintStrs {
			doc.Set(fmt.Sprintf("hint_%d", i+1), h)
		}
	}

	// Cache stats
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
