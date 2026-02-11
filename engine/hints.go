package engine

import (
	"strconv"
	"sync"

	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// hasHint vérifie si un hint spécifique est présent.
func hasHint(hints []parser.QueryHint, t parser.HintType) bool {
	for _, h := range hints {
		if h.Type == t {
			return true
		}
	}
	return false
}

// getHintParam retourne le paramètre d'un hint, ou "" si absent.
func getHintParam(hints []parser.QueryHint, t parser.HintType) string {
	for _, h := range hints {
		if h.Type == t {
			return h.Param
		}
	}
	return ""
}

// parallelDegree retourne le degré de parallélisme demandé par le hint PARALLEL.
func parallelDegree(hints []parser.QueryHint) int {
	param := getHintParam(hints, parser.HintParallel)
	if param == "" {
		return 4 // défaut
	}
	n, err := strconv.Atoi(param)
	if err != nil || n < 1 {
		return 4
	}
	return n
}

// parallelScan exécute un scan parallèle d'une collection en N goroutines.
// Chaque goroutine scanne un sous-ensemble des pages.
func (ex *Executor) parallelScan(collName string, where parser.Expr, degree int) ([]*ResultDoc, error) {
	coll := ex.pager.GetCollection(collName)
	if coll == nil {
		return nil, nil
	}

	// Collecter tous les page IDs
	var pageIDs []uint32
	pageID := coll.FirstPageID
	for pageID != 0 {
		pageIDs = append(pageIDs, pageID)
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		pageID = page.NextPageID()
	}

	if len(pageIDs) == 0 {
		return nil, nil
	}

	// Ajuster le degré si plus de goroutines que de pages
	if degree > len(pageIDs) {
		degree = len(pageIDs)
	}

	// Répartir les pages en chunks
	type chunk struct {
		pages []uint32
	}
	chunks := make([]chunk, degree)
	for i, pid := range pageIDs {
		chunks[i%degree].pages = append(chunks[i%degree].pages, pid)
	}

	// Scanner en parallèle
	type scanOutput struct {
		docs []*ResultDoc
		err  error
	}
	results := make([]scanOutput, degree)
	var wg sync.WaitGroup

	for i := 0; i < degree; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var docs []*ResultDoc
			for _, pid := range chunks[idx].pages {
				page, err := ex.pager.ReadPage(pid)
				if err != nil {
					results[idx] = scanOutput{err: err}
					return
				}
				slots := page.ReadRecords()
				for _, slot := range slots {
					if slot.Deleted {
						continue
					}
					doc, err := storage.Decode(slot.Data)
					if err != nil {
						continue
					}
					match, err := EvalExpr(where, doc)
					if err != nil {
						results[idx] = scanOutput{err: err}
						return
					}
					if match {
						docs = append(docs, &ResultDoc{RecordID: slot.RecordID, Doc: doc})
					}
				}
			}
			results[idx] = scanOutput{docs: docs}
		}(i)
	}

	wg.Wait()

	// Fusionner les résultats
	var merged []*ResultDoc
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		merged = append(merged, r.docs...)
	}
	return merged, nil
}

// hintsToStrings retourne une description textuelle des hints actifs.
func hintsToStrings(hints []parser.QueryHint) []string {
	var out []string
	for _, h := range hints {
		switch h.Type {
		case parser.HintParallel:
			if h.Param != "" {
				out = append(out, "PARALLEL("+h.Param+")")
			} else {
				out = append(out, "PARALLEL(4)")
			}
		case parser.HintNoCache:
			out = append(out, "NO_CACHE")
		case parser.HintFullScan:
			out = append(out, "FULL_SCAN")
		case parser.HintForceIndex:
			out = append(out, "FORCE_INDEX("+h.Param+")")
		case parser.HintHashJoin:
			out = append(out, "HASH_JOIN")
		case parser.HintNestedLoop:
			out = append(out, "NESTED_LOOP")
		}
	}
	return out
}
