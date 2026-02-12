// Package index fournit un index B+ Tree persistant sur disque.
package index

import (
	"fmt"
	"sync"

	"github.com/Felmond13/novusdb/storage"
)

// RecordLoc représente la localisation physique d'un record.
type RecordLoc struct {
	RecordID uint64
	PageID   uint32
	SlotOff  uint16
}

// Index représente un index sur un champ d'une collection, adossé à un B-Tree.
type Index struct {
	Collection string
	Field      string
	btree      *BTree
	mu         sync.RWMutex
}

// NewIndex crée un index vide avec un nouveau B-Tree.
func NewIndex(collection, field string, pager *storage.Pager) (*Index, error) {
	bt, err := NewBTree(pager)
	if err != nil {
		return nil, err
	}
	return &Index{Collection: collection, Field: field, btree: bt}, nil
}

// OpenIndex ouvre un index existant à partir de la page racine du B-Tree.
func OpenIndex(collection, field string, pager *storage.Pager, rootPageID uint32) *Index {
	return &Index{
		Collection: collection,
		Field:      field,
		btree:      OpenBTree(pager, rootPageID),
	}
}

// RootPageID retourne l'identifiant de la page racine du B-Tree.
func (idx *Index) RootPageID() uint32 {
	return idx.btree.RootPageID
}

// BulkEntry représente une entrée pour le chargement en masse d'un index.
type BulkEntry struct {
	Key      string
	RecordID uint64
	PageID   uint32
	SlotOff  uint16
}

// BulkLoad construit l'index à partir d'un slice d'entrées pré-triées par Key.
// O(N) au lieu de O(N log N) pour N appels à Add.
func (idx *Index) BulkLoad(entries []BulkEntry) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	// Convertir en btreeEntry
	btEntries := make([]btreeEntry, len(entries))
	for i, e := range entries {
		btEntries[i] = btreeEntry{Key: e.Key, RecordID: e.RecordID, PageID: e.PageID, SlotOff: e.SlotOff}
	}
	return idx.btree.BulkLoad(btEntries)
}

// Add ajoute un record avec sa localisation pour la clé donnée.
func (idx *Index) Add(key string, recordID uint64, pageID uint32, slotOff uint16) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.btree.Insert(key, recordID, pageID, slotOff)
}

// Remove supprime un record_id pour la clé donnée.
func (idx *Index) Remove(key string, recordID uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.btree.Remove(key, recordID)
}

// Lookup retourne les localisations associées à une clé.
func (idx *Index) Lookup(key string) ([]RecordLoc, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entries, err := idx.btree.Lookup(key)
	if err != nil {
		return nil, err
	}
	locs := make([]RecordLoc, len(entries))
	for i, e := range entries {
		locs[i] = RecordLoc{RecordID: e.RecordID, PageID: e.PageID, SlotOff: e.SlotOff}
	}
	return locs, nil
}

// LookupLimit retourne au plus 'limit' localisations associées à une clé.
// Si limit <= 0, retourne toutes les localisations (comme Lookup).
func (idx *Index) LookupLimit(key string, limit int) ([]RecordLoc, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entries, err := idx.btree.LookupLimit(key, limit)
	if err != nil {
		return nil, err
	}
	locs := make([]RecordLoc, len(entries))
	for i, e := range entries {
		locs[i] = RecordLoc{RecordID: e.RecordID, PageID: e.PageID, SlotOff: e.SlotOff}
	}
	return locs, nil
}

// RangeScan retourne les localisations dont la clé est dans l'intervalle [minKey, maxKey].
func (idx *Index) RangeScan(minKey, maxKey string) ([]RecordLoc, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entries, err := idx.btree.RangeScan(minKey, maxKey)
	if err != nil {
		return nil, err
	}
	locs := make([]RecordLoc, len(entries))
	for i, e := range entries {
		locs[i] = RecordLoc{RecordID: e.RecordID, PageID: e.PageID, SlotOff: e.SlotOff}
	}
	return locs, nil
}

// AllEntries retourne toutes les entrées de l'index (pour debug/test).
func (idx *Index) AllEntries() map[string][]uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	entries, _ := idx.btree.AllEntries()
	if entries == nil {
		return make(map[string][]uint64)
	}
	return entries
}

// ---------- IndexManager gère tous les index ----------

// Manager gère les index de toutes les collections.
type Manager struct {
	mu      sync.RWMutex
	indexes map[indexKey]*Index
	pager   *storage.Pager
}

type indexKey struct {
	collection string
	field      string
}

// NewManager crée un nouveau gestionnaire d'index.
func NewManager(pager *storage.Pager) *Manager {
	return &Manager{
		indexes: make(map[indexKey]*Index),
		pager:   pager,
	}
}

// CreateIndex crée un nouvel index pour une collection et un champ.
func (m *Manager) CreateIndex(collection, field string) (*Index, error) {
	key := indexKey{collection, field}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[key]; exists {
		return nil, fmt.Errorf("index: index on %s.%s already exists", collection, field)
	}
	idx, err := NewIndex(collection, field, m.pager)
	if err != nil {
		return nil, err
	}
	m.indexes[key] = idx
	return idx, nil
}

// OpenIndex ouvre un index existant (au démarrage).
func (m *Manager) OpenIndex(collection, field string, rootPageID uint32) *Index {
	key := indexKey{collection, field}
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := OpenIndex(collection, field, m.pager, rootPageID)
	m.indexes[key] = idx
	return idx
}

// DropIndex supprime un index.
func (m *Manager) DropIndex(collection, field string) error {
	key := indexKey{collection, field}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[key]; !exists {
		return fmt.Errorf("index: index on %s.%s not found", collection, field)
	}
	delete(m.indexes, key)
	return nil
}

// GetIndex retourne l'index pour une collection et un champ, ou nil.
func (m *Manager) GetIndex(collection, field string) *Index {
	key := indexKey{collection, field}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.indexes[key]
}

// DropAllForCollection supprime tous les index d'une collection.
func (m *Manager) DropAllForCollection(collection string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.indexes {
		if k.collection == collection {
			delete(m.indexes, k)
		}
	}
}

// GetIndexesForCollection retourne tous les index d'une collection.
func (m *Manager) GetIndexesForCollection(collection string) []*Index {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*Index
	for k, idx := range m.indexes {
		if k.collection == collection {
			result = append(result, idx)
		}
	}
	return result
}

// ValueToKey convertit une valeur de champ en clé d'index (string).
func ValueToKey(v interface{}) string {
	if v == nil {
		return "\x00null"
	}
	switch val := v.(type) {
	case string:
		return "s:" + val
	case int64:
		// Format fixe pour tri lexicographique correct
		return fmt.Sprintf("i:%020d", val)
	case float64:
		return fmt.Sprintf("f:%.15e", val)
	case bool:
		if val {
			return "b:true"
		}
		return "b:false"
	default:
		return fmt.Sprintf("?:%v", val)
	}
}
