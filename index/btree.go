// Package index — B+ Tree persistant sur disque via le Pager.
// Chaque nœud occupe une page (4 KB). Les feuilles sont chaînées pour le range scan.
package index

import (
	"encoding/binary"
	"sort"

	"github.com/Felmond13/novusdb/storage"
)

// Offsets dans une page B-Tree (après le PageHeader de 16 octets).
const (
	btreeNodeTypeOff = storage.PageHeaderSize // byte 16 : 0=internal, 1=leaf
	btreeNumKeysOff  = btreeNodeTypeOff + 1   // bytes 17-18 : uint16
	btreeNextLeafOff = btreeNumKeysOff + 2    // bytes 19-22 : uint32 (leaf only)
	leafDataOff      = btreeNextLeafOff + 4   // byte 23
	internalDataOff  = btreeNumKeysOff + 2    // byte 19

	nodeTypeInternal = byte(0)
	nodeTypeLeaf     = byte(1)

	maxLeafPayload     = storage.PageSize - leafDataOff     // 4073
	maxInternalPayload = storage.PageSize - internalDataOff // 4077
)

// btreeEntry est une paire (clé, recordID, localisation) stockée dans une feuille.
type btreeEntry struct {
	Key      string
	RecordID uint64
	PageID   uint32 // page de données contenant le record
	SlotOff  uint16 // offset du slot dans la page
}

// internalNode représente un nœud interne chargé en mémoire.
type internalNode struct {
	keys     []string
	children []uint32 // len == len(keys) + 1
}

// BTree est un B+ Tree adossé aux pages du Pager.
type BTree struct {
	RootPageID uint32
	pager      *storage.Pager
}

// NewBTree crée un B-Tree vide (une feuille racine vide).
func NewBTree(pager *storage.Pager) (*BTree, error) {
	pageID, err := pager.AllocatePage(storage.PageTypeIndex)
	if err != nil {
		return nil, err
	}
	page, err := pager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	page.Data[btreeNodeTypeOff] = nodeTypeLeaf
	binary.LittleEndian.PutUint16(page.Data[btreeNumKeysOff:], 0)
	binary.LittleEndian.PutUint32(page.Data[btreeNextLeafOff:], 0)
	if err := pager.WritePage(page); err != nil {
		return nil, err
	}
	return &BTree{RootPageID: pageID, pager: pager}, nil
}

// OpenBTree ouvre un B-Tree existant à partir de sa page racine.
func OpenBTree(pager *storage.Pager, rootPageID uint32) *BTree {
	return &BTree{RootPageID: rootPageID, pager: pager}
}

// -------- lecture / écriture de nœuds --------

func readLeafEntries(page *storage.Page) []btreeEntry {
	num := binary.LittleEndian.Uint16(page.Data[btreeNumKeysOff:])
	off := uint16(leafDataOff)
	entries := make([]btreeEntry, 0, num)
	for i := 0; i < int(num); i++ {
		if int(off)+2 > storage.PageSize {
			break
		}
		kl := binary.LittleEndian.Uint16(page.Data[off:])
		off += 2
		if int(off)+int(kl)+14 > storage.PageSize {
			break
		}
		key := string(page.Data[off : off+kl])
		off += kl
		rid := binary.LittleEndian.Uint64(page.Data[off:])
		off += 8
		pid := binary.LittleEndian.Uint32(page.Data[off:])
		off += 4
		soff := binary.LittleEndian.Uint16(page.Data[off:])
		off += 2
		entries = append(entries, btreeEntry{Key: key, RecordID: rid, PageID: pid, SlotOff: soff})
	}
	return entries
}

func readLeafNext(page *storage.Page) uint32 {
	return binary.LittleEndian.Uint32(page.Data[btreeNextLeafOff:])
}

func writeLeafNode(page *storage.Page, entries []btreeEntry, nextLeaf uint32) {
	page.Data[btreeNodeTypeOff] = nodeTypeLeaf
	binary.LittleEndian.PutUint16(page.Data[btreeNumKeysOff:], uint16(len(entries)))
	binary.LittleEndian.PutUint32(page.Data[btreeNextLeafOff:], nextLeaf)
	off := uint16(leafDataOff)
	for _, e := range entries {
		kb := []byte(e.Key)
		binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(kb)))
		off += 2
		copy(page.Data[off:], kb)
		off += uint16(len(kb))
		binary.LittleEndian.PutUint64(page.Data[off:], e.RecordID)
		off += 8
		binary.LittleEndian.PutUint32(page.Data[off:], e.PageID)
		off += 4
		binary.LittleEndian.PutUint16(page.Data[off:], e.SlotOff)
		off += 2
	}
}

func readInternalNode(page *storage.Page) internalNode {
	numKeys := binary.LittleEndian.Uint16(page.Data[btreeNumKeysOff:])
	off := uint16(internalDataOff)
	node := internalNode{
		keys:     make([]string, 0, numKeys),
		children: make([]uint32, 0, numKeys+1),
	}
	child0 := binary.LittleEndian.Uint32(page.Data[off:])
	off += 4
	node.children = append(node.children, child0)
	for i := 0; i < int(numKeys); i++ {
		kl := binary.LittleEndian.Uint16(page.Data[off:])
		off += 2
		key := string(page.Data[off : off+kl])
		off += kl
		child := binary.LittleEndian.Uint32(page.Data[off:])
		off += 4
		node.keys = append(node.keys, key)
		node.children = append(node.children, child)
	}
	return node
}

func writeInternalNode(page *storage.Page, node internalNode) {
	page.Data[btreeNodeTypeOff] = nodeTypeInternal
	binary.LittleEndian.PutUint16(page.Data[btreeNumKeysOff:], uint16(len(node.keys)))
	off := uint16(internalDataOff)
	binary.LittleEndian.PutUint32(page.Data[off:], node.children[0])
	off += 4
	for i, key := range node.keys {
		kb := []byte(key)
		binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(kb)))
		off += 2
		copy(page.Data[off:], kb)
		off += uint16(len(kb))
		binary.LittleEndian.PutUint32(page.Data[off:], node.children[i+1])
		off += 4
	}
}

// -------- calculs de taille --------

func leafEntriesSize(entries []btreeEntry) int {
	s := 0
	for _, e := range entries {
		s += 2 + len(e.Key) + 8 + 4 + 2 // keyLen + key + recordID + pageID + slotOff
	}
	return s
}

func internalNodeSize(node internalNode) int {
	s := 4 // child0
	for _, k := range node.keys {
		s += 2 + len(k) + 4
	}
	return s
}

// -------- recherche --------

func (bt *BTree) findLeaf(key string) (*storage.Page, error) {
	pageID := bt.RootPageID
	for {
		page, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		if page.Data[btreeNodeTypeOff] == nodeTypeLeaf {
			return page, nil
		}
		node := readInternalNode(page)
		childIdx := sort.Search(len(node.keys), func(i int) bool {
			return node.keys[i] > key
		})
		pageID = node.children[childIdx]
	}
}

func (bt *BTree) findLeftmostLeaf() (*storage.Page, error) {
	pageID := bt.RootPageID
	for {
		page, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return nil, err
		}
		if page.Data[btreeNodeTypeOff] == nodeTypeLeaf {
			return page, nil
		}
		node := readInternalNode(page)
		pageID = node.children[0]
	}
}

// -------- Lookup --------

// Lookup retourne tous les entrées associées à la clé (avec localisation).
func (bt *BTree) Lookup(key string) ([]btreeEntry, error) {
	page, err := bt.findLeaf(key)
	if err != nil {
		return nil, err
	}
	var result []btreeEntry
	for {
		entries := readLeafEntries(page)
		for _, e := range entries {
			if e.Key == key {
				result = append(result, e)
			} else if e.Key > key {
				return result, nil
			}
		}
		next := readLeafNext(page)
		if next == 0 {
			break
		}
		page, err = bt.pager.ReadPage(next)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// LookupLimit retourne au plus 'limit' entrées associées à la clé.
// Si limit <= 0, retourne toutes les entrées (comme Lookup).
func (bt *BTree) LookupLimit(key string, limit int) ([]btreeEntry, error) {
	if limit <= 0 {
		return bt.Lookup(key)
	}
	page, err := bt.findLeaf(key)
	if err != nil {
		return nil, err
	}
	var result []btreeEntry
	for {
		entries := readLeafEntries(page)
		for _, e := range entries {
			if e.Key == key {
				result = append(result, e)
				if len(result) >= limit {
					return result, nil
				}
			} else if e.Key > key {
				return result, nil
			}
		}
		next := readLeafNext(page)
		if next == 0 {
			break
		}
		page, err = bt.pager.ReadPage(next)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// -------- RangeScan --------

// RangeScan retourne les entrées dont la clé est dans [minKey, maxKey].
func (bt *BTree) RangeScan(minKey, maxKey string) ([]btreeEntry, error) {
	var page *storage.Page
	var err error
	if minKey != "" {
		page, err = bt.findLeaf(minKey)
	} else {
		page, err = bt.findLeftmostLeaf()
	}
	if err != nil {
		return nil, err
	}
	var result []btreeEntry
	for {
		entries := readLeafEntries(page)
		for _, e := range entries {
			if minKey != "" && e.Key < minKey {
				continue
			}
			if maxKey != "" && e.Key > maxKey {
				return result, nil
			}
			result = append(result, e)
		}
		next := readLeafNext(page)
		if next == 0 {
			break
		}
		page, err = bt.pager.ReadPage(next)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// -------- Insert --------

type splitResult struct {
	key       string
	newPageID uint32
}

// Insert ajoute une entrée (key, recordID, localisation) dans le B-Tree.
func (bt *BTree) Insert(key string, recordID uint64, dataPageID uint32, slotOff uint16) error {
	split, err := bt.insertRecursive(bt.RootPageID, key, recordID, dataPageID, slotOff)
	if err != nil {
		return err
	}
	if split != nil {
		newRootID, err := bt.pager.AllocatePage(storage.PageTypeIndex)
		if err != nil {
			return err
		}
		newRoot, err := bt.pager.ReadPage(newRootID)
		if err != nil {
			return err
		}
		writeInternalNode(newRoot, internalNode{
			keys:     []string{split.key},
			children: []uint32{bt.RootPageID, split.newPageID},
		})
		if err := bt.pager.WritePage(newRoot); err != nil {
			return err
		}
		bt.RootPageID = newRootID
	}
	return nil
}

func (bt *BTree) insertRecursive(pageID uint32, key string, recordID uint64, dataPageID uint32, slotOff uint16) (*splitResult, error) {
	page, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	if page.Data[btreeNodeTypeOff] == nodeTypeLeaf {
		return bt.insertIntoLeaf(page, key, recordID, dataPageID, slotOff)
	}
	node := readInternalNode(page)
	childIdx := sort.Search(len(node.keys), func(i int) bool {
		return node.keys[i] > key
	})
	childSplit, err := bt.insertRecursive(node.children[childIdx], key, recordID, dataPageID, slotOff)
	if err != nil {
		return nil, err
	}
	if childSplit == nil {
		return nil, nil
	}
	return bt.insertIntoInternal(page, node, childIdx, childSplit)
}

func (bt *BTree) insertIntoLeaf(page *storage.Page, key string, recordID uint64, dataPageID uint32, slotOff uint16) (*splitResult, error) {
	entries := readLeafEntries(page)
	nextLeaf := readLeafNext(page)

	entry := btreeEntry{Key: key, RecordID: recordID, PageID: dataPageID, SlotOff: slotOff}
	pos := sort.Search(len(entries), func(i int) bool {
		if entries[i].Key == key {
			return entries[i].RecordID >= recordID
		}
		return entries[i].Key >= key
	})

	// Insérer à la position pos
	entries = append(entries, btreeEntry{})
	copy(entries[pos+1:], entries[pos:])
	entries[pos] = entry

	if leafEntriesSize(entries) <= maxLeafPayload {
		writeLeafNode(page, entries, nextLeaf)
		return nil, bt.pager.WritePage(page)
	}

	// Split : couper en deux moitiés
	mid := len(entries) / 2
	leftEntries := make([]btreeEntry, mid)
	copy(leftEntries, entries[:mid])
	rightEntries := make([]btreeEntry, len(entries)-mid)
	copy(rightEntries, entries[mid:])

	newPageID, err := bt.pager.AllocatePage(storage.PageTypeIndex)
	if err != nil {
		return nil, err
	}
	newPage, err := bt.pager.ReadPage(newPageID)
	if err != nil {
		return nil, err
	}

	writeLeafNode(newPage, rightEntries, nextLeaf)
	if err := bt.pager.WritePage(newPage); err != nil {
		return nil, err
	}

	writeLeafNode(page, leftEntries, newPageID)
	if err := bt.pager.WritePage(page); err != nil {
		return nil, err
	}

	return &splitResult{
		key:       rightEntries[0].Key,
		newPageID: newPageID,
	}, nil
}

func (bt *BTree) insertIntoInternal(page *storage.Page, node internalNode, childIdx int, split *splitResult) (*splitResult, error) {
	// Insérer la clé de séparation et le nouveau child
	node.keys = append(node.keys, "")
	copy(node.keys[childIdx+1:], node.keys[childIdx:])
	node.keys[childIdx] = split.key

	node.children = append(node.children, 0)
	copy(node.children[childIdx+2:], node.children[childIdx+1:])
	node.children[childIdx+1] = split.newPageID

	if internalNodeSize(node) <= maxInternalPayload {
		writeInternalNode(page, node)
		return nil, bt.pager.WritePage(page)
	}

	// Split du nœud interne
	mid := len(node.keys) / 2
	pushUpKey := node.keys[mid]

	leftNode := internalNode{
		keys:     make([]string, mid),
		children: make([]uint32, mid+1),
	}
	copy(leftNode.keys, node.keys[:mid])
	copy(leftNode.children, node.children[:mid+1])

	rightNode := internalNode{
		keys:     make([]string, len(node.keys)-mid-1),
		children: make([]uint32, len(node.children)-mid-1),
	}
	copy(rightNode.keys, node.keys[mid+1:])
	copy(rightNode.children, node.children[mid+1:])

	newPageID, err := bt.pager.AllocatePage(storage.PageTypeIndex)
	if err != nil {
		return nil, err
	}
	newPage, err := bt.pager.ReadPage(newPageID)
	if err != nil {
		return nil, err
	}

	writeInternalNode(newPage, rightNode)
	if err := bt.pager.WritePage(newPage); err != nil {
		return nil, err
	}

	writeInternalNode(page, leftNode)
	if err := bt.pager.WritePage(page); err != nil {
		return nil, err
	}

	return &splitResult{
		key:       pushUpKey,
		newPageID: newPageID,
	}, nil
}

// -------- BulkLoad --------

// BulkLoad construit le B-Tree à partir d'un slice d'entrées DÉJÀ TRIÉES par key.
// Beaucoup plus rapide que N appels à Insert : O(N) au lieu de O(N log N).
func (bt *BTree) BulkLoad(entries []btreeEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// 1) Créer les feuilles séquentiellement
	type leafInfo struct {
		pageID   uint32
		firstKey string
	}
	var leaves []leafInfo
	var currentEntries []btreeEntry

	flushLeaf := func(nextLeaf uint32) error {
		if len(currentEntries) == 0 {
			return nil
		}
		pageID, err := bt.pager.AllocatePage(storage.PageTypeIndex)
		if err != nil {
			return err
		}
		page, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return err
		}
		writeLeafNode(page, currentEntries, nextLeaf)
		if err := bt.pager.WritePage(page); err != nil {
			return err
		}
		leaves = append(leaves, leafInfo{pageID: pageID, firstKey: currentEntries[0].Key})
		return nil
	}

	// Remplir les feuilles (target ~75% du payload max pour laisser de la place aux insertions futures)
	targetPayload := maxLeafPayload * 3 / 4
	currentSize := 0

	for i := range entries {
		entrySize := 2 + len(entries[i].Key) + 8 + 4 + 2
		if currentSize+entrySize > targetPayload && len(currentEntries) > 0 {
			// Feuille pleine → flush (nextLeaf sera patché après)
			if err := flushLeaf(0); err != nil {
				return err
			}
			currentEntries = nil
			currentSize = 0
		}
		currentEntries = append(currentEntries, entries[i])
		currentSize += entrySize
	}
	// Flush la dernière feuille
	if err := flushLeaf(0); err != nil {
		return err
	}

	// 2) Chaîner les feuilles (nextLeaf pointers) — en ordre inverse
	for i := len(leaves) - 2; i >= 0; i-- {
		page, err := bt.pager.ReadPage(leaves[i].pageID)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(page.Data[btreeNextLeafOff:], leaves[i+1].pageID)
		if err := bt.pager.WritePage(page); err != nil {
			return err
		}
	}

	// 3) Construire les nœuds internes bottom-up
	if len(leaves) == 1 {
		bt.RootPageID = leaves[0].pageID
		return nil
	}

	// Niveau courant = les pageIDs des feuilles
	type childEntry struct {
		pageID uint32
		key    string // clé de séparation (première clé de ce child, sauf child[0])
	}
	level := make([]childEntry, len(leaves))
	for i, l := range leaves {
		level[i] = childEntry{pageID: l.pageID, key: l.firstKey}
	}

	for len(level) > 1 {
		var nextLevel []childEntry
		i := 0
		for i < len(level) {
			// Combien d'enfants peut contenir un nœud interne ?
			// Chaque clé coûte 2 + len(key) + 4 bytes, plus 4 bytes pour child0
			node := internalNode{
				children: []uint32{level[i].pageID},
			}
			i++
			for i < len(level) {
				keyCost := 2 + len(level[i].key) + 4
				if internalNodeSize(node)+keyCost > maxInternalPayload {
					break
				}
				node.keys = append(node.keys, level[i].key)
				node.children = append(node.children, level[i].pageID)
				i++
			}

			pageID, err := bt.pager.AllocatePage(storage.PageTypeIndex)
			if err != nil {
				return err
			}
			page, err := bt.pager.ReadPage(pageID)
			if err != nil {
				return err
			}
			writeInternalNode(page, node)
			if err := bt.pager.WritePage(page); err != nil {
				return err
			}

			firstKey := ""
			if len(node.keys) > 0 {
				firstKey = node.keys[0]
			}
			nextLevel = append(nextLevel, childEntry{pageID: pageID, key: firstKey})
		}
		level = nextLevel
	}

	bt.RootPageID = level[0].pageID
	return nil
}

// -------- Remove --------

// Remove supprime une entrée (key, recordID) de la feuille.
// Pas de rééquilibrage — les feuilles vides restent (compactables via VACUUM).
func (bt *BTree) Remove(key string, recordID uint64) error {
	page, err := bt.findLeaf(key)
	if err != nil {
		return err
	}
	entries := readLeafEntries(page)
	nextLeaf := readLeafNext(page)
	for i, e := range entries {
		if e.Key == key && e.RecordID == recordID {
			entries = append(entries[:i], entries[i+1:]...)
			writeLeafNode(page, entries, nextLeaf)
			return bt.pager.WritePage(page)
		}
	}
	return nil // not found — nothing to do
}

// -------- AllEntries (pour tests/debug) --------

// AllEntries parcourt toutes les feuilles et retourne map[key][]recordID.
func (bt *BTree) AllEntries() (map[string][]uint64, error) {
	page, err := bt.findLeftmostLeaf()
	if err != nil {
		return nil, err
	}
	result := make(map[string][]uint64)
	for {
		entries := readLeafEntries(page)
		for _, e := range entries {
			result[e.Key] = append(result[e.Key], e.RecordID)
		}
		next := readLeafNext(page)
		if next == 0 {
			break
		}
		page, err = bt.pager.ReadPage(next)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
