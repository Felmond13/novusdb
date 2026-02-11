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

// btreeEntry est une paire (clé, recordID) stockée dans une feuille.
type btreeEntry struct {
	Key      string
	RecordID uint64
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
		if int(off)+int(kl)+8 > storage.PageSize {
			break
		}
		key := string(page.Data[off : off+kl])
		off += kl
		rid := binary.LittleEndian.Uint64(page.Data[off:])
		off += 8
		entries = append(entries, btreeEntry{Key: key, RecordID: rid})
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
		s += 2 + len(e.Key) + 8
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

// Lookup retourne tous les recordIDs associés à la clé.
func (bt *BTree) Lookup(key string) ([]uint64, error) {
	page, err := bt.findLeaf(key)
	if err != nil {
		return nil, err
	}
	var result []uint64
	for {
		entries := readLeafEntries(page)
		for _, e := range entries {
			if e.Key == key {
				result = append(result, e.RecordID)
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

// RangeScan retourne les recordIDs dont la clé est dans [minKey, maxKey].
func (bt *BTree) RangeScan(minKey, maxKey string) ([]uint64, error) {
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
	var result []uint64
	for {
		entries := readLeafEntries(page)
		for _, e := range entries {
			if minKey != "" && e.Key < minKey {
				continue
			}
			if maxKey != "" && e.Key > maxKey {
				return result, nil
			}
			result = append(result, e.RecordID)
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

// Insert ajoute une entrée (key, recordID) dans le B-Tree.
func (bt *BTree) Insert(key string, recordID uint64) error {
	split, err := bt.insertRecursive(bt.RootPageID, key, recordID)
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

func (bt *BTree) insertRecursive(pageID uint32, key string, recordID uint64) (*splitResult, error) {
	page, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	if page.Data[btreeNodeTypeOff] == nodeTypeLeaf {
		return bt.insertIntoLeaf(page, key, recordID)
	}
	node := readInternalNode(page)
	childIdx := sort.Search(len(node.keys), func(i int) bool {
		return node.keys[i] > key
	})
	childSplit, err := bt.insertRecursive(node.children[childIdx], key, recordID)
	if err != nil {
		return nil, err
	}
	if childSplit == nil {
		return nil, nil
	}
	return bt.insertIntoInternal(page, node, childIdx, childSplit)
}

func (bt *BTree) insertIntoLeaf(page *storage.Page, key string, recordID uint64) (*splitResult, error) {
	entries := readLeafEntries(page)
	nextLeaf := readLeafNext(page)

	entry := btreeEntry{Key: key, RecordID: recordID}
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
