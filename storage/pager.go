package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/klauspost/compress/snappy"
)

// MetaPage layout (page 0) :
//   [PageHeader 16 bytes]
//   [16] totalPages  uint32
//   [20] numCollections uint16
//   [22..] pour chaque collection :
//       [nameLen uint16][name bytes][firstPageID uint32][nextRecordID uint64]

const metaHeaderOffset = PageHeaderSize

// CollectionMeta stocke les métadonnées d'une collection.
type CollectionMeta struct {
	Name         string
	FirstPageID  uint32
	NextRecordID uint64
}

// Pager gère l'accès au fichier paginé unique.
// IndexDef décrit un index persisté (collection + champ).
type IndexDef struct {
	Collection string
	Field      string
	RootPageID uint32
}

// Pager gère l'accès au fichier paginé unique.
type Pager struct {
	mu   sync.RWMutex // RWMutex : multi-reader / single-writer
	file StorageFile
	path string
	wal  *WAL      // Write-Ahead Log (nil si désactivé)
	lock *fileLock // OS-level file lock (inter-process)

	totalPages  uint32
	collections map[string]*CollectionMeta
	indexDefs   []IndexDef        // définitions d'index persistées
	viewDefs    map[string]string // nom de vue → requête SQL source
	readOnly    bool              // true = reject all writes

	// LRU page cache
	cache *lruCache

	// Transaction support
	inTx          bool
	txUndoLog     map[uint32][PageSize]byte  // pageID → before-image
	txNewPages    map[uint32]bool            // pages allouées pendant la tx
	txTotalPages  uint32                     // totalPages au début de la tx
	txCollections map[string]*CollectionMeta // snapshot des collections
	txIndexDefs   []IndexDef                 // snapshot des indexDefs
	txViewDefs    map[string]string          // snapshot des viewDefs
}

// ErrReadOnly is returned when a write operation is attempted on a read-only database.
var ErrReadOnly = errors.New("pager: database is read-only")

// OpenPager ouvre ou crée le fichier de base de données.
func OpenPager(path string) (*Pager, error) {
	return openPager(path, false)
}

// OpenPagerReadOnly ouvre le fichier de base de données en mode lecture seule.
// Toute tentative d'écriture retournera ErrReadOnly.
func OpenPagerReadOnly(path string) (*Pager, error) {
	return openPager(path, true)
}

func openPager(path string, readOnly bool) (*Pager, error) {
	// Acquire OS-level file lock to prevent concurrent access from another process
	lock, err := lockFile(path)
	if err != nil {
		return nil, err
	}

	flags := os.O_RDWR | os.O_CREATE
	if readOnly {
		flags = os.O_RDONLY
	}
	file, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		lock.unlock()
		return nil, fmt.Errorf("pager: cannot open file: %w", err)
	}

	p := &Pager{
		file:        file,
		path:        path,
		lock:        lock,
		collections: make(map[string]*CollectionMeta),
		viewDefs:    make(map[string]string),
		cache:       newLRUCache(1024), // 1024 pages = 4 MB cache
		readOnly:    readOnly,
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		if readOnly {
			file.Close()
			lock.unlock()
			return nil, errors.New("pager: cannot create database in read-only mode")
		}
		// Nouveau fichier : créer la meta page
		if err := p.initMetaPage(); err != nil {
			file.Close()
			lock.unlock()
			return nil, err
		}
	} else {
		if err := p.loadMetaPage(); err != nil {
			file.Close()
			lock.unlock()
			return nil, err
		}
	}

	if !readOnly {
		// Ouvrir le WAL
		wal, err := OpenWAL(path)
		if err != nil {
			file.Close()
			lock.unlock()
			return nil, fmt.Errorf("pager: %w", err)
		}
		p.wal = wal

		// Recovery : rejouer le WAL si nécessaire
		if err := p.recoverFromWAL(); err != nil {
			wal.Close()
			file.Close()
			lock.unlock()
			return nil, fmt.Errorf("pager: recovery failed: %w", err)
		}
	}

	return p, nil
}

// OpenPagerMemory crée un pager entièrement en mémoire (sans fichier ni WAL).
// Utilisé pour le mode WASM / playground.
func OpenPagerMemory() (*Pager, error) {
	mem := NewMemFile()
	p := &Pager{
		file:        mem,
		path:        ":memory:",
		collections: make(map[string]*CollectionMeta),
		viewDefs:    make(map[string]string),
		cache:       newLRUCache(1024),
	}
	if err := p.initMetaPage(); err != nil {
		return nil, err
	}
	// pas de WAL en mode mémoire
	return p, nil
}

// Close ferme le fichier proprement.
// Effectue un checkpoint final puis ferme le WAL et le fichier data.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.readOnly {
		if err := p.flushMeta(); err != nil {
			return err
		}
		if err := p.file.Sync(); err != nil {
			return err
		}
	}
	if p.wal != nil {
		// Checkpoint final : tronquer le WAL car tout est persisté
		p.wal.Truncate()
		p.wal.Close()
	}
	fileErr := p.file.Close()
	if p.lock != nil {
		p.lock.unlock()
	}
	return fileErr
}

// IsReadOnly returns true if the database is opened in read-only mode.
func (p *Pager) IsReadOnly() bool {
	return p.readOnly
}

// ReadPage lit une page depuis le fichier.
// Utilise RLock pour permettre des lectures concurrentes.
func (p *Pager) ReadPage(pageID uint32) (*Page, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.readPageUnlocked(pageID)
}

func (p *Pager) readPageUnlocked(pageID uint32) (*Page, error) {
	if pageID >= p.totalPages {
		return nil, fmt.Errorf("pager: page %d out of range (total=%d)", pageID, p.totalPages)
	}
	// LRU cache hit?
	if data, ok := p.cache.get(pageID); ok {
		page := &Page{}
		page.Data = data
		return page, nil
	}
	// Cache miss → lecture disque
	page := &Page{}
	_, err := p.file.ReadAt(page.Data[:], int64(pageID)*PageSize)
	if err != nil {
		return nil, fmt.Errorf("pager: read page %d: %w", pageID, err)
	}
	p.cache.put(pageID, page.Data)
	return page, nil
}

// WritePage écrit une page sur disque.
func (p *Pager) WritePage(page *Page) error {
	if p.readOnly {
		return ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writePageUnlocked(page)
}

func (p *Pager) writePageUnlocked(page *Page) error {
	pid := page.PageID()
	if pid >= p.totalPages {
		return fmt.Errorf("pager: page %d out of range (total=%d)", pid, p.totalPages)
	}
	// Transaction : capturer le before-image si on est dans une tx
	if p.inTx {
		if _, exists := p.txUndoLog[pid]; !exists {
			if !p.txNewPages[pid] {
				// Lire la version actuelle depuis le disque
				old, err := p.readPageUnlocked(pid)
				if err == nil {
					p.txUndoLog[pid] = old.Data
				}
			}
		}
	}
	// WAL : logger l'after-image avant d'écrire dans le fichier data
	if p.wal != nil {
		if _, err := p.wal.LogPageWrite(pid, page.Data[:]); err != nil {
			return fmt.Errorf("pager: wal log: %w", err)
		}
	}
	_, err := p.file.WriteAt(page.Data[:], int64(pid)*PageSize)
	if err == nil {
		p.cache.put(pid, page.Data)
	}
	return err
}

// AllocatePage alloue une nouvelle page et retourne son ID.
func (p *Pager) AllocatePage(ptype PageType) (uint32, error) {
	if p.readOnly {
		return 0, ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocatePageUnlocked(ptype)
}

// allocatePageUnlocked alloue une page sans prendre le lock (doit être appelé sous lock).
func (p *Pager) allocatePageUnlocked(ptype PageType) (uint32, error) {
	newID := p.totalPages
	p.totalPages++ // incrémenter d'abord pour que writePageUnlocked accepte la page
	page := NewPage(ptype, newID)

	if p.inTx {
		p.txNewPages[newID] = true
	}

	if err := p.writePageUnlocked(page); err != nil {
		p.totalPages-- // rollback en cas d'erreur
		if p.inTx {
			delete(p.txNewPages, newID)
		}
		return 0, fmt.Errorf("pager: allocate page: %w", err)
	}
	return newID, nil
}

// GetCollection retourne les métadonnées d'une collection, ou nil.
func (p *Pager) GetCollection(name string) *CollectionMeta {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.collections[name]
}

// CreateCollection crée une nouvelle collection avec une première data page.
func (p *Pager) CreateCollection(name string) (*CollectionMeta, error) {
	if p.readOnly {
		return nil, ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// Vérifier si elle existe déjà
	if _, exists := p.collections[name]; exists {
		return nil, fmt.Errorf("pager: collection %q already exists", name)
	}

	// Allouer la première data page (inline, car on tient déjà le lock)
	pageID, err := p.allocatePageUnlocked(PageTypeData)
	if err != nil {
		return nil, err
	}

	meta := &CollectionMeta{
		Name:         name,
		FirstPageID:  pageID,
		NextRecordID: 1,
	}
	p.collections[name] = meta

	if err := p.flushMeta(); err != nil {
		return nil, err
	}
	return meta, nil
}

// GetOrCreateCollection retourne ou crée une collection.
func (p *Pager) GetOrCreateCollection(name string) (*CollectionMeta, error) {
	if c := p.GetCollection(name); c != nil {
		return c, nil
	}
	return p.CreateCollection(name)
}

// NextRecordID retourne et incrémente l'ID du prochain record pour une collection.
func (p *Pager) NextRecordID(collName string) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	c, ok := p.collections[collName]
	if !ok {
		return 0, fmt.Errorf("pager: collection %q not found", collName)
	}
	id := c.NextRecordID
	c.NextRecordID++
	return id, nil
}

// FlushMeta persiste les métadonnées sur disque. Doit être appelé sous lock.
func (p *Pager) FlushMeta() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flushMeta()
}

func (p *Pager) flushMeta() error {
	page := NewPage(PageTypeMeta, 0)

	off := uint16(metaHeaderOffset)
	binary.LittleEndian.PutUint32(page.Data[off:], p.totalPages)
	off += 4
	binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(p.collections)))
	off += 2

	for _, c := range p.collections {
		nameBytes := []byte(c.Name)
		binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(nameBytes)))
		off += 2
		copy(page.Data[off:], nameBytes)
		off += uint16(len(nameBytes))
		binary.LittleEndian.PutUint32(page.Data[off:], c.FirstPageID)
		off += 4
		binary.LittleEndian.PutUint64(page.Data[off:], c.NextRecordID)
		off += 8
	}

	// Index definitions : [numIndexes:2] puis [collLen:2][coll][fieldLen:2][field]
	binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(p.indexDefs)))
	off += 2
	for _, idx := range p.indexDefs {
		collBytes := []byte(idx.Collection)
		binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(collBytes)))
		off += 2
		copy(page.Data[off:], collBytes)
		off += uint16(len(collBytes))
		fieldBytes := []byte(idx.Field)
		binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(fieldBytes)))
		off += 2
		copy(page.Data[off:], fieldBytes)
		off += uint16(len(fieldBytes))
		binary.LittleEndian.PutUint32(page.Data[off:], idx.RootPageID)
		off += 4
	}

	// View definitions : [numViews:2] puis [nameLen:2][name][queryLen:2][query]
	binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(p.viewDefs)))
	off += 2
	for name, query := range p.viewDefs {
		nameBytes := []byte(name)
		binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(nameBytes)))
		off += 2
		copy(page.Data[off:], nameBytes)
		off += uint16(len(nameBytes))
		queryBytes := []byte(query)
		binary.LittleEndian.PutUint16(page.Data[off:], uint16(len(queryBytes)))
		off += 2
		copy(page.Data[off:], queryBytes)
		off += uint16(len(queryBytes))
	}

	// WAL : logger la meta page avant écriture
	if p.wal != nil {
		if _, err := p.wal.LogPageWrite(0, page.Data[:]); err != nil {
			return fmt.Errorf("pager: wal log meta: %w", err)
		}
	}

	_, err := p.file.WriteAt(page.Data[:], 0)
	return err
}

func (p *Pager) initMetaPage() error {
	p.totalPages = 1 // page 0 = meta
	return p.flushMeta()
}

func (p *Pager) loadMetaPage() error {
	page := &Page{}
	_, err := p.file.ReadAt(page.Data[:], 0)
	if err != nil {
		return fmt.Errorf("pager: read meta page: %w", err)
	}
	if page.Type() != PageTypeMeta {
		return errors.New("pager: page 0 is not a meta page")
	}

	off := uint16(metaHeaderOffset)
	p.totalPages = binary.LittleEndian.Uint32(page.Data[off:])
	off += 4
	numColl := binary.LittleEndian.Uint16(page.Data[off:])
	off += 2

	for i := 0; i < int(numColl); i++ {
		nameLen := binary.LittleEndian.Uint16(page.Data[off:])
		off += 2
		name := string(page.Data[off : off+nameLen])
		off += nameLen
		firstPage := binary.LittleEndian.Uint32(page.Data[off:])
		off += 4
		nextRID := binary.LittleEndian.Uint64(page.Data[off:])
		off += 8

		p.collections[name] = &CollectionMeta{
			Name:         name,
			FirstPageID:  firstPage,
			NextRecordID: nextRID,
		}
	}

	// Charger les index definitions (si présentes)
	if int(off)+2 <= len(page.Data) {
		numIdx := binary.LittleEndian.Uint16(page.Data[off:])
		off += 2
		p.indexDefs = nil
		for i := 0; i < int(numIdx); i++ {
			collLen := binary.LittleEndian.Uint16(page.Data[off:])
			off += 2
			coll := string(page.Data[off : off+collLen])
			off += collLen
			fieldLen := binary.LittleEndian.Uint16(page.Data[off:])
			off += 2
			field := string(page.Data[off : off+fieldLen])
			off += fieldLen
			rootPageID := binary.LittleEndian.Uint32(page.Data[off:])
			off += 4
			p.indexDefs = append(p.indexDefs, IndexDef{Collection: coll, Field: field, RootPageID: rootPageID})
		}
	}

	// Charger les view definitions (si présentes)
	if int(off)+2 <= len(page.Data) {
		numViews := binary.LittleEndian.Uint16(page.Data[off:])
		off += 2
		p.viewDefs = make(map[string]string)
		for i := 0; i < int(numViews); i++ {
			nameLen := binary.LittleEndian.Uint16(page.Data[off:])
			off += 2
			name := string(page.Data[off : off+nameLen])
			off += nameLen
			queryLen := binary.LittleEndian.Uint16(page.Data[off:])
			off += 2
			query := string(page.Data[off : off+queryLen])
			off += queryLen
			p.viewDefs[name] = query
		}
	}

	return nil
}

// AddIndexDef ajoute une définition d'index persistée et flush la meta.
func (p *Pager) AddIndexDef(collection, field string, rootPageID uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Vérifier doublon
	for i, d := range p.indexDefs {
		if d.Collection == collection && d.Field == field {
			p.indexDefs[i].RootPageID = rootPageID
			return p.flushMeta()
		}
	}
	p.indexDefs = append(p.indexDefs, IndexDef{Collection: collection, Field: field, RootPageID: rootPageID})
	return p.flushMeta()
}

// RemoveIndexDef supprime une définition d'index persistée et flush la meta.
func (p *Pager) RemoveIndexDef(collection, field string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, d := range p.indexDefs {
		if d.Collection == collection && d.Field == field {
			p.indexDefs = append(p.indexDefs[:i], p.indexDefs[i+1:]...)
			return p.flushMeta()
		}
	}
	return nil
}

// RemoveAllIndexDefsForCollection supprime toutes les définitions d'index d'une collection.
func (p *Pager) RemoveAllIndexDefsForCollection(collection string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var kept []IndexDef
	for _, d := range p.indexDefs {
		if d.Collection != collection {
			kept = append(kept, d)
		}
	}
	p.indexDefs = kept
	return p.flushMeta()
}

// IndexDefs retourne la liste des définitions d'index persistées.
func (p *Pager) IndexDefs() []IndexDef {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cp := make([]IndexDef, len(p.indexDefs))
	copy(cp, p.indexDefs)
	return cp
}

// ---------- Views ----------

// AddView ajoute ou remplace une définition de vue et flush la meta.
func (p *Pager) AddView(name, query string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.viewDefs[name] = query
	return p.flushMeta()
}

// RemoveView supprime une définition de vue et flush la meta.
func (p *Pager) RemoveView(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.viewDefs, name)
	return p.flushMeta()
}

// GetView retourne la requête SQL d'une vue, ou "" si inexistante.
func (p *Pager) GetView(name string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	q, ok := p.viewDefs[name]
	return q, ok
}

// ListViews retourne les noms de toutes les vues.
func (p *Pager) ListViews() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.viewDefs))
	for n := range p.viewDefs {
		names = append(names, n)
	}
	return names
}

// ListCollections retourne les noms de toutes les collections.
func (p *Pager) ListCollections() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.collections))
	for name := range p.collections {
		names = append(names, name)
	}
	return names
}

// AllocateAndChain alloue une nouvelle page et la chaîne à la page courante.
func (p *Pager) AllocateAndChain(currentPageID uint32, ptype PageType) (uint32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	newID, err := p.allocatePageUnlocked(ptype)
	if err != nil {
		return 0, err
	}

	current, err := p.readPageUnlocked(currentPageID)
	if err != nil {
		return 0, err
	}
	current.SetNextPageID(newID)
	if err := p.writePageUnlocked(current); err != nil {
		return 0, err
	}
	return newID, nil
}

// MarkDeletedAtomic marque un record comme supprimé de manière atomique (read-modify-write sous lock).
func (p *Pager) MarkDeletedAtomic(pageID uint32, slotOffset uint16) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, err := p.readPageUnlocked(pageID)
	if err != nil {
		return err
	}
	page.MarkDeleted(slotOffset)
	return p.writePageUnlocked(page)
}

// UpdateRecordAtomic met à jour un record in-place de manière atomique.
// Si la taille diffère, marque l'ancien comme supprimé et insère le nouveau
// dans la collection via InsertRecordAtomic (appelé sans lock, car cette méthode relâche le sien).
func (p *Pager) UpdateRecordAtomic(coll *CollectionMeta, pageID uint32, slotOffset uint16, recordID uint64, newData []byte) error {
	p.mu.Lock()

	page, err := p.readPageUnlocked(pageID)
	if err != nil {
		p.mu.Unlock()
		return err
	}

	if page.UpdateRecordInPlace(slotOffset, newData) {
		err = p.writePageUnlocked(page)
		p.mu.Unlock()
		return err
	}

	// Taille différente : marquer supprimé puis réinsérer
	page.MarkDeleted(slotOffset)
	if err := p.writePageUnlocked(page); err != nil {
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()

	// Réinsérer avec le même record_id (InsertRecordAtomic prend son propre lock)
	return p.InsertRecordAtomic(coll, recordID, newData)
}

// maxInlineRecordSize est la taille max d'un record stockable directement dans une data page.
const maxInlineRecordSize = PageSize - PageHeaderSize - RecordSlotHeaderSize

// InsertRecordAtomic insère un record dans les pages d'une collection de manière atomique.
// Si le record dépasse maxInlineRecordSize, il est stocké dans des overflow pages.
func (p *Pager) InsertRecordAtomic(coll *CollectionMeta, recordID uint64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Compresser avec snappy si ça réduit la taille
	storeData, storeFlag := p.compressRecord(data)

	// Gros document → overflow pages
	if len(storeData) > maxInlineRecordSize {
		return p.insertOverflowRecord(coll, recordID, data)
	}

	pageID := coll.FirstPageID
	var lastPageID uint32

	for pageID != 0 {
		page, err := p.readPageUnlocked(pageID)
		if err != nil {
			return err
		}
		if page.AppendRecordWithFlag(recordID, storeData, storeFlag) {
			return p.writePageUnlocked(page)
		}
		lastPageID = pageID
		pageID = page.NextPageID()
	}

	// Aucune page n'a assez d'espace : allouer et chaîner
	newID, err := p.allocatePageUnlocked(PageTypeData)
	if err != nil {
		return err
	}

	prev, err := p.readPageUnlocked(lastPageID)
	if err != nil {
		return err
	}
	prev.SetNextPageID(newID)
	if err := p.writePageUnlocked(prev); err != nil {
		return err
	}

	newPage, err := p.readPageUnlocked(newID)
	if err != nil {
		return err
	}
	if !newPage.AppendRecordWithFlag(recordID, storeData, storeFlag) {
		return fmt.Errorf("pager: record too large for a single page")
	}
	return p.writePageUnlocked(newPage)
}

// insertOverflowRecord stocke un gros record dans des overflow pages chaînées,
// puis insère un overflow pointer dans la data page de la collection.
func (p *Pager) insertOverflowRecord(coll *CollectionMeta, recordID uint64, data []byte) error {
	totalLen := uint32(len(data))

	// Allouer les overflow pages et écrire les chunks
	var firstOverflowID uint32
	var prevOverflowPage *Page
	offset := 0
	for offset < len(data) {
		ovID, err := p.allocatePageUnlocked(PageTypeOverflow)
		if err != nil {
			return err
		}
		if firstOverflowID == 0 {
			firstOverflowID = ovID
		}
		// Chaîner la page précédente
		if prevOverflowPage != nil {
			prevOverflowPage.SetNextPageID(ovID)
			if err := p.writePageUnlocked(prevOverflowPage); err != nil {
				return err
			}
		}

		ovPage, err := p.readPageUnlocked(ovID)
		if err != nil {
			return err
		}
		chunkEnd := offset + OverflowDataCapacity
		if chunkEnd > len(data) {
			chunkEnd = len(data)
		}
		ovPage.WriteOverflowData(data[offset:chunkEnd])
		offset = chunkEnd
		prevOverflowPage = ovPage
	}
	// Écrire la dernière overflow page (NextPageID = 0)
	if prevOverflowPage != nil {
		if err := p.writePageUnlocked(prevOverflowPage); err != nil {
			return err
		}
	}

	// Insérer l'overflow pointer dans la data page de la collection
	pageID := coll.FirstPageID
	var lastPageID uint32
	for pageID != 0 {
		page, err := p.readPageUnlocked(pageID)
		if err != nil {
			return err
		}
		if page.AppendOverflowPointer(recordID, totalLen, firstOverflowID) {
			return p.writePageUnlocked(page)
		}
		lastPageID = pageID
		pageID = page.NextPageID()
	}

	// Nouvelle data page pour le pointer
	newID, err := p.allocatePageUnlocked(PageTypeData)
	if err != nil {
		return err
	}
	prev, err := p.readPageUnlocked(lastPageID)
	if err != nil {
		return err
	}
	prev.SetNextPageID(newID)
	if err := p.writePageUnlocked(prev); err != nil {
		return err
	}
	newPage, err := p.readPageUnlocked(newID)
	if err != nil {
		return err
	}
	if !newPage.AppendOverflowPointer(recordID, totalLen, firstOverflowID) {
		return fmt.Errorf("pager: cannot write overflow pointer")
	}
	return p.writePageUnlocked(newPage)
}

// ReadOverflowData reconstitue les données d'un record stocké dans des overflow pages.
func (p *Pager) ReadOverflowData(totalLen uint32, firstPageID uint32) ([]byte, error) {
	result := make([]byte, 0, totalLen)
	remaining := int(totalLen)
	pageID := firstPageID

	for pageID != 0 && remaining > 0 {
		page, err := p.readPageUnlocked(pageID)
		if err != nil {
			return nil, err
		}
		chunkLen := remaining
		if chunkLen > OverflowDataCapacity {
			chunkLen = OverflowDataCapacity
		}
		result = append(result, page.ReadOverflowData(chunkLen)...)
		remaining -= chunkLen
		pageID = page.NextPageID()
	}
	return result, nil
}

// FreeOverflowPages libère les overflow pages chaînées à partir de firstPageID.
func (p *Pager) FreeOverflowPages(firstPageID uint32) error {
	pageID := firstPageID
	for pageID != 0 {
		page, err := p.readPageUnlocked(pageID)
		if err != nil {
			return err
		}
		nextID := page.NextPageID()
		// Marquer comme page libre
		page.Data[0] = byte(PageTypeFree)
		page.SetNextPageID(0)
		if err := p.writePageUnlocked(page); err != nil {
			return err
		}
		pageID = nextID
	}
	return nil
}

// ---------- Transaction Support ----------

// BeginTx démarre une transaction. Capture un snapshot de l'état actuel.
// Une seule transaction à la fois (single-writer).
func (p *Pager) BeginTx() error {
	if p.readOnly {
		return ErrReadOnly
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inTx {
		return fmt.Errorf("pager: transaction already active")
	}
	p.inTx = true
	p.txUndoLog = make(map[uint32][PageSize]byte)
	p.txNewPages = make(map[uint32]bool)
	p.txTotalPages = p.totalPages

	// Snapshot des collections
	p.txCollections = make(map[string]*CollectionMeta, len(p.collections))
	for k, v := range p.collections {
		cp := *v
		p.txCollections[k] = &cp
	}
	// Snapshot des indexDefs
	p.txIndexDefs = make([]IndexDef, len(p.indexDefs))
	copy(p.txIndexDefs, p.indexDefs)
	// Snapshot des viewDefs
	p.txViewDefs = make(map[string]string, len(p.viewDefs))
	for k, v := range p.viewDefs {
		p.txViewDefs[k] = v
	}

	return nil
}

// CommitTx valide la transaction courante. Les écritures deviennent permanentes.
func (p *Pager) CommitTx() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.inTx {
		return fmt.Errorf("pager: no active transaction")
	}

	// Flush meta + WAL commit pour rendre les écritures durables
	if err := p.flushMeta(); err != nil {
		return err
	}
	if p.wal != nil {
		if err := p.wal.Commit(); err != nil {
			return err
		}
	}

	p.txUndoLog = nil
	p.txNewPages = nil
	p.txCollections = nil
	p.txIndexDefs = nil
	p.txViewDefs = nil
	p.inTx = false
	return nil
}

// RollbackTx annule la transaction courante. Restaure les before-images.
func (p *Pager) RollbackTx() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.inTx {
		return fmt.Errorf("pager: no active transaction")
	}

	// Restaurer toutes les pages modifiées avec les before-images
	for pid, data := range p.txUndoLog {
		dataCopy := data // copie locale pour éviter les problèmes de pointeur
		if _, err := p.file.WriteAt(dataCopy[:], int64(pid)*PageSize); err != nil {
			return fmt.Errorf("pager: rollback write page %d: %w", pid, err)
		}
	}

	// Restaurer totalPages (les pages allouées pendant la tx sont abandonnées)
	p.totalPages = p.txTotalPages

	// Restaurer les métadonnées
	p.collections = p.txCollections
	p.indexDefs = p.txIndexDefs
	p.viewDefs = p.txViewDefs

	// Flush meta restaurée
	if err := p.flushMeta(); err != nil {
		return err
	}
	if err := p.file.Sync(); err != nil {
		return err
	}

	// Invalider le cache (les pages ont été restaurées à leur état avant-tx)
	p.cache.clear()

	// Tronquer le WAL (les écritures de la tx sont invalides)
	if p.wal != nil {
		p.wal.Truncate()
	}

	p.txUndoLog = nil
	p.txNewPages = nil
	p.txCollections = nil
	p.txIndexDefs = nil
	p.txViewDefs = nil
	p.inTx = false
	return nil
}

// ClearCache vide le cache LRU (utilisé par le hint NO_CACHE).
func (p *Pager) ClearCache() {
	p.cache.clear()
}

// CacheStats retourne les statistiques du cache LRU (hits, misses, size, capacity).
func (p *Pager) CacheStats() (hits, misses uint64, size, capacity int) {
	return p.cache.stats() // cache est thread-safe via son propre mutex
}

// CacheHitRate retourne le taux de hit du cache (0.0 à 1.0).
func (p *Pager) CacheHitRate() float64 {
	return p.cache.hitRate() // cache est thread-safe via son propre mutex
}

// InTx retourne true si une transaction est active.
func (p *Pager) InTx() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.inTx
}

// ---------- WAL Integration ----------

// CommitWAL écrit un marqueur de commit dans le WAL et fait un fsync.
// Doit être appelé après chaque opération d'écriture complète (insert, update, delete).
// Si une transaction est active, le commit est différé jusqu'à CommitTx().
func (p *Pager) CommitWAL() error {
	if p.wal == nil {
		return nil
	}
	p.mu.RLock()
	inTx := p.inTx
	p.mu.RUnlock()
	if inTx {
		return nil // différé — CommitTx() fera le commit WAL
	}
	return p.wal.Commit()
}

// Checkpoint applique les écritures committées du WAL dans le fichier data, puis tronque le WAL.
func (p *Pager) Checkpoint() error {
	if p.wal == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	records := p.wal.CommittedPageWrites()
	for _, rec := range records {
		if len(rec.Data) != PageSize {
			continue
		}
		// Étendre le fichier si nécessaire (page allouée mais pas encore écrite)
		for rec.PageID >= p.totalPages {
			p.totalPages = rec.PageID + 1
		}
		if _, err := p.file.WriteAt(rec.Data, int64(rec.PageID)*PageSize); err != nil {
			return fmt.Errorf("pager: checkpoint write page %d: %w", rec.PageID, err)
		}
	}

	// fsync le fichier data
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("pager: checkpoint fsync: %w", err)
	}

	// Tronquer le WAL
	return p.wal.Truncate()
}

// recoverFromWAL rejoue les écritures committées du WAL dans le fichier data.
// Appelé automatiquement à l'ouverture du pager pour récupérer après un crash.
func (p *Pager) recoverFromWAL() error {
	if p.wal == nil {
		return nil
	}

	records := p.wal.CommittedPageWrites()
	if len(records) == 0 {
		return nil
	}

	// Rejouer toutes les écritures committées
	for _, rec := range records {
		if len(rec.Data) != PageSize {
			continue
		}
		// Étendre le fichier si la page n'existe pas encore
		for rec.PageID >= p.totalPages {
			p.totalPages = rec.PageID + 1
		}
		if _, err := p.file.WriteAt(rec.Data, int64(rec.PageID)*PageSize); err != nil {
			return fmt.Errorf("recovery: write page %d: %w", rec.PageID, err)
		}
	}

	// fsync pour persister le recovery
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("recovery: fsync: %w", err)
	}

	// Recharger les métadonnées depuis la page 0 (potentiellement mise à jour par le WAL)
	if err := p.loadMetaPage(); err != nil {
		return fmt.Errorf("recovery: reload meta: %w", err)
	}

	// Tronquer le WAL maintenant que tout est appliqué
	return p.wal.Truncate()
}

// DropCollection supprime une collection et ses métadonnées.
// Les pages de données ne sont pas physiquement libérées (v1), mais la collection
// est retirée de la meta page et ne sera plus accessible.
func (p *Pager) DropCollection(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.collections[name]; !ok {
		return fmt.Errorf("pager: collection %q not found", name)
	}
	delete(p.collections, name)
	return p.flushMeta()
}

// VacuumCollection compacte une collection en réécrivant les pages sans les records supprimés.
// Retourne le nombre de records récupérés.
func (p *Pager) VacuumCollection(collName string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	coll, ok := p.collections[collName]
	if !ok {
		return 0, fmt.Errorf("pager: collection %q not found", collName)
	}

	// Lire tous les records vivants
	var liveRecords []struct {
		recordID uint64
		data     []byte
	}
	var reclaimedCount int

	pageID := coll.FirstPageID
	for pageID != 0 {
		page, err := p.readPageUnlocked(pageID)
		if err != nil {
			return 0, err
		}
		for _, slot := range page.ReadRecords() {
			if slot.Deleted {
				// Libérer les overflow pages des records supprimés
				if slot.Overflow || page.SlotFlags(slot.Offset) == SlotFlagDelOver {
					if len(slot.Data) >= 8 {
						_, firstOvPage := slot.OverflowInfo()
						p.FreeOverflowPages(firstOvPage)
					}
				}
				reclaimedCount++
			} else if slot.Overflow {
				// Record vivant avec overflow : lire les données complètes
				totalLen, firstOvPage := slot.OverflowInfo()
				fullData, err := p.ReadOverflowData(totalLen, firstOvPage)
				if err != nil {
					return 0, err
				}
				// Libérer les anciennes overflow pages
				p.FreeOverflowPages(firstOvPage)
				liveRecords = append(liveRecords, struct {
					recordID uint64
					data     []byte
				}{slot.RecordID, fullData})
			} else {
				recData := slot.Data
				// Décompresser si le record est compressé
				if slot.Compressed {
					dec, err := snappy.Decode(nil, slot.Data)
					if err != nil {
						return 0, fmt.Errorf("vacuum: snappy decode: %w", err)
					}
					recData = dec
				}
				liveRecords = append(liveRecords, struct {
					recordID uint64
					data     []byte
				}{slot.RecordID, recData})
			}
		}
		pageID = page.NextPageID()
	}

	if reclaimedCount == 0 {
		return 0, nil // rien à compacter
	}

	// Allouer une nouvelle première page
	newFirstPageID, err := p.allocatePageUnlocked(PageTypeData)
	if err != nil {
		return 0, err
	}

	// Réinsérer les records vivants dans les nouvelles pages
	currentPageID := newFirstPageID
	// Construire un CollectionMeta temporaire pour insertOverflowRecord
	tempColl := &CollectionMeta{FirstPageID: newFirstPageID}

	for _, rec := range liveRecords {
		// Gros record → overflow
		if len(rec.data) > maxInlineRecordSize {
			tempColl.FirstPageID = currentPageID
			if err := p.insertOverflowRecord(tempColl, rec.recordID, rec.data); err != nil {
				return 0, err
			}
			// Mettre à jour currentPageID (la chaîne a pu s'allonger)
			pid := tempColl.FirstPageID
			for pid != 0 {
				pg, _ := p.readPageUnlocked(pid)
				if pg.NextPageID() == 0 {
					currentPageID = pid
					break
				}
				pid = pg.NextPageID()
			}
			continue
		}

		// Compresser avant réécriture
		storeData, storeFlag := p.compressRecord(rec.data)

		page, err := p.readPageUnlocked(currentPageID)
		if err != nil {
			return 0, err
		}
		if !page.AppendRecordWithFlag(rec.recordID, storeData, storeFlag) {
			// Page pleine, allouer une nouvelle
			nextID, err := p.allocatePageUnlocked(PageTypeData)
			if err != nil {
				return 0, err
			}
			page.SetNextPageID(nextID)
			if err := p.writePageUnlocked(page); err != nil {
				return 0, err
			}
			currentPageID = nextID
			newPage, err := p.readPageUnlocked(nextID)
			if err != nil {
				return 0, err
			}
			newPage.AppendRecordWithFlag(rec.recordID, storeData, storeFlag)
			if err := p.writePageUnlocked(newPage); err != nil {
				return 0, err
			}
			continue
		}
		if err := p.writePageUnlocked(page); err != nil {
			return 0, err
		}
	}

	// Mettre à jour la collection pour pointer vers la nouvelle chaîne
	coll.FirstPageID = newFirstPageID

	// Marquer les anciennes pages comme libres (on ne les libère pas physiquement pour v1)
	if err := p.flushMeta(); err != nil {
		return 0, err
	}

	return reclaimedCount, nil
}

// WALPath retourne le chemin du fichier WAL.
func (p *Pager) WALPath() string {
	if p.wal == nil {
		return ""
	}
	return p.wal.path
}

// ---------- Snappy Compression ----------

// compressRecord compresse les données avec snappy.
// Retourne les données à stocker et le flag approprié.
// Si la compression n'apporte pas de gain, retourne les données originales avec SlotFlagActive.
func (p *Pager) compressRecord(data []byte) ([]byte, byte) {
	compressed := snappy.Encode(nil, data)
	if len(compressed) < len(data) {
		return compressed, SlotFlagCompressed
	}
	return data, SlotFlagActive
}

// DecompressRecord décompresse les données d'un record si nécessaire.
// Si le slot n'est pas compressé, retourne les données telles quelles.
func DecompressRecord(slot *RecordSlot) ([]byte, error) {
	if !slot.Compressed {
		return slot.Data, nil
	}
	decoded, err := snappy.Decode(nil, slot.Data)
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	return decoded, nil
}
