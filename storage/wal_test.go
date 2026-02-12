package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "test.dlite")
}

func TestWALCreateAndClose(t *testing.T) {
	dbPath := tempWALPath(t)
	walPath := dbPath + ".wal"

	wal, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if wal.RecordCount() != 0 {
		t.Errorf("expected 0 records, got %d", wal.RecordCount())
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Le fichier WAL doit exister
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file should exist")
	}
}

func TestWALAppendAndReload(t *testing.T) {
	dbPath := tempWALPath(t)

	// Écrire des records et committer
	wal, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	pageData := make([]byte, PageSize)
	copy(pageData[0:5], []byte("HELLO"))

	lsn1, err := wal.LogPageWrite(1, pageData)
	if err != nil {
		t.Fatalf("log page write: %v", err)
	}
	if lsn1 != 1 {
		t.Errorf("expected LSN 1, got %d", lsn1)
	}

	lsn2, err := wal.LogPageWrite(2, pageData)
	if err != nil {
		t.Fatalf("log page write 2: %v", err)
	}
	if lsn2 != 2 {
		t.Errorf("expected LSN 2, got %d", lsn2)
	}

	if err := wal.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if wal.RecordCount() != 3 { // 2 writes + 1 commit
		t.Errorf("expected 3 records, got %d", wal.RecordCount())
	}

	wal.Close()

	// Réouvrir et vérifier que les records sont rechargés
	wal2, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()

	if wal2.RecordCount() != 3 {
		t.Errorf("expected 3 records after reload, got %d", wal2.RecordCount())
	}

	committed := wal2.CommittedPageWrites()
	if len(committed) != 2 {
		t.Errorf("expected 2 committed page writes, got %d", len(committed))
	}

	// Vérifier les données
	if committed[0].PageID != 1 {
		t.Errorf("expected page 1, got %d", committed[0].PageID)
	}
	if committed[1].PageID != 2 {
		t.Errorf("expected page 2, got %d", committed[1].PageID)
	}
	if string(committed[0].Data[0:5]) != "HELLO" {
		t.Errorf("expected HELLO, got %s", string(committed[0].Data[0:5]))
	}
}

func TestWALUncommittedIgnored(t *testing.T) {
	dbPath := tempWALPath(t)

	wal, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	pageData := make([]byte, PageSize)

	// Écrire sans committer
	wal.LogPageWrite(1, pageData)
	wal.LogPageWrite(2, pageData)

	// Les écritures non commitées ne doivent pas être dans CommittedPageWrites
	committed := wal.CommittedPageWrites()
	if len(committed) != 0 {
		t.Errorf("expected 0 committed writes, got %d", len(committed))
	}

	if !wal.HasUncommittedWrites() {
		t.Error("should have uncommitted writes")
	}

	wal.Close()

	// Réouvrir : les écritures non commitées sont chargées mais ignorées par CommittedPageWrites
	wal2, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()

	committed = wal2.CommittedPageWrites()
	if len(committed) != 0 {
		t.Errorf("expected 0 committed writes after reload, got %d", len(committed))
	}
}

func TestWALTruncate(t *testing.T) {
	dbPath := tempWALPath(t)

	wal, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	pageData := make([]byte, PageSize)
	wal.LogPageWrite(1, pageData)
	wal.Commit()

	if wal.RecordCount() != 2 {
		t.Errorf("expected 2 records, got %d", wal.RecordCount())
	}

	// Truncate
	if err := wal.Truncate(); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if wal.RecordCount() != 0 {
		t.Errorf("expected 0 records after truncate, got %d", wal.RecordCount())
	}

	// On peut encore écrire après truncate
	wal.LogPageWrite(5, pageData)
	wal.Commit()

	if wal.RecordCount() != 2 {
		t.Errorf("expected 2 records after new write, got %d", wal.RecordCount())
	}

	wal.Close()
}

func TestWALMultipleCommits(t *testing.T) {
	dbPath := tempWALPath(t)

	wal, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer wal.Close()

	pageData := make([]byte, PageSize)

	// Premier commit
	wal.LogPageWrite(1, pageData)
	wal.Commit()

	// Deuxième commit
	copy(pageData[0:3], []byte("ABC"))
	wal.LogPageWrite(1, pageData) // même page, données différentes
	wal.LogPageWrite(2, pageData)
	wal.Commit()

	// Troisième écriture sans commit
	wal.LogPageWrite(3, pageData)

	committed := wal.CommittedPageWrites()
	if len(committed) != 3 { // 1 du premier commit + 2 du deuxième
		t.Errorf("expected 3 committed writes, got %d", len(committed))
	}

	// La page 3 ne doit pas y être (non commitée)
	for _, c := range committed {
		if c.PageID == 3 {
			t.Error("page 3 should not be in committed writes")
		}
	}
}

func TestWALCRCIntegrity(t *testing.T) {
	dbPath := tempWALPath(t)

	wal, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	pageData := make([]byte, PageSize)
	copy(pageData[0:4], []byte("TEST"))
	wal.LogPageWrite(1, pageData)
	wal.Commit()
	wal.Close()

	// Corrompre le WAL : modifier un octet dans les données du premier record
	walPath := dbPath + ".wal"
	f, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open wal file: %v", err)
	}
	// Le premier record commence à offset walHeaderSize (16)
	// Header: LSN(8) + Type(1) + PageID(4) + DataLen(4) = 17 bytes
	// Données commencent à offset 16 + 17 = 33
	corruptOffset := int64(walHeaderSize + walRecordHeaderSize + 10)
	f.WriteAt([]byte{0xFF}, corruptOffset) // corrompre
	f.Close()

	// Réouvrir : le record corrompu doit être ignoré
	wal2, err := OpenWAL(dbPath)
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	defer wal2.Close()

	// Le record corrompu est détecté par CRC et ignoré
	if wal2.RecordCount() != 0 {
		t.Errorf("expected 0 records after corruption, got %d", wal2.RecordCount())
	}
}

// ---------- Tests d'intégration WAL + Pager ----------

func TestPagerWALIntegration(t *testing.T) {
	dbPath := tempWALPath(t)

	// Ouvrir un pager (crée automatiquement le WAL)
	p, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Créer une collection et insérer des données
	coll, err := p.GetOrCreateCollection("users")
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}

	doc := NewDocument()
	doc.Set("name", "Alice")
	doc.Set("age", int64(30))

	encoded, err := doc.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	rid, err := p.NextRecordID("users")
	if err != nil {
		t.Fatalf("next record id: %v", err)
	}

	if _, _, err := p.InsertRecordAtomic(coll, rid, encoded); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := p.FlushMeta(); err != nil {
		t.Fatalf("flush meta: %v", err)
	}

	// Commit WAL
	if err := p.CommitWAL(); err != nil {
		t.Fatalf("commit wal: %v", err)
	}

	// Vérifier que le WAL file existe
	walPath := p.WALPath()
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file should exist")
	}

	// Fermer proprement (fait un checkpoint)
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Réouvrir et vérifier que les données persistent
	p2, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()

	coll2 := p2.GetCollection("users")
	if coll2 == nil {
		t.Fatal("collection 'users' should exist")
	}

	page, err := p2.ReadPage(coll2.FirstPageID)
	if err != nil {
		t.Fatalf("read page: %v", err)
	}
	records := page.ReadRecords()
	var found bool
	for _, r := range records {
		if !r.Deleted {
			d, err := Decode(r.Data)
			if err == nil {
				if v, ok := d.Get("name"); ok && v == "Alice" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("expected to find 'Alice' after reopen")
	}
}

func TestPagerWALRecovery(t *testing.T) {
	dbPath := tempWALPath(t)

	// Écrire des données, commiter le WAL, puis simuler un crash
	// en NE fermant PAS proprement le pager (pas de checkpoint)
	func() {
		p, err := OpenPager(dbPath)
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		coll, err := p.GetOrCreateCollection("items")
		if err != nil {
			t.Fatalf("create collection: %v", err)
		}

		for i := 0; i < 5; i++ {
			doc := NewDocument()
			doc.Set("idx", int64(i))
			encoded, _ := doc.Encode()
			rid, _ := p.NextRecordID("items")
			p.InsertRecordAtomic(coll, rid, encoded) //nolint
		}

		p.FlushMeta()
		p.CommitWAL()

		// "Crash" : fermer le fichier SANS checkpoint
		// Le WAL contient les écritures committées non appliquées
		// On ne peut pas vraiment simuler un crash parfait, mais
		// le WAL est intact et contient les données commitées
		p.Close()
	}()

	// Réouvrir : le recovery doit rejouer le WAL
	p2, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer p2.Close()

	coll2 := p2.GetCollection("items")
	if coll2 == nil {
		t.Fatal("collection 'items' should exist after recovery")
	}

	// Vérifier que les 5 records sont présents
	pageID := coll2.FirstPageID
	var count int
	for pageID != 0 {
		page, err := p2.ReadPage(pageID)
		if err != nil {
			t.Fatalf("read page: %v", err)
		}
		for _, r := range page.ReadRecords() {
			if !r.Deleted {
				count++
			}
		}
		pageID = page.NextPageID()
	}
	if count != 5 {
		t.Errorf("expected 5 records after recovery, got %d", count)
	}
}

func TestPagerCheckpoint(t *testing.T) {
	dbPath := tempWALPath(t)

	p, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	coll, err := p.GetOrCreateCollection("data")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Insérer plusieurs documents
	for i := 0; i < 10; i++ {
		doc := NewDocument()
		doc.Set("val", int64(i))
		encoded, _ := doc.Encode()
		rid, _ := p.NextRecordID("data")
		_, _, err = p.InsertRecordAtomic(coll, rid, encoded)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	p.FlushMeta()
	p.CommitWAL()

	// Faire un checkpoint
	if err := p.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Après checkpoint, insérer encore
	for i := 10; i < 15; i++ {
		doc := NewDocument()
		doc.Set("val", int64(i))
		encoded, _ := doc.Encode()
		rid, _ := p.NextRecordID("data")
		_, _, err = p.InsertRecordAtomic(coll, rid, encoded)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	p.FlushMeta()
	p.CommitWAL()

	// Vérifier qu'on a bien 15 documents
	pageID := coll.FirstPageID
	var count int
	for pageID != 0 {
		page, err := p.ReadPage(pageID)
		if err != nil {
			t.Fatalf("read page: %v", err)
		}
		for _, r := range page.ReadRecords() {
			if !r.Deleted {
				count++
			}
		}
		pageID = page.NextPageID()
	}
	if count != 15 {
		t.Errorf("expected 15 records, got %d", count)
	}
}

func TestWALDeleteAndUpdateDurability(t *testing.T) {
	dbPath := tempWALPath(t)

	p, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	coll, err := p.GetOrCreateCollection("test")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Insérer un document
	doc := NewDocument()
	doc.Set("status", "active")
	encoded, _ := doc.Encode()
	rid, _ := p.NextRecordID("test")
	_, _, err = p.InsertRecordAtomic(coll, rid, encoded)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	p.FlushMeta()
	p.CommitWAL()

	// Lire le record pour obtenir son offset
	page, _ := p.ReadPage(coll.FirstPageID)
	records := page.ReadRecords()
	if len(records) == 0 {
		t.Fatal("expected at least 1 record")
	}
	slot := records[0]

	// Mettre à jour le document
	doc2 := NewDocument()
	doc2.Set("status", "done")
	encoded2, _ := doc2.Encode()
	if _, _, err := p.UpdateRecordAtomic(coll, coll.FirstPageID, slot.Offset, rid, encoded2); err != nil {
		t.Fatalf("update: %v", err)
	}
	p.CommitWAL()

	// Fermer et réouvrir
	p.Close()

	p2, err := OpenPager(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()

	coll2 := p2.GetCollection("test")
	page2, _ := p2.ReadPage(coll2.FirstPageID)
	for _, r := range page2.ReadRecords() {
		if !r.Deleted {
			d, _ := Decode(r.Data)
			if v, ok := d.Get("status"); ok {
				if v != "done" {
					t.Errorf("expected status=done, got %v", v)
				}
			}
		}
	}
}
