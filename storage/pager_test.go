package storage

import (
	"os"
	"sync"
	"testing"
)

func tempPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "NovusDB_pager_*.db")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func TestPagerCreateClose(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Fichier doit exister et faire au moins 4096 octets (1 page meta)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() < PageSize {
		t.Errorf("expected file >= %d bytes, got %d", PageSize, info.Size())
	}
}

func TestPagerReopenPersistence(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	// Ouvrir, créer une collection, fermer
	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	_, err = p.CreateCollection("jobs")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	p.Close()

	// Rouvrir et vérifier
	p2, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer p2.Close()

	coll := p2.GetCollection("jobs")
	if coll == nil {
		t.Fatal("expected collection 'jobs' after reopen")
	}
	if coll.Name != "jobs" {
		t.Errorf("expected name 'jobs', got %q", coll.Name)
	}
}

func TestPagerDuplicateCollection(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	_, err = p.CreateCollection("jobs")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err = p.CreateCollection("jobs")
	if err == nil {
		t.Fatal("expected error on duplicate collection")
	}
}

func TestPagerGetOrCreate(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	c1, err := p.GetOrCreateCollection("jobs")
	if err != nil {
		t.Fatalf("get or create 1: %v", err)
	}
	c2, err := p.GetOrCreateCollection("jobs")
	if err != nil {
		t.Fatalf("get or create 2: %v", err)
	}
	if c1.FirstPageID != c2.FirstPageID {
		t.Error("second GetOrCreate should return same collection")
	}
}

func TestPagerNextRecordID(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	p.CreateCollection("jobs")

	id1, err := p.NextRecordID("jobs")
	if err != nil {
		t.Fatalf("next1: %v", err)
	}
	id2, err := p.NextRecordID("jobs")
	if err != nil {
		t.Fatalf("next2: %v", err)
	}
	if id1 >= id2 {
		t.Errorf("IDs should be monotonically increasing: %d, %d", id1, id2)
	}

	_, err = p.NextRecordID("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent collection")
	}
}

func TestPagerAllocateAndChain(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	coll, _ := p.CreateCollection("jobs")
	firstPageID := coll.FirstPageID

	// Chaîner une nouvelle page
	newID, err := p.AllocateAndChain(firstPageID, PageTypeData)
	if err != nil {
		t.Fatalf("chain: %v", err)
	}

	// Vérifier le chaînage
	firstPage, err := p.ReadPage(firstPageID)
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	if firstPage.NextPageID() != newID {
		t.Errorf("expected next=%d, got %d", newID, firstPage.NextPageID())
	}
}

func TestPagerInsertRecordAtomic(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	coll, _ := p.CreateCollection("jobs")

	// Insérer plusieurs records
	for i := uint64(1); i <= 10; i++ {
		data := []byte{byte(i), 0, 0, 0}
		if err := p.InsertRecordAtomic(coll, i, data); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Relire et vérifier
	page, err := p.ReadPage(coll.FirstPageID)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	records := page.ReadRecords()
	if len(records) != 10 {
		t.Errorf("expected 10 records, got %d", len(records))
	}
}

func TestPagerConcurrentInserts(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	coll, _ := p.CreateCollection("bench")

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				rid := uint64(gid*20 + i + 1)
				data := make([]byte, 50)
				data[0] = byte(gid)
				data[1] = byte(i)
				if err := p.InsertRecordAtomic(coll, rid, data); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent insert: %v", err)
	}

	// Compter tous les records
	count := 0
	pageID := coll.FirstPageID
	for pageID != 0 {
		page, err := p.ReadPage(pageID)
		if err != nil {
			t.Fatalf("read page %d: %v", pageID, err)
		}
		for _, slot := range page.ReadRecords() {
			if !slot.Deleted {
				count++
			}
		}
		pageID = page.NextPageID()
	}
	if count != 200 {
		t.Errorf("expected 200 records, got %d", count)
	}
}

func TestPageAppendAndRead(t *testing.T) {
	page := NewPage(PageTypeData, 1)

	ok := page.AppendRecord(100, []byte{1, 2, 3, 4})
	if !ok {
		t.Fatal("should be able to append")
	}
	ok = page.AppendRecord(200, []byte{5, 6, 7, 8})
	if !ok {
		t.Fatal("should be able to append second")
	}

	if page.NumRecords() != 2 {
		t.Errorf("expected 2 records, got %d", page.NumRecords())
	}

	slots := page.ReadRecords()
	if len(slots) != 2 {
		t.Fatalf("expected 2 slots, got %d", len(slots))
	}
	if slots[0].RecordID != 100 {
		t.Errorf("expected record id 100, got %d", slots[0].RecordID)
	}
	if slots[1].RecordID != 200 {
		t.Errorf("expected record id 200, got %d", slots[1].RecordID)
	}
}

func TestPageMarkDeleted(t *testing.T) {
	page := NewPage(PageTypeData, 1)
	page.AppendRecord(100, []byte{1, 2, 3, 4})
	page.AppendRecord(200, []byte{5, 6, 7, 8})

	slots := page.ReadRecords()
	page.MarkDeleted(slots[0].Offset)

	slots = page.ReadRecords()
	if !slots[0].Deleted {
		t.Error("first record should be deleted")
	}
	if slots[1].Deleted {
		t.Error("second record should not be deleted")
	}
}

func TestPageUpdateInPlace(t *testing.T) {
	page := NewPage(PageTypeData, 1)
	page.AppendRecord(100, []byte{1, 2, 3, 4})

	slots := page.ReadRecords()

	// Même taille → in-place
	ok := page.UpdateRecordInPlace(slots[0].Offset, []byte{9, 8, 7, 6})
	if !ok {
		t.Fatal("should update in-place with same size")
	}
	slots = page.ReadRecords()
	if slots[0].Data[0] != 9 {
		t.Errorf("expected updated data[0]=9, got %d", slots[0].Data[0])
	}

	// Taille différente → échec
	ok = page.UpdateRecordInPlace(slots[0].Offset, []byte{1, 2})
	if ok {
		t.Fatal("should fail with different size")
	}
}

func TestPageFillUp(t *testing.T) {
	page := NewPage(PageTypeData, 1)

	// Remplir la page jusqu'à ce qu'on ne puisse plus insérer
	count := 0
	for {
		data := make([]byte, 100)
		if !page.AppendRecord(uint64(count+1), data) {
			break
		}
		count++
	}

	if count == 0 {
		t.Fatal("should be able to insert at least one record")
	}

	// Capacité attendue : (4096 - 16) / (11 + 100) ≈ 36
	if count < 30 || count > 40 {
		t.Errorf("unexpected page capacity: %d records of 100 bytes", count)
	}
}

func TestListCollections(t *testing.T) {
	path := tempPath(t)
	defer os.Remove(path)

	p, err := OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer p.Close()

	p.CreateCollection("alpha")
	p.CreateCollection("beta")
	p.CreateCollection("gamma")

	names := p.ListCollections()
	if len(names) != 3 {
		t.Errorf("expected 3 collections, got %d", len(names))
	}
}
