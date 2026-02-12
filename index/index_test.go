package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Felmond13/novusdb/storage"
)

func tempPager(t *testing.T) *storage.Pager {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	p, err := storage.OpenPager(path)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	t.Cleanup(func() {
		p.Close()
		os.Remove(path)
	})
	return p
}

func TestIndexAddLookup(t *testing.T) {
	pager := tempPager(t)
	idx, err := NewIndex("jobs", "type", pager)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	idx.Add("s:oracle", 1, 0, 0)
	idx.Add("s:oracle", 4, 0, 0)
	idx.Add("s:mysql", 2, 0, 0)

	ids, _ := idx.Lookup("s:oracle")
	if len(ids) != 2 {
		t.Errorf("expected 2 ids for oracle, got %d", len(ids))
	}
	ids, _ = idx.Lookup("s:mysql")
	if len(ids) != 1 {
		t.Errorf("expected 1 id for mysql, got %d", len(ids))
	}
	ids, _ = idx.Lookup("s:postgres")
	if len(ids) != 0 {
		t.Errorf("expected 0 ids for postgres, got %d", len(ids))
	}
}

func TestIndexRemove(t *testing.T) {
	pager := tempPager(t)
	idx, _ := NewIndex("jobs", "type", pager)
	idx.Add("s:oracle", 1, 0, 0)
	idx.Add("s:oracle", 4, 0, 0)

	idx.Remove("s:oracle", 1)
	ids, _ := idx.Lookup("s:oracle")
	if len(ids) != 1 || ids[0].RecordID != 4 {
		t.Errorf("expected [4], got %v", ids)
	}

	idx.Remove("s:oracle", 4)
	ids, _ = idx.Lookup("s:oracle")
	if len(ids) != 0 {
		t.Errorf("expected empty after removing all, got %v", ids)
	}
}

func TestIndexRemoveNonExistent(t *testing.T) {
	pager := tempPager(t)
	idx, _ := NewIndex("jobs", "type", pager)
	idx.Add("s:oracle", 1, 0, 0)
	// Ne doit pas paniquer
	idx.Remove("s:oracle", 999)
	idx.Remove("s:nonexistent", 1)
}

func TestIndexRangeScan(t *testing.T) {
	pager := tempPager(t)
	idx, _ := NewIndex("jobs", "priority", pager)
	idx.Add("i:00000000000000000001", 10, 0, 0)
	idx.Add("i:00000000000000000003", 30, 0, 0)
	idx.Add("i:00000000000000000005", 50, 0, 0)
	idx.Add("i:00000000000000000007", 70, 0, 0)

	ids, _ := idx.RangeScan("i:00000000000000000002", "i:00000000000000000006")
	if len(ids) != 2 {
		t.Errorf("expected 2 ids in range [2,6], got %d: %v", len(ids), ids)
	}

	// Pas de borne min
	ids, _ = idx.RangeScan("", "i:00000000000000000004")
	if len(ids) != 2 {
		t.Errorf("expected 2 ids with max=4, got %d", len(ids))
	}

	// Pas de borne max
	ids, _ = idx.RangeScan("i:00000000000000000004", "")
	if len(ids) != 2 {
		t.Errorf("expected 2 ids with min=4, got %d", len(ids))
	}
}

func TestIndexAllEntries(t *testing.T) {
	pager := tempPager(t)
	idx, _ := NewIndex("jobs", "type", pager)
	idx.Add("s:oracle", 1, 0, 0)
	idx.Add("s:mysql", 2, 0, 0)

	entries := idx.AllEntries()
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
	// Vérifier que c'est une copie
	entries["s:oracle"] = append(entries["s:oracle"], 999)
	original, _ := idx.Lookup("s:oracle")
	if len(original) != 1 {
		t.Error("AllEntries should return a copy, not a reference")
	}
}

func TestValueToKey(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "\x00null"},
		{"hello", "s:hello"},
		{int64(42), "i:00000000000000000042"},
		{true, "b:true"},
		{false, "b:false"},
	}
	for _, tt := range tests {
		got := ValueToKey(tt.input)
		if got != tt.expected {
			t.Errorf("ValueToKey(%v) = %q, expected %q", tt.input, got, tt.expected)
		}
	}
}

func TestManagerCreateDropIndex(t *testing.T) {
	pager := tempPager(t)
	mgr := NewManager(pager)

	idx, err := mgr.CreateIndex("jobs", "type")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if idx == nil {
		t.Fatal("expected non-nil index")
	}

	// Doublon
	_, err = mgr.CreateIndex("jobs", "type")
	if err == nil {
		t.Fatal("expected error on duplicate index")
	}

	// Get
	got := mgr.GetIndex("jobs", "type")
	if got != idx {
		t.Error("GetIndex should return the same index")
	}

	// Drop
	if err := mgr.DropIndex("jobs", "type"); err != nil {
		t.Fatalf("drop: %v", err)
	}

	// Drop inexistant
	if err := mgr.DropIndex("jobs", "type"); err == nil {
		t.Fatal("expected error on dropping non-existent index")
	}

	// Get après drop
	if mgr.GetIndex("jobs", "type") != nil {
		t.Error("GetIndex should return nil after drop")
	}
}

func TestManagerGetIndexesForCollection(t *testing.T) {
	pager := tempPager(t)
	mgr := NewManager(pager)
	mgr.CreateIndex("jobs", "type")
	mgr.CreateIndex("jobs", "retry")
	mgr.CreateIndex("logs", "level")

	jobIndexes := mgr.GetIndexesForCollection("jobs")
	if len(jobIndexes) != 2 {
		t.Errorf("expected 2 indexes for jobs, got %d", len(jobIndexes))
	}
	logIndexes := mgr.GetIndexesForCollection("logs")
	if len(logIndexes) != 1 {
		t.Errorf("expected 1 index for logs, got %d", len(logIndexes))
	}
	noneIndexes := mgr.GetIndexesForCollection("nonexistent")
	if len(noneIndexes) != 0 {
		t.Errorf("expected 0 indexes for nonexistent, got %d", len(noneIndexes))
	}
}

func TestBTreePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	// Phase 1 : créer et peupler
	pager, err := storage.OpenPager(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	idx, err := NewIndex("jobs", "type", pager)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	idx.Add("s:oracle", 1, 0, 0)
	idx.Add("s:mysql", 2, 0, 0)
	idx.Add("s:oracle", 3, 0, 0)
	rootID := idx.RootPageID()
	pager.Close()

	// Phase 2 : rouvrir et vérifier
	pager2, err := storage.OpenPager(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer pager2.Close()

	idx2 := OpenIndex("jobs", "type", pager2, rootID)
	ids, _ := idx2.Lookup("s:oracle")
	if len(ids) != 2 {
		t.Errorf("expected 2 oracle ids after reopen, got %d", len(ids))
	}
	ids, _ = idx2.Lookup("s:mysql")
	if len(ids) != 1 {
		t.Errorf("expected 1 mysql id after reopen, got %d", len(ids))
	}
}

func TestBTreeSplitManyEntries(t *testing.T) {
	pager := tempPager(t)
	idx, _ := NewIndex("bench", "id", pager)

	// Insérer suffisamment d'entrées pour forcer au moins un split
	for i := uint64(0); i < 200; i++ {
		key := ValueToKey(int64(i))
		if err := idx.Add(key, i, 0, 0); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}

	// Vérifier que toutes les entrées sont retrouvables
	for i := uint64(0); i < 200; i++ {
		key := ValueToKey(int64(i))
		ids, err := idx.Lookup(key)
		if err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
		if len(ids) != 1 || ids[0].RecordID != i {
			t.Errorf("lookup(%d): expected [%d], got %v", i, i, ids)
		}
	}
}
