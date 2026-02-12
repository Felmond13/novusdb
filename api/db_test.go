package api

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/Felmond13/novusdb/storage"
)

func tempDBPath(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "NovusDB_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func tempDBPathB(b *testing.B) string {
	b.Helper()
	f, err := os.CreateTemp("", "NovusDB_bench_*.db")
	if err != nil {
		b.Fatal(err)
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	return path
}

func TestInsertAndSelect(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert
	res, err := db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", res.RowsAffected)
	}

	// Insert second
	_, err = db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	if err != nil {
		t.Fatalf("insert2: %v", err)
	}

	// Select all
	res, err = db.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(res.Docs))
	}

	// Select with WHERE
	res, err = db.Exec(`SELECT * FROM jobs WHERE retry > 3`)
	if err != nil {
		t.Fatalf("select where: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc with retry>3, got %d", len(res.Docs))
	}
}

func TestUpdateAndDelete(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert
	_, err = db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5, enabled=true)`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2, enabled=false)`)
	if err != nil {
		t.Fatalf("insert2: %v", err)
	}

	// Update
	res, err := db.Exec(`UPDATE jobs SET retry=10 WHERE type="oracle"`)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row updated, got %d", res.RowsAffected)
	}

	// Verify update
	res, err = db.Exec(`SELECT * FROM jobs WHERE type="oracle"`)
	if err != nil {
		t.Fatalf("select after update: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	v, _ := res.Docs[0].Doc.Get("retry")
	if v != int64(10) {
		t.Errorf("expected retry=10 after update, got %v", v)
	}

	// Delete
	res, err = db.Exec(`DELETE FROM jobs WHERE enabled=false`)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row deleted, got %d", res.RowsAffected)
	}

	// Verify delete
	res, err = db.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select after delete: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc remaining, got %d", len(res.Docs))
	}
}

func TestNestedFields(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT INTO jobs VALUES (type="oracle", params.timeout=30, params.retry=3)`)
	if err != nil {
		t.Fatalf("insert nested: %v", err)
	}

	// Update nested field
	res, err := db.Exec(`UPDATE jobs SET params.timeout=60 WHERE params.timeout=30`)
	if err != nil {
		t.Fatalf("update nested: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row updated, got %d", res.RowsAffected)
	}

	// Verify
	res, err = db.Exec(`SELECT * FROM jobs WHERE params.timeout=60`)
	if err != nil {
		t.Fatalf("select nested: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc with params.timeout=60, got %d", len(res.Docs))
	}
}

func TestIndex(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert some docs
	for i := 0; i < 20; i++ {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO jobs VALUES (type="type%d", retry=%d)`, i%5, i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Create index
	_, err = db.Exec(`CREATE INDEX ON jobs (type)`)
	if err != nil {
		t.Fatalf("create index: %v", err)
	}

	// Select using index
	res, err := db.Exec(`SELECT * FROM jobs WHERE type="type0"`)
	if err != nil {
		t.Fatalf("select with index: %v", err)
	}
	if len(res.Docs) != 4 {
		t.Errorf("expected 4 docs with type0, got %d", len(res.Docs))
	}
}

func TestMultiCollection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT INTO jobs VALUES (name="job1")`)
	if err != nil {
		t.Fatalf("insert jobs: %v", err)
	}
	_, err = db.Exec(`INSERT INTO workflows VALUES (name="wf1")`)
	if err != nil {
		t.Fatalf("insert workflows: %v", err)
	}

	res, err := db.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select jobs: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 job, got %d", len(res.Docs))
	}

	res, err = db.Exec(`SELECT * FROM workflows`)
	if err != nil {
		t.Fatalf("select workflows: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 workflow, got %d", len(res.Docs))
	}

	collections := db.Collections()
	if len(collections) != 2 {
		t.Errorf("expected 2 collections, got %d", len(collections))
	}
}

func TestPersistence(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	// Open, insert, close
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	db.Close()

	// Reopen and verify
	db2, err := Open(path)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer db2.Close()

	res, err := db2.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc after reopen, got %d", len(res.Docs))
	}
	v, _ := res.Docs[0].Doc.Get("type")
	if v != "oracle" {
		t.Errorf("expected type=oracle, got %v", v)
	}
}

func TestConcurrentInserts(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Créer la collection
	_, err = db.Exec(`INSERT INTO jobs VALUES (type="seed")`)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// 10 goroutines insérant chacune 10 documents
	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				q := fmt.Sprintf(`INSERT INTO jobs VALUES (type="g%d", idx=%d)`, gid, i)
				_, err := db.Exec(q)
				if err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent insert error: %v", err)
	}

	// Vérifier : 1 seed + 100 inserts = 101
	res, err := db.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 101 {
		t.Errorf("expected 101 docs, got %d", len(res.Docs))
	}
}

func TestConcurrentUpdates(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insérer des documents distincts
	for i := 0; i < 10; i++ {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO jobs VALUES (id=%d, val=0)`, i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// 10 goroutines mettant à jour des documents différents
	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			q := fmt.Sprintf(`UPDATE jobs SET val=999 WHERE id=%d`, gid)
			_, err := db.Exec(q)
			if err != nil {
				errCh <- err
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent update error: %v", err)
	}

	// Vérifier que tous les documents ont été mis à jour
	res, err := db.Exec(`SELECT * FROM jobs WHERE val=999`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 10 {
		t.Errorf("expected 10 updated docs, got %d", len(res.Docs))
	}
}

func TestOrderByAndLimit(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO jobs VALUES (name="job%d", priority=%d)`, i, i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// ORDER BY priority DESC LIMIT 3
	res, err := db.Exec(`SELECT * FROM jobs ORDER BY priority DESC LIMIT 3`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(res.Docs))
	}
	// Le premier doit être priority=9
	v, _ := res.Docs[0].Doc.Get("priority")
	if v != int64(9) {
		t.Errorf("expected first doc priority=9, got %v", v)
	}
}

func TestGroupBy(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert docs with different types
	for i := 0; i < 12; i++ {
		var typeName string
		switch i % 3 {
		case 0:
			typeName = "A"
		case 1:
			typeName = "B"
		case 2:
			typeName = "C"
		}
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO jobs VALUES (type="%s", val=%d)`, typeName, i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	res, err := db.Exec(`SELECT type, COUNT(*) FROM jobs GROUP BY type`)
	if err != nil {
		t.Fatalf("group by: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Errorf("expected 3 groups, got %d", len(res.Docs))
	}

	for _, doc := range res.Docs {
		count, _ := doc.Doc.Get("COUNT")
		if count != int64(4) {
			typeName, _ := doc.Doc.Get("type")
			t.Errorf("expected COUNT=4 for type=%v, got %v", typeName, count)
		}
	}
}

// ---------- Tests supplémentaires : edge cases ----------

func TestParseError(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Requête invalide
	_, err = db.Exec(`INVALID QUERY`)
	if err == nil {
		t.Fatal("expected parse error on invalid query")
	}

	// Requête incomplète
	_, err = db.Exec(`SELECT FROM`)
	if err == nil {
		t.Fatal("expected error on incomplete query")
	}
}

func TestSelectEmptyCollection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// SELECT sur collection inexistante → résultat vide, pas d'erreur
	res, err := db.Exec(`SELECT * FROM nonexistent`)
	if err != nil {
		t.Fatalf("select nonexistent: %v", err)
	}
	if len(res.Docs) != 0 {
		t.Errorf("expected 0 docs, got %d", len(res.Docs))
	}
}

func TestDeleteNoMatch(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle")`)

	res, err := db.Exec(`DELETE FROM jobs WHERE type="nonexistent"`)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected, got %d", res.RowsAffected)
	}

	// Le document original doit toujours être là
	res, err = db.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc, got %d", len(res.Docs))
	}
}

func TestUpdateNoMatch(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)

	res, err := db.Exec(`UPDATE jobs SET retry=99 WHERE type="nonexistent"`)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected, got %d", res.RowsAffected)
	}
}

func TestInsertDocProgrammatic(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	doc := storage.NewDocument()
	doc.Set("name", "prog_test")
	doc.Set("value", int64(42))

	rid, err := db.InsertDoc("jobs", doc)
	if err != nil {
		t.Fatalf("InsertDoc: %v", err)
	}
	if rid == 0 {
		t.Error("expected non-zero record ID")
	}

	res, err := db.Exec(`SELECT * FROM jobs WHERE name="prog_test"`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	v, _ := res.Docs[0].Doc.Get("value")
	if v != int64(42) {
		t.Errorf("expected value=42, got %v", v)
	}
}

func TestDropIndex(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle")`)
	_, err = db.Exec(`CREATE INDEX ON jobs (type)`)
	if err != nil {
		t.Fatalf("create index: %v", err)
	}

	_, err = db.Exec(`DROP INDEX ON jobs (type)`)
	if err != nil {
		t.Fatalf("drop index: %v", err)
	}

	// Drop inexistant
	_, err = db.Exec(`DROP INDEX ON jobs (type)`)
	if err == nil {
		t.Fatal("expected error on dropping non-existent index")
	}
}

func TestSelectWithProjection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5, enabled=true)`)

	res, err := db.Exec(`SELECT type, retry FROM jobs`)
	if err != nil {
		t.Fatalf("select projection: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}

	// Le document projeté ne doit contenir que type et retry
	doc := res.Docs[0].Doc
	if _, ok := doc.Get("type"); !ok {
		t.Error("expected 'type' in projection")
	}
	if _, ok := doc.Get("retry"); !ok {
		t.Error("expected 'retry' in projection")
	}
	if _, ok := doc.Get("enabled"); ok {
		t.Error("'enabled' should not be in projection")
	}
}

func TestSelectOffset(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO jobs VALUES (idx=%d)`, i))
	}

	res, err := db.Exec(`SELECT * FROM jobs LIMIT 2 OFFSET 3`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs with LIMIT 2 OFFSET 3, got %d", len(res.Docs))
	}
}

func TestLargeInsertMultiPage(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insérer assez de documents pour remplir plusieurs pages
	for i := 0; i < 200; i++ {
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO jobs VALUES (name="job_%d", description="this is a description for job number %d which should take some space", idx=%d)`, i, i, i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	res, err := db.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 200 {
		t.Errorf("expected 200 docs, got %d", len(res.Docs))
	}
}

func TestConcurrentMixedOps(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Seed
	for i := 0; i < 20; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO jobs VALUES (id=%d, val=0)`, i))
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	// Readers concurrents
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				_, err := db.Exec(`SELECT * FROM jobs`)
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	// Writers concurrents sur des documents différents
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				q := fmt.Sprintf(`INSERT INTO jobs VALUES (id=%d, gid=%d)`, 100+gid*10+i, gid)
				_, err := db.Exec(q)
				if err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent error: %v", err)
	}

	// Vérifier le total
	res, err := db.Exec(`SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	expected := 20 + 50 // seed + inserts
	if len(res.Docs) != expected {
		t.Errorf("expected %d docs, got %d", expected, len(res.Docs))
	}
}

// ---------- Tests JOIN ----------

func TestInnerJoin(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Table jobs
	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	db.Exec(`INSERT INTO jobs VALUES (type="postgres", retry=0)`)

	// Table logs avec un champ type commun
	db.Exec(`INSERT INTO logs VALUES (type="oracle", msg="started")`)
	db.Exec(`INSERT INTO logs VALUES (type="oracle", msg="finished")`)
	db.Exec(`INSERT INTO logs VALUES (type="mysql", msg="error")`)

	// INNER JOIN
	res, err := db.Exec(`SELECT * FROM logs JOIN jobs ON jobs.type = logs.type`)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	// oracle a 2 logs × 1 job = 2, mysql a 1 log × 1 job = 1 → total 3
	if len(res.Docs) != 3 {
		t.Errorf("expected 3 joined docs, got %d", len(res.Docs))
	}

	// Vérifier que les champs des DEUX tables sont présents
	for _, rd := range res.Docs {
		// Champ de logs (niveau racine)
		if _, ok := rd.Doc.Get("msg"); !ok {
			t.Error("expected 'msg' from logs table in joined doc")
		}
		// Champ de jobs (niveau racine, écrase type de logs)
		if _, ok := rd.Doc.Get("retry"); !ok {
			t.Error("expected 'retry' from jobs table in joined doc")
		}
		// Accès qualifié : jobs.retry via sous-document
		if v, ok := rd.Doc.GetNested([]string{"jobs", "retry"}); !ok {
			t.Error("expected qualified 'jobs.retry' in joined doc")
		} else if v == nil {
			t.Error("jobs.retry should not be nil")
		}
		// Accès qualifié : logs.msg via sous-document
		if v, ok := rd.Doc.GetNested([]string{"logs", "msg"}); !ok {
			t.Error("expected qualified 'logs.msg' in joined doc")
		} else if v == nil {
			t.Error("logs.msg should not be nil")
		}
	}
}

func TestLeftJoin(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	db.Exec(`INSERT INTO jobs VALUES (type="postgres", retry=0)`)

	db.Exec(`INSERT INTO logs VALUES (type="oracle", msg="started")`)

	// LEFT JOIN : tous les jobs, même sans log
	res, err := db.Exec(`SELECT * FROM jobs LEFT JOIN logs ON jobs.type = logs.type`)
	if err != nil {
		t.Fatalf("left join: %v", err)
	}
	// oracle: 1 match, mysql: 0 matches (kept), postgres: 0 matches (kept) → 3
	if len(res.Docs) != 3 {
		t.Errorf("expected 3 left-joined docs, got %d", len(res.Docs))
	}
}

func TestRightJoin(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	db.Exec(`INSERT INTO jobs VALUES (type="postgres", retry=0)`)

	db.Exec(`INSERT INTO logs VALUES (type="oracle", msg="started")`)
	db.Exec(`INSERT INTO logs VALUES (type="redis", msg="connected")`)

	// RIGHT JOIN : tous les logs, même sans job correspondant
	res, err := db.Exec(`SELECT * FROM jobs RIGHT JOIN logs ON jobs.type = logs.type`)
	if err != nil {
		t.Fatalf("right join: %v", err)
	}
	// oracle: match, redis: no match (kept with NULL jobs) → 2
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 right-joined docs, got %d", len(res.Docs))
		for i, d := range res.Docs {
			t.Logf("  doc[%d]: %+v", i, d.Doc.Fields)
		}
	}

	// Verify redis row exists (right side kept)
	found := false
	for _, d := range res.Docs {
		if v, _ := d.Doc.Get("msg"); v == "connected" {
			found = true
		}
	}
	if !found {
		t.Error("expected redis log row to be preserved in RIGHT JOIN")
	}
}

func TestRightJoinWithAlias(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO employees VALUES (name="Alice", dept_id=1)`)
	db.Exec(`INSERT INTO employees VALUES (name="Bob", dept_id=2)`)

	db.Exec(`INSERT INTO departments VALUES (id=1, dname="Engineering")`)
	db.Exec(`INSERT INTO departments VALUES (id=2, dname="Sales")`)
	db.Exec(`INSERT INTO departments VALUES (id=3, dname="HR")`)

	// RIGHT JOIN : all departments, even without employees
	res, err := db.Exec(`SELECT * FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatalf("right join alias: %v", err)
	}
	// Alice→Engineering, Bob→Sales, HR→no employee = 3
	if len(res.Docs) != 3 {
		t.Errorf("expected 3, got %d", len(res.Docs))
		for i, d := range res.Docs {
			t.Logf("  doc[%d]: %+v", i, d.Doc.Fields)
		}
	}
}

func TestJoinWithAlias(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO logs VALUES (type="oracle", msg="started")`)

	// JOIN avec aliases
	res, err := db.Exec(`SELECT * FROM jobs j JOIN logs l ON j.type = l.type`)
	if err != nil {
		t.Fatalf("join alias: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 joined doc, got %d", len(res.Docs))
	}

	// Accès via alias
	doc := res.Docs[0].Doc
	if v, ok := doc.GetNested([]string{"j", "retry"}); !ok || v != int64(5) {
		t.Errorf("expected j.retry=5, got %v (ok=%v)", v, ok)
	}
	if v, ok := doc.GetNested([]string{"l", "msg"}); !ok || v != "started" {
		t.Errorf("expected l.msg=started, got %v (ok=%v)", v, ok)
	}
}

func TestJoinWithProjection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO logs VALUES (type="oracle", msg="started")`)

	// Projection avec noms qualifiés
	res, err := db.Exec(`SELECT jobs.type, logs.msg FROM jobs JOIN logs ON jobs.type = logs.type`)
	if err != nil {
		t.Fatalf("join projection: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}

	doc := res.Docs[0].Doc
	// Les DotExpr dans la projection accèdent aux sous-documents
	if v, ok := doc.Get("jobs.type"); !ok {
		// Peut être stocké comme champ plat "jobs.type" par la projection
		t.Logf("jobs.type not found as flat key, checking nested")
		if v2, ok2 := doc.GetNested([]string{"jobs", "type"}); !ok2 {
			t.Error("expected jobs.type in projection")
		} else if v2 != "oracle" {
			t.Errorf("expected jobs.type=oracle, got %v", v2)
		}
	} else if v != "oracle" {
		t.Errorf("expected jobs.type=oracle, got %v", v)
	}
}

func TestJoinNoMatch(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle")`)
	db.Exec(`INSERT INTO logs VALUES (type="mysql", msg="error")`)

	// INNER JOIN sans correspondance → 0 résultats
	res, err := db.Exec(`SELECT * FROM jobs JOIN logs ON jobs.type = logs.type`)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if len(res.Docs) != 0 {
		t.Errorf("expected 0 joined docs, got %d", len(res.Docs))
	}
}

func TestJoinWithWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	db.Exec(`INSERT INTO logs VALUES (type="oracle", msg="started")`)
	db.Exec(`INSERT INTO logs VALUES (type="mysql", msg="error")`)

	// JOIN + WHERE filtre sur un champ
	res, err := db.Exec(`SELECT * FROM jobs JOIN logs ON jobs.type = logs.type WHERE retry > 3`)
	if err != nil {
		t.Fatalf("join where: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc (oracle only), got %d", len(res.Docs))
	}
}

// ---------- Tests INSERT INTO ... SELECT ----------

func TestInsertFromSelectAll(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Créer la source
	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	db.Exec(`INSERT INTO jobs VALUES (type="postgres", retry=0)`)

	// Copier toute la table
	res, err := db.Exec(`INSERT INTO backup SELECT * FROM jobs`)
	if err != nil {
		t.Fatalf("insert-select: %v", err)
	}
	if res.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", res.RowsAffected)
	}

	// Vérifier la copie
	res2, err := db.Exec(`SELECT * FROM backup`)
	if err != nil {
		t.Fatalf("select backup: %v", err)
	}
	if len(res2.Docs) != 3 {
		t.Errorf("expected 3 docs in backup, got %d", len(res2.Docs))
	}
}

func TestInsertFromSelectWithWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	db.Exec(`INSERT INTO jobs VALUES (type="postgres", retry=0)`)

	// Copier seulement les jobs avec retry > 0
	res, err := db.Exec(`INSERT INTO active_jobs SELECT * FROM jobs WHERE retry > 0`)
	if err != nil {
		t.Fatalf("insert-select where: %v", err)
	}
	if res.RowsAffected != 2 {
		t.Errorf("expected 2 rows affected, got %d", res.RowsAffected)
	}

	res2, err := db.Exec(`SELECT * FROM active_jobs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res2.Docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(res2.Docs))
	}
}

func TestInsertFromSelectWithProjection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5, enabled=true)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2, enabled=false)`)

	// Copier seulement certains champs
	res, err := db.Exec(`INSERT INTO types SELECT type FROM jobs`)
	if err != nil {
		t.Fatalf("insert-select projection: %v", err)
	}
	if res.RowsAffected != 2 {
		t.Errorf("expected 2, got %d", res.RowsAffected)
	}

	res2, err := db.Exec(`SELECT * FROM types`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	for _, rd := range res2.Docs {
		if _, ok := rd.Doc.Get("type"); !ok {
			t.Error("expected 'type' field in copied doc")
		}
		// retry ne devrait PAS être copié
		if _, ok := rd.Doc.Get("retry"); ok {
			t.Error("'retry' should not be in copied doc (projection)")
		}
	}
}

func TestInsertFromSelectEmpty(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle")`)

	// WHERE qui ne matche rien
	res, err := db.Exec(`INSERT INTO empty SELECT * FROM jobs WHERE type = "nonexistent"`)
	if err != nil {
		t.Fatalf("insert-select empty: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected, got %d", res.RowsAffected)
	}
}

// ---------- Tests LIKE ----------

func TestLike(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (name="Alice", city="Paris")`)
	db.Exec(`INSERT INTO users VALUES (name="Bob", city="Bordeaux")`)
	db.Exec(`INSERT INTO users VALUES (name="Charlie", city="Lyon")`)
	db.Exec(`INSERT INTO users VALUES (name="Alain", city="Marseille")`)

	// LIKE avec %
	res, err := db.Exec(`SELECT * FROM users WHERE name LIKE "Al%"`)
	if err != nil {
		t.Fatalf("like: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs (Alice, Alain), got %d", len(res.Docs))
	}

	// LIKE avec _
	res, err = db.Exec(`SELECT * FROM users WHERE name LIKE "Bo_"`)
	if err != nil {
		t.Fatalf("like underscore: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc (Bob), got %d", len(res.Docs))
	}

	// NOT LIKE
	res, err = db.Exec(`SELECT * FROM users WHERE name NOT LIKE "Al%"`)
	if err != nil {
		t.Fatalf("not like: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs (Bob, Charlie), got %d", len(res.Docs))
	}

	// LIKE case insensitive
	res, err = db.Exec(`SELECT * FROM users WHERE name LIKE "al%"`)
	if err != nil {
		t.Fatalf("like case: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs case-insensitive, got %d", len(res.Docs))
	}
}

// ---------- Tests DISTINCT ----------

func TestDistinct(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO logs VALUES (level="INFO", msg="start")`)
	db.Exec(`INSERT INTO logs VALUES (level="INFO", msg="start")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR", msg="fail")`)
	db.Exec(`INSERT INTO logs VALUES (level="INFO", msg="start")`)

	res, err := db.Exec(`SELECT level FROM logs`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 4 {
		t.Errorf("expected 4 docs, got %d", len(res.Docs))
	}

	res, err = db.Exec(`SELECT DISTINCT level FROM logs`)
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 distinct levels, got %d", len(res.Docs))
	}
}

// ---------- Tests COUNT(*) sans GROUP BY ----------

func TestCountWithoutGroupBy(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO items VALUES (name="A")`)
	db.Exec(`INSERT INTO items VALUES (name="B")`)
	db.Exec(`INSERT INTO items VALUES (name="C")`)

	res, err := db.Exec(`SELECT COUNT(*) FROM items`)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 result doc, got %d", len(res.Docs))
	}
	val, ok := res.Docs[0].Doc.Get("COUNT")
	if !ok {
		t.Fatal("expected COUNT field")
	}
	if val != int64(3) {
		t.Errorf("expected COUNT=3, got %v", val)
	}
}

func TestCountWithWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO items VALUES (name="A", active=true)`)
	db.Exec(`INSERT INTO items VALUES (name="B", active=false)`)
	db.Exec(`INSERT INTO items VALUES (name="C", active=true)`)

	res, err := db.Exec(`SELECT COUNT(*) FROM items WHERE active = true`)
	if err != nil {
		t.Fatalf("count where: %v", err)
	}
	val, _ := res.Docs[0].Doc.Get("COUNT")
	if val != int64(2) {
		t.Errorf("expected COUNT=2, got %v", val)
	}
}

// ---------- Tests HAVING avec agrégats ----------

func TestHavingWithAggregate(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO logs VALUES (level="INFO", idx=%d)`, i))
	}
	db.Exec(`INSERT INTO logs VALUES (level="ERROR", idx=99)`)

	res, err := db.Exec(`SELECT level, COUNT(*) FROM logs GROUP BY level HAVING COUNT(*) > 1`)
	if err != nil {
		t.Fatalf("having: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 group (INFO), got %d", len(res.Docs))
	}
	if len(res.Docs) > 0 {
		v, _ := res.Docs[0].Doc.Get("level")
		if v != "INFO" {
			t.Errorf("expected INFO group, got %v", v)
		}
	}
}

// ---------- Tests Vacuum ----------

func TestVacuum(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO data VALUES (idx=%d)`, i))
	}
	db.Exec(`DELETE FROM data WHERE idx < 5`)

	res, _ := db.Exec(`SELECT * FROM data`)
	if len(res.Docs) != 5 {
		t.Errorf("expected 5 docs before vacuum, got %d", len(res.Docs))
	}

	n, err := db.Vacuum()
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 reclaimed records, got %d", n)
	}

	res, _ = db.Exec(`SELECT * FROM data`)
	if len(res.Docs) != 5 {
		t.Errorf("expected 5 docs after vacuum, got %d", len(res.Docs))
	}
}

// ---------- Tests SUM/AVG/MIN/MAX sans GROUP BY ----------

func TestStandaloneAggregates(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO nums VALUES (val=10)`)
	db.Exec(`INSERT INTO nums VALUES (val=20)`)
	db.Exec(`INSERT INTO nums VALUES (val=30)`)

	res, err := db.Exec(`SELECT SUM(val) FROM nums`)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	if v, _ := res.Docs[0].Doc.Get("SUM"); v != int64(60) {
		t.Errorf("expected SUM=60, got %v", v)
	}

	res, err = db.Exec(`SELECT MIN(val) FROM nums`)
	if err != nil {
		t.Fatalf("min: %v", err)
	}
	if v, _ := res.Docs[0].Doc.Get("MIN"); v != int64(10) {
		t.Errorf("expected MIN=10, got %v", v)
	}

	res, err = db.Exec(`SELECT MAX(val) FROM nums`)
	if err != nil {
		t.Fatalf("max: %v", err)
	}
	if v, _ := res.Docs[0].Doc.Get("MAX"); v != int64(30) {
		t.Errorf("expected MAX=30, got %v", v)
	}
}

// ---------- Tests DROP TABLE ----------

func TestDropTable(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO temp VALUES (x=1)`)
	db.Exec(`INSERT INTO temp VALUES (x=2)`)
	db.Exec(`INSERT INTO keep VALUES (y=99)`)

	// Vérifier que temp existe
	colls := db.Collections()
	found := false
	for _, c := range colls {
		if c == "temp" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected 'temp' collection to exist")
	}

	// DROP TABLE
	_, err = db.Exec(`DROP TABLE temp`)
	if err != nil {
		t.Fatalf("drop table: %v", err)
	}

	// temp ne doit plus exister
	colls = db.Collections()
	for _, c := range colls {
		if c == "temp" {
			t.Error("'temp' should not exist after DROP TABLE")
		}
	}

	// keep doit toujours exister
	res, err := db.Exec(`SELECT * FROM keep`)
	if err != nil {
		t.Fatalf("select keep: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc in keep, got %d", len(res.Docs))
	}

	// DROP TABLE inexistant => erreur
	_, err = db.Exec(`DROP TABLE nonexistent`)
	if err == nil {
		t.Error("expected error dropping nonexistent table")
	}
}

// ---------- Tests Schema ----------

func TestSchema(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO people VALUES (name="Alice", age=30)`)
	db.Exec(`INSERT INTO people VALUES (name="Bob", age=25, email="bob@test.com")`)

	schemas := db.Schema()
	if len(schemas) == 0 {
		t.Fatal("expected at least 1 schema")
	}

	var peopleSchema *CollectionSchema
	for i := range schemas {
		if schemas[i].Name == "people" {
			peopleSchema = &schemas[i]
		}
	}
	if peopleSchema == nil {
		t.Fatal("expected 'people' schema")
	}
	if peopleSchema.DocCount != 2 {
		t.Errorf("expected 2 docs, got %d", peopleSchema.DocCount)
	}
	// email devrait apparaître avec count=1
	for _, f := range peopleSchema.Fields {
		if f.Name == "email" && f.Count != 1 {
			t.Errorf("expected email count=1, got %d", f.Count)
		}
	}
}

// ---------- Tests BETWEEN ----------

func TestBetween(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 1; i <= 10; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO nums VALUES (val=%d)`, i))
	}

	res, err := db.Exec(`SELECT * FROM nums WHERE val BETWEEN 3 AND 7`)
	if err != nil {
		t.Fatalf("between: %v", err)
	}
	if len(res.Docs) != 5 {
		t.Errorf("expected 5 docs (3..7), got %d", len(res.Docs))
	}

	res, err = db.Exec(`SELECT * FROM nums WHERE val NOT BETWEEN 3 AND 7`)
	if err != nil {
		t.Fatalf("not between: %v", err)
	}
	if len(res.Docs) != 5 {
		t.Errorf("expected 5 docs (1,2,8,9,10), got %d", len(res.Docs))
	}
}

// ---------- Tests COUNT(field) ----------

func TestCountField(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO items VALUES (name="A", tag="x")`)
	db.Exec(`INSERT INTO items VALUES (name="B")`)
	db.Exec(`INSERT INTO items VALUES (name="C", tag="y")`)

	// COUNT(*) = 3
	res, err := db.Exec(`SELECT COUNT(*) FROM items`)
	if err != nil {
		t.Fatalf("count *: %v", err)
	}
	if v, _ := res.Docs[0].Doc.Get("COUNT"); v != int64(3) {
		t.Errorf("expected COUNT(*)=3, got %v", v)
	}

	// COUNT(tag) = 2 (B n'a pas de tag)
	res, err = db.Exec(`SELECT COUNT(tag) FROM items`)
	if err != nil {
		t.Fatalf("count field: %v", err)
	}
	if v, _ := res.Docs[0].Doc.Get("COUNT"); v != int64(2) {
		t.Errorf("expected COUNT(tag)=2, got %v", v)
	}
}

// ---------- Tests EXPLAIN ----------

func TestExplain(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)

	res, err := db.Exec(`EXPLAIN SELECT * FROM jobs WHERE retry > 3`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 explain doc, got %d", len(res.Docs))
	}

	tp, _ := res.Docs[0].Doc.Get("type")
	if tp != "SELECT" {
		t.Errorf("expected type=SELECT, got %v", tp)
	}
	scan, _ := res.Docs[0].Doc.Get("scan")
	if scan != "FULL SCAN" {
		t.Errorf("expected scan=FULL SCAN, got %v", scan)
	}
	filter, _ := res.Docs[0].Doc.Get("filter")
	if filter != "WHERE" {
		t.Errorf("expected filter=WHERE, got %v", filter)
	}
}

func TestExplainWithIndex(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle")`)
	db.Exec(`CREATE INDEX ON jobs (type)`)

	res, err := db.Exec(`EXPLAIN SELECT * FROM jobs WHERE type = "oracle"`)
	if err != nil {
		t.Fatalf("explain index: %v", err)
	}
	scan, _ := res.Docs[0].Doc.Get("scan")
	if scan != "INDEX LOOKUP" {
		t.Errorf("expected INDEX LOOKUP, got %v", scan)
	}
}

// ---------- Tests AVG standalone ----------

func TestAvgStandalone(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO scores VALUES (val=10)`)
	db.Exec(`INSERT INTO scores VALUES (val=20)`)
	db.Exec(`INSERT INTO scores VALUES (val=30)`)

	res, err := db.Exec(`SELECT AVG(val) FROM scores`)
	if err != nil {
		t.Fatalf("avg: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	v, _ := res.Docs[0].Doc.Get("AVG")
	// AVG(10,20,30) = 20.0
	switch val := v.(type) {
	case float64:
		if val != 20.0 {
			t.Errorf("expected AVG=20.0, got %v", val)
		}
	case int64:
		if val != 20 {
			t.Errorf("expected AVG=20, got %v", val)
		}
	default:
		t.Errorf("unexpected AVG type %T: %v", v, v)
	}
}

// ---------- Edge cases ----------

func TestUpdateEmptyCollection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	res, err := db.Exec(`UPDATE ghost SET x=1 WHERE x=0`)
	if err != nil {
		t.Fatalf("update empty: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Errorf("expected 0 rows, got %d", res.RowsAffected)
	}
}

func TestDeleteEmptyCollection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	res, err := db.Exec(`DELETE FROM ghost WHERE x=0`)
	if err != nil {
		t.Fatalf("delete empty: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Errorf("expected 0 rows, got %d", res.RowsAffected)
	}
}

func TestBetweenStrings(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO words VALUES (w="apple")`)
	db.Exec(`INSERT INTO words VALUES (w="banana")`)
	db.Exec(`INSERT INTO words VALUES (w="cherry")`)
	db.Exec(`INSERT INTO words VALUES (w="date")`)

	res, err := db.Exec(`SELECT * FROM words WHERE w BETWEEN "banana" AND "cherry"`)
	if err != nil {
		t.Fatalf("between strings: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 (banana, cherry), got %d", len(res.Docs))
	}
}

func TestMultipleAggregatesStandalone(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO data VALUES (v=5)`)
	db.Exec(`INSERT INTO data VALUES (v=15)`)
	db.Exec(`INSERT INTO data VALUES (v=25)`)

	res, err := db.Exec(`SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM data`)
	if err != nil {
		t.Fatalf("multi agg: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	d := res.Docs[0].Doc
	if cnt, _ := d.Get("COUNT"); cnt != int64(3) {
		t.Errorf("COUNT: expected 3, got %v", cnt)
	}
	if sum, _ := d.Get("SUM"); sum != int64(45) {
		t.Errorf("SUM: expected 45, got %v", sum)
	}
	if mn, _ := d.Get("MIN"); mn != int64(5) {
		t.Errorf("MIN: expected 5, got %v", mn)
	}
	if mx, _ := d.Get("MAX"); mx != int64(25) {
		t.Errorf("MAX: expected 25, got %v", mx)
	}
}

// ---------- Tests IF EXISTS / IF NOT EXISTS ----------

func TestDropTableIfExists(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// DROP TABLE IF EXISTS sur collection inexistante → pas d'erreur
	_, err = db.Exec(`DROP TABLE IF EXISTS ghost`)
	if err != nil {
		t.Errorf("expected no error with IF EXISTS, got %v", err)
	}

	// DROP TABLE sans IF EXISTS → erreur
	_, err = db.Exec(`DROP TABLE ghost`)
	if err == nil {
		t.Error("expected error dropping nonexistent table without IF EXISTS")
	}
}

func TestCreateIndexIfNotExists(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO jobs VALUES (type="oracle")`)
	db.Exec(`CREATE INDEX ON jobs (type)`)

	// CREATE INDEX IF NOT EXISTS sur index existant → pas d'erreur
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS ON jobs (type)`)
	if err != nil {
		t.Errorf("expected no error with IF NOT EXISTS, got %v", err)
	}

	// CREATE INDEX sans IF NOT EXISTS → erreur
	_, err = db.Exec(`CREATE INDEX ON jobs (type)`)
	if err == nil {
		t.Error("expected error creating duplicate index without IF NOT EXISTS")
	}
}

func TestDropIndexIfExists(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// DROP INDEX IF EXISTS sur index inexistant → pas d'erreur
	_, err = db.Exec(`DROP INDEX IF EXISTS ON jobs (type)`)
	if err != nil {
		t.Errorf("expected no error with IF EXISTS, got %v", err)
	}
}

// ---------- Tests Aggregate Aliases ----------

func TestAggregateAlias(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO items VALUES (price=10)`)
	db.Exec(`INSERT INTO items VALUES (price=20)`)
	db.Exec(`INSERT INTO items VALUES (price=30)`)

	res, err := db.Exec(`SELECT COUNT(*) AS total, SUM(price) AS revenue FROM items`)
	if err != nil {
		t.Fatalf("alias: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	d := res.Docs[0].Doc
	if v, ok := d.Get("total"); !ok || v != int64(3) {
		t.Errorf("expected total=3, got %v (ok=%v)", v, ok)
	}
	if v, ok := d.Get("revenue"); !ok || v != int64(60) {
		t.Errorf("expected revenue=60, got %v (ok=%v)", v, ok)
	}
}

// ---------- Tests INSERT OR REPLACE ----------

func TestInsertOrReplace(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert initial
	db.Exec(`INSERT INTO users VALUES (email="alice@test.com", name="Alice", score=10)`)
	db.Exec(`INSERT INTO users VALUES (email="bob@test.com", name="Bob", score=20)`)

	// UPSERT : alice existe → update
	_, err = db.Exec(`INSERT OR REPLACE INTO users VALUES (email="alice@test.com", name="Alice Updated", score=99)`)
	if err != nil {
		t.Fatalf("upsert existing: %v", err)
	}

	// Vérifier que Alice a été mise à jour, pas dupliquée
	res, _ := db.Exec(`SELECT * FROM users WHERE email = "alice@test.com"`)
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 alice, got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "Alice Updated" {
		t.Errorf("expected 'Alice Updated', got %v", name)
	}
	score, _ := res.Docs[0].Doc.Get("score")
	if score != int64(99) {
		t.Errorf("expected score=99, got %v", score)
	}

	// UPSERT : charlie n'existe pas → insert
	_, err = db.Exec(`INSERT OR REPLACE INTO users VALUES (email="charlie@test.com", name="Charlie", score=50)`)
	if err != nil {
		t.Fatalf("upsert new: %v", err)
	}

	// Vérifier total = 3
	res, _ = db.Exec(`SELECT COUNT(*) FROM users`)
	cnt, _ := res.Docs[0].Doc.Get("COUNT")
	if cnt != int64(3) {
		t.Errorf("expected 3 users, got %v", cnt)
	}
}

// ---------- Tests Persistent Index ----------

func TestPersistentIndex(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	defer os.Remove(path + ".wal")

	// Ouvrir, insérer, créer index, fermer
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
	db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)
	db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=10)`)
	db.Exec(`CREATE INDEX ON jobs (type)`)

	// Vérifier que EXPLAIN montre INDEX LOOKUP
	res, _ := db.Exec(`EXPLAIN SELECT * FROM jobs WHERE type = "oracle"`)
	scan, _ := res.Docs[0].Doc.Get("scan")
	if scan != "INDEX LOOKUP" {
		t.Errorf("before close: expected INDEX LOOKUP, got %v", scan)
	}
	db.Close()

	// Réouvrir — l'index doit être reconstruit automatiquement
	db2, err := Open(path)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer db2.Close()

	// EXPLAIN doit toujours montrer INDEX LOOKUP
	res, _ = db2.Exec(`EXPLAIN SELECT * FROM jobs WHERE type = "oracle"`)
	scan, _ = res.Docs[0].Doc.Get("scan")
	if scan != "INDEX LOOKUP" {
		t.Errorf("after reopen: expected INDEX LOOKUP, got %v", scan)
	}

	// Les données doivent être intactes
	res, _ = db2.Exec(`SELECT * FROM jobs WHERE type = "oracle"`)
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 oracle jobs, got %d", len(res.Docs))
	}

	// DROP INDEX, fermer, réouvrir → plus d'index
	db2.Exec(`DROP INDEX ON jobs (type)`)
	db2.Close()

	db3, err := Open(path)
	if err != nil {
		t.Fatalf("open3: %v", err)
	}
	defer db3.Close()

	res, _ = db3.Exec(`EXPLAIN SELECT * FROM jobs WHERE type = "oracle"`)
	scan, _ = res.Docs[0].Doc.Get("scan")
	if scan != "FULL SCAN" {
		t.Errorf("after drop+reopen: expected FULL SCAN, got %v", scan)
	}
}

// ---------- Tests Batch INSERT ----------

func TestBatchInsert(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	res, err := db.Exec(`INSERT INTO colors VALUES (name="red", hex="#ff0000"), (name="green", hex="#00ff00"), (name="blue", hex="#0000ff")`)
	if err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	if res.RowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", res.RowsAffected)
	}

	res, err = db.Exec(`SELECT * FROM colors`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Errorf("expected 3 docs, got %d", len(res.Docs))
	}
}

func TestBatchInsertSingle(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Single VALUES group still works
	res, err := db.Exec(`INSERT INTO things VALUES (x=1)`)
	if err != nil {
		t.Fatalf("single insert: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("expected 1 row, got %d", res.RowsAffected)
	}
}

// ---------- Tests Complex WHERE ----------

func TestComplexWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO p VALUES (name="Alice", age=30, city="Paris")`)
	db.Exec(`INSERT INTO p VALUES (name="Bob", age=25, city="Lyon")`)
	db.Exec(`INSERT INTO p VALUES (name="Charlie", age=35, city="Paris")`)
	db.Exec(`INSERT INTO p VALUES (name="Diana", age=28, city="Lyon")`)

	// (age > 27 AND city = "Paris") OR name = "Bob"
	res, _ := db.Exec(`SELECT * FROM p WHERE (age > 27 AND city = "Paris") OR name = "Bob"`)
	if len(res.Docs) != 3 {
		t.Errorf("expected 3 (Alice, Charlie, Bob), got %d", len(res.Docs))
	}

	// NOT (city = "Paris")
	res, _ = db.Exec(`SELECT * FROM p WHERE NOT city = "Paris"`)
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 (Bob, Diana), got %d", len(res.Docs))
	}

	// BETWEEN combined with AND
	res, _ = db.Exec(`SELECT * FROM p WHERE age BETWEEN 26 AND 31 AND city = "Lyon"`)
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 (Diana), got %d", len(res.Docs))
	}
}

// ---------- Tests NOT IN ----------

func TestNotIn(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO fruits VALUES (name="apple")`)
	db.Exec(`INSERT INTO fruits VALUES (name="banana")`)
	db.Exec(`INSERT INTO fruits VALUES (name="cherry")`)
	db.Exec(`INSERT INTO fruits VALUES (name="date")`)

	// IN
	res, _ := db.Exec(`SELECT * FROM fruits WHERE name IN ("apple", "cherry")`)
	if len(res.Docs) != 2 {
		t.Errorf("IN: expected 2, got %d", len(res.Docs))
	}

	// NOT IN
	res, _ = db.Exec(`SELECT * FROM fruits WHERE name NOT IN ("apple", "cherry")`)
	if len(res.Docs) != 2 {
		t.Errorf("NOT IN: expected 2, got %d", len(res.Docs))
	}
}

// ---------- Tests GROUP BY + ORDER BY ----------

func TestGroupByOrderBy(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO logs VALUES (level="INFO", msg="a")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR", msg="b")`)
	db.Exec(`INSERT INTO logs VALUES (level="INFO", msg="c")`)
	db.Exec(`INSERT INTO logs VALUES (level="WARN", msg="d")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR", msg="e")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR", msg="f")`)

	// GROUP BY + ORDER BY COUNT DESC
	res, err := db.Exec(`SELECT level, COUNT(*) AS cnt FROM logs GROUP BY level ORDER BY cnt DESC`)
	if err != nil {
		t.Fatalf("group+order: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(res.Docs))
	}
	// ERROR=3, INFO=2, WARN=1
	first, _ := res.Docs[0].Doc.Get("level")
	if first != "ERROR" {
		t.Errorf("expected first=ERROR, got %v", first)
	}
	last, _ := res.Docs[2].Doc.Get("level")
	if last != "WARN" {
		t.Errorf("expected last=WARN, got %v", last)
	}
}

// ---------- Tests GROUP BY + HAVING + LIMIT ----------

func TestGroupByHavingLimit(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO ev VALUES (type="A", v=%d)`, i))
	}
	for i := 0; i < 3; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO ev VALUES (type="B", v=%d)`, i))
	}
	db.Exec(`INSERT INTO ev VALUES (type="C", v=0)`)

	// Without LIMIT first to check GROUP BY + HAVING works
	res, err := db.Exec(`SELECT type, COUNT(*) AS cnt FROM ev GROUP BY type HAVING COUNT(*) > 1`)
	if err != nil {
		t.Fatalf("having: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 groups (A=5, B=3), got %d", len(res.Docs))
		for _, d := range res.Docs {
			tp, _ := d.Doc.Get("type")
			cn, _ := d.Doc.Get("cnt")
			t.Logf("  type=%v cnt=%v", tp, cn)
		}
	}

	// HAVING + LIMIT
	res, err = db.Exec(`SELECT type, COUNT(*) AS cnt FROM ev GROUP BY type HAVING COUNT(*) > 1 LIMIT 1`)
	if err != nil {
		t.Fatalf("having+limit: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc (LIMIT 1), got %d", len(res.Docs))
	}
}

// ---------- Tests Nested Queries ----------

func TestNestedDocumentQuery(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO conf VALUES (name="srv1", net.ip="10.0.0.1", net.port=8080)`)
	db.Exec(`INSERT INTO conf VALUES (name="srv2", net.ip="10.0.0.2", net.port=9090)`)

	// Query on nested field
	res, _ := db.Exec(`SELECT * FROM conf WHERE net.port > 8080`)
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 (srv2), got %d", len(res.Docs))
	}

	// Projection of nested field
	res, _ = db.Exec(`SELECT name, net.ip FROM conf`)
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(res.Docs))
	}
}

// ---------- Tests UPDATE with Expressions ----------

func TestUpdateWithExpression(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO counters VALUES (name="hits", value=10)`)
	db.Exec(`INSERT INTO counters VALUES (name="errors", value=3)`)

	// SET value = value + 5
	_, err = db.Exec(`UPDATE counters SET value = value + 5 WHERE name = "hits"`)
	if err != nil {
		t.Fatalf("update expr: %v", err)
	}

	res, _ := db.Exec(`SELECT * FROM counters WHERE name = "hits"`)
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	val, _ := res.Docs[0].Doc.Get("value")
	if val != int64(15) {
		t.Errorf("expected value=15, got %v", val)
	}

	// SET value = value * 2
	db.Exec(`UPDATE counters SET value = value * 2 WHERE name = "errors"`)
	res, _ = db.Exec(`SELECT * FROM counters WHERE name = "errors"`)
	val, _ = res.Docs[0].Doc.Get("value")
	if val != int64(6) {
		t.Errorf("expected value=6, got %v", val)
	}

	// SET value = value - 1
	db.Exec(`UPDATE counters SET value = value - 1 WHERE name = "hits"`)
	res, _ = db.Exec(`SELECT * FROM counters WHERE name = "hits"`)
	val, _ = res.Docs[0].Doc.Get("value")
	if val != int64(14) {
		t.Errorf("expected value=14, got %v", val)
	}
}

func TestSelectWithArithmetic(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO items VALUES (price=100, qty=3)`)

	// WHERE with arithmetic: price * qty > 200
	res, _ := db.Exec(`SELECT * FROM items WHERE price * qty > 200`)
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc, got %d", len(res.Docs))
	}

	// Negative number
	db.Exec(`INSERT INTO items VALUES (price=-5, qty=10)`)
	res, _ = db.Exec(`SELECT * FROM items WHERE price < 0`)
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 negative price, got %d", len(res.Docs))
	}
}

// ---------- Tests NULL in VALUES ----------

func TestNullInValues(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT INTO t VALUES (name="Alice", email=null)`)
	if err != nil {
		t.Fatalf("insert null: %v", err)
	}

	res, _ := db.Exec(`SELECT * FROM t WHERE email IS NULL`)
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 doc with null email, got %d", len(res.Docs))
	}
}

// ---------- Tests COUNT DISTINCT ----------

func TestCountDistinct(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO logs VALUES (level="INFO")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR")`)
	db.Exec(`INSERT INTO logs VALUES (level="INFO")`)
	db.Exec(`INSERT INTO logs VALUES (level="WARN")`)
	db.Exec(`INSERT INTO logs VALUES (level="ERROR")`)

	// COUNT(*) = 5
	res, _ := db.Exec(`SELECT COUNT(*) FROM logs`)
	cnt, _ := res.Docs[0].Doc.Get("COUNT")
	if cnt != int64(5) {
		t.Errorf("expected COUNT=5, got %v", cnt)
	}

	// SELECT DISTINCT level → 3 unique
	res, _ = db.Exec(`SELECT DISTINCT level FROM logs`)
	if len(res.Docs) != 3 {
		t.Errorf("expected 3 distinct levels, got %d", len(res.Docs))
	}
}

// ---------- Tests UPDATE multiple fields ----------

func TestUpdateMultipleFields(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (name="Alice", age=30, score=100)`)

	_, err = db.Exec(`UPDATE users SET age = age + 1, score = score * 2 WHERE name = "Alice"`)
	if err != nil {
		t.Fatalf("update multi: %v", err)
	}

	res, _ := db.Exec(`SELECT * FROM users WHERE name = "Alice"`)
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	age, _ := res.Docs[0].Doc.Get("age")
	if age != int64(31) {
		t.Errorf("expected age=31, got %v", age)
	}
	score, _ := res.Docs[0].Doc.Get("score")
	if score != int64(200) {
		t.Errorf("expected score=200, got %v", score)
	}
}

// ---------- Tests TRUNCATE TABLE ----------

func TestTruncateTable(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO logs VALUES (msg="a")`)
	db.Exec(`INSERT INTO logs VALUES (msg="b")`)
	db.Exec(`INSERT INTO logs VALUES (msg="c")`)

	res, _ := db.Exec(`SELECT COUNT(*) FROM logs`)
	cnt, _ := res.Docs[0].Doc.Get("COUNT")
	if cnt != int64(3) {
		t.Errorf("expected 3 before truncate, got %v", cnt)
	}

	_, err = db.Exec(`TRUNCATE TABLE logs`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}

	res, _ = db.Exec(`SELECT COUNT(*) FROM logs`)
	if len(res.Docs) == 0 {
		// Collection vide, pas de docs
	} else {
		cnt, _ = res.Docs[0].Doc.Get("COUNT")
		if cnt != int64(0) {
			t.Errorf("expected 0 after truncate, got %v", cnt)
		}
	}

	// Can still insert after truncate
	_, err = db.Exec(`INSERT INTO logs VALUES (msg="new")`)
	if err != nil {
		t.Fatalf("insert after truncate: %v", err)
	}
	res, _ = db.Exec(`SELECT * FROM logs`)
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 after re-insert, got %d", len(res.Docs))
	}
}

func TestTruncateNonexistent(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`TRUNCATE TABLE ghost`)
	if err == nil {
		t.Error("expected error truncating nonexistent table")
	}
}

// ---------- Tests Transactions ----------

func TestTxCommit(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert hors transaction
	db.Exec(`INSERT INTO accounts VALUES (name="Alice", balance=100)`)
	db.Exec(`INSERT INTO accounts VALUES (name="Bob", balance=50)`)

	// Transaction : transférer 30 de Alice à Bob
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	tx.Exec(`UPDATE accounts SET balance = balance - 30 WHERE name = "Alice"`)
	tx.Exec(`UPDATE accounts SET balance = balance + 30 WHERE name = "Bob"`)

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Vérifier les soldes
	res, _ := db.Exec(`SELECT * FROM accounts WHERE name = "Alice"`)
	bal, _ := res.Docs[0].Doc.Get("balance")
	if bal != int64(70) {
		t.Errorf("Alice expected 70, got %v", bal)
	}
	res, _ = db.Exec(`SELECT * FROM accounts WHERE name = "Bob"`)
	bal, _ = res.Docs[0].Doc.Get("balance")
	if bal != int64(80) {
		t.Errorf("Bob expected 80, got %v", bal)
	}
}

func TestTxRollback(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO items VALUES (name="widget", qty=10)`)

	// Transaction : modifier puis rollback
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	tx.Exec(`UPDATE items SET qty = 999 WHERE name = "widget"`)
	tx.Exec(`INSERT INTO items VALUES (name="gadget", qty=5)`)

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// La modification doit être annulée
	res, _ := db.Exec(`SELECT * FROM items WHERE name = "widget"`)
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 widget, got %d", len(res.Docs))
	}
	qty, _ := res.Docs[0].Doc.Get("qty")
	if qty != int64(10) {
		t.Errorf("qty expected 10 after rollback, got %v", qty)
	}

	// L'insert doit aussi être annulé
	res, _ = db.Exec(`SELECT * FROM items WHERE name = "gadget"`)
	if len(res.Docs) != 0 {
		t.Errorf("expected 0 gadgets after rollback, got %d", len(res.Docs))
	}
}

func TestTxRollbackInsert(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Transaction : insérer puis rollback
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	tx.Exec(`INSERT INTO fresh VALUES (x=1)`)
	tx.Exec(`INSERT INTO fresh VALUES (x=2)`)

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// La collection doit être vide ou inexistante
	res, _ := db.Exec(`SELECT * FROM fresh`)
	if res != nil && len(res.Docs) > 0 {
		t.Errorf("expected 0 docs after rollback, got %d", len(res.Docs))
	}
}

func TestTxDoubleBeginError(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	// Deuxième Begin doit échouer
	_, err = db.Begin()
	if err == nil {
		t.Error("expected error on double begin")
	}
}

func TestTxCommitThenContinue(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Transaction commit, puis opérations normales
	tx, _ := db.Begin()
	tx.Exec(`INSERT INTO t VALUES (v=1)`)
	tx.Commit()

	// Opérations hors tx doivent fonctionner
	_, err = db.Exec(`INSERT INTO t VALUES (v=2)`)
	if err != nil {
		t.Fatalf("exec after commit: %v", err)
	}

	res, _ := db.Exec(`SELECT * FROM t`)
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(res.Docs))
	}
}

func TestTxRollbackDelete(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO d VALUES (k=1)`)
	db.Exec(`INSERT INTO d VALUES (k=2)`)
	db.Exec(`INSERT INTO d VALUES (k=3)`)

	// Transaction : supprimer puis rollback
	tx, _ := db.Begin()
	tx.Exec(`DELETE FROM d WHERE k = 2`)

	res, _ := tx.Exec(`SELECT * FROM d`)
	if len(res.Docs) != 2 {
		t.Errorf("within tx: expected 2 docs, got %d", len(res.Docs))
	}

	tx.Rollback()

	// Le delete doit être annulé
	res, _ = db.Exec(`SELECT * FROM d`)
	if len(res.Docs) != 3 {
		t.Errorf("after rollback: expected 3 docs, got %d", len(res.Docs))
	}
}

// ---------- Tests SELECT expressions & qualified star ----------

func TestSelectComputedLiteral(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO personne VALUES (nom="Alice")`)
	db.Exec(`INSERT INTO personne VALUES (nom="Bob")`)

	// SELECT 1+3 AS cpt FROM personne → doit retourner 4 pour chaque ligne
	res, err := db.Exec(`SELECT 1+3 AS cpt FROM personne`)
	if err != nil {
		t.Fatalf("select computed: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
	for i, rd := range res.Docs {
		v, ok := rd.Doc.Get("cpt")
		if !ok {
			t.Errorf("row %d: missing 'cpt'", i)
		} else if v != int64(4) {
			t.Errorf("row %d: expected cpt=4, got %v (%T)", i, v, v)
		}
	}
}

func TestSelectStringLiteral(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (x=1)`)
	db.Exec(`INSERT INTO t VALUES (x=2)`)

	// SELECT "koko" AS col1, x FROM t
	res, err := db.Exec(`SELECT "koko" AS col1, x FROM t`)
	if err != nil {
		t.Fatalf("select string literal: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
	for i, rd := range res.Docs {
		v, ok := rd.Doc.Get("col1")
		if !ok || v != "koko" {
			t.Errorf("row %d: expected col1=koko, got %v", i, v)
		}
		vx, ok := rd.Doc.Get("x")
		if !ok {
			t.Errorf("row %d: missing 'x'", i)
		}
		_ = vx
	}
}

func TestSelectQualifiedStar(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO personne VALUES (nom="Alice", age=30)`)
	db.Exec(`INSERT INTO personne VALUES (nom="Bob", age=25)`)

	// SELECT A.* FROM personne A
	res, err := db.Exec(`SELECT A.* FROM personne A`)
	if err != nil {
		t.Fatalf("select A.*: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
	for i, rd := range res.Docs {
		if _, ok := rd.Doc.Get("nom"); !ok {
			t.Errorf("row %d: missing 'nom'", i)
		}
		if _, ok := rd.Doc.Get("age"); !ok {
			t.Errorf("row %d: missing 'age'", i)
		}
	}
}

func TestSelectMixedLiteralAndQualifiedStar(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO personne VALUES (nom="Alice", age=30)`)

	// SELECT "koko" AS col1, A.* FROM personne A
	res, err := db.Exec(`SELECT "koko" AS col1, A.* FROM personne A`)
	if err != nil {
		t.Fatalf("select mixed: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	d := res.Docs[0].Doc
	if v, ok := d.Get("col1"); !ok || v != "koko" {
		t.Errorf("expected col1=koko, got %v", v)
	}
	if _, ok := d.Get("nom"); !ok {
		t.Error("missing 'nom'")
	}
	if _, ok := d.Get("age"); !ok {
		t.Error("missing 'age'")
	}
}

func TestSelectIntegerLiteralNoAlias(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (x=1)`)

	// SELECT 42 FROM t → colonne nommée "42" par défaut
	res, err := db.Exec(`SELECT 42 FROM t`)
	if err != nil {
		t.Fatalf("select literal no alias: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	v, ok := res.Docs[0].Doc.Get("42")
	if !ok || v != int64(42) {
		t.Errorf("expected 42, got %v (ok=%v)", v, ok)
	}
}

func TestSelectArithmeticWithField(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (price=10)`)
	db.Exec(`INSERT INTO t VALUES (price=20)`)

	// SELECT price * 2 AS double_price FROM t
	res, err := db.Exec(`SELECT price * 2 AS double_price FROM t`)
	if err != nil {
		t.Fatalf("select arithmetic: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
	v0, _ := res.Docs[0].Doc.Get("double_price")
	v1, _ := res.Docs[1].Doc.Get("double_price")
	if v0 != int64(20) {
		t.Errorf("row 0: expected 20, got %v (%T)", v0, v0)
	}
	if v1 != int64(40) {
		t.Errorf("row 1: expected 40, got %v (%T)", v1, v1)
	}
}

// ---------- Tests Wildcard paths (* and **) ----------

func TestWildcardStarDirectChildren(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Document avec sous-document notes
	db.Exec(`INSERT INTO eleves VALUES (nom="Bouk", notes={math=19, physique=17, arabe=10, anglais=23})`)
	db.Exec(`INSERT INTO eleves VALUES (nom="Ali", notes={math=8, physique=9, arabe=7, anglais=6})`)

	// notes.* > 20 → Bouk (anglais=23), pas Ali
	res, err := db.Exec(`SELECT * FROM eleves WHERE notes.* > 20`)
	if err != nil {
		t.Fatalf("wildcard select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	nom, _ := res.Docs[0].Doc.Get("nom")
	if nom != "Bouk" {
		t.Errorf("expected Bouk, got %v", nom)
	}
}

func TestWildcardStarBetween(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO eleves VALUES (nom="Bouk", notes={math=19, physique=17, arabe=10})`)
	db.Exec(`INSERT INTO eleves VALUES (nom="Ali", notes={math=5, physique=4, arabe=3})`)

	// notes.* BETWEEN 15 AND 20 → Bouk (math=19, physique=17)
	res, err := db.Exec(`SELECT * FROM eleves WHERE notes.* BETWEEN 15 AND 20`)
	if err != nil {
		t.Fatalf("wildcard between: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	nom, _ := res.Docs[0].Doc.Get("nom")
	if nom != "Bouk" {
		t.Errorf("expected Bouk, got %v", nom)
	}
}

func TestWildcardStarIn(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO eleves VALUES (nom="Bouk", notes={math=19, physique=17})`)
	db.Exec(`INSERT INTO eleves VALUES (nom="Ali", notes={math=5, physique=4})`)

	// notes.* IN (19, 4) → les deux matchent
	res, err := db.Exec(`SELECT * FROM eleves WHERE notes.* IN (19, 4)`)
	if err != nil {
		t.Fatalf("wildcard in: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
}

func TestWildcardDoubleStarDeep(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Document avec imbrication profonde : notes.math est un sous-doc
	db.Exec(`INSERT INTO eleves VALUES (nom="Bouk", notes={math={exam=15, homework=18}, physique=17})`)
	db.Exec(`INSERT INTO eleves VALUES (nom="Ali", notes={math={exam=5, homework=6}, physique=4})`)

	// notes.** > 16 → Bouk (homework=18, physique=17), pas Ali
	res, err := db.Exec(`SELECT * FROM eleves WHERE notes.** > 16`)
	if err != nil {
		t.Fatalf("deep wildcard: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	nom, _ := res.Docs[0].Doc.Get("nom")
	if nom != "Bouk" {
		t.Errorf("expected Bouk, got %v", nom)
	}
}

func TestWildcardDoubleStarWithSuffix(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// notes.**.exam = chercher "exam" à n'importe quelle profondeur
	db.Exec(`INSERT INTO eleves VALUES (nom="Bouk", notes={math={exam=15, homework=18}, physique={exam=12}})`)
	db.Exec(`INSERT INTO eleves VALUES (nom="Ali", notes={math={exam=5, homework=6}, physique={exam=3}})`)

	// notes.**.exam > 14 → Bouk (math.exam=15)
	res, err := db.Exec(`SELECT * FROM eleves WHERE notes.**.exam > 14`)
	if err != nil {
		t.Fatalf("deep wildcard suffix: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	nom, _ := res.Docs[0].Doc.Get("nom")
	if nom != "Bouk" {
		t.Errorf("expected Bouk, got %v", nom)
	}
}

func TestWildcardStarIsNotNull(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO eleves VALUES (nom="Bouk", notes={math=19})`)
	db.Exec(`INSERT INTO eleves VALUES (nom="Ali")`) // pas de notes

	// notes.* IS NOT NULL → seulement Bouk
	res, err := db.Exec(`SELECT * FROM eleves WHERE notes.* IS NOT NULL`)
	if err != nil {
		t.Fatalf("wildcard is not null: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	nom, _ := res.Docs[0].Doc.Get("nom")
	if nom != "Bouk" {
		t.Errorf("expected Bouk, got %v", nom)
	}
}

func TestWildcardMixedTypes(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Types mixtes dans le sous-document
	db.Exec(`INSERT INTO eleves VALUES (nom="Bouk", info={age=25, ville="Paris", actif=true})`)

	// info.* = "Paris" → matche ville
	res, err := db.Exec(`SELECT * FROM eleves WHERE info.* = "Paris"`)
	if err != nil {
		t.Fatalf("wildcard mixed: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}

	// info.* > 20 → matche age=25 (ignore string et bool)
	res, err = db.Exec(`SELECT * FROM eleves WHERE info.* > 20`)
	if err != nil {
		t.Fatalf("wildcard mixed numeric: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
}

// ---------- Tests Join Strategies ----------

func TestHashJoinInnerBasic(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Pas d'index → Hash Join automatique pour equi-join
	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob")`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Laptop")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=2, product="Phone")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Mouse")`)

	res, err := db.Exec(`SELECT U.name, O.product FROM users U INNER JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("hash join: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Docs))
	}

	// Vérifier EXPLAIN montre HASH JOIN
	res, err = db.Exec(`EXPLAIN SELECT * FROM users U INNER JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	join1, _ := res.Docs[0].Doc.Get("join_1")
	if j, ok := join1.(string); !ok || !strings.Contains(j, "HASH JOIN") {
		t.Errorf("expected HASH JOIN in explain, got %v", join1)
	}
}

func TestHashJoinLeftJoin(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob")`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Laptop")`)

	res, err := db.Exec(`SELECT U.name, O.product FROM users U LEFT JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("hash left join: %v", err)
	}
	// Alice+Laptop, Bob+null, Charlie+null
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Docs))
	}
}

func TestIndexLookupJoin(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Créer un index sur orders.user_id → déclenchera Index Lookup Join
	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Laptop")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=2, product="Phone")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Mouse")`)
	db.Exec(`CREATE INDEX ON orders (user_id)`)

	res, err := db.Exec(`SELECT U.name, O.product FROM users U INNER JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("index lookup join: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Docs))
	}

	// Vérifier EXPLAIN montre INDEX LOOKUP JOIN
	res, err = db.Exec(`EXPLAIN SELECT * FROM users U INNER JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	join1, _ := res.Docs[0].Doc.Get("join_1")
	if j, ok := join1.(string); !ok || !strings.Contains(j, "INDEX LOOKUP JOIN") {
		t.Errorf("expected INDEX LOOKUP JOIN in explain, got %v", join1)
	}
}

func TestIndexLookupJoinLeftJoin(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob")`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Laptop")`)
	db.Exec(`CREATE INDEX ON orders (user_id)`)

	res, err := db.Exec(`SELECT U.name, O.product FROM users U LEFT JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("index left join: %v", err)
	}
	// Alice+Laptop, Bob+null, Charlie+null
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Docs))
	}
}

func TestHashJoinMultipleMatches(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Cas many-to-many : 2 users, chacun a 3 commandes
	for i := 1; i <= 2; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO users VALUES (id=%d, name="User%d")`, i, i))
		for j := 1; j <= 3; j++ {
			db.Exec(fmt.Sprintf(`INSERT INTO orders VALUES (user_id=%d, item="Item%d_%d")`, i, i, j))
		}
	}

	res, err := db.Exec(`SELECT * FROM users U INNER JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("hash join many: %v", err)
	}
	if len(res.Docs) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(res.Docs))
	}
}

func TestJoinStrategyWithWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Laptop", price=1000)`)
	db.Exec(`INSERT INTO orders VALUES (user_id=2, product="Phone", price=500)`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, product="Mouse", price=25)`)

	// Hash join + WHERE filter
	res, err := db.Exec(`SELECT U.name, O.product FROM users U INNER JOIN orders O ON U.id = O.user_id WHERE O.price > 100`)
	if err != nil {
		t.Fatalf("join+where: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
}

// ---------- Benchmark Join Strategies ----------

func BenchmarkNestedLoopJoin(b *testing.B) {
	benchmarkJoinStrategy(b, false, 500)
}

func BenchmarkHashJoin(b *testing.B) {
	benchmarkJoinStrategy(b, false, 500)
}

func BenchmarkIndexLookupJoin(b *testing.B) {
	benchmarkJoinStrategy(b, true, 500)
}

func TestExplainWithStats(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO users VALUES (id=%d, name="User%d")`, i, i))
	}
	for i := 0; i < 30; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO orders VALUES (user_id=%d, product="Prod%d")`, i%20, i))
	}

	// EXPLAIN simple SELECT
	res, err := db.Exec(`EXPLAIN SELECT * FROM users WHERE id = 5`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	doc := res.Docs[0].Doc

	typ, _ := doc.Get("type")
	if typ != "SELECT" {
		t.Errorf("expected SELECT, got %v", typ)
	}
	rows, _ := doc.Get("estimated_rows")
	if rows != int64(20) {
		t.Errorf("expected 20 rows, got %v", rows)
	}
	sel, ok := doc.Get("selectivity")
	if !ok {
		t.Error("expected selectivity field")
	}
	if s, ok := sel.(float64); !ok || s <= 0 || s >= 1 {
		t.Errorf("expected selectivity between 0 and 1, got %v", sel)
	}

	// EXPLAIN with JOIN
	res, err = db.Exec(`EXPLAIN SELECT * FROM users U INNER JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("explain join: %v", err)
	}
	doc = res.Docs[0].Doc
	j1, ok := doc.Get("join_1")
	if !ok {
		t.Error("expected join_1 field in EXPLAIN")
	}
	if j, ok := j1.(string); !ok || !strings.Contains(j, "HASH JOIN") {
		t.Errorf("expected HASH JOIN, got %v", j1)
	}
	cost, ok := doc.Get("join_1_cost")
	if !ok {
		t.Error("expected join_1_cost field in EXPLAIN")
	}
	if c, ok := cost.(string); !ok || !strings.Contains(c, "O(n+m)") {
		t.Errorf("expected O(n+m) cost, got %v", cost)
	}
}

func TestExplainIndexLookupJoinCost(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO users VALUES (id=%d, name="User%d")`, i, i))
		db.Exec(fmt.Sprintf(`INSERT INTO orders VALUES (user_id=%d, product="P%d")`, i, i))
	}
	db.Exec(`CREATE INDEX ON orders (user_id)`)

	res, err := db.Exec(`EXPLAIN SELECT * FROM users U INNER JOIN orders O ON U.id = O.user_id`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	doc := res.Docs[0].Doc
	cost, ok := doc.Get("join_1_cost")
	if !ok {
		t.Error("expected join_1_cost")
	}
	if c, ok := cost.(string); !ok || !strings.Contains(c, "log") {
		t.Errorf("expected log cost for index lookup, got %v", cost)
	}
}

// ---------- Tests Subqueries ----------

func TestSubqueryWhereInSelect(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice", dept="engineering")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob", dept="sales")`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie", dept="engineering")`)
	db.Exec(`INSERT INTO users VALUES (id=4, name="Diana", dept="hr")`)
	db.Exec(`INSERT INTO depts VALUES (name="engineering", budget=100000)`)
	db.Exec(`INSERT INTO depts VALUES (name="sales", budget=50000)`)

	// WHERE dept IN (SELECT name FROM depts WHERE budget > 60000) → engineering only
	res, err := db.Exec(`SELECT * FROM users WHERE dept IN (SELECT name FROM depts WHERE budget > 60000)`)
	if err != nil {
		t.Fatalf("subquery IN: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows (Alice+Charlie), got %d", len(res.Docs))
	}
}

func TestSubqueryWhereNotInSelect(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice", dept="engineering")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob", dept="sales")`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie", dept="engineering")`)
	db.Exec(`INSERT INTO depts VALUES (name="engineering", budget=100000)`)

	// NOT IN subquery → only Bob (sales not in depts with budget > 60000)
	res, err := db.Exec(`SELECT * FROM users WHERE dept NOT IN (SELECT name FROM depts WHERE budget > 60000)`)
	if err != nil {
		t.Fatalf("subquery NOT IN: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row (Bob), got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "Bob" {
		t.Errorf("expected Bob, got %v", name)
	}
}

func TestSubqueryScalarComparison(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO scores VALUES (name="Alice", score=90)`)
	db.Exec(`INSERT INTO scores VALUES (name="Bob", score=70)`)
	db.Exec(`INSERT INTO scores VALUES (name="Charlie", score=85)`)

	// WHERE score > (SELECT AVG(score) FROM scores) → AVG = 81.67 → Alice(90), Charlie(85)
	res, err := db.Exec(`SELECT name FROM scores WHERE score > (SELECT AVG(score) FROM scores)`)
	if err != nil {
		t.Fatalf("scalar subquery: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
}

func TestSubqueryScalarEquals(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO items VALUES (id=1, name="Widget", max_price=100)`)
	db.Exec(`INSERT INTO items VALUES (id=2, name="Gadget", max_price=200)`)
	db.Exec(`INSERT INTO config VALUES (key="price_limit", val=100)`)

	// WHERE max_price = (SELECT val FROM config WHERE key = "price_limit")
	res, err := db.Exec(`SELECT name FROM items WHERE max_price = (SELECT val FROM config WHERE key = "price_limit")`)
	if err != nil {
		t.Fatalf("scalar = subquery: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "Widget" {
		t.Errorf("expected Widget, got %v", name)
	}
}

func TestSubqueryInSelectClause(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (name="Bob")`)
	db.Exec(`INSERT INTO orders VALUES (user="Alice", amount=100)`)
	db.Exec(`INSERT INTO orders VALUES (user="Alice", amount=200)`)
	db.Exec(`INSERT INTO orders VALUES (user="Bob", amount=50)`)

	// SELECT name, (SELECT COUNT(*) FROM orders) AS total_orders FROM users
	res, err := db.Exec(`SELECT name, (SELECT COUNT(*) FROM orders) AS total_orders FROM users`)
	if err != nil {
		t.Fatalf("scalar subquery in SELECT: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
	for _, rd := range res.Docs {
		total, ok := rd.Doc.Get("total_orders")
		if !ok {
			t.Error("missing total_orders field")
		} else if total != int64(3) {
			t.Errorf("expected total_orders=3, got %v (%T)", total, total)
		}
	}
}

func TestSubqueryInUpdate(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice", role="user")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob", role="user")`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie", role="user")`)
	db.Exec(`INSERT INTO admins VALUES (user_id=1)`)
	db.Exec(`INSERT INTO admins VALUES (user_id=3)`)

	// UPDATE users SET role="admin" WHERE id IN (SELECT user_id FROM admins)
	res, err := db.Exec(`UPDATE users SET role="admin" WHERE id IN (SELECT user_id FROM admins)`)
	if err != nil {
		t.Fatalf("update with subquery: %v", err)
	}
	if res.RowsAffected != 2 {
		t.Fatalf("expected 2 affected, got %d", res.RowsAffected)
	}

	// Vérifier que Bob est resté "user"
	res, err = db.Exec(`SELECT * FROM users WHERE role = "user"`)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 user row, got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "Bob" {
		t.Errorf("expected Bob, got %v", name)
	}
}

func TestSubqueryInDelete(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO orders VALUES (id=1, user_id=1, product="Laptop")`)
	db.Exec(`INSERT INTO orders VALUES (id=2, user_id=2, product="Phone")`)
	db.Exec(`INSERT INTO orders VALUES (id=3, user_id=1, product="Mouse")`)
	db.Exec(`INSERT INTO banned VALUES (user_id=2)`)

	// DELETE FROM orders WHERE user_id IN (SELECT user_id FROM banned)
	res, err := db.Exec(`DELETE FROM orders WHERE user_id IN (SELECT user_id FROM banned)`)
	if err != nil {
		t.Fatalf("delete with subquery: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("expected 1 deleted, got %d", res.RowsAffected)
	}

	// Vérifier qu'il reste 2 commandes
	res, err = db.Exec(`SELECT * FROM orders`)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 remaining orders, got %d", len(res.Docs))
	}
}

func TestSubqueryWithAlias(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO personne VALUES (nom="Bouk", prenom="Anouar")`)
	db.Exec(`INSERT INTO personne VALUES (nom="Bouk", prenom="Nouredine")`)

	// Bug fix: A.prenom = (SELECT ...) avec alias FROM doit filtrer correctement
	res, err := db.Exec(`SELECT A.nom, A.* FROM personne A WHERE A.prenom = (SELECT X.prenom FROM personne X WHERE X.prenom = "Anouar")`)
	if err != nil {
		t.Fatalf("alias subquery: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 row (Anouar only), got %d", len(res.Docs))
	}
	prenom, _ := res.Docs[0].Doc.Get("prenom")
	if prenom != "Anouar" {
		t.Errorf("expected Anouar, got %v", prenom)
	}
}

func TestCorrelatedSubqueryInSelect(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO personne VALUES (nom="Bouk", prenom="Anouar")`)
	db.Exec(`INSERT INTO personne VALUES (nom="Dupont", prenom="Nouredine")`)

	// Correlated subquery: inner query references outer alias A.prenom
	res, err := db.Exec(`SELECT A.nom, (SELECT B.prenom FROM personne B WHERE B.prenom = A.prenom) AS X FROM personne A`)
	if err != nil {
		t.Fatalf("correlated subquery: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Docs))
	}
	// Strict assertions: EVERY row must have both nom and X non-nil
	for i, rd := range res.Docs {
		nom, nomOK := rd.Doc.Get("nom")
		x, xOK := rd.Doc.Get("X")
		t.Logf("Row %d: nom=%v (ok=%v), X=%v (ok=%v), fields=%v", i, nom, nomOK, x, xOK, rd.Doc.Fields)
		if !nomOK || nom == nil {
			t.Errorf("Row %d: nom field missing or nil", i)
		}
		if !xOK || x == nil {
			t.Errorf("Row %d: X field missing or nil", i)
		}
	}
	// Check specific values
	found := map[string]string{}
	for _, rd := range res.Docs {
		nom, _ := rd.Doc.Get("nom")
		x, _ := rd.Doc.Get("X")
		if n, ok := nom.(string); ok {
			if v, ok := x.(string); ok {
				found[n] = v
			}
		}
	}
	if found["Bouk"] != "Anouar" {
		t.Errorf("expected Bouk→Anouar, got Bouk→%v", found["Bouk"])
	}
	if found["Dupont"] != "Nouredine" {
		t.Errorf("expected Dupont→Nouredine, got Dupont→%v", found["Dupont"])
	}
}

func TestCorrelatedSubqueryInWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob")`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, amount=100)`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, amount=200)`)
	db.Exec(`INSERT INTO orders VALUES (user_id=3, amount=50)`)

	// Correlated: WHERE id IN (SELECT user_id FROM orders WHERE user_id = A.id)
	res, err := db.Exec(`SELECT A.name FROM users A WHERE A.id IN (SELECT O.user_id FROM orders O WHERE O.user_id = A.id)`)
	if err != nil {
		t.Fatalf("correlated WHERE: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 rows (Alice+Charlie), got %d", len(res.Docs))
	}
}

func TestSubqueryEmpty(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)

	// Sous-requête vide → IN (rien) → aucun résultat
	res, err := db.Exec(`SELECT * FROM users WHERE id IN (SELECT id FROM phantom)`)
	if err != nil {
		t.Fatalf("empty subquery: %v", err)
	}
	if len(res.Docs) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(res.Docs))
	}
}

// ---------- Comprehensive SQL Edge Cases ----------

func TestAliasWithOrderBy(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (name="Charlie", age=30)`)
	db.Exec(`INSERT INTO t VALUES (name="Alice", age=25)`)
	db.Exec(`INSERT INTO t VALUES (name="Bob", age=35)`)

	res, err := db.Exec(`SELECT A.name, A.age FROM t A ORDER BY A.age`)
	if err != nil {
		t.Fatalf("alias order by: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(res.Docs))
	}
	names := []string{}
	for _, rd := range res.Docs {
		n, _ := rd.Doc.Get("name")
		names = append(names, fmt.Sprintf("%v", n))
	}
	if names[0] != "Alice" || names[1] != "Charlie" || names[2] != "Bob" {
		t.Errorf("wrong order: %v", names)
	}
}

func TestAliasWithGroupBy(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO sales VALUES (dept="A", amount=100)`)
	db.Exec(`INSERT INTO sales VALUES (dept="B", amount=200)`)
	db.Exec(`INSERT INTO sales VALUES (dept="A", amount=150)`)

	res, err := db.Exec(`SELECT S.dept, SUM(S.amount) AS total FROM sales S GROUP BY S.dept ORDER BY S.dept`)
	if err != nil {
		t.Fatalf("alias group by: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(res.Docs))
	}
	for _, rd := range res.Docs {
		dept, _ := rd.Doc.Get("dept")
		total, _ := rd.Doc.Get("total")
		if dept == "A" && total != int64(250) {
			t.Errorf("dept A: expected total=250, got %v", total)
		}
		if dept == "B" && total != int64(200) {
			t.Errorf("dept B: expected total=200, got %v", total)
		}
	}
}

func TestAliasWithWhereAndLimit(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO items VALUES (id=%d, val=%d)`, i, i*10))
	}

	res, err := db.Exec(`SELECT X.id, X.val FROM items X WHERE X.val >= 50 ORDER BY X.id LIMIT 3`)
	if err != nil {
		t.Fatalf("alias where+limit: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3, got %d", len(res.Docs))
	}
	id0, _ := res.Docs[0].Doc.Get("id")
	if id0 != int64(5) {
		t.Errorf("expected first id=5, got %v", id0)
	}
}

func TestNestedSubquery(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO a VALUES (id=1, name="X")`)
	db.Exec(`INSERT INTO a VALUES (id=2, name="Y")`)
	db.Exec(`INSERT INTO b VALUES (a_id=1)`)
	db.Exec(`INSERT INTO c VALUES (b_a_id=1)`)

	// Nested: WHERE id IN (SELECT a_id FROM b WHERE a_id IN (SELECT b_a_id FROM c))
	res, err := db.Exec(`SELECT * FROM a WHERE id IN (SELECT a_id FROM b WHERE a_id IN (SELECT b_a_id FROM c))`)
	if err != nil {
		t.Fatalf("nested subquery: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1, got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "X" {
		t.Errorf("expected X, got %v", name)
	}
}

func TestSubqueryWithAggregateScalar(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO scores VALUES (name="A", score=80)`)
	db.Exec(`INSERT INTO scores VALUES (name="B", score=60)`)
	db.Exec(`INSERT INTO scores VALUES (name="C", score=90)`)
	db.Exec(`INSERT INTO scores VALUES (name="D", score=70)`)

	// COUNT subquery
	res, err := db.Exec(`SELECT name FROM scores WHERE score > (SELECT AVG(score) FROM scores)`)
	if err != nil {
		t.Fatalf("avg subquery: %v", err)
	}
	// AVG = 75 → A(80), C(90) above average
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 above avg, got %d", len(res.Docs))
	}

	// MAX subquery
	res, err = db.Exec(`SELECT name FROM scores WHERE score = (SELECT MAX(score) FROM scores)`)
	if err != nil {
		t.Fatalf("max subquery: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 max, got %d", len(res.Docs))
	}
	n, _ := res.Docs[0].Doc.Get("name")
	if n != "C" {
		t.Errorf("expected C, got %v", n)
	}

	// MIN subquery
	res, err = db.Exec(`SELECT name FROM scores WHERE score = (SELECT MIN(score) FROM scores)`)
	if err != nil {
		t.Fatalf("min subquery: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 min, got %d", len(res.Docs))
	}
	n, _ = res.Docs[0].Doc.Get("name")
	if n != "B" {
		t.Errorf("expected B, got %v", n)
	}
}

func TestAliasNoJoinSelectStar(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (a=1, b=2)`)

	// A.* dans un contexte non-JOIN
	res, err := db.Exec(`SELECT X.* FROM t X WHERE X.a = 1`)
	if err != nil {
		t.Fatalf("alias star: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1, got %d", len(res.Docs))
	}
	a, aOK := res.Docs[0].Doc.Get("a")
	b, bOK := res.Docs[0].Doc.Get("b")
	if !aOK || a != int64(1) {
		t.Errorf("expected a=1, got %v (ok=%v)", a, aOK)
	}
	if !bOK || b != int64(2) {
		t.Errorf("expected b=2, got %v (ok=%v)", b, bOK)
	}
}

func TestAliasWithNestedDotPath(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (name="Alice", notes={math=19, physics=15})`)
	db.Exec(`INSERT INTO t VALUES (name="Bob", notes={math=12, physics=18})`)

	// A.notes.math — alias + nested path
	res, err := db.Exec(`SELECT P.name, P.notes.math FROM t P WHERE P.notes.math > 15`)
	if err != nil {
		t.Fatalf("alias nested: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1, got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "Alice" {
		t.Errorf("expected Alice, got %v", name)
	}
}

// ---------- UNION ----------

func TestUnion(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO a VALUES (name="Alice")`)
	db.Exec(`INSERT INTO a VALUES (name="Bob")`)
	db.Exec(`INSERT INTO b VALUES (name="Bob")`)
	db.Exec(`INSERT INTO b VALUES (name="Charlie")`)

	// UNION (deduplicated)
	res, err := db.Exec(`SELECT name FROM a UNION SELECT name FROM b`)
	if err != nil {
		t.Fatalf("union: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Errorf("UNION: expected 3 unique, got %d", len(res.Docs))
	}

	// UNION ALL (no dedup)
	res, err = db.Exec(`SELECT name FROM a UNION ALL SELECT name FROM b`)
	if err != nil {
		t.Fatalf("union all: %v", err)
	}
	if len(res.Docs) != 4 {
		t.Errorf("UNION ALL: expected 4, got %d", len(res.Docs))
	}
}

func TestUnionWithWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t1 VALUES (id=1, val=10)`)
	db.Exec(`INSERT INTO t1 VALUES (id=2, val=20)`)
	db.Exec(`INSERT INTO t2 VALUES (id=3, val=30)`)
	db.Exec(`INSERT INTO t2 VALUES (id=4, val=40)`)

	res, err := db.Exec(`SELECT id, val FROM t1 WHERE val > 15 UNION ALL SELECT id, val FROM t2 WHERE val < 35`)
	if err != nil {
		t.Fatalf("union where: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 (id=2 + id=3), got %d", len(res.Docs))
	}
}

// ---------- CASE WHEN ----------

func TestCaseWhenInSelect(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (name="Alice", score=90)`)
	db.Exec(`INSERT INTO t VALUES (name="Bob", score=55)`)
	db.Exec(`INSERT INTO t VALUES (name="Charlie", score=75)`)

	res, err := db.Exec(`SELECT name, CASE WHEN score >= 80 THEN "A" WHEN score >= 60 THEN "B" ELSE "C" END AS grade FROM t`)
	if err != nil {
		t.Fatalf("case when: %v", err)
	}
	if len(res.Docs) != 3 {
		t.Fatalf("expected 3, got %d", len(res.Docs))
	}
	grades := map[string]string{}
	for _, rd := range res.Docs {
		n, _ := rd.Doc.Get("name")
		g, _ := rd.Doc.Get("grade")
		if ns, ok := n.(string); ok {
			if gs, ok := g.(string); ok {
				grades[ns] = gs
			}
		}
	}
	if grades["Alice"] != "A" {
		t.Errorf("Alice: expected A, got %v", grades["Alice"])
	}
	if grades["Bob"] != "C" {
		t.Errorf("Bob: expected C, got %v", grades["Bob"])
	}
	if grades["Charlie"] != "B" {
		t.Errorf("Charlie: expected B, got %v", grades["Charlie"])
	}
}

func TestCaseWhenInWhere(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (x=1)`)
	db.Exec(`INSERT INTO t VALUES (x=2)`)
	db.Exec(`INSERT INTO t VALUES (x=3)`)

	// CASE dans WHERE : filtrer les lignes où CASE retourne "yes"
	res, err := db.Exec(`SELECT x FROM t WHERE CASE WHEN x > 1 THEN "yes" ELSE "no" END = "yes"`)
	if err != nil {
		t.Fatalf("case where: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 (x=2,3), got %d", len(res.Docs))
	}
}

func TestCaseWhenNoElse(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (x=10)`)
	db.Exec(`INSERT INTO t VALUES (x=20)`)

	res, err := db.Exec(`SELECT x, CASE WHEN x > 15 THEN "big" END AS label FROM t`)
	if err != nil {
		t.Fatalf("case no else: %v", err)
	}
	for _, rd := range res.Docs {
		x, _ := rd.Doc.Get("x")
		label, _ := rd.Doc.Get("label")
		if x == int64(10) && label != nil {
			t.Errorf("x=10: expected nil label, got %v", label)
		}
		if x == int64(20) && label != "big" {
			t.Errorf("x=20: expected big, got %v", label)
		}
	}
}

// ---------- CREATE VIEW ----------

func TestCreateView(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice", age=30)`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob", age=25)`)
	db.Exec(`INSERT INTO users VALUES (id=3, name="Charlie", age=35)`)

	// Create a view
	_, err = db.Exec(`CREATE VIEW seniors AS SELECT name, age FROM users WHERE age >= 30`)
	if err != nil {
		t.Fatalf("create view: %v", err)
	}

	// Query the view
	res, err := db.Exec(`SELECT * FROM seniors`)
	if err != nil {
		t.Fatalf("select view: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 seniors, got %d", len(res.Docs))
	}
}

func TestViewWithProjection(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (a=1, b=10)`)
	db.Exec(`INSERT INTO t VALUES (a=2, b=20)`)
	db.Exec(`INSERT INTO t VALUES (a=3, b=30)`)

	db.Exec(`CREATE VIEW v AS SELECT a, b FROM t`)

	// Query view with WHERE on top
	res, err := db.Exec(`SELECT a FROM v WHERE b > 15`)
	if err != nil {
		t.Fatalf("view where: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2, got %d", len(res.Docs))
	}
}

func TestDropView(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (x=1)`)
	db.Exec(`CREATE VIEW v AS SELECT x FROM t`)

	// View works
	res, _ := db.Exec(`SELECT * FROM v`)
	if len(res.Docs) != 1 {
		t.Errorf("expected 1, got %d", len(res.Docs))
	}

	// Drop view
	_, err = db.Exec(`DROP VIEW v`)
	if err != nil {
		t.Fatalf("drop view: %v", err)
	}

	// View no longer exists — should return empty (collection doesn't exist)
	res, _ = db.Exec(`SELECT * FROM v`)
	if len(res.Docs) != 0 {
		t.Errorf("expected 0 after drop, got %d", len(res.Docs))
	}

	// DROP VIEW IF EXISTS (no error)
	_, err = db.Exec(`DROP VIEW IF EXISTS v`)
	if err != nil {
		t.Errorf("drop view if exists should not error: %v", err)
	}
}

func TestViewPersistence(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	// Create view and close
	db, _ := Open(path)
	db.Exec(`INSERT INTO t VALUES (x=42)`)
	db.Exec(`CREATE VIEW myview AS SELECT x FROM t`)
	db.Close()

	// Reopen and query
	db2, _ := Open(path)
	defer db2.Close()
	res, err := db2.Exec(`SELECT * FROM myview`)
	if err != nil {
		t.Fatalf("view after reopen: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1, got %d", len(res.Docs))
	}
	x, _ := res.Docs[0].Doc.Get("x")
	if x != int64(42) {
		t.Errorf("expected 42, got %v", x)
	}
}

// ---------- COUNT(DISTINCT) ----------

func TestCountDistinctAdvanced(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (dept="A", name="Alice")`)
	db.Exec(`INSERT INTO t VALUES (dept="A", name="Bob")`)
	db.Exec(`INSERT INTO t VALUES (dept="B", name="Alice")`)
	db.Exec(`INSERT INTO t VALUES (dept="B", name="Charlie")`)
	db.Exec(`INSERT INTO t VALUES (dept="B", name="Charlie")`)

	// COUNT(DISTINCT name) global
	res, err := db.Exec(`SELECT COUNT(DISTINCT name) AS cnt FROM t`)
	if err != nil {
		t.Fatalf("count distinct: %v", err)
	}
	cnt, _ := res.Docs[0].Doc.Get("cnt")
	if cnt != int64(3) {
		t.Errorf("expected 3 distinct names, got %v", cnt)
	}

	// COUNT(DISTINCT name) avec GROUP BY
	res, err = db.Exec(`SELECT dept, COUNT(DISTINCT name) AS cnt FROM t GROUP BY dept ORDER BY dept`)
	if err != nil {
		t.Fatalf("count distinct group: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(res.Docs))
	}
	for _, rd := range res.Docs {
		dept, _ := rd.Doc.Get("dept")
		c, _ := rd.Doc.Get("cnt")
		if dept == "A" && c != int64(2) {
			t.Errorf("dept A: expected 2, got %v", c)
		}
		if dept == "B" && c != int64(2) {
			t.Errorf("dept B: expected 2 (Alice+Charlie), got %v", c)
		}
	}
}

// ---------- Overflow (multi-page documents) ----------

func TestOverflowInsertAndSelect(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Build a document with many fields to exceed 4KB
	var fields []string
	for i := 0; i < 200; i++ {
		fields = append(fields, fmt.Sprintf(`f%d="value_%d_padding_to_make_it_longer_%s"`, i, i, strings.Repeat("x", 20)))
	}
	sql := `INSERT INTO big VALUES (` + strings.Join(fields, ", ") + `)`
	_, err = db.Exec(sql)
	if err != nil {
		t.Fatalf("insert large doc: %v", err)
	}

	// Verify we can read it back
	res, err := db.Exec(`SELECT * FROM big`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	// Check a few fields
	v0, _ := res.Docs[0].Doc.Get("f0")
	if v0 == nil {
		t.Error("f0 is nil")
	}
	v199, _ := res.Docs[0].Doc.Get("f199")
	if v199 == nil {
		t.Error("f199 is nil")
	}
}

func TestOverflowPersistence(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	// Insert large doc, close, reopen, verify
	db1, _ := Open(path)
	var fields []string
	for i := 0; i < 200; i++ {
		fields = append(fields, fmt.Sprintf(`f%d="val_%d_%s"`, i, i, strings.Repeat("y", 20)))
	}
	db1.Exec(`INSERT INTO big VALUES (` + strings.Join(fields, ", ") + `)`)
	db1.Close()

	db2, _ := Open(path)
	defer db2.Close()
	res, err := db2.Exec(`SELECT * FROM big`)
	if err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	v50, _ := res.Docs[0].Doc.Get("f50")
	if v50 == nil {
		t.Error("f50 is nil after reopen")
	}
}

func TestOverflowWithJSON(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Build a large JSON object
	jsonFields := make([]string, 150)
	for i := 0; i < 150; i++ {
		jsonFields[i] = fmt.Sprintf(`"field_%d": "value_%d_%s"`, i, i, strings.Repeat("z", 30))
	}
	jsonStr := `{` + strings.Join(jsonFields, ", ") + `}`
	_, err = db.InsertJSON("bigjson", jsonStr)
	if err != nil {
		t.Fatalf("InsertJSON large: %v", err)
	}

	res, err := db.Exec(`SELECT * FROM bigjson`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1, got %d", len(res.Docs))
	}
	v0, _ := res.Docs[0].Doc.Get("field_0")
	if v0 == nil {
		t.Error("field_0 is nil")
	}
}

func TestOverflowDelete(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert large doc + small doc
	var fields []string
	for i := 0; i < 200; i++ {
		fields = append(fields, fmt.Sprintf(`f%d="val_%d_%s"`, i, i, strings.Repeat("a", 20)))
	}
	db.Exec(`INSERT INTO t VALUES (` + strings.Join(fields, ", ") + `)`)
	db.Exec(`INSERT INTO t VALUES (name="small")`)

	// Delete large doc
	_, err = db.Exec(`DELETE FROM t WHERE f0 IS NOT NULL`)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	res, err := db.Exec(`SELECT * FROM t`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 after delete, got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "small" {
		t.Errorf("expected small, got %v", name)
	}
}

func TestOverflowVacuum(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insert 2 large docs, delete one, vacuum
	for j := 0; j < 2; j++ {
		var fields []string
		for i := 0; i < 200; i++ {
			fields = append(fields, fmt.Sprintf(`f%d="val_%d_%d_%s"`, i, j, i, strings.Repeat("b", 20)))
		}
		db.Exec(`INSERT INTO t VALUES (` + strings.Join(fields, ", ") + `)`)
	}

	db.Exec(`DELETE FROM t WHERE f0="val_0_0_` + strings.Repeat("b", 20) + `"`)

	n, err := db.Vacuum()
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if n < 1 {
		t.Errorf("expected at least 1 reclaimed, got %d", n)
	}

	// Remaining doc should still be readable
	res, err := db.Exec(`SELECT * FROM t`)
	if err != nil {
		t.Fatalf("select after vacuum: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1 after vacuum, got %d", len(res.Docs))
	}
}

// ---------- JSON INSERT ----------

func TestInsertJSONSyntax(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// JSON syntax with colon separator and quoted keys
	_, err = db.Exec(`INSERT INTO users VALUES ({"name": "Alice", "age": 30})`)
	if err != nil {
		t.Fatalf("insert json in parens: %v", err)
	}

	// Bare JSON (no parens)
	_, err = db.Exec(`INSERT INTO users VALUES {"name": "Bob", "age": 25}`)
	if err != nil {
		t.Fatalf("insert bare json: %v", err)
	}

	res, err := db.Exec(`SELECT * FROM users`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 docs, got %d", len(res.Docs))
	}
	for _, rd := range res.Docs {
		name, _ := rd.Doc.Get("name")
		age, _ := rd.Doc.Get("age")
		if name == nil || age == nil {
			t.Errorf("missing fields: name=%v age=%v", name, age)
		}
	}
}

func TestInsertJSONArray(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT INTO t VALUES {"name": "Alice", "tags": ["admin", "user", "premium"]}`)
	if err != nil {
		t.Fatalf("insert with array: %v", err)
	}

	res, err := db.Exec(`SELECT * FROM t`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	tags, _ := res.Docs[0].Doc.Get("tags")
	arr, ok := tags.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", tags)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 tags, got %d", len(arr))
	}
	if arr[0] != "admin" || arr[1] != "user" || arr[2] != "premium" {
		t.Errorf("unexpected tags: %v", arr)
	}
}

func TestInsertJSONNested(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`INSERT INTO t VALUES {"user": {"name": "Alice", "scores": [95, 88, 72]}}`)
	if err != nil {
		t.Fatalf("insert nested json: %v", err)
	}

	res, err := db.Exec(`SELECT * FROM t`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(res.Docs))
	}
	userVal, _ := res.Docs[0].Doc.Get("user")
	userDoc, ok := userVal.(*storage.Document)
	if !ok {
		t.Fatalf("expected *Document for user, got %T", userVal)
	}
	name, _ := userDoc.Get("name")
	if name != "Alice" {
		t.Errorf("expected Alice, got %v", name)
	}
	scores, _ := userDoc.Get("scores")
	arr, ok := scores.([]interface{})
	if !ok {
		t.Fatalf("expected array for scores, got %T", scores)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 scores, got %d", len(arr))
	}
}

func TestInsertJSONAPI(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	_, err = db.InsertJSON("products", `{"name": "Widget", "price": 9.99, "tags": ["sale", "new"], "meta": {"color": "blue"}}`)
	if err != nil {
		t.Fatalf("InsertJSON: %v", err)
	}

	res, err := db.Exec(`SELECT * FROM products`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1, got %d", len(res.Docs))
	}
	doc := res.Docs[0].Doc
	name, _ := doc.Get("name")
	if name != "Widget" {
		t.Errorf("expected Widget, got %v", name)
	}
	price, _ := doc.Get("price")
	if price != float64(9.99) {
		t.Errorf("expected 9.99, got %v", price)
	}
	tags, _ := doc.Get("tags")
	arr, ok := tags.([]interface{})
	if !ok || len(arr) != 2 {
		t.Errorf("expected 2 tags, got %v", tags)
	}
	meta, _ := doc.Get("meta")
	metaDoc, ok := meta.(*storage.Document)
	if !ok {
		t.Fatalf("expected *Document for meta, got %T", meta)
	}
	color, _ := metaDoc.Get("color")
	if color != "blue" {
		t.Errorf("expected blue, got %v", color)
	}
}

func TestInsertJSONArrayPersistence(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	// Insert with array, close, reopen, verify
	db1, _ := Open(path)
	db1.Exec(`INSERT INTO t VALUES {"items": [1, 2, 3]}`)
	db1.Close()

	db2, _ := Open(path)
	defer db2.Close()
	res, err := db2.Exec(`SELECT * FROM t`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Fatalf("expected 1, got %d", len(res.Docs))
	}
	items, _ := res.Docs[0].Doc.Get("items")
	arr, ok := items.([]interface{})
	if !ok || len(arr) != 3 {
		t.Errorf("expected 3 items after reopen, got %v (%T)", items, items)
	}
}

// ---------- Dump ----------

func TestDump(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (name="Alice", age=30)`)
	db.Exec(`INSERT INTO users VALUES (name="Bob", age=25)`)
	db.Exec(`CREATE INDEX ON users (name)`)
	db.Exec(`CREATE VIEW seniors AS SELECT name, age FROM users WHERE age >= 30`)

	dump := db.Dump()

	// Should contain INSERT statements
	if !strings.Contains(dump, "INSERT INTO users VALUES") {
		t.Errorf("dump should contain INSERT INTO users, got:\n%s", dump)
	}
	// Should contain CREATE INDEX
	if !strings.Contains(dump, "CREATE INDEX idx_users_name ON users (name)") {
		t.Errorf("dump should contain CREATE INDEX, got:\n%s", dump)
	}
	// Should contain CREATE VIEW
	if !strings.Contains(dump, "CREATE VIEW seniors AS") {
		t.Errorf("dump should contain CREATE VIEW, got:\n%s", dump)
	}
	// Should contain field values
	if !strings.Contains(dump, `"Alice"`) {
		t.Errorf("dump should contain Alice, got:\n%s", dump)
	}
}

func TestDumpRestore(t *testing.T) {
	path1 := tempDBPath(t)
	defer os.Remove(path1)
	path2 := tempDBPath(t)
	defer os.Remove(path2)

	// Create and populate db1
	db1, _ := Open(path1)
	db1.Exec(`INSERT INTO t VALUES (x=1, y="hello")`)
	db1.Exec(`INSERT INTO t VALUES (x=2, y="world")`)
	dump := db1.Dump()
	db1.Close()

	// Restore into db2
	db2, _ := Open(path2)
	defer db2.Close()
	for _, line := range strings.Split(dump, ";\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			db2.Exec(line)
		}
	}

	// Verify
	res, err := db2.Exec(`SELECT * FROM t`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2 after restore, got %d", len(res.Docs))
	}
}

// ---------- Query Hints ----------

func TestHintParallelScan(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO t VALUES (id=%d, val=%d)`, i, i*10))
	}

	// PARALLEL(4) doit retourner les mêmes résultats qu'un scan normal
	resNormal, _ := db.Exec(`SELECT * FROM t WHERE val >= 100`)
	resParallel, err := db.Exec(`SELECT /*+ PARALLEL(4) */ * FROM t WHERE val >= 100`)
	if err != nil {
		t.Fatalf("parallel: %v", err)
	}
	if len(resParallel.Docs) != len(resNormal.Docs) {
		t.Errorf("PARALLEL: expected %d rows, got %d", len(resNormal.Docs), len(resParallel.Docs))
	}

	// PARALLEL sans param → défaut 4
	res2, err := db.Exec(`SELECT /*+ PARALLEL */ * FROM t`)
	if err != nil {
		t.Fatalf("parallel default: %v", err)
	}
	if len(res2.Docs) != 20 {
		t.Errorf("expected 20, got %d", len(res2.Docs))
	}
}

func TestHintNoCache(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (a=1)`)

	// Le hint NO_CACHE ne doit pas changer les résultats
	res, err := db.Exec(`SELECT /*+ NO_CACHE */ * FROM t`)
	if err != nil {
		t.Fatalf("no_cache: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1, got %d", len(res.Docs))
	}
}

func TestHintFullScan(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (id=1, name="A")`)
	db.Exec(`INSERT INTO t VALUES (id=2, name="B")`)
	db.Exec(`CREATE INDEX ON t (id)`)

	// FULL_SCAN ignore l'index, mais retourne les mêmes résultats
	resIdx, _ := db.Exec(`SELECT * FROM t WHERE id = 1`)
	resFull, err := db.Exec(`SELECT /*+ FULL_SCAN */ * FROM t WHERE id = 1`)
	if err != nil {
		t.Fatalf("full_scan: %v", err)
	}
	if len(resFull.Docs) != len(resIdx.Docs) {
		t.Errorf("FULL_SCAN: expected %d, got %d", len(resIdx.Docs), len(resFull.Docs))
	}
}

func TestHintForceIndex(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (id=1, name="A")`)
	db.Exec(`INSERT INTO t VALUES (id=2, name="B")`)
	db.Exec(`INSERT INTO t VALUES (id=3, name="C")`)
	db.Exec(`CREATE INDEX ON t (id)`)

	res, err := db.Exec(`SELECT /*+ FORCE_INDEX(id) */ * FROM t WHERE id = 2`)
	if err != nil {
		t.Fatalf("force_index: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1, got %d", len(res.Docs))
	}
	name, _ := res.Docs[0].Doc.Get("name")
	if name != "B" {
		t.Errorf("expected B, got %v", name)
	}
}

func TestHintHashJoin(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO users VALUES (id=1, name="Alice")`)
	db.Exec(`INSERT INTO users VALUES (id=2, name="Bob")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=1, item="Book")`)
	db.Exec(`INSERT INTO orders VALUES (user_id=2, item="Pen")`)

	// Force HASH_JOIN
	res, err := db.Exec(`SELECT /*+ HASH_JOIN */ u.name, o.item FROM users u JOIN orders o ON u.id = o.user_id`)
	if err != nil {
		t.Fatalf("hash_join: %v", err)
	}
	if len(res.Docs) != 2 {
		t.Errorf("expected 2, got %d", len(res.Docs))
	}
}

func TestHintNestedLoop(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO a VALUES (id=1, name="X")`)
	db.Exec(`INSERT INTO b VALUES (a_id=1, val=42)`)

	// Force NESTED_LOOP
	res, err := db.Exec(`SELECT /*+ NESTED_LOOP */ a.name, b.val FROM a JOIN b ON a.id = b.a_id`)
	if err != nil {
		t.Fatalf("nested_loop: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1, got %d", len(res.Docs))
	}
}

func TestHintMultiple(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO t VALUES (id=%d)`, i))
	}

	// Multiple hints
	res, err := db.Exec(`SELECT /*+ PARALLEL(2) NO_CACHE */ * FROM t`)
	if err != nil {
		t.Fatalf("multi hint: %v", err)
	}
	if len(res.Docs) != 10 {
		t.Errorf("expected 10, got %d", len(res.Docs))
	}
}

func TestHintExplain(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (id=1)`)

	// EXPLAIN devrait montrer le hint
	res, err := db.Exec(`EXPLAIN SELECT /*+ FULL_SCAN */ * FROM t WHERE id = 1`)
	if err != nil {
		t.Fatalf("explain hint: %v", err)
	}
	if len(res.Docs) == 0 {
		t.Fatal("expected explain output")
	}
	hint, ok := res.Docs[0].Doc.Get("hint_1")
	if !ok || hint != "FULL_SCAN" {
		t.Errorf("expected hint_1=FULL_SCAN, got %v (ok=%v)", hint, ok)
	}
	// FULL_SCAN devrait forcer un full scan même si index existe
	scan, _ := res.Docs[0].Doc.Get("scan")
	if scan != "FULL SCAN" {
		t.Errorf("expected FULL SCAN, got %v", scan)
	}
}

func TestHintComment(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.Exec(`INSERT INTO t VALUES (a=1)`)

	// Regular comment /* ... */ should be ignored (not treated as hint)
	res, err := db.Exec(`SELECT /* this is a comment */ * FROM t`)
	if err != nil {
		t.Fatalf("comment: %v", err)
	}
	if len(res.Docs) != 1 {
		t.Errorf("expected 1, got %d", len(res.Docs))
	}
}

func TestConcurrentReads(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insérer des données
	for i := 0; i < 100; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO users VALUES (id=%d, name="User%d", age=%d)`, i, i, 20+i%30))
	}

	// Lancer 10 goroutines de lecture concurrente
	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				res, err := db.Exec(`SELECT * FROM users WHERE age > 30`)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d iter %d: %v", gID, i, err)
					return
				}
				if len(res.Docs) == 0 {
					errCh <- fmt.Errorf("goroutine %d iter %d: expected rows, got 0", gID, i)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

func TestConcurrentReadsWhileWriting(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Seed data
	for i := 0; i < 50; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO items VALUES (id=%d, val=%d)`, i, i))
	}

	// Readers and a writer running concurrently
	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// 5 readers
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				res, err := db.Exec(`SELECT * FROM items`)
				if err != nil {
					errCh <- fmt.Errorf("reader %d: %v", gID, err)
					return
				}
				if len(res.Docs) < 50 {
					// At least the initial 50, possibly more from writer
					continue
				}
				_ = res
			}
		}(g)
	}

	// 1 writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; i < 80; i++ {
			_, err := db.Exec(fmt.Sprintf(`INSERT INTO items VALUES (id=%d, val=%d)`, i, i))
			if err != nil {
				errCh <- fmt.Errorf("writer: %v", err)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// Verify final state
	res, err := db.Exec(`SELECT * FROM items`)
	if err != nil {
		t.Fatalf("final select: %v", err)
	}
	if len(res.Docs) != 80 {
		t.Errorf("expected 80 rows after concurrent ops, got %d", len(res.Docs))
	}
}

func TestCacheHitRateAfterRepeatedQueries(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 50; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO items VALUES (id=%d, name="Item%d")`, i, i))
	}

	// Première requête : cache miss pour les pages
	db.Exec(`SELECT * FROM items`)

	// Deuxième requête : devrait être 100% cache hits
	db.Exec(`SELECT * FROM items`)

	hits, misses, size, capacity := db.CacheStats()
	rate := db.CacheHitRate()

	if hits == 0 {
		t.Error("expected cache hits > 0")
	}
	if size == 0 {
		t.Error("expected cache size > 0")
	}
	if capacity != 8192 {
		t.Errorf("expected capacity 8192, got %d", capacity)
	}
	if rate < 0.3 {
		t.Errorf("expected hit rate >= 30%%, got %.1f%% (hits=%d, misses=%d)", rate*100, hits, misses)
	}
}

func benchmarkJoinStrategy(b *testing.B, withIndex bool, n int) {
	path := tempDBPathB(b)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Insérer n users et n orders
	for i := 0; i < n; i++ {
		db.Exec(fmt.Sprintf(`INSERT INTO users VALUES (id=%d, name="User%d")`, i, i))
		db.Exec(fmt.Sprintf(`INSERT INTO orders VALUES (user_id=%d, product="Prod%d")`, i, i))
	}

	if withIndex {
		db.Exec(`CREATE INDEX ON orders (user_id)`)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(`SELECT * FROM users U INNER JOIN orders O ON U.id = O.user_id`)
		if err != nil {
			b.Fatalf("join: %v", err)
		}
	}
}
