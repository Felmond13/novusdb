package api

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// ============================================================
// PRIMARY KEY Tests
// ============================================================

func TestPrimaryKeyBasic(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Créer la table et ajouter la PK
	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "name": "Alice"}`)
	mustExec(t, db, `INSERT INTO users VALUES {"id": 2, "name": "Bob"}`)
	mustExec(t, db, `ALTER TABLE users ADD PRIMARY KEY (id)`)

	// Vérifier que le doublon est rejeté
	_, err = db.Exec(`INSERT INTO users VALUES {"id": 1, "name": "Dup"}`)
	if err == nil {
		t.Fatal("expected PK violation error on duplicate id")
	}
	if !strings.Contains(err.Error(), "duplicate") && !strings.Contains(err.Error(), "violates") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Vérifier que les données originales sont intactes
	rows := mustQuery(t, db, `SELECT name FROM users ORDER BY id`)
	assertRowCount(t, rows, 2)
	assertFieldEquals(t, rows[0], "name", "Alice")
	assertFieldEquals(t, rows[1], "name", "Bob")
}

func TestPrimaryKeyNotNull(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "name": "Alice"}`)
	mustExec(t, db, `ALTER TABLE users ADD PRIMARY KEY (id)`)

	// Insérer sans champ id → PK violation (NOT NULL)
	_, err = db.Exec(`INSERT INTO users VALUES {"name": "NoID"}`)
	if err == nil {
		t.Fatal("expected PK NOT NULL violation")
	}
}

func TestPrimaryKeyOnExistingDuplicates(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "name": "Alice"}`)
	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "name": "Dup"}`)

	// ALTER TABLE devrait refuser la PK car il y a déjà des doublons
	_, err = db.Exec(`ALTER TABLE users ADD PRIMARY KEY (id)`)
	if err == nil {
		t.Fatal("expected error: existing duplicates should prevent PK creation")
	}
}

func TestPrimaryKeyOnlyOnePerTable(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "email": "a@b.com"}`)
	mustExec(t, db, `ALTER TABLE users ADD PRIMARY KEY (id)`)

	// Deuxième PK doit être rejetée
	_, err = db.Exec(`ALTER TABLE users ADD PRIMARY KEY (email)`)
	if err == nil {
		t.Fatal("expected error: only one PK per table")
	}
}

func TestPrimaryKeyAutoIndex(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "name": "Alice"}`)
	mustExec(t, db, `ALTER TABLE users ADD PRIMARY KEY (id)`)

	// L'EXPLAIN devrait montrer INDEX LOOKUP grâce à l'index auto-créé
	rows := mustQuery(t, db, `EXPLAIN SELECT * FROM users WHERE id = 1`)
	found := false
	for _, r := range rows {
		if v, ok := r["scan"]; ok {
			if s, ok := v.(string); ok && strings.Contains(s, "INDEX") {
				found = true
			}
		}
	}
	if !found {
		t.Log("EXPLAIN rows:", rows)
		t.Error("expected PK auto-index to be used in EXPLAIN")
	}
}

// ============================================================
// UNIQUE Constraint Tests
// ============================================================

func TestUniqueBasic(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "email": "alice@test.com"}`)
	mustExec(t, db, `INSERT INTO users VALUES {"id": 2, "email": "bob@test.com"}`)
	mustExec(t, db, `ALTER TABLE users ADD UNIQUE (email)`)

	// Doublon email rejeté
	_, err = db.Exec(`INSERT INTO users VALUES {"id": 3, "email": "alice@test.com"}`)
	if err == nil {
		t.Fatal("expected UNIQUE violation error")
	}

	// Email différent accepté
	mustExec(t, db, `INSERT INTO users VALUES {"id": 3, "email": "charlie@test.com"}`)
}

func TestUniqueAllowsNull(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "email": "a@test.com"}`)
	mustExec(t, db, `ALTER TABLE users ADD UNIQUE (email)`)

	// NULL (champ absent) est autorisé même avec UNIQUE
	mustExec(t, db, `INSERT INTO users VALUES {"id": 2}`)
	mustExec(t, db, `INSERT INTO users VALUES {"id": 3}`)

	rows := mustQuery(t, db, `SELECT * FROM users`)
	assertRowCount(t, rows, 3)
}

func TestMultipleUniqueConstraints(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "email": "a@test.com", "username": "alice"}`)
	mustExec(t, db, `ALTER TABLE users ADD UNIQUE (email)`)
	mustExec(t, db, `ALTER TABLE users ADD UNIQUE (username)`)

	// Doublon email
	_, err = db.Exec(`INSERT INTO users VALUES {"id": 2, "email": "a@test.com", "username": "bob"}`)
	if err == nil {
		t.Fatal("expected UNIQUE violation on email")
	}
	// Doublon username
	_, err = db.Exec(`INSERT INTO users VALUES {"id": 2, "email": "b@test.com", "username": "alice"}`)
	if err == nil {
		t.Fatal("expected UNIQUE violation on username")
	}
	// Tout unique → OK
	mustExec(t, db, `INSERT INTO users VALUES {"id": 2, "email": "b@test.com", "username": "bob"}`)
}

// ============================================================
// FOREIGN KEY Tests
// ============================================================

func TestForeignKeyBasic(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Parent table
	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Engineering"}`)
	mustExec(t, db, `INSERT INTO departments VALUES {"id": 2, "name": "Sales"}`)
	mustExec(t, db, `ALTER TABLE departments ADD PRIMARY KEY (id)`)

	// Child table
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "name": "Alice", "dept_id": 1}`)
	mustExec(t, db, `ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id)`)

	// FK valide
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 2, "name": "Bob", "dept_id": 2}`)

	// FK invalide
	_, err = db.Exec(`INSERT INTO employees VALUES {"id": 3, "name": "Eve", "dept_id": 99}`)
	if err == nil {
		t.Fatal("expected FK violation: dept_id 99 does not exist")
	}
}

func TestForeignKeyNullAllowed(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Eng"}`)
	mustExec(t, db, `ALTER TABLE departments ADD PRIMARY KEY (id)`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "name": "Alice", "dept_id": 1}`)
	mustExec(t, db, `ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id)`)

	// NULL FK autorisé (employé sans département)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 2, "name": "Freelancer"}`)
}

func TestForeignKeyOnDeleteRestrict(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Eng"}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "name": "Alice", "dept_id": 1}`)
	mustExec(t, db, `ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id)`)

	// DELETE du parent doit être bloqué (RESTRICT par défaut)
	_, err = db.Exec(`DELETE FROM departments WHERE id = 1`)
	if err == nil {
		t.Fatal("expected FK RESTRICT error: cannot delete parent with children")
	}
}

func TestForeignKeyOnDeleteCascade(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Eng"}`)
	mustExec(t, db, `INSERT INTO departments VALUES {"id": 2, "name": "Sales"}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "name": "Alice", "dept_id": 1}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 2, "name": "Bob", "dept_id": 1}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 3, "name": "Charlie", "dept_id": 2}`)
	mustExec(t, db, `ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE CASCADE`)

	// DELETE parent → enfants supprimés automatiquement
	mustExec(t, db, `DELETE FROM departments WHERE id = 1`)

	// Alice et Bob (dept_id=1) doivent être supprimés
	rows := mustQuery(t, db, `SELECT name FROM employees`)
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "name", "Charlie")
}

func TestForeignKeyOnDeleteSetNull(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Eng"}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "name": "Alice", "dept_id": 1}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 2, "name": "Bob", "dept_id": 1}`)
	mustExec(t, db, `ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE SET NULL`)

	mustExec(t, db, `DELETE FROM departments WHERE id = 1`)

	// Les employés doivent rester mais avec dept_id = null
	rows := mustQuery(t, db, `SELECT name FROM employees ORDER BY id`)
	assertRowCount(t, rows, 2)
	assertFieldEquals(t, rows[0], "name", "Alice")
	assertFieldEquals(t, rows[1], "name", "Bob")

	// dept_id devrait être null (champ absent ou nil)
	for _, r := range rows {
		if deptID, ok := r["dept_id"]; ok && deptID != nil {
			t.Errorf("expected dept_id to be null after SET NULL, got %v", deptID)
		}
	}
}

func TestForeignKeyOnExistingInvalidData(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Eng"}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "name": "Alice", "dept_id": 99}`) // 99 n'existe pas

	// ALTER TABLE devrait refuser la FK car données existantes invalides
	_, err = db.Exec(`ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id)`)
	if err == nil {
		t.Fatal("expected error: existing data violates FK")
	}
}

// ============================================================
// CONSTRAINT Naming Tests
// ============================================================

func TestConstraintNaming(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "email": "a@b.com"}`)
	mustExec(t, db, `ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id)`)
	mustExec(t, db, `ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email)`)

	// Dupliquer le nom → erreur
	_, err = db.Exec(`ALTER TABLE users ADD CONSTRAINT pk_users UNIQUE (email)`)
	if err == nil {
		t.Fatal("expected error: duplicate constraint name")
	}
}

// ============================================================
// Auto-ID Tests
// ============================================================

func TestAutoIDGeneration(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// INSERT sans aucun champ id → _id auto-généré
	mustExec(t, db, `INSERT INTO items VALUES {"name": "Widget", "price": 10}`)
	mustExec(t, db, `INSERT INTO items VALUES {"name": "Gadget", "price": 20}`)

	rows := mustQuery(t, db, `SELECT _id, name FROM items ORDER BY _id`)
	assertRowCount(t, rows, 2)

	// _id doit être présent et différent
	id1, ok1 := rows[0]["_id"]
	id2, ok2 := rows[1]["_id"]
	if !ok1 || !ok2 {
		t.Fatal("expected _id field to be auto-generated")
	}
	if fmt.Sprintf("%v", id1) == fmt.Sprintf("%v", id2) {
		t.Errorf("expected distinct _id values, got %v and %v", id1, id2)
	}
}

func TestAutoIDPreservesExistingID(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Si le doc a déjà un _id, ne pas le remplacer
	mustExec(t, db, `INSERT INTO items VALUES {"_id": 42, "name": "Custom"}`)
	rows := mustQuery(t, db, `SELECT _id FROM items`)
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "_id", 42)
}

func TestAutoIDFromIDField(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Si le doc a un "id", le copier en _id
	mustExec(t, db, `INSERT INTO items VALUES {"id": 100, "name": "Named"}`)
	rows := mustQuery(t, db, `SELECT _id, id FROM items`)
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "_id", 100)
	assertFieldEquals(t, rows[0], "id", 100)
}

func TestAutoIDFromNestedJSON(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Si un sous-document contient "id", le copier en _id
	mustExec(t, db, `INSERT INTO items VALUES {"meta": {"id": 999}, "name": "Nested"}`)
	rows := mustQuery(t, db, `SELECT _id, name FROM items`)
	assertRowCount(t, rows, 1)
	assertFieldEquals(t, rows[0], "_id", 999)
}

// ============================================================
// Persistence Tests
// ============================================================

func TestConstraintsPersistAcrossReopen(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	// Phase 1: créer les contraintes
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `INSERT INTO users VALUES {"id": 1, "name": "Alice"}`)
	mustExec(t, db, `ALTER TABLE users ADD PRIMARY KEY (id)`)
	db.Close()

	// Phase 2: rouvrir et vérifier que la PK est toujours active
	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	_, err = db2.Exec(`INSERT INTO users VALUES {"id": 1, "name": "Dup"}`)
	if err == nil {
		t.Fatal("expected PK violation after reopen")
	}

	// INSERT avec id différent doit fonctionner
	mustExec(t, db2, `INSERT INTO users VALUES {"id": 2, "name": "Bob"}`)
}

func TestForeignKeyPersistsAcrossReopen(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Eng"}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "dept_id": 1, "name": "Alice"}`)
	mustExec(t, db, `ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id)`)
	db.Close()

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// FK doit toujours être active
	_, err = db2.Exec(`INSERT INTO employees VALUES {"id": 2, "dept_id": 99, "name": "Ghost"}`)
	if err == nil {
		t.Fatal("expected FK violation after reopen")
	}
}

// ============================================================
// Combined PK + FK + UNIQUE
// ============================================================

func TestFullConstraintScenario(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup
	mustExec(t, db, `INSERT INTO departments VALUES {"id": 1, "name": "Eng", "code": "ENG"}`)
	mustExec(t, db, `INSERT INTO departments VALUES {"id": 2, "name": "Sales", "code": "SAL"}`)
	mustExec(t, db, `ALTER TABLE departments ADD PRIMARY KEY (id)`)
	mustExec(t, db, `ALTER TABLE departments ADD UNIQUE (code)`)

	mustExec(t, db, `INSERT INTO employees VALUES {"id": 1, "name": "Alice", "email": "alice@co.com", "dept_id": 1}`)
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 2, "name": "Bob", "email": "bob@co.com", "dept_id": 2}`)
	mustExec(t, db, `ALTER TABLE employees ADD PRIMARY KEY (id)`)
	mustExec(t, db, `ALTER TABLE employees ADD UNIQUE (email)`)
	mustExec(t, db, `ALTER TABLE employees ADD FOREIGN KEY (dept_id) REFERENCES departments(id)`)

	// PK violation on dept
	_, err = db.Exec(`INSERT INTO departments VALUES {"id": 1, "name": "Dup", "code": "DUP"}`)
	if err == nil {
		t.Error("expected PK violation on departments")
	}

	// UNIQUE violation on dept code
	_, err = db.Exec(`INSERT INTO departments VALUES {"id": 3, "name": "New", "code": "ENG"}`)
	if err == nil {
		t.Error("expected UNIQUE violation on departments.code")
	}

	// PK violation on employee
	_, err = db.Exec(`INSERT INTO employees VALUES {"id": 1, "name": "Dup", "email": "x@co.com", "dept_id": 1}`)
	if err == nil {
		t.Error("expected PK violation on employees")
	}

	// UNIQUE violation on employee email
	_, err = db.Exec(`INSERT INTO employees VALUES {"id": 3, "name": "New", "email": "alice@co.com", "dept_id": 1}`)
	if err == nil {
		t.Error("expected UNIQUE violation on employees.email")
	}

	// FK violation
	_, err = db.Exec(`INSERT INTO employees VALUES {"id": 3, "name": "Eve", "email": "eve@co.com", "dept_id": 99}`)
	if err == nil {
		t.Error("expected FK violation on employees.dept_id")
	}

	// Valid insert
	mustExec(t, db, `INSERT INTO employees VALUES {"id": 3, "name": "Charlie", "email": "charlie@co.com", "dept_id": 2}`)

	rows := mustQuery(t, db, `SELECT name FROM employees ORDER BY id`)
	assertRowCount(t, rows, 3)
}
