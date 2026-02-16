package engine

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Felmond13/novusdb/index"
	"github.com/Felmond13/novusdb/parser"
	"github.com/Felmond13/novusdb/storage"
)

// ---------- In-memory constraint cache ----------

// Constraint représente une contrainte persistée (PK, FK, UNIQUE).
type Constraint struct {
	Name      string
	Type      string // "PRIMARY_KEY", "FOREIGN_KEY", "UNIQUE"
	Table     string
	Columns   []string
	RefTable  string                // FK only
	RefColumn string                // FK only
	OnDelete  parser.OnDeleteAction // FK only
}

const constraintsCollection = "_novusdb_constraints"

// ---------- Constraint cache on Executor ----------

// getConstraints retourne les contraintes d'une table (depuis le cache).
func (ex *Executor) getConstraints(table string) []*Constraint {
	if ex.constraints == nil {
		return nil
	}
	return ex.constraints[table]
}

// getPrimaryKey retourne la contrainte PK d'une table, ou nil.
func (ex *Executor) getPrimaryKey(table string) *Constraint {
	for _, c := range ex.getConstraints(table) {
		if c.Type == "PRIMARY_KEY" {
			return c
		}
	}
	return nil
}

// getUniqueConstraints retourne les contraintes UNIQUE d'une table.
func (ex *Executor) getUniqueConstraints(table string) []*Constraint {
	var ucs []*Constraint
	for _, c := range ex.getConstraints(table) {
		if c.Type == "UNIQUE" {
			ucs = append(ucs, c)
		}
	}
	return ucs
}

// getForeignKeysFrom retourne les FK dont la table source est `table`.
func (ex *Executor) getForeignKeysFrom(table string) []*Constraint {
	var fks []*Constraint
	for _, c := range ex.getConstraints(table) {
		if c.Type == "FOREIGN_KEY" {
			fks = append(fks, c)
		}
	}
	return fks
}

// getForeignKeysTo retourne toutes les FK qui référencent `table` (pour DELETE enforcement).
func (ex *Executor) getForeignKeysTo(table string) []*Constraint {
	if ex.constraints == nil {
		return nil
	}
	var fks []*Constraint
	for _, constraints := range ex.constraints {
		for _, c := range constraints {
			if c.Type == "FOREIGN_KEY" && c.RefTable == table {
				fks = append(fks, c)
			}
		}
	}
	return fks
}

// ---------- ALTER TABLE execution ----------

func (ex *Executor) execAlterTable(stmt *parser.AlterTableStatement) (*Result, error) {
	cdef := stmt.Constraint
	if cdef == nil {
		return nil, fmt.Errorf("alter table: no constraint defined")
	}

	// Vérifier que la table existe (au moins 1 document)
	coll := ex.pager.GetCollection(stmt.Table)
	if coll == nil {
		return nil, fmt.Errorf("alter table: collection %q does not exist", stmt.Table)
	}

	// Générer un nom de contrainte si non fourni
	name := cdef.Name
	if name == "" {
		switch cdef.Type {
		case "PRIMARY_KEY":
			name = fmt.Sprintf("pk_%s", stmt.Table)
		case "FOREIGN_KEY":
			name = fmt.Sprintf("fk_%s_%s", stmt.Table, strings.Join(cdef.Columns, "_"))
		case "UNIQUE":
			name = fmt.Sprintf("uq_%s_%s", stmt.Table, strings.Join(cdef.Columns, "_"))
		}
	}

	// Vérifier les doublons
	for _, existing := range ex.getConstraints(stmt.Table) {
		if existing.Name == name {
			return nil, fmt.Errorf("alter table: constraint %q already exists on %q", name, stmt.Table)
		}
		// Une seule PK par table
		if cdef.Type == "PRIMARY_KEY" && existing.Type == "PRIMARY_KEY" {
			return nil, fmt.Errorf("alter table: table %q already has a primary key (%s)", stmt.Table, existing.Name)
		}
	}

	c := &Constraint{
		Name:      name,
		Type:      cdef.Type,
		Table:     stmt.Table,
		Columns:   cdef.Columns,
		RefTable:  cdef.RefTable,
		RefColumn: cdef.RefColumn,
		OnDelete:  cdef.OnDelete,
	}

	// Valider les données existantes
	if err := ex.validateExistingData(c); err != nil {
		return nil, fmt.Errorf("alter table: %w", err)
	}

	// Ajouter au cache
	if ex.constraints == nil {
		ex.constraints = make(map[string][]*Constraint)
	}
	ex.constraints[stmt.Table] = append(ex.constraints[stmt.Table], c)

	// Créer un index automatique pour PK et UNIQUE, peuplé avec les données existantes
	if cdef.Type == "PRIMARY_KEY" || cdef.Type == "UNIQUE" {
		for _, col := range cdef.Columns {
			existingIdx := ex.indexMgr.GetIndex(stmt.Table, col)
			if existingIdx == nil {
				if err := ex.createAndPopulateIndex(stmt.Table, col, fmt.Sprintf("idx_%s_%s", name, col)); err != nil {
					return nil, fmt.Errorf("alter table: auto-index: %w", err)
				}
			}
		}
	}

	// Persister
	ex.persistConstraint(c)

	return &Result{RowsAffected: 1}, nil
}

// createAndPopulateIndex crée un index et le peuple avec les données existantes via bulk load.
func (ex *Executor) createAndPopulateIndex(table, field, idxName string) error {
	idx, err := ex.indexMgr.CreateIndex(table, field)
	if err != nil {
		return err
	}

	coll := ex.pager.GetCollection(table)
	if coll == nil {
		return nil
	}

	fieldPath := strings.Split(field, ".")
	pageID := coll.FirstPageID

	var entries []index.BulkEntry
	for pageID != 0 {
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return err
		}
		for _, slot := range page.ReadRecords() {
			if slot.Deleted {
				continue
			}
			data := slot.Data
			if slot.Overflow {
				totalLen, firstPage := slot.OverflowInfo()
				var err2 error
				data, err2 = ex.pager.ReadOverflowData(totalLen, firstPage)
				if err2 != nil {
					continue
				}
			}
			if slot.Compressed && !slot.Overflow {
				var err2 error
				data, err2 = storage.DecompressRecord(&slot)
				if err2 != nil {
					continue
				}
			}
			doc, err := storage.Decode(data)
			if err != nil {
				continue
			}
			val, ok := doc.GetNested(fieldPath)
			if ok {
				entries = append(entries, index.BulkEntry{
					Key:      index.ValueToKey(val),
					RecordID: slot.RecordID,
					PageID:   pageID,
					SlotOff:  slot.Offset,
				})
			}
		}
		pageID = page.NextPageID()
	}

	// Trier par clé puis par recordID
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key != entries[j].Key {
			return entries[i].Key < entries[j].Key
		}
		return entries[i].RecordID < entries[j].RecordID
	})

	if err := idx.BulkLoad(entries); err != nil {
		return err
	}

	// Persister la définition de l'index
	if err := ex.pager.AddIndexDef(idxName, table, field, idx.RootPageID()); err != nil {
		return err
	}
	return nil
}

// validateExistingData vérifie que les données existantes respectent la contrainte.
func (ex *Executor) validateExistingData(c *Constraint) error {
	switch c.Type {
	case "PRIMARY_KEY", "UNIQUE":
		return ex.validateUniqueness(c.Table, c.Columns)
	case "FOREIGN_KEY":
		return ex.validateForeignKey(c)
	}
	return nil
}

// validateUniqueness vérifie qu'il n'y a pas de doublons pour les colonnes données.
func (ex *Executor) validateUniqueness(table string, columns []string) error {
	results, err := ex.scanCollectionRaw(table, nil)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, r := range results {
		key := buildConstraintKey(r.doc, columns)
		if key == "" {
			continue // NULL → pas de violation d'unicité
		}
		if seen[key] {
			return fmt.Errorf("duplicate value %q for columns %v in table %q", key, columns, table)
		}
		seen[key] = true
	}
	return nil
}

// validateForeignKey vérifie que toutes les valeurs FK existent dans la table référencée.
func (ex *Executor) validateForeignKey(c *Constraint) error {
	// Charger les valeurs de la table référencée
	refValues := make(map[string]bool)
	refResults, err := ex.scanCollectionRaw(c.RefTable, nil)
	if err != nil {
		return err
	}
	for _, r := range refResults {
		val, ok := r.doc.Get(c.RefColumn)
		if ok && val != nil {
			refValues[fmt.Sprintf("%v", val)] = true
		}
	}

	// Vérifier que chaque FK pointe vers une valeur existante
	results, err := ex.scanCollectionRaw(c.Table, nil)
	if err != nil {
		return err
	}
	for _, r := range results {
		if len(c.Columns) == 0 {
			continue
		}
		val, ok := r.doc.Get(c.Columns[0])
		if !ok || val == nil {
			continue // NULL FK est autorisé
		}
		key := fmt.Sprintf("%v", val)
		if !refValues[key] {
			return fmt.Errorf("foreign key violation: value %q in %s.%s does not exist in %s.%s",
				key, c.Table, c.Columns[0], c.RefTable, c.RefColumn)
		}
	}
	return nil
}

// buildConstraintKey construit une clé composite pour vérifier l'unicité.
func buildConstraintKey(doc *storage.Document, columns []string) string {
	parts := make([]string, len(columns))
	for i, col := range columns {
		val, ok := doc.Get(col)
		if !ok || val == nil {
			return "" // NULL → pas de clé (NULL != NULL en SQL)
		}
		parts[i] = fmt.Sprintf("%v", val)
	}
	return strings.Join(parts, "\x00")
}

// ---------- Enforcement on INSERT ----------

// checkInsertConstraints vérifie PK, UNIQUE et FK avant un INSERT.
func (ex *Executor) checkInsertConstraints(table string, doc *storage.Document) error {
	constraints := ex.getConstraints(table)
	if len(constraints) == 0 {
		return nil
	}

	for _, c := range constraints {
		switch c.Type {
		case "PRIMARY_KEY":
			// Vérifier NOT NULL + unicité
			for _, col := range c.Columns {
				val, ok := doc.Get(col)
				if !ok || val == nil {
					return fmt.Errorf("primary key violation: column %q cannot be NULL in table %q", col, table)
				}
			}
			if err := ex.checkUniquenessForDoc(table, c.Columns, doc); err != nil {
				return err
			}

		case "UNIQUE":
			key := buildConstraintKey(doc, c.Columns)
			if key != "" { // NULL → pas de vérification
				if err := ex.checkUniquenessForDoc(table, c.Columns, doc); err != nil {
					return err
				}
			}

		case "FOREIGN_KEY":
			// Vérifier que la valeur FK existe dans la table référencée
			if len(c.Columns) == 0 {
				continue
			}
			val, ok := doc.Get(c.Columns[0])
			if !ok || val == nil {
				continue // NULL FK autorisé
			}
			if err := ex.checkForeignKeyExists(c, val); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkUniquenessForDoc vérifie qu'aucun doc existant n'a la même valeur pour les colonnes.
func (ex *Executor) checkUniquenessForDoc(table string, columns []string, doc *storage.Document) error {
	newKey := buildConstraintKey(doc, columns)
	if newKey == "" {
		return nil
	}

	// Essayer d'abord via l'index (O(log n)) si disponible
	if len(columns) == 1 {
		idx := ex.indexMgr.GetIndex(table, columns[0])
		if idx != nil {
			val, _ := doc.Get(columns[0])
			locs, _ := idx.Lookup(index.ValueToKey(val))
			if len(locs) > 0 {
				return fmt.Errorf("duplicate key value violates constraint: %q = %v in table %q",
					columns[0], val, table)
			}
			return nil
		}
	}

	// Fallback : full scan
	results, err := ex.scanCollectionRaw(table, nil)
	if err != nil {
		return err
	}
	for _, r := range results {
		existingKey := buildConstraintKey(r.doc, columns)
		if existingKey == newKey {
			return fmt.Errorf("duplicate key value violates constraint: columns %v = %q in table %q",
				columns, newKey, table)
		}
	}
	return nil
}

// checkForeignKeyExists vérifie que la valeur FK existe dans la table parent.
func (ex *Executor) checkForeignKeyExists(c *Constraint, val interface{}) error {
	// Essayer via l'index
	idx := ex.indexMgr.GetIndex(c.RefTable, c.RefColumn)
	if idx != nil {
		locs, _ := idx.Lookup(index.ValueToKey(val))
		if len(locs) > 0 {
			return nil
		}
		return fmt.Errorf("foreign key violation: %s.%s = %v does not exist in %s.%s",
			c.Table, c.Columns[0], val, c.RefTable, c.RefColumn)
	}

	// Fallback : full scan
	results, err := ex.scanCollectionRaw(c.RefTable, nil)
	if err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	valStr := fmt.Sprintf("%v", val)
	for _, r := range results {
		refVal, ok := r.doc.Get(c.RefColumn)
		if ok && fmt.Sprintf("%v", refVal) == valStr {
			return nil
		}
	}
	return fmt.Errorf("foreign key violation: %s.%s = %v does not exist in %s.%s",
		c.Table, c.Columns[0], val, c.RefTable, c.RefColumn)
}

// ---------- Enforcement on UPDATE ----------

// checkUpdateConstraints vérifie PK, UNIQUE et FK après un UPDATE.
func (ex *Executor) checkUpdateConstraints(table string, doc *storage.Document, recordID uint64) error {
	constraints := ex.getConstraints(table)
	if len(constraints) == 0 {
		return nil
	}

	for _, c := range constraints {
		switch c.Type {
		case "PRIMARY_KEY":
			for _, col := range c.Columns {
				val, ok := doc.Get(col)
				if !ok || val == nil {
					return fmt.Errorf("primary key violation: column %q cannot be NULL in table %q", col, table)
				}
			}
			if err := ex.checkUniquenessForUpdate(table, c.Columns, doc, recordID); err != nil {
				return err
			}
		case "UNIQUE":
			key := buildConstraintKey(doc, c.Columns)
			if key != "" {
				if err := ex.checkUniquenessForUpdate(table, c.Columns, doc, recordID); err != nil {
					return err
				}
			}
		case "FOREIGN_KEY":
			if len(c.Columns) == 0 {
				continue
			}
			val, ok := doc.Get(c.Columns[0])
			if !ok || val == nil {
				continue
			}
			if err := ex.checkForeignKeyExists(c, val); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkUniquenessForUpdate vérifie l'unicité en excluant le record courant.
func (ex *Executor) checkUniquenessForUpdate(table string, columns []string, doc *storage.Document, excludeID uint64) error {
	newKey := buildConstraintKey(doc, columns)
	if newKey == "" {
		return nil
	}

	results, err := ex.scanCollectionRaw(table, nil)
	if err != nil {
		return err
	}
	for _, r := range results {
		if r.recordID == excludeID {
			continue
		}
		existingKey := buildConstraintKey(r.doc, columns)
		if existingKey == newKey {
			return fmt.Errorf("duplicate key value violates constraint: columns %v = %q in table %q",
				columns, newKey, table)
		}
	}
	return nil
}

// ---------- Enforcement on DELETE ----------

// checkDeleteConstraints vérifie les FK référençant la table avant un DELETE.
// Retourne les actions FK à exécuter après le DELETE.
func (ex *Executor) checkDeleteConstraints(table string, doc *storage.Document) error {
	fks := ex.getForeignKeysTo(table)
	if len(fks) == 0 {
		return nil
	}

	for _, fk := range fks {
		refVal, ok := doc.Get(fk.RefColumn)
		if !ok || refVal == nil {
			continue
		}

		// Chercher les enfants
		children, err := ex.scanCollectionRaw(fk.Table, nil)
		if err != nil {
			continue
		}
		refStr := fmt.Sprintf("%v", refVal)
		hasChildren := false
		for _, child := range children {
			if len(fk.Columns) == 0 {
				continue
			}
			childVal, ok := child.doc.Get(fk.Columns[0])
			if ok && fmt.Sprintf("%v", childVal) == refStr {
				hasChildren = true
				break
			}
		}

		if !hasChildren {
			continue
		}

		switch fk.OnDelete {
		case parser.OnDeleteRestrict, parser.OnDeleteNoAction:
			return fmt.Errorf("foreign key violation: cannot delete from %q, referenced by %q.%s",
				table, fk.Table, fk.Columns[0])
		case parser.OnDeleteCascade:
			// Supprimer les enfants
			if err := ex.cascadeDelete(fk, refVal); err != nil {
				return err
			}
		case parser.OnDeleteSetNull:
			// Mettre les FK enfants à NULL
			if err := ex.setNullChildren(fk, refVal); err != nil {
				return err
			}
		}
	}
	return nil
}

// cascadeDelete supprime les enfants qui référencent la valeur supprimée.
func (ex *Executor) cascadeDelete(fk *Constraint, parentVal interface{}) error {
	if len(fk.Columns) == 0 {
		return nil
	}
	results, err := ex.scanCollectionRaw(fk.Table, nil)
	if err != nil {
		return err
	}
	parentStr := fmt.Sprintf("%v", parentVal)
	for _, r := range results {
		childVal, ok := r.doc.Get(fk.Columns[0])
		if ok && fmt.Sprintf("%v", childVal) == parentStr {
			// Récursivement vérifier les FK sur les enfants
			if err := ex.checkDeleteConstraints(fk.Table, r.doc); err != nil {
				return err
			}
			ex.pager.MarkDeletedAtomic(r.pageID, r.slotOffset)
			ex.updateIndexesAfterDelete(fk.Table, r.recordID, r.doc)
		}
	}
	return nil
}

// setNullChildren met à NULL les colonnes FK des enfants.
func (ex *Executor) setNullChildren(fk *Constraint, parentVal interface{}) error {
	if len(fk.Columns) == 0 {
		return nil
	}
	results, err := ex.scanCollectionRaw(fk.Table, nil)
	if err != nil {
		return err
	}
	parentStr := fmt.Sprintf("%v", parentVal)
	coll := ex.pager.GetCollection(fk.Table)
	if coll == nil {
		return nil
	}
	for _, r := range results {
		childVal, ok := r.doc.Get(fk.Columns[0])
		if ok && fmt.Sprintf("%v", childVal) == parentStr {
			// Mettre le champ FK à nil
			r.doc.Set(fk.Columns[0], nil)
			encoded, err := r.doc.Encode()
			if err != nil {
				continue
			}
			// Delete + re-insert (simpler than in-place update)
			ex.pager.MarkDeletedAtomic(r.pageID, r.slotOffset)
			ex.pager.InsertRecordAtomic(coll, r.recordID, encoded)
		}
	}
	return nil
}

// ---------- Auto-ID Generation ----------

// ensureAutoID ajoute un champ _id au document s'il n'en a pas.
// Cherche d'abord "id", "_id", ou des champs nested "id" dans le JSON.
func ensureAutoID(doc *storage.Document, recordID uint64) {
	// Si le doc a déjà _id, ne rien faire
	if _, ok := doc.Get("_id"); ok {
		return
	}
	// Si le doc a un champ "id", le copier en _id aussi
	if val, ok := doc.Get("id"); ok {
		doc.Set("_id", val)
		return
	}
	// Chercher un champ "id" nested dans les sous-documents
	for _, f := range doc.Fields {
		if sub, ok := f.Value.(*storage.Document); ok {
			if val, ok := sub.Get("id"); ok {
				doc.Set("_id", val)
				return
			}
		}
	}
	// Aucun id trouvé → générer avec le recordID
	doc.Set("_id", int64(recordID))
}

// ---------- Persistence ----------

// persistConstraint sauvegarde une contrainte dans _novusdb_constraints.
func (ex *Executor) persistConstraint(c *Constraint) {
	doc := storage.NewDocument()
	doc.Set("_type", "constraint")
	doc.Set("name", c.Name)
	doc.Set("type", c.Type)
	doc.Set("table", c.Table)
	doc.Set("columns", strings.Join(c.Columns, ","))
	if c.Type == "FOREIGN_KEY" {
		doc.Set("ref_table", c.RefTable)
		doc.Set("ref_column", c.RefColumn)
		doc.Set("on_delete", int64(c.OnDelete))
	}
	doc.Set("created_at", time.Now().Format(time.RFC3339))

	data, err := doc.Encode()
	if err != nil {
		return
	}
	coll, err := ex.pager.GetOrCreateCollection(constraintsCollection)
	if err != nil {
		return
	}
	id, err := ex.pager.NextRecordID(constraintsCollection)
	if err != nil {
		return
	}
	ex.pager.InsertRecordAtomic(coll, id, data)
	ex.pager.FlushMeta()
	ex.pager.CommitWAL()
}

// LoadConstraints charge les contraintes depuis _novusdb_constraints au démarrage.
func (ex *Executor) LoadConstraints() {
	coll := ex.pager.GetCollection(constraintsCollection)
	if coll == nil {
		return
	}
	if ex.constraints == nil {
		ex.constraints = make(map[string][]*Constraint)
	}

	pageID := coll.FirstPageID
	for pageID != 0 {
		page, err := ex.pager.ReadPage(pageID)
		if err != nil {
			return
		}
		for _, slot := range page.ReadRecords() {
			if slot.Deleted {
				continue
			}
			data := slot.Data
			if slot.Overflow {
				totalLen, firstPage := slot.OverflowInfo()
				var err2 error
				data, err2 = ex.pager.ReadOverflowData(totalLen, firstPage)
				if err2 != nil {
					continue
				}
			}
			if slot.Compressed && !slot.Overflow {
				var err2 error
				data, err2 = storage.DecompressRecord(&slot)
				if err2 != nil {
					continue
				}
			}
			doc, err := storage.Decode(data)
			if err != nil {
				continue
			}
			c := decodeConstraint(doc)
			if c != nil {
				ex.constraints[c.Table] = append(ex.constraints[c.Table], c)
			}
		}
		pageID = page.NextPageID()
	}
}

// decodeConstraint reconstruit un Constraint depuis un document persisté.
func decodeConstraint(doc *storage.Document) *Constraint {
	nameIface, ok := doc.Get("name")
	if !ok {
		return nil
	}
	name, _ := nameIface.(string)
	typeIface, _ := doc.Get("type")
	ctype, _ := typeIface.(string)
	tableIface, _ := doc.Get("table")
	table, _ := tableIface.(string)
	colsIface, _ := doc.Get("columns")
	colsStr, _ := colsIface.(string)

	if name == "" || ctype == "" || table == "" {
		return nil
	}

	c := &Constraint{
		Name:    name,
		Type:    ctype,
		Table:   table,
		Columns: strings.Split(colsStr, ","),
	}

	if ctype == "FOREIGN_KEY" {
		if rt, ok := doc.Get("ref_table"); ok {
			c.RefTable, _ = rt.(string)
		}
		if rc, ok := doc.Get("ref_column"); ok {
			c.RefColumn, _ = rc.(string)
		}
		if od, ok := doc.Get("on_delete"); ok {
			if odInt, ok2 := od.(int64); ok2 {
				c.OnDelete = parser.OnDeleteAction(odInt)
			}
		}
	}

	return c
}
