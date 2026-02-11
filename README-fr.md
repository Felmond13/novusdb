<p align="center">
  <img src="website/logo-banner.svg" alt="NovusDB" width="420">
</p>

<p align="center">
  <strong>Une base de données qui tient dans votre app. Un fichier, zéro config, toute la puissance du SQL.</strong>
</p>

<p align="center">
  <a href="https://novusdb.dev/playground.html"><b>▶ Essayer le Playground</b></a> &nbsp;·&nbsp;
  <a href="https://novusdb.dev">🌐 Site web</a> &nbsp;·&nbsp;
  <a href="README.md">🇬🇧 English</a>
</p>

---

NovusDB est un SGBD embarqué léger écrit en pur Go. Déposez un seul fichier dans votre projet et vous avez une couche de données complète — aucun serveur à installer, aucun schéma à définir, aucune migration à lancer.

Stockez des documents JSON avec des champs imbriqués, interrogez-les avec une syntaxe SQL familière (JOINs, agrégations, sous-requêtes, CASE WHEN), et passez à l'échelle avec **des millions de documents** grâce à l'indexation B+ Tree automatique.

**Points clés :**
- ⚡ **Zéro config** — ouvrez un fichier, commencez à requêter
- 🔒 **Résistant aux crashs** — Write-Ahead Log avec vérification CRC32
- 🔗 **JOINs intelligents** — Hash Join O(n+m), Index Lookup O(n·log m), stratégie automatique
- 📦 **Schema-free** — documents imbriqués, tableaux, dot-notation (`user.address.city`)
- 🧠 **Planificateur de requêtes** — EXPLAIN avec estimation de coût + hints Oracle-style
- 🌍 **Multi-langage** — drivers Go, Python, Node.js, Java
- 🖥️ **Lumen** — interface web d'admin intégrée (Vue 3 + Tailwind CSS)
- 🌐 **API REST** — serveur HTTP inclus pour accès distant

---

## Fonctionnalités clés

- **Schema-free** : documents imbriqués, champs dynamiques, types mixtes
- **WAL (Write-Ahead Log)** : durabilité garantie, récupération automatique après crash
- **JOIN optimisé** : INNER JOIN, LEFT JOIN et RIGHT JOIN avec sélection automatique de stratégie :
  - **Hash Join** O(n+m) pour les equi-joins sans index
  - **Index Lookup Join** O(n × log m) quand un B+ Tree existe sur le champ de jointure
  - **Nested Loop** O(n×m) fallback pour les conditions non-equi
- **Agrégations** : COUNT, SUM, AVG, MIN, MAX — avec ou sans GROUP BY
- **DISTINCT**, **LIKE** / **NOT LIKE**, **IN** / **NOT IN**, **IS NULL** / **IS NOT NULL**, **BETWEEN**
- **Expressions arithmétiques** : `+`, `-`, `*`, `/` dans SELECT, WHERE et UPDATE SET
- **Colonnes calculées** : `SELECT 1+3 AS cpt`, `SELECT "label" AS col1`, `SELECT price*2 AS double`
- **Qualified star** : `SELECT A.* FROM table A`, mixable avec d’autres colonnes
- **Sous-documents imbriqués** : `INSERT INTO t VALUES (notes={math=19, physique={exam=15, homework=18}})`
- **Wildcard paths** : `WHERE notes.* > 15` (enfants directs), `WHERE notes.** > 15` (récursif profond)
- **Sous-requêtes exécutables** : non corrélées (`WHERE x IN (SELECT ...)`), corrélées (`WHERE x = (SELECT ... WHERE y = A.x)`), scalaires dans SELECT
- **INSERT INTO ... SELECT** : copie de données entre collections
- **INSERT OR REPLACE** : UPSERT (insert ou mise à jour sur le premier champ)
- **UNION / UNION ALL** : combine les résultats de deux SELECT, avec déduplication ou non
- **CASE WHEN ... THEN ... ELSE ... END** : expressions conditionnelles dans SELECT et WHERE
- **COUNT(DISTINCT field)** : comptage de valeurs uniques, avec ou sans GROUP BY
- **CREATE VIEW / DROP VIEW** : vues virtuelles persistées sur disque, résolues transparemment dans SELECT
- **Backup `.dump`** : export complet de la base en SQL reproductible (index, vues, données)
- **INSERT JSON natif** : `INSERT INTO t VALUES {"name": "Alice", "tags": [1, 2, 3]}` — syntaxe JSON avec `:`, tableaux `[]`, objets imbriqués
- **API InsertJSON** : `db.InsertJSON("col", jsonString)` — insertion programmatique de JSON brut
- **Tableaux (arrays)** : type `FieldArray` persisté sur disque, support dans INSERT, SELECT, Dump
- **Documents multi-pages (overflow)** : les documents > 4 KB sont automatiquement stockés dans des overflow pages chaînées, transparents pour l'utilisateur
- **Serveur HTTP REST** : `NovusDB-server` avec endpoints `/query`, `/insert/{col}`, `/collections`, `/views`, `/schema`, `/dump`, `/cache`
- **Import JSON** : `.import <collection> <fichier.json>` — importe un fichier JSON (objet ou tableau d'objets)
- **DROP TABLE** / **TRUNCATE TABLE** : suppression ou vidage de collections
- **Query Hints Oracle-style** : `/*+ PARALLEL(n) */`, `/*+ NO_CACHE */`, `/*+ FULL_SCAN */`, `/*+ FORCE_INDEX(field) */`, `/*+ HASH_JOIN */`, `/*+ NESTED_LOOP */`
- **Commentaires SQL** : `/* commentaire */` ignorés par le lexer
- **EXPLAIN** avec query planner : cardinalité, sélectivité, coût par join, hints actifs, cache stats
- **Vacuum** : compaction des records supprimés
- **LRU Page Cache** : cache mémoire 4 MB (1024 pages), O(1) get/put/evict, statistiques `.cache`
- **Index B+ Tree persistants** : stockés sur disque, ouverture instantanée au redémarrage
- **Transactions** : BEGIN / COMMIT / ROLLBACK avec undo log (isolation single-writer)
- **Concurrence** : RWMutex multi-reader / single-writer, verrous record-level, inserts parallèles
- **CLI interactif** : REPL avec `.schema`, `.vacuum`, `.tables`, `.dump`, `.views`, `.cache`, `.help`
- **Zéro dépendances** : bibliothèque standard Go uniquement

---

## Architecture

```
NovusDB/
├── api/            # Interface utilisateur (DB.Open, DB.Exec, DB.Close, DB.Schema, DB.Vacuum)
├── parser/         # Lexer, AST, Parser SQL-like
├── engine/         # Exécuteur CRUD + évaluateur WHERE + JOIN + agrégations
├── storage/        # Pager, Page (4 KB), Document binaire, WAL
├── index/          # Index hash-map clé → []record_id
├── concurrency/    # Lock manager record-level
├── cmd/NovusDB/    # CLI interactif (REPL)
└── cmd/example/    # Exemple d'utilisation programmatique
```

### Modules

| Module | Rôle |
|---|---|
| **storage** | Fichier paginé (4 KB/page). Document binaire typé. WAL avec CRC32, commit/recovery/checkpoint. Vacuum. |
| **parser** | Lexer + Parser → AST. Supporte SELECT DISTINCT, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, LIKE, IN, IS NULL, sous-requêtes, INSERT...SELECT, DROP TABLE. |
| **engine** | Exécute les statements. Nested loop JOIN. Agrégats standalone et groupés. DISTINCT. Évaluateur avec court-circuit. |
| **index** | Hash-map en mémoire : clé sérialisée → record_ids. Lookup automatique pour WHERE égalité. |
| **concurrency** | Verrou exclusif par record. Politique configurable (Wait/Fail). Timeout. |
| **api** | `Open()`, `Exec()`, `Close()`, `InsertDoc()`, `Schema()`, `Vacuum()`, `Collections()`. |

---

## Format de stockage

- **Fichier unique** paginé (4 096 octets par page)
- **Page 0** : métadonnées (nombre de pages, collections)
- **Pages data** : slots `[record_id:8][data_len:2][deleted:1][data...]`
- **Documents binaires** : `[nb_fields:2]` puis `[name_len:2][name][type:1][value...]`
- **Types** : null(0), string(1), int64(2), float64(3), bool(4), embedded document(5)
- **WAL** : fichier `.wal` adjacent — `[LSN:8][Type:1][PageID:4][DataLen:4][Data][CRC32:4]`

---

## Langage de requête

### SELECT
```sql
SELECT * FROM jobs WHERE retry > 3
SELECT * FROM jobs WHERE type = "oracle" AND enabled = true
SELECT DISTINCT type FROM jobs
SELECT * FROM jobs WHERE name LIKE "ora%"
SELECT * FROM jobs WHERE name NOT LIKE "%test%"
SELECT * FROM jobs WHERE type IN ("oracle", "mysql")
SELECT * FROM jobs WHERE type NOT IN ("oracle", "mysql")
SELECT * FROM jobs WHERE params IS NOT NULL
SELECT * FROM jobs WHERE retry BETWEEN 3 AND 10
SELECT * FROM jobs WHERE retry NOT BETWEEN 1 AND 5
SELECT * FROM jobs ORDER BY retry DESC LIMIT 10 OFFSET 5
SELECT COUNT(*) FROM jobs
SELECT COUNT(email) FROM jobs              -- non-null seulement
SELECT COUNT(*), type FROM jobs GROUP BY type
SELECT type, COUNT(*) FROM jobs GROUP BY type HAVING COUNT(*) > 1
SELECT SUM(retry), MIN(retry), MAX(retry) FROM jobs
SELECT * FROM jobs AS j JOIN results AS r ON j.type = r.type
SELECT * FROM jobs LEFT JOIN logs ON jobs.type = logs.type
```

### INSERT
```sql
INSERT INTO jobs VALUES (type="oracle", retry=5, enabled=true)
INSERT INTO jobs VALUES (type="mysql", params.timeout=60)
INSERT INTO backup SELECT * FROM jobs WHERE retry > 0
INSERT OR REPLACE INTO jobs VALUES (type="oracle", retry=99)  -- UPSERT
INSERT INTO jobs VALUES (type="a", retry=1), (type="b", retry=2)  -- batch
```

### UPDATE
```sql
UPDATE jobs SET retry=10 WHERE type="oracle"
UPDATE jobs SET retry = retry + 1 WHERE type="oracle"  -- expressions
UPDATE jobs SET params.timeout=120 WHERE params.timeout < 30
```

### DELETE
```sql
DELETE FROM jobs WHERE enabled = false
```

### INDEX
```sql
CREATE INDEX ON jobs (type)
CREATE INDEX IF NOT EXISTS ON jobs (type)
DROP INDEX ON jobs (type)
DROP INDEX IF EXISTS ON jobs (type)
```

### DDL
```sql
DROP TABLE temp
DROP TABLE IF EXISTS temp
TRUNCATE TABLE temp
```

### EXPLAIN
```sql
EXPLAIN SELECT * FROM jobs WHERE retry > 3
EXPLAIN SELECT * FROM jobs WHERE type = "oracle"  -- INDEX LOOKUP si indexé
```

---

## Durabilité (WAL)

Chaque opération d'écriture est **journalisée dans le WAL avant d'être appliquée** :

```
INSERT → WAL.LogPageWrite() → Data file write → WAL.Commit() + fsync
```

- **Crash recovery** : au redémarrage, les écritures committées sont rejouées automatiquement
- **Intégrité** : CRC32 sur chaque record WAL — les records corrompus sont ignorés
- **Uncommitted** : les écritures sans commit sont abandonnées au recovery
- **Checkpoint** : le WAL est tronqué au `Close()` après fsync du fichier data

---

## Concurrence

- **INSERT** : atomique, parallélisable entre goroutines
- **UPDATE / DELETE** : verrou exclusif par record (`record-level lock`)
- **Politique** : `Wait` (attente avec timeout) ou `Fail` (échec immédiat)
- **Index** : verrou coarse-grained pour les mises à jour

---

## Utilisation

```go
package main

import (
    "fmt"
    "log"
    "github.com/Felmond13/novusdb/api"
)

func main() {
    db, err := api.Open("mydata.dlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    db.Exec(`INSERT INTO jobs VALUES (type="oracle", retry=5)`)
    db.Exec(`INSERT INTO jobs VALUES (type="mysql", retry=2)`)

    res, _ := db.Exec(`SELECT * FROM jobs WHERE retry > 3`)
    for _, doc := range res.Docs {
        name, _ := doc.Doc.Get("type")
        fmt.Println(name)
    }

    // Agrégat standalone
    res, _ = db.Exec(`SELECT COUNT(*) FROM jobs`)
    count, _ := res.Docs[0].Doc.Get("COUNT")
    fmt.Println("Total:", count)

    // Schema
    for _, s := range db.Schema() {
        fmt.Printf("%s (%d docs)\n", s.Name, s.DocCount)
        for _, f := range s.Fields {
            fmt.Printf("  %s: %v (%d/%d)\n", f.Name, f.Types, f.Count, s.DocCount)
        }
    }

    // Vacuum
    n, _ := db.Vacuum()
    fmt.Printf("Reclaimed %d records\n", n)

    // Transactions
    tx, _ := db.Begin()
    tx.Exec(`UPDATE accounts SET balance = balance - 30 WHERE name = "Alice"`)
    tx.Exec(`UPDATE accounts SET balance = balance + 30 WHERE name = "Bob"`)
    tx.Commit()   // atomique — ou tx.Rollback() pour tout annuler
}
```

### CLI interactif (REPL)

```bash
go build -o NovusDB ./cmd/NovusDB/
./NovusDB mydata.dlite    # ou ./NovusDB pour mode mémoire
```

```
NovusDB> INSERT INTO jobs VALUES (type="oracle", retry=5, params.timeout=60)
  OK — 1 ligne(s) affectée(s), dernier ID : 1

NovusDB> SELECT * FROM jobs
  [#1] type="oracle", retry=5, params={timeout=60}
  --- 1 document(s)

NovusDB> SELECT COUNT(*) FROM jobs
  [#0] COUNT=1
  --- 1 document(s)

NovusDB> .schema
  jobs (1 document(s))
    ├─ type                      string (1/1 = 100%)
    ├─ retry                     int64 (1/1 = 100%)
    ├─ params.timeout            int64 (1/1 = 100%)

NovusDB> .vacuum
  Vacuum terminé — 0 record(s) récupéré(s)

NovusDB> .tables
  jobs
```

**Commandes spéciales** : `.help`, `.tables`, `.schema`, `.vacuum`, `.clear`, `.version`, `.quit`

### Lancer les tests

```bash
go test ./... -v
```

---

## Tests

250+ tests couvrant tous les modules :

| Module | Couverture |
|---|---|
| **storage** | Document CRUD, Encode/Decode, Nested, Page ops, Pager, WAL (append, reload, CRC, truncate, commit), WAL+Pager integration, recovery, checkpoint, LRU cache (eviction, stats, invalidation) |
| **parser** | Lexer, Parser (SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY, ORDER BY, IN, LIKE, DISTINCT, BETWEEN, sous-requêtes, INSERT...SELECT, DROP TABLE, EXPLAIN) |
| **engine** | Évaluation WHERE (tous opérateurs), LIKE, BETWEEN, HAVING avec agrégats |
| **concurrency** | Acquire/Release, politiques Wait/Fail, timeout, contention |
| **index** | B+ Tree CRUD, RangeScan, Split, Persistence, ValueToKey, Manager |
| **api** | CRUD, nested, index, multi-collection, persistance, ORDER BY, GROUP BY, HAVING, projection, JOIN (INNER/LEFT/RIGHT), INSERT...SELECT, INSERT OR REPLACE, DISTINCT, LIKE, BETWEEN, Vacuum, DROP TABLE, Schema, EXPLAIN, IF EXISTS, persistent index, transactions, computed columns, qualified star, wildcard paths, nested documents, Hash/Index Lookup/Nested Loop Join, LRU cache, concurrent reads+writes, subqueries (correlated), alias edge cases, hints (6 types), UNION/UNION ALL, CASE WHEN, COUNT(DISTINCT), Create/Drop VIEW, Dump/Restore, JSON INSERT (syntax + API + arrays + nested + persistence), Overflow (insert/select, persistence, JSON, delete, vacuum) |

---

## Limitations actuelles

- **Sous-requêtes corrélées** : supportées via référence d'alias (ex: `A.field`), pas de référence par nom de table

---

## Roadmap

| Priorité | Fonctionnalité |
|---|---|
| ~~**Fait**~~ | ~~Wildcard paths (`*`, `**`), nested document literals `{key=val}`~~ |
| ~~**Fait**~~ | ~~Hash Join O(n+m), Index Lookup Join O(n × log m), sélection automatique de stratégie~~ |
| ~~**Fait**~~ | ~~LRU Page Cache 4 MB (1024 pages), O(1) get/put/evict, `.cache` CLI~~ |
| ~~**Fait**~~ | ~~Query planner : cardinalité, sélectivité, coût par join, cache stats dans EXPLAIN~~ |
| ~~**Fait**~~ | ~~MVCC multi-reader : RWMutex, thread-safe LRU cache, lectures concurrentes~~ |
| ~~**Fait**~~ | ~~Sous-requêtes : non corrélées + corrélées (SELECT, WHERE, UPDATE, DELETE), alias stripping~~ |
| ~~**Fait**~~ | ~~Query Hints Oracle-style : PARALLEL, NO_CACHE, FULL_SCAN, FORCE_INDEX, HASH_JOIN, NESTED_LOOP~~ |
| ~~**Fait**~~ | ~~UNION/UNION ALL, CASE WHEN, COUNT(DISTINCT), CREATE VIEW, Backup .dump, Serveur HTTP REST~~ |
| ~~**Fait**~~ | ~~INSERT JSON natif, tableaux (arrays), InsertJSON API~~ |
| ~~**Fait**~~ | ~~Documents multi-pages (overflow pages chaînées), import JSON fichier~~ |
| ~~**Fait**~~ | ~~RIGHT JOIN~~ |
| **Prochain** | Streaming résultats intermédiaires, MVCC snapshot isolation |
| **Futur** | CTEs (WITH ... AS), ALTER TABLE, FULL OUTER JOIN |

---

## Zéro dépendances

NovusDB n'utilise que la bibliothèque standard Go. Aucune dépendance externe.

## Licence

**NovusDB License** (Source-Available, Non-Commercial)

| Cas d'usage | Coût |
|---|---|
| Personnel, apprentissage, éducation, recherche | **Gratuit** pour toujours |
| Associations & projets open-source | **Gratuit** pour toujours |
| Petites entreprises < 15K$/an de CA | **Gratuit** pour toujours |
| Usage commercial (> 15K$ CA, SaaS, produits payants) | **Licence payante** — essai gratuit 30 jours |

Le code est **visible et modifiable**, mais l'usage commercial au-delà du seuil nécessite une licence payante.

📧 **Contact** : noureddine.boukadoum@gmail.com  |  🌐 **Site web** : [novusdb.dev](https://novusdb.dev)
