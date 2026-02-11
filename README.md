<p align="center">
  <img src="website/logo-banner.svg" alt="NovusDB" width="420">
</p>

<p align="center">
  <strong>A database that fits inside your app. One file, zero setup, full SQL power.</strong>
</p>

<p align="center">
  <a href="https://novusdb.dev/playground.html"><b>▶ Try the Playground</b></a> &nbsp;·&nbsp;
  <a href="https://novusdb.dev">🌐 Website</a> &nbsp;·&nbsp;
  <a href="README-fr.md">🇫🇷 Français</a>
</p>

---

NovusDB is a lightweight embedded document database written in pure Go. Drop a single file into your project and you get a complete data layer — no server to install, no schema to define, no migration to run.

Store JSON documents with nested fields, query them with familiar SQL syntax (JOINs, aggregations, subqueries, CASE WHEN), and scale to **millions of documents** with automatic B+ Tree indexing.

**Key highlights:**
- ⚡ **Zero config** — open a file, start querying
- 🔒 **Crash-safe** — Write-Ahead Log with CRC32 integrity checks
- 🔗 **Smart JOINs** — Hash Join O(n+m), Index Lookup O(n·log m), automatic strategy
- 📦 **Schema-free** — nested documents, arrays, dot-notation (`user.address.city`)
- 🧠 **Query planner** — EXPLAIN with cost estimation + Oracle-style hints
- 🌍 **Multi-language** — Go, Python, Node.js, Java drivers
- 🖥️ **Lumen** — built-in web admin UI (Vue 3 + Tailwind CSS)
- 🌐 **REST API** — HTTP server included for remote access

---

## Key Features

- **Schema-free**: nested documents, dynamic fields, mixed types
- **WAL (Write-Ahead Log)**: guaranteed durability, automatic crash recovery
- **Optimized JOINs**: INNER JOIN, LEFT JOIN and RIGHT JOIN with automatic strategy selection:
  - **Hash Join** O(n+m) for equi-joins without index
  - **Index Lookup Join** O(n × log m) when a B+ Tree exists on the join field
  - **Nested Loop** O(n×m) fallback for non-equi conditions
- **Aggregations**: COUNT, SUM, AVG, MIN, MAX — with or without GROUP BY
- **DISTINCT**, **LIKE** / **NOT LIKE**, **IN** / **NOT IN**, **IS NULL** / **IS NOT NULL**, **BETWEEN**
- **Arithmetic expressions**: `+`, `-`, `*`, `/` in SELECT, WHERE and UPDATE SET
- **Computed columns**: `SELECT 1+3 AS cpt`, `SELECT "label" AS col1`, `SELECT price*2 AS double`
- **Qualified star**: `SELECT A.* FROM table A`, mixable with other columns
- **Nested documents**: `INSERT INTO t VALUES (notes={math=19, physics={exam=15, homework=18}})`
- **Wildcard paths**: `WHERE notes.* > 15` (direct children), `WHERE notes.** > 15` (deep recursive)
- **Executable subqueries**: non-correlated (`WHERE x IN (SELECT ...)`), correlated (`WHERE x = (SELECT ... WHERE y = A.x)`), scalar in SELECT
- **INSERT INTO ... SELECT**: copy data between collections
- **INSERT OR REPLACE**: UPSERT (insert or update on the first field)
- **UNION / UNION ALL**: combine results of two SELECTs, with or without deduplication
- **CASE WHEN ... THEN ... ELSE ... END**: conditional expressions in SELECT and WHERE
- **COUNT(DISTINCT field)**: unique value counting, with or without GROUP BY
- **CREATE VIEW / DROP VIEW**: virtual views persisted on disk, transparently resolved in SELECT
- **Backup `.dump`**: full database export as reproducible SQL (indexes, views, data)
- **Native JSON INSERT**: `INSERT INTO t VALUES {"name": "Alice", "tags": [1, 2, 3]}` — JSON syntax with `:`, arrays `[]`, nested objects
- **InsertJSON API**: `db.InsertJSON("col", jsonString)` — programmatic raw JSON insertion
- **Arrays**: `FieldArray` type persisted on disk, supported in INSERT, SELECT, Dump
- **Multi-page documents (overflow)**: documents > 4 KB are automatically stored in chained overflow pages, transparent to the user
- **HTTP REST server**: `NovusDB-server` with endpoints `/query`, `/insert/{col}`, `/collections`, `/views`, `/schema`, `/dump`, `/cache`
- **JSON import**: `.import <collection> <file.json>` — imports a JSON file (object or array of objects)
- **DROP TABLE** / **TRUNCATE TABLE**: delete or empty collections
- **Oracle-style Query Hints**: `/*+ PARALLEL(n) */`, `/*+ NO_CACHE */`, `/*+ FULL_SCAN */`, `/*+ FORCE_INDEX(field) */`, `/*+ HASH_JOIN */`, `/*+ NESTED_LOOP */`
- **SQL comments**: `/* comment */` ignored by the lexer
- **EXPLAIN** with query planner: cardinality, selectivity, cost per join, active hints, cache stats
- **Vacuum**: compaction of deleted records
- **LRU Page Cache**: 4 MB in-memory cache (1024 pages), O(1) get/put/evict, `.cache` stats
- **Persistent B+ Tree indexes**: stored on disk, instant loading on restart
- **Transactions**: BEGIN / COMMIT / ROLLBACK with undo log (single-writer isolation)
- **Concurrency**: RWMutex multi-reader / single-writer, record-level locks, parallel inserts
- **Interactive CLI**: REPL with `.schema`, `.vacuum`, `.tables`, `.dump`, `.views`, `.cache`, `.help`
- **Zero dependencies**: Go standard library only

---

## Architecture

```
NovusDB/
├── api/            # User interface (DB.Open, DB.Exec, DB.Close, DB.Schema, DB.Vacuum)
├── parser/         # Lexer, AST, SQL-like Parser
├── engine/         # CRUD executor + WHERE evaluator + JOIN + aggregations
├── storage/        # Pager, Page (4 KB), Binary document, WAL
├── index/          # B+ Tree index: key → []record_id
├── concurrency/    # Record-level lock manager
├── cmd/NovusDB/    # Interactive CLI (REPL)
├── cmd/server/     # HTTP REST server
├── drivers/        # C/Python/Node.js/Java bindings
├── lumen/          # Web admin UI (Vue 3 + Tailwind)
└── cmd/example/    # Programmatic usage example
```

### Modules

| Module | Role |
|---|---|
| **storage** | Paged file (4 KB/page). Typed binary document. WAL with CRC32, commit/recovery/checkpoint. Vacuum. |
| **parser** | Lexer + Parser → AST. Supports SELECT DISTINCT, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, LIKE, IN, IS NULL, subqueries, INSERT...SELECT, DROP TABLE. |
| **engine** | Executes statements. Nested loop JOIN. Standalone and grouped aggregates. DISTINCT. Short-circuit evaluator. |
| **index** | In-memory hash-map: serialized key → record_ids. Automatic lookup for WHERE equality. |
| **concurrency** | Exclusive lock per record. Configurable policy (Wait/Fail). Timeout. |
| **api** | `Open()`, `Exec()`, `Close()`, `InsertDoc()`, `Schema()`, `Vacuum()`, `Collections()`. |

---

## Storage Format

- **Single paged file** (4,096 bytes per page)
- **Page 0**: metadata (page count, collections)
- **Data pages**: slots `[record_id:8][data_len:2][deleted:1][data...]`
- **Binary documents**: `[nb_fields:2]` then `[name_len:2][name][type:1][value...]`
- **Types**: null(0), string(1), int64(2), float64(3), bool(4), embedded document(5)
- **WAL**: adjacent `.wal` file — `[LSN:8][Type:1][PageID:4][DataLen:4][Data][CRC32:4]`

---

## Query Language

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

## Durability (WAL)

Every write operation is **logged to WAL before being applied**:

```
INSERT → WAL.LogPageWrite() → Data file write → WAL.Commit() + fsync
```

- **Crash recovery**: on restart, committed writes are automatically replayed
- **Integrity**: CRC32 on each WAL record — corrupted records are ignored
- **Uncommitted**: writes without commit are discarded on recovery
- **Checkpoint**: WAL is truncated on `Close()` after data file fsync

---

## Concurrency

- **INSERT**: atomic, parallelizable across goroutines
- **UPDATE / DELETE**: exclusive lock per record (`record-level lock`)
- **Policy**: `Wait` (wait with timeout) or `Fail` (immediate failure)
- **Index**: coarse-grained lock for updates

---

## Usage

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

    // Standalone aggregate
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

### Interactive CLI (REPL)

```bash
go build -o NovusDB ./cmd/NovusDB/
./NovusDB mydata.dlite    # or ./NovusDB for in-memory mode
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

**Special commands**: `.help`, `.tables`, `.schema`, `.vacuum`, `.clear`, `.version`, `.quit`

### Running Tests

```bash
go test ./... -v
```

---

## Tests

250+ tests covering all modules:

| Module | Coverage |
|---|---|
| **storage** | Document CRUD, Encode/Decode, Nested, Page ops, Pager, WAL (append, reload, CRC, truncate, commit), WAL+Pager integration, recovery, checkpoint, LRU cache (eviction, stats, invalidation) |
| **parser** | Lexer, Parser (SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY, ORDER BY, IN, LIKE, DISTINCT, BETWEEN, subqueries, INSERT...SELECT, DROP TABLE, EXPLAIN) |
| **engine** | WHERE evaluation (all operators), LIKE, BETWEEN, HAVING with aggregates |
| **concurrency** | Acquire/Release, Wait/Fail policies, timeout, contention |
| **index** | B+ Tree CRUD, RangeScan, Split, Persistence, ValueToKey, Manager |
| **api** | CRUD, nested, index, multi-collection, persistence, ORDER BY, GROUP BY, HAVING, projection, JOIN (INNER/LEFT/RIGHT), INSERT...SELECT, INSERT OR REPLACE, DISTINCT, LIKE, BETWEEN, Vacuum, DROP TABLE, Schema, EXPLAIN, IF EXISTS, persistent index, transactions, computed columns, qualified star, wildcard paths, nested documents, Hash/Index Lookup/Nested Loop Join, LRU cache, concurrent reads+writes, subqueries (correlated), alias edge cases, hints (6 types), UNION/UNION ALL, CASE WHEN, COUNT(DISTINCT), Create/Drop VIEW, Dump/Restore, JSON INSERT (syntax + API + arrays + nested + persistence), Overflow (insert/select, persistence, JSON, delete, vacuum), RIGHT JOIN |

---

## Current Limitations

- **Correlated subqueries**: supported via alias reference (e.g., `A.field`), not by table name

---

## Roadmap

| Priority | Feature |
|---|---|
| ~~**Done**~~ | ~~Wildcard paths (`*`, `**`), nested document literals `{key=val}`~~ |
| ~~**Done**~~ | ~~Hash Join O(n+m), Index Lookup Join O(n × log m), automatic strategy selection~~ |
| ~~**Done**~~ | ~~LRU Page Cache 4 MB (1024 pages), O(1) get/put/evict, `.cache` CLI~~ |
| ~~**Done**~~ | ~~Query planner: cardinality, selectivity, cost per join, cache stats in EXPLAIN~~ |
| ~~**Done**~~ | ~~MVCC multi-reader: RWMutex, thread-safe LRU cache, concurrent reads~~ |
| ~~**Done**~~ | ~~Subqueries: non-correlated + correlated (SELECT, WHERE, UPDATE, DELETE), alias stripping~~ |
| ~~**Done**~~ | ~~Oracle-style Query Hints: PARALLEL, NO_CACHE, FULL_SCAN, FORCE_INDEX, HASH_JOIN, NESTED_LOOP~~ |
| ~~**Done**~~ | ~~UNION/UNION ALL, CASE WHEN, COUNT(DISTINCT), CREATE VIEW, Backup .dump, HTTP REST server~~ |
| ~~**Done**~~ | ~~Native JSON INSERT, arrays, InsertJSON API~~ |
| ~~**Done**~~ | ~~Multi-page documents (chained overflow pages), JSON file import~~ |
| ~~**Done**~~ | ~~RIGHT JOIN, C/Python/Node.js/Java drivers, Lumen web admin UI~~ |
| **Next** | Streaming intermediate results, MVCC snapshot isolation |
| **Future** | CTEs (WITH ... AS), ALTER TABLE, FULL OUTER JOIN |

---

## Zero Dependencies

NovusDB uses only the Go standard library. No external dependencies.

## Multi-Language Drivers

NovusDB compiles into a **C shared library** (`novusdb.dll` / `libnovusdb.so`) with thin wrappers for:

- **Python** — `ctypes` (zero dependencies)
- **Node.js** — `ffi-napi`
- **Java** — JNA

See [`drivers/README.md`](drivers/README.md) for details.

## Lumen — Web Admin UI

Built-in web admin interface (Vue 3 + Tailwind CSS) with SQL editor, collection browser, schema viewer, cache stats, and SQL export. See [`lumen/README.md`](lumen/README.md).

## License

**NovusDB License** (Source-Available, Non-Commercial)

| Use case | Cost |
|---|---|
| Personal, learning, education, research | **Free** forever |
| Non-profits & open-source projects | **Free** forever |
| Small businesses < $15K/year revenue | **Free** forever |
| Commercial use (> $15K revenue, SaaS, paid products) | **Paid license** — 30-day free trial |

The code is **visible and modifiable**, but commercial use above the threshold requires a paid license.

📧 **Contact**: noureddine.boukadoum@gmail.com  |  🌐 **Website**: [novusdb.dev](https://novusdb.dev)
