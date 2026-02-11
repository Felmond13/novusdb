# NovusDB Drivers — Multi-Language Bindings

**SQLite-style** architecture: a single C shared library (`novusdb.dll` / `libnovusdb.so` / `libnovusdb.dylib`) built from Go, called via native FFI from each language.

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│  Python  │  │  Node.js │  │   Java   │
│ (ctypes) │  │(ffi-napi)│  │  (JNA)   │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │              │
     └─────────────┼──────────────┘
                   │
          ┌────────▼────────┐
          │   novusdb.dll   │
          │  (C shared lib) │
          │   built from Go │
          └────────┬────────┘
                   │
          ┌────────▼────────┐
          │   NovusDB Core  │
          │  (Go engine)    │
          └─────────────────┘
```

---

## 1. Build the Shared Library

### Prerequisites

- **Go** ≥ 1.21
- **GCC** (MinGW-w64 on Windows, `gcc` on Linux/macOS)

### Windows

```powershell
# Install MinGW if not already done (admin required)
choco install mingw -y

# Build
.\drivers\build.ps1
```

### Linux / macOS

```bash
chmod +x drivers/build.sh
./drivers/build.sh
```

### Manual

```bash
CGO_ENABLED=1 go build -buildmode=c-shared -o drivers/c/novusdb.dll ./drivers/c/
```

Output files:
- `novusdb.dll` (or `.so` / `.dylib`) — the shared library
- `NovusDB.h` — C header (provided, not generated)

---

## 2. C API

```c
#include "NovusDB.h"

long long db = NovusDB_open("ma_base.dlite");

char* result = NovusDB_exec(db, "SELECT * FROM users");
printf("%s\n", result);  // JSON
NovusDB_free(result);

long long id = NovusDB_insert_json(db, "users", "{\"name\": \"Alice\"}");

char* cols = NovusDB_collections(db);
NovusDB_free(cols);

NovusDB_close(db);
```

| Function | Description |
|---|---|
| `NovusDB_open(path)` | Open a database, returns a handle |
| `NovusDB_close(handle)` | Close the connection |
| `NovusDB_exec(handle, sql)` | Execute SQL, returns JSON |
| `NovusDB_insert_json(handle, col, json)` | Insert a JSON document |
| `NovusDB_collections(handle)` | List collections (JSON) |
| `NovusDB_error(handle)` | Last error |
| `NovusDB_dump(handle)` | Full SQL export |
| `NovusDB_free(ptr)` | Free a returned string |

---

## 3. Python

```python
from NovusDB import NovusDB

with NovusDB("ma_base.dlite") as db:
    db.exec('INSERT INTO users VALUES (name="Alice", age=30)')
    result = db.exec("SELECT * FROM users")
    print(result["docs"])

    db.insert_json("users", '{"name": "Bob", "age": 25}')
    print(db.collections())
```

**Dependencies**: none (uses `ctypes` from the stdlib).

---

## 4. Node.js

```javascript
const { NovusDB } = require('./NovusDB');

const db = new NovusDB('ma_base.dlite');
db.exec('INSERT INTO users VALUES (name="Alice", age=30)');
const result = db.exec('SELECT * FROM users');
console.log(result.docs);

db.insertJSON('users', '{"name": "Bob", "age": 25}');
console.log(db.collections());
db.close();
```

**Dependencies**: `ffi-napi`, `ref-napi` (`npm install`).

---

## 5. Java

```java
try (NovusDB db = new NovusDB("ma_base.dlite")) {
    String result = db.exec("SELECT * FROM users");
    System.out.println(result);

    long id = db.insertJSON("users", "{\"name\": \"Alice\"}");
    System.out.println(db.collections());
}
```

**Dependencies**: [JNA](https://github.com/java-native-access/jna) on the classpath.

---

## Structure

```
drivers/
├── c/
│   ├── NovusDB.go      # Go → C exports (cgo)
│   ├── NovusDB.h       # C header
│   └── novusdb.dll     # Shared library (after build)
├── python/
│   └── NovusDB.py      # ctypes wrapper
├── node/
│   ├── NovusDB.js      # ffi-napi wrapper
│   └── package.json
├── java/
│   └── NovusDB.java    # JNA wrapper
├── build.ps1           # Windows build script
├── build.sh            # Linux/macOS build script
└── README.md
```
