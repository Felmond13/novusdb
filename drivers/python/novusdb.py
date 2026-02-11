"""
NovusDB Python Driver — Thin ctypes wrapper over the NovusDB C shared library.

Usage:
    from NovusDB import NovusDB

    db = NovusDB("ma_base.dlite")
    db.exec("INSERT INTO users VALUES (name=\"Alice\", age=30)")
    result = db.exec("SELECT * FROM users")
    print(result)  # {"docs": [...], "rows_affected": 0, "last_insert_id": 0}

    db.insert_json("users", '{"name": "Bob", "age": 25}')
    print(db.collections())  # ["users"]
    print(db.dump())

    db.close()
"""

import ctypes
import json
import os
import sys
import platform


def _find_library():
    """Locate the NovusDB shared library."""
    base = os.path.dirname(os.path.abspath(__file__))
    system = platform.system()

    if system == "Windows":
        names = ["NovusDB.dll"]
    elif system == "Darwin":
        names = ["libNovusDB.dylib", "NovusDB.dylib"]
    else:
        names = ["libNovusDB.so", "NovusDB.so"]

    search_paths = [
        base,
        os.path.join(base, "..", "c"),
        os.path.join(base, ".."),
        os.getcwd(),
    ]

    for d in search_paths:
        for name in names:
            path = os.path.join(d, name)
            if os.path.isfile(path):
                return os.path.abspath(path)

    raise FileNotFoundError(
        f"Cannot find NovusDB shared library. "
        f"Searched for {names} in {search_paths}. "
        f"Build it with: go build -buildmode=c-shared -o NovusDB.dll ./drivers/c/"
    )


class NovusDB:
    """NovusDB database connection."""

    def __init__(self, path: str, lib_path: str = None):
        """
        Open a NovusDB database.

        Args:
            path: Path to the .dlite database file.
            lib_path: Optional explicit path to the shared library.
        """
        if lib_path:
            lib_file = lib_path
        else:
            lib_file = _find_library()

        self._lib = ctypes.CDLL(lib_file)

        # Define function signatures
        self._lib.NovusDB_open.argtypes = [ctypes.c_char_p]
        self._lib.NovusDB_open.restype = ctypes.c_longlong

        self._lib.NovusDB_close.argtypes = [ctypes.c_longlong]
        self._lib.NovusDB_close.restype = ctypes.c_int

        self._lib.NovusDB_exec.argtypes = [ctypes.c_longlong, ctypes.c_char_p]
        self._lib.NovusDB_exec.restype = ctypes.c_char_p

        self._lib.NovusDB_insert_json.argtypes = [
            ctypes.c_longlong,
            ctypes.c_char_p,
            ctypes.c_char_p,
        ]
        self._lib.NovusDB_insert_json.restype = ctypes.c_longlong

        self._lib.NovusDB_collections.argtypes = [ctypes.c_longlong]
        self._lib.NovusDB_collections.restype = ctypes.c_char_p

        self._lib.NovusDB_error.argtypes = [ctypes.c_longlong]
        self._lib.NovusDB_error.restype = ctypes.c_char_p

        self._lib.NovusDB_dump.argtypes = [ctypes.c_longlong]
        self._lib.NovusDB_dump.restype = ctypes.c_char_p

        self._lib.NovusDB_free.argtypes = [ctypes.c_char_p]
        self._lib.NovusDB_free.restype = None

        # Open the database
        self._handle = self._lib.NovusDB_open(path.encode("utf-8"))
        if self._handle == 0:
            raise RuntimeError(f"Failed to open database: {path}")

    def exec(self, sql: str) -> dict:
        """
        Execute a SQL query.

        Returns:
            dict with keys: docs (list), rows_affected (int), last_insert_id (int)
            or: error (str) if the query failed.
        """
        raw = self._lib.NovusDB_exec(self._handle, sql.encode("utf-8"))
        result = json.loads(raw.decode("utf-8"))
        # Note: in production, we should call NovusDB_free on the raw pointer.
        # ctypes handles this automatically for c_char_p return values.
        if "error" in result:
            raise RuntimeError(result["error"])
        return result

    def insert_json(self, collection: str, json_str: str) -> int:
        """
        Insert a raw JSON document into a collection.

        Returns:
            The inserted document ID.
        """
        doc_id = self._lib.NovusDB_insert_json(
            self._handle,
            collection.encode("utf-8"),
            json_str.encode("utf-8"),
        )
        if doc_id < 0:
            err = self.last_error()
            raise RuntimeError(err or "insert_json failed")
        return doc_id

    def collections(self) -> list:
        """Return the list of collection names."""
        raw = self._lib.NovusDB_collections(self._handle)
        return json.loads(raw.decode("utf-8"))

    def last_error(self) -> str:
        """Return the last error message for this connection."""
        raw = self._lib.NovusDB_error(self._handle)
        return raw.decode("utf-8") if raw else ""

    def dump(self) -> str:
        """Return the full SQL dump of the database."""
        raw = self._lib.NovusDB_dump(self._handle)
        return raw.decode("utf-8") if raw else ""

    def close(self):
        """Close the database connection."""
        if self._handle:
            self._lib.NovusDB_close(self._handle)
            self._handle = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()


# CLI usage
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python NovusDB.py <database.dlite> [sql]")
        sys.exit(1)

    db_path = sys.argv[1]
    with NovusDB(db_path) as db:
        if len(sys.argv) > 2:
            sql = " ".join(sys.argv[2:])
            result = db.exec(sql)
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print(f"Connected to {db_path}")
            print(f"Collections: {db.collections()}")
