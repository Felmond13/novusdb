/**
 * NovusDB Node.js Driver — FFI wrapper over the NovusDB C shared library.
 *
 * Usage:
 *   const { NovusDB } = require('./NovusDB');
 *
 *   const db = new NovusDB('ma_base.dlite');
 *   db.exec('INSERT INTO users VALUES (name="Alice", age=30)');
 *   const result = db.exec('SELECT * FROM users');
 *   console.log(result); // { docs: [...], rows_affected: 0, last_insert_id: 0 }
 *
 *   db.insertJSON('users', '{"name": "Bob", "age": 25}');
 *   console.log(db.collections()); // ["users"]
 *
 *   db.close();
 */

const ffi = require('ffi-napi');
const ref = require('ref-napi');
const path = require('path');
const fs = require('fs');

const charPtr = ref.refType(ref.types.char);

function findLibrary() {
  const base = __dirname;
  const isWin = process.platform === 'win32';
  const isMac = process.platform === 'darwin';

  const names = isWin
    ? ['novusdb.dll']
    : isMac
      ? ['libnovusdb.dylib', 'novusdb.dylib']
      : ['libnovusdb.so', 'novusdb.so'];

  const dirs = [
    base,
    path.join(base, '..', 'c'),
    path.join(base, '..'),
    process.cwd(),
  ];

  for (const dir of dirs) {
    for (const name of names) {
      const full = path.join(dir, name);
      if (fs.existsSync(full)) return full;
    }
  }

  throw new Error(
    `Cannot find NovusDB shared library. Searched for ${names.join(', ')} in ${dirs.join(', ')}. ` +
    `Build it with: go build -buildmode=c-shared -o novusdb.dll ./drivers/c/`
  );
}

class NovusDB {
  /**
   * Open a NovusDB database.
   * @param {string} dbPath - Path to the .dlite database file.
   * @param {string} [libPath] - Optional explicit path to the shared library.
   */
  constructor(dbPath, libPath) {
    const libFile = libPath || findLibrary();

    this._lib = ffi.Library(libFile, {
      NovusDB_open:        ['longlong', ['string']],
      NovusDB_close:       ['int',      ['longlong']],
      NovusDB_exec:        ['string',   ['longlong', 'string']],
      NovusDB_insert_json: ['longlong', ['longlong', 'string', 'string']],
      NovusDB_collections: ['string',   ['longlong']],
      NovusDB_error:       ['string',   ['longlong']],
      NovusDB_dump:        ['string',   ['longlong']],
      NovusDB_free:        ['void',     ['pointer']],
    });

    this._handle = this._lib.NovusDB_open(dbPath);
    if (this._handle === 0) {
      throw new Error(`Failed to open database: ${dbPath}`);
    }
  }

  /**
   * Execute a SQL query.
   * @param {string} sql - SQL query string.
   * @returns {object} { docs, rows_affected, last_insert_id }
   */
  exec(sql) {
    const raw = this._lib.NovusDB_exec(this._handle, sql);
    const result = JSON.parse(raw);
    if (result.error) throw new Error(result.error);
    return result;
  }

  /**
   * Insert a raw JSON document into a collection.
   * @param {string} collection - Collection name.
   * @param {string} jsonStr - JSON document string.
   * @returns {number} Inserted document ID.
   */
  insertJSON(collection, jsonStr) {
    const id = this._lib.NovusDB_insert_json(this._handle, collection, jsonStr);
    if (id < 0) {
      const err = this.lastError();
      throw new Error(err || 'insertJSON failed');
    }
    return id;
  }

  /**
   * List all collections.
   * @returns {string[]}
   */
  collections() {
    const raw = this._lib.NovusDB_collections(this._handle);
    return JSON.parse(raw);
  }

  /**
   * Get the last error message.
   * @returns {string}
   */
  lastError() {
    return this._lib.NovusDB_error(this._handle) || '';
  }

  /**
   * Get the full SQL dump of the database.
   * @returns {string}
   */
  dump() {
    return this._lib.NovusDB_dump(this._handle) || '';
  }

  /**
   * Close the database connection.
   */
  close() {
    if (this._handle) {
      this._lib.NovusDB_close(this._handle);
      this._handle = 0;
    }
  }
}

module.exports = { NovusDB };
