'use strict';

// WAL connection pool.
//
// node:sqlite is synchronous and single-threaded, so a Pool does NOT make
// writes concurrent. What it gives you on a WAL database is:
//
//   1. A dedicated writer connection, so a long-running read iterator (which
//      holds its own connection busy until it finishes) does not block writes.
//   2. N read-only reader connections that read committed data concurrently
//      with the writer, which WAL allows.
//
// A Pool needs a real file on disk: separate ':memory:' (and '') connections
// are DISTINCT databases and would not see each other's writes, which defeats
// the point. The constructor throws for an in-memory or temporary filename.
//
// Required lazily to avoid a require cycle: lib/database.js exposes the pool as
// Database.pool(), so this module must not pull database.js in at load time.

function DatabaseClass() { return require('./database'); }

class Pool {
  constructor(filename, options = {}) {
    const name = typeof filename === 'string' ? filename.trim() : filename;
    if (typeof name !== 'string' || name === '' || name === ':memory:' || name.startsWith('file::memory:')) {
      throw new TypeError('Pool requires a file path (":memory:" and temporary databases are per-connection and cannot be pooled)');
    }
    if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');

    const readers = options.readers === undefined ? 4 : options.readers;
    if (!Number.isInteger(readers) || readers < 1) throw new TypeError('Expected the "readers" option to be a positive integer');
    const dbOptions = options.dbOptions || {};
    const journalMode = options.journalMode === undefined ? 'WAL' : options.journalMode;

    const Database = DatabaseClass();
    this._readers = [];
    this._next = 0;
    this._open = true;
    this._writer = null;
    try {
      // The writer is always read-write, even if dbOptions asked for readonly.
      this._writer = new Database(filename, Object.assign({}, dbOptions, { readonly: false }));
      this._writer.pragma(`journal_mode = ${journalMode}`);
      if (dbOptions.timeout !== undefined) this._writer.pragma(`busy_timeout = ${dbOptions.timeout}`);
      for (let i = 0; i < readers; i += 1) {
        this._readers.push(new Database(filename, Object.assign({}, dbOptions, { readonly: true })));
      }
    } catch (err) {
      // Never leak a half-open pool.
      this._closeAll();
      this._open = false;
      throw err;
    }
  }

  /** Number of reader connections. */
  get size() { return this._readers.length; }
  /** Whether the pool is still open. */
  get open() { return this._open; }

  /** The single read-write connection. */
  writer() {
    this._assertOpen();
    return this._writer;
  }

  /** The next reader connection, round-robin. */
  reader() {
    this._assertOpen();
    const r = this._readers[this._next];
    this._next = (this._next + 1) % this._readers.length;
    return r;
  }

  /** Run fn with a round-robin reader; returns fn's result (value or promise). */
  read(fn) {
    if (typeof fn !== 'function') throw new TypeError('Expected first argument to be a function');
    return fn(this.reader());
  }

  /** Run fn with the writer; returns fn's result (value or promise). */
  write(fn) {
    if (typeof fn !== 'function') throw new TypeError('Expected first argument to be a function');
    return fn(this.writer());
  }

  /** Run fn(writer) inside a transaction on the writer; returns fn's result. */
  transaction(fn) {
    if (typeof fn !== 'function') throw new TypeError('Expected first argument to be a function');
    const writer = this.writer();
    return writer.transaction(fn)(writer);
  }

  /** Close the writer and every reader. Idempotent. */
  close() {
    if (!this._open) return this;
    this._open = false;
    this._closeAll();
    return this;
  }

  _closeAll() {
    const conns = [this._writer, ...this._readers].filter(Boolean);
    for (const c of conns) {
      try { c.close(); } catch { /* already closed */ }
    }
    this._writer = null;
    this._readers = [];
  }

  _assertOpen() {
    if (!this._open) throw new TypeError('The pool is closed');
  }
}

function createPool(filename, options) {
  return new Pool(filename, options);
}

module.exports = { createPool, Pool };
