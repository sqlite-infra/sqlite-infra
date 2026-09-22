'use strict';

// db.kv(namespace): a typed key-value store over SQLite, mixed onto
// Database.prototype by lib/database.js. A common use of SQLite is as a durable
// KV store; this gives a small, correct one with namespaces and lazy TTL.
//
// Values are JSON-encoded with a `type` tag so a number stays a number, a
// boolean a boolean, null a null and an object/array itself — none collapse to
// a string on the way back. TTL is lazy: an expired entry reads as absent and
// is purged when next touched, so there is no background timer.
//
// Rows live in `_better_sqlite_kv` (a `_better_sqlite_*` table, so the sync and
// this package's own introspection skip it). An extension beyond the
// better-sqlite3 surface.

const { requireOpen, kDb } = require('./util');
const { tables } = require('./dictionary');
const { quote } = require('./schema');

const TABLE = tables.kv;
const T = quote(TABLE);

function ensureTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS ${T} (
    ns         TEXT NOT NULL,
    k          TEXT NOT NULL,
    v          TEXT,
    type       TEXT NOT NULL,
    expires_at INTEGER,
    PRIMARY KEY (ns, k)
  )`);
}

// Encode a value to { v, type }, so decode restores the exact JS value.
function encode(value) {
  const type = value === null ? 'null'
    : Array.isArray(value) ? 'array'
      : typeof value;
  if (type === 'undefined') throw new TypeError('Cannot store undefined; use delete() to remove a key');
  if (type === 'function' || type === 'symbol' || type === 'bigint') {
    throw new TypeError(`Cannot store a value of type ${type}`);
  }
  return { v: JSON.stringify(value), type };
}

function decode(row) {
  return row.v === null ? null : JSON.parse(row.v);
}

class KvStore {
  constructor(db, namespace) {
    this._db = db;
    this.namespace = namespace;
    ensureTable(db);
  }

  // Deletes an entry when expired; returns the live row or undefined.
  _live(key) {
    const row = this._db.prepare(`SELECT v, type, expires_at FROM ${T} WHERE ns = ? AND k = ?`).get(this.namespace, key);
    if (row === undefined) return undefined;
    if (row.expires_at !== null && row.expires_at <= Date.now()) {
      this._db.prepare(`DELETE FROM ${T} WHERE ns = ? AND k = ?`).run(this.namespace, key);
      return undefined;
    }
    return row;
  }

  get(key) {
    const row = this._live(String(key));
    return row === undefined ? undefined : decode(row);
  }

  set(key, value, options = {}) {
    const { v, type } = encode(value);
    const expiresAt = options && Number.isFinite(options.ttlMs) && options.ttlMs > 0 ? Date.now() + Math.floor(options.ttlMs) : null;
    this._db.prepare(`
      INSERT INTO ${T} (ns, k, v, type, expires_at) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (ns, k) DO UPDATE SET v = excluded.v, type = excluded.type, expires_at = excluded.expires_at
    `).run(this.namespace, String(key), v, type, expiresAt);
    return this;
  }

  has(key) {
    return this._live(String(key)) !== undefined;
  }

  delete(key) {
    return this._db.prepare(`DELETE FROM ${T} WHERE ns = ? AND k = ?`).run(this.namespace, String(key)).changes > 0;
  }

  // Purges expired entries in this namespace, then returns the live rows.
  _rows() {
    this._db.prepare(`DELETE FROM ${T} WHERE ns = ? AND expires_at IS NOT NULL AND expires_at <= ?`).run(this.namespace, Date.now());
    return this._db.prepare(`SELECT k, v, type FROM ${T} WHERE ns = ? ORDER BY k`).all(this.namespace);
  }

  keys() {
    return this._rows().map((r) => r.k);
  }

  values() {
    return this._rows().map((r) => decode(r));
  }

  entries() {
    return this._rows().map((r) => [r.k, decode(r)]);
  }

  incr(key, by = 1) {
    if (typeof by !== 'number' || !Number.isFinite(by)) throw new TypeError('Expected the increment to be a finite number');
    const k = String(key);
    const current = this.get(k);
    const base = current === undefined ? 0 : current;
    if (typeof base !== 'number') throw new TypeError(`Cannot incr() key "${k}": stored value is not a number`);
    const next = base + by;
    // Preserve the existing entry's expiry when it has one.
    const row = this._db.prepare(`SELECT expires_at FROM ${T} WHERE ns = ? AND k = ?`).get(this.namespace, k);
    const ttlMs = row && row.expires_at !== null ? row.expires_at - Date.now() : undefined;
    this.set(k, next, ttlMs !== undefined && ttlMs > 0 ? { ttlMs } : {});
    return next;
  }

  getSet(key, producer, options = {}) {
    if (typeof producer !== 'function') throw new TypeError('Expected the producer to be a function');
    const k = String(key);
    const existing = this._live(k);
    if (existing !== undefined) return decode(existing);
    const value = producer();
    this.set(k, value, options);
    return value;
  }

  clear() {
    return this._db.prepare(`DELETE FROM ${T} WHERE ns = ?`).run(this.namespace).changes;
  }

  size() {
    this._db.prepare(`DELETE FROM ${T} WHERE ns = ? AND expires_at IS NOT NULL AND expires_at <= ?`).run(this.namespace, Date.now());
    return this._db.prepare(`SELECT count(*) FROM ${T} WHERE ns = ?`).pluck().get(this.namespace);
  }
}

// db.kv(namespace = 'default'): a KV store scoped to `namespace`.
function kv(namespace = 'default') {
  if (typeof namespace !== 'string' || !namespace) throw new TypeError('Expected the namespace to be a non-empty string');
  requireOpen(this[kDb]);
  return new KvStore(this, namespace);
}

module.exports = { kv, KvStore };
