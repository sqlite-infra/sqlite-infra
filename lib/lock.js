'use strict';

// Advisory (application) locks, mixed onto Database.prototype by lib/database.js.
// A row in `_better_sqlite_locks` names a held lock; because SQLite serializes
// writers on one file, the INSERT that claims a lock is atomic across every
// process and connection sharing that file. This is how workers that share a
// database — the queue's dispatchers, the scheduler's tickers — coordinate
// exclusive work without a separate lock service.
//
// Locks are advisory: they bind only code that calls lock()/withLock(), never
// SQLite's own row locking. A `ttlMs` guards against a crashed holder never
// releasing — an expired lock is reclaimable by anyone. Timestamps are epoch-ms.
//
// An extension beyond the better-sqlite3 surface.

const crypto = require('crypto');
const { requireOpen, kDb } = require('./util');
const { tables } = require('./dictionary');
const { quote } = require('./schema');

const TABLE = tables.locks;

function ensureTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS ${quote(TABLE)} (
    name        TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    acquired_at INTEGER NOT NULL,
    expires_at  INTEGER
  )`);
}

function assertName(name) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected the lock name to be a non-empty string');
  return name;
}

// Blocks the current thread for `ms` without a busy loop.
function sleepMs(ms) {
  if (ms > 0) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

// db.lock(name, { ttlMs, owner }): acquire the named lock. Returns true if it
// was acquired (or refreshed by the same owner), false if held by someone else
// and not yet expired.
function lock(name, options = {}) {
  requireOpen(this[kDb]);
  assertName(name);
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  const owner = options.owner === undefined ? crypto.randomUUID() : String(options.owner);
  let ttlMs = null;
  if (options.ttlMs !== undefined) {
    if (!Number.isInteger(options.ttlMs) || options.ttlMs <= 0) throw new TypeError('Expected the "ttlMs" option to be a positive integer');
    ttlMs = options.ttlMs;
  }
  ensureTable(this);

  const acquire = this.transaction(() => {
    const now = Date.now();
    const expiresAt = ttlMs === null ? null : now + ttlMs;
    const row = this.prepare(`SELECT owner, expires_at FROM ${quote(TABLE)} WHERE name = ?`).get(name);
    if (row !== undefined) {
      const held = row.expires_at === null || row.expires_at > now;
      if (held && row.owner !== owner) return false;
      // Free (expired) or owned by us: take/refresh it.
      this.prepare(`UPDATE ${quote(TABLE)} SET owner = ?, acquired_at = ?, expires_at = ? WHERE name = ?`)
        .run(owner, now, expiresAt, name);
      return true;
    }
    this.prepare(`INSERT INTO ${quote(TABLE)} (name, owner, acquired_at, expires_at) VALUES (?, ?, ?, ?)`)
      .run(name, owner, now, expiresAt);
    return true;
  });
  return acquire();
}

// db.unlock(name, { owner }): release the lock. With an owner, releases only if
// it matches. Returns whether a lock row was removed.
function unlock(name, options = {}) {
  requireOpen(this[kDb]);
  assertName(name);
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  ensureTable(this);
  if (options.owner !== undefined) {
    return this.prepare(`DELETE FROM ${quote(TABLE)} WHERE name = ? AND owner = ?`).run(name, String(options.owner)).changes > 0;
  }
  return this.prepare(`DELETE FROM ${quote(TABLE)} WHERE name = ?`).run(name).changes > 0;
}

// db.withLock(name, fn, { ttlMs, owner, wait }): run fn() while holding the
// lock, releasing it afterwards even if fn throws. With `wait` (ms) it retries
// acquisition with a short synchronous backoff up to that long; if it still
// cannot acquire, it throws.
function withLock(name, fn, options = {}) {
  requireOpen(this[kDb]);
  assertName(name);
  if (typeof fn !== 'function') throw new TypeError('Expected second argument to be a function');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected third argument to be an options object');
  const owner = options.owner === undefined ? crypto.randomUUID() : String(options.owner);
  const wait = options.wait === undefined ? 0 : options.wait;
  if (!Number.isInteger(wait) || wait < 0) throw new TypeError('Expected the "wait" option to be a non-negative integer');

  const acquireOptions = { owner };
  if (options.ttlMs !== undefined) acquireOptions.ttlMs = options.ttlMs;

  const deadline = Date.now() + wait;
  let delay = 5;
  while (!this.lock(name, acquireOptions)) {
    if (Date.now() >= deadline) throw new Error(`Could not acquire lock "${name}"`);
    sleepMs(Math.min(delay, Math.max(0, deadline - Date.now())));
    delay = Math.min(delay * 2, 100);
  }
  try {
    return fn();
  } finally {
    this.unlock(name, { owner });
  }
}

// db.locks(): currently-held (non-expired) locks. Expired rows are purged first.
function locks() {
  requireOpen(this[kDb]);
  ensureTable(this);
  const now = Date.now();
  this.prepare(`DELETE FROM ${quote(TABLE)} WHERE expires_at IS NOT NULL AND expires_at <= ?`).run(now);
  return this.prepare(`SELECT name, owner, acquired_at, expires_at FROM ${quote(TABLE)} ORDER BY acquired_at`).all()
    .map((r) => ({ name: r.name, owner: r.owner, acquiredAt: r.acquired_at, expiresAt: r.expires_at }));
}

module.exports = { lock, unlock, withLock, locks };
