'use strict';

// db.migrate(migrations): a small, dependency-free schema migration runner,
// mixed onto Database.prototype by lib/database.js.
//
// A migration is `{ name, up }`: a unique name and a synchronous function that
// receives the database and makes the schema change. migrate() applies, in
// array order, every migration whose name is not yet recorded, each inside its
// own transaction, and records the name so a later run skips it. A migration
// that throws rolls back and aborts the run, leaving the already-applied ones
// committed. Applying is therefore idempotent and safe to call on every boot.
//
// Migrations are keyed by name, not by position, so inserting a new migration
// among already-applied ones is fine as long as names stay unique. The record
// lives in `_better_sqlite_migrations` (a `_better_sqlite_*` table, so the sync
// and this package's own introspection skip it).
//
// An extension beyond the better-sqlite3 surface. `up` must be synchronous:
// db.transaction() rejects a function that returns a promise, because the
// transaction would commit before the async work finished.

const fs = require('fs');
const path = require('path');
const { requireOpen, kDb } = require('../adapters/util');
const { tables } = require('./dictionary');
const { quote } = require('./schema');

const TABLE = tables.migrations;

function ensureTable(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS ${quote(TABLE)} (
    name       TEXT PRIMARY KEY,
    applied_at INTEGER NOT NULL
  )`);
}

// db.migrate(migrations): apply pending migrations. Returns
// { applied: string[], pending: number, current: number } — the names applied
// by this call, how many were still pending before it (0 on success), and the
// total number of migrations recorded afterwards.
function migrate(migrations) {
  requireOpen(this[kDb]);
  if (!Array.isArray(migrations)) throw new TypeError('Expected first argument to be an array of migrations');

  const names = new Set();
  for (const m of migrations) {
    if (m === null || typeof m !== 'object') throw new TypeError('Each migration must be an object with "name" and "up"');
    if (typeof m.name !== 'string' || !m.name) throw new TypeError('Each migration must have a non-empty "name" string');
    if (typeof m.up !== 'function') throw new TypeError(`Migration "${m.name}" must have an "up" function`);
    if (names.has(m.name)) throw new TypeError(`Duplicate migration name: ${m.name}`);
    names.add(m.name);
  }

  ensureTable(this);

  const done = new Set(this.prepare(`SELECT name FROM ${quote(TABLE)}`).pluck().all());
  const record = this.prepare(`INSERT INTO ${quote(TABLE)} (name, applied_at) VALUES (?, ?)`);
  const pendingBefore = migrations.filter((m) => !done.has(m.name)).length;

  const applied = [];
  for (const m of migrations) {
    if (done.has(m.name)) continue;
    // Each migration in its own transaction: the schema change and the record
    // of it commit together, so a crash never leaves one without the other.
    const step = this.transaction(() => {
      // Return up()'s value so db.transaction() rejects a promise-returning
      // migration (which would commit before its async work finished).
      const rv = m.up(this);
      record.run(m.name, Date.now());
      return rv;
    });
    step();
    applied.push(m.name);
  }

  return { applied, pending: pendingBefore - applied.length, current: done.size + applied.length };
}

// db.rollback(migrations, options?): undo the most recently applied migrations
// by running their `down` functions in reverse order, each in its own
// transaction, and removing their record. `options.steps` (default 1) is how
// many to undo; `options.to` rolls back until (and not including) the named
// migration. A migration selected for rollback must define a `down` function,
// or a TypeError is thrown before anything runs. Returns
// { rolledBack: string[] }.
function rollback(migrations, options = {}) {
  requireOpen(this[kDb]);
  if (!Array.isArray(migrations)) throw new TypeError('Expected first argument to be an array of migrations');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');

  const byName = new Map();
  for (const m of migrations) {
    if (m === null || typeof m !== 'object' || typeof m.name !== 'string' || !m.name) {
      throw new TypeError('Each migration must be an object with a non-empty "name"');
    }
    byName.set(m.name, m);
  }

  ensureTable(this);
  // rowid preserves application order; newest first.
  const appliedNames = this.prepare(`SELECT name FROM ${quote(TABLE)} ORDER BY rowid DESC`).pluck().all();

  let targets;
  if (options.to !== undefined) {
    if (typeof options.to !== 'string') throw new TypeError('Expected the "to" option to be a string');
    const stop = appliedNames.indexOf(options.to);
    targets = stop === -1 ? appliedNames.slice() : appliedNames.slice(0, stop);
  } else {
    const steps = options.steps === undefined ? 1 : options.steps;
    if (!Number.isInteger(steps) || steps < 0) throw new TypeError('Expected the "steps" option to be a non-negative integer');
    targets = appliedNames.slice(0, steps);
  }

  // Validate every selected migration has a down() before running any.
  for (const name of targets) {
    const m = byName.get(name);
    if (!m || typeof m.down !== 'function') throw new TypeError(`Migration "${name}" has no "down" function to roll back`);
  }

  const remove = this.prepare(`DELETE FROM ${quote(TABLE)} WHERE name = ?`);
  const rolledBack = [];
  for (const name of targets) {
    const m = byName.get(name);
    const step = this.transaction(() => {
      const rv = m.down(this);
      remove.run(name);
      return rv;
    });
    step();
    rolledBack.push(name);
  }
  return { rolledBack };
}

// Database.loadMigrations(dir): read a directory of migration modules and return
// them sorted by filename, ready for migrate()/rollback(). Each `.js`/`.cjs`
// file exports `{ up, down? }` (or `{ name?, up, down? }`); the name defaults to
// the filename without extension, so lexical filenames like `001-init.js` order
// correctly. A static helper, not tied to a connection.
function loadMigrations(dir) {
  if (typeof dir !== 'string' || !dir) throw new TypeError('Expected first argument to be a directory path');
  const files = fs.readdirSync(dir)
    .filter((f) => /\.(c?js)$/.test(f))
    .sort();
  return files.map((file) => {
    const mod = require(path.resolve(dir, file));
    const spec = mod && mod.default ? mod.default : mod;
    if (!spec || typeof spec.up !== 'function') throw new TypeError(`Migration file "${file}" must export an "up" function`);
    return {
      name: typeof spec.name === 'string' && spec.name ? spec.name : file.replace(/\.(c?js)$/, ''),
      up: spec.up,
      down: typeof spec.down === 'function' ? spec.down : undefined
    };
  });
}

module.exports = { migrate, rollback, loadMigrations, TABLE };
