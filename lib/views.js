'use strict';

// Views and "materialized views", mixed onto Database.prototype by
// lib/database.js.
//
// SQLite has real VIEWs (createView/dropView) but no materialized views: a
// materialized view here is an ordinary table populated from a SELECT, with a
// refresh operation that recomputes it. materialize() records the defining
// query in an in-process registry so refreshView() can re-run it; that registry
// lives on the connection object, so refreshView() works for the process that
// materialized the view (a fresh process would materialize() again). The table
// itself is durable — only the "how to refresh" definition is in memory.
//
// A SELECT `source` may be a raw SQL string, a `{ sql, params? }` object, or any
// builder with a `.toSQL()` (e.g. db.from(...)). Extensions beyond
// better-sqlite3. Identifiers are quoted; values bound.

const { requireOpen, kDb } = require('./util');
const schema = require('./schema');

const q = schema.quote;

// name -> { sql, params } per connection, so refreshView() can recompute.
const registries = new WeakMap();
function registryFor(db) {
  let reg = registries.get(db);
  if (!reg) registries.set(db, (reg = new Map()));
  return reg;
}

function assertName(name) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected a non-empty string name');
  return name;
}

// A SELECT source -> { sql, params }.
function resolveSelect(source) {
  if (typeof source === 'string') return { sql: source, params: [] };
  if (source && typeof source.toSQL === 'function') {
    const out = source.toSQL();
    return { sql: out.sql, params: out.params || [] };
  }
  if (source && typeof source.sql === 'string') return { sql: source.sql, params: source.params || [] };
  throw new TypeError('Expected the source to be a SQL string, a { sql, params } object, or a builder with toSQL()');
}

function normalizeColumns(cols) {
  if (cols === undefined) return null;
  const list = Array.isArray(cols) ? cols : [cols];
  if (list.length === 0 || list.some((c) => typeof c !== 'string' || !c)) {
    throw new TypeError('Expected columns to be a non-empty string or array of column names');
  }
  return list;
}

// db.createView(name, source, { ifNotExists, temporary }): a real SQL VIEW.
// The source cannot carry bound parameters (CREATE VIEW cannot bind).
function createView(name, source, options = {}) {
  requireOpen(this[kDb]);
  assertName(name);
  if (options === null || typeof options !== 'object') throw new TypeError('Expected the options to be an object');
  const { sql, params } = resolveSelect(source);
  if (params.length) throw new TypeError('A VIEW source cannot use bound parameters');
  const temp = options.temporary ? 'TEMP ' : '';
  const ifNot = options.ifNotExists ? 'IF NOT EXISTS ' : '';
  this.exec(`CREATE ${temp}VIEW ${ifNot}${q(name)} AS ${sql}`);
  return this;
}

// db.dropView(name, { ifExists }).
function dropView(name, options = {}) {
  requireOpen(this[kDb]);
  assertName(name);
  this.exec(`DROP VIEW ${options.ifExists ? 'IF EXISTS ' : ''}${q(name)}`);
  return this;
}

// Populate a not-yet-existing table `name` from a select. Returns column names.
function createFromSelect(db, name, sql, params) {
  if (params.length === 0) {
    db.exec(`CREATE TABLE ${q(name)} AS ${sql}`);
    return null;
  }
  // Parameterised: CREATE TABLE ... AS cannot bind, so take the shape from the
  // prepared statement and bulk-insert the rows in one transaction.
  const columns = db.prepare(sql).columns().map((c) => c.name);
  db.exec(`CREATE TABLE ${q(name)} (${columns.map(q).join(', ')})`);
  fillRows(db, name, columns, sql, params);
  return columns;
}

function fillRows(db, name, columns, sql, params) {
  const rows = db.prepare(sql).raw().all(...params);
  const insert = db.prepare(`INSERT INTO ${q(name)} VALUES (${columns.map(() => '?').join(', ')})`);
  const run = db.transaction((data) => { for (const r of data) insert.run(...r); });
  run(rows);
}

// db.materialize(name, source, { indexes, unique }): create table `name` from a
// SELECT and populate it. `indexes` is an array of column lists (each a string
// or string[]); `unique` is one column list. Returns { name, rows }.
function materialize(name, source, options = {}) {
  requireOpen(this[kDb]);
  assertName(name);
  if (options === null || typeof options !== 'object') throw new TypeError('Expected the options to be an object');
  const { sql, params } = resolveSelect(source);

  createFromSelect(this, name, sql, params);
  registryFor(this).set(name, { sql, params });

  let i = 0;
  for (const cols of options.indexes || []) {
    const list = normalizeColumns(cols);
    this.exec(`CREATE INDEX ${q(`${name}_idx_${i++}`)} ON ${q(name)} (${list.map(q).join(', ')})`);
  }
  if (options.unique !== undefined) {
    const list = normalizeColumns(options.unique);
    this.exec(`CREATE UNIQUE INDEX ${q(`${name}_uniq`)} ON ${q(name)} (${list.map(q).join(', ')})`);
  }

  const rows = this.prepare(`SELECT count(*) AS n FROM ${q(name)}`).pluck().get();
  return { name, rows };
}

// db.refreshView(name): recompute a materialized view from its stored
// definition, atomically. Throws if `name` was not materialize()d in this
// process.
function refreshView(name) {
  requireOpen(this[kDb]);
  assertName(name);
  const def = registryFor(this).get(name);
  if (!def) throw new TypeError(`"${name}" was not materialized in this process; call materialize() first`);

  const db = this;
  const refill = db.transaction(() => {
    db.exec(`DELETE FROM ${q(name)}`);
    if (def.params.length === 0) {
      db.exec(`INSERT INTO ${q(name)} ${def.sql}`);
    } else {
      const columns = db.prepare(def.sql).columns().map((c) => c.name);
      fillRows(db, name, columns, def.sql, def.params);
    }
  });
  refill();

  const rows = this.prepare(`SELECT count(*) AS n FROM ${q(name)}`).pluck().get();
  return { name, rows };
}

// db.dropMaterialized(name, { ifExists }): drop the table and forget the
// refresh definition.
function dropMaterialized(name, options = {}) {
  requireOpen(this[kDb]);
  assertName(name);
  this.exec(`DROP TABLE ${options.ifExists ? 'IF EXISTS ' : ''}${q(name)}`);
  registryFor(this).delete(name);
  return this;
}

module.exports = { createView, dropView, materialize, refreshView, dropMaterialized };
