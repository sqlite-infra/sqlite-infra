'use strict';

// Public, read-only schema introspection, mixed onto Database.prototype by
// lib/database.js. A typed layer over sqlite_master and SQLite's PRAGMA table
// functions, so a caller does not have to hand-write the catalog queries.
//
// "User" objects are everything that is neither SQLite's own (`sqlite_%`) nor
// this package's bookkeeping (`_better_sqlite_%`): the same definition the sync
// uses, so these methods and the sync agree on what a host's schema is. The
// exclusion lives once in lib/schema.js and is reused here.
//
// These are extensions beyond the better-sqlite3 surface. They read the schema
// only; nothing here writes. Each throws the usual TypeError on a closed
// connection (through the prepare() it issues) and never spends the caller's
// connection on an open iterator.

// Every method issues a prepare(), whose own guard throws the usual
// "database connection is not open" TypeError on a closed connection.
const schema = require('./schema');
const { tables: TABLES } = require('./dictionary');

// db.tables(): user table names, sorted.
function tables() {
  return schema.userTables(this);
}

// db.views(): user view names, sorted.
function views() {
  return schema.userViews(this);
}

// db.tableExists(name): whether a table or view with this name exists.
function tableExists(name) {
  if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  return schema.tableExists(this, name);
}

// db.columns(name): the columns of a table or view, as
// [{ name, type, notnull, default, primaryKey }]. `primaryKey` is the 1-based
// position in a (possibly composite) key, or 0. Throws for an unknown name.
function columns(name) {
  if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  if (!schema.tableExists(this, name)) throw new TypeError(`No such table or view: ${name}`);
  return schema.tableInfo(this, name).map((c) => ({
    name: c.name,
    type: c.type || null,
    notnull: Boolean(c.notnull),
    default: c.dflt_value === undefined ? null : c.dflt_value,
    primaryKey: c.pk
  }));
}

// db.indexes(name?): indexes on one table, or every user index when no name is
// given, as [{ name, table, unique, origin, partial, columns }]. `origin` is
// 'c' (CREATE INDEX), 'u' (UNIQUE constraint) or 'pk' (PRIMARY KEY). `columns`
// lists the indexed column names in order (auto-generated expression columns
// come back as null from SQLite and are kept as null).
function indexes(name) {
  if (name !== undefined && typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  const targets = name === undefined ? schema.userTables(this) : [name];
  if (name !== undefined && !schema.tableExists(this, name)) throw new TypeError(`No such table or view: ${name}`);
  const out = [];
  for (const table of targets) {
    const list = this.prepare(`PRAGMA index_list(${schema.quote(table)})`).all();
    for (const idx of list) {
      const cols = this.prepare(`PRAGMA index_info(${schema.quote(idx.name)})`).all()
        .sort((a, b) => a.seqno - b.seqno)
        .map((c) => c.name);
      out.push({
        name: idx.name,
        table,
        unique: Boolean(idx.unique),
        origin: idx.origin,
        partial: Boolean(idx.partial),
        columns: cols
      });
    }
  }
  return out;
}

// db.foreignKeys(name): the foreign keys declared on a table, as
// [{ id, table, from, to, onUpdate, onDelete, match }]. Throws for an unknown
// table.
function foreignKeys(name) {
  if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  if (!schema.tableExists(this, name)) throw new TypeError(`No such table or view: ${name}`);
  return this.prepare(`PRAGMA foreign_key_list(${schema.quote(name)})`).all().map((fk) => ({
    id: fk.id,
    table: fk.table,
    from: fk.from,
    to: fk.to,
    onUpdate: fk.on_update,
    onDelete: fk.on_delete,
    match: fk.match
  }));
}

// db.databases(): the attached databases (PRAGMA database_list), as
// [{ seq, name, file }]. `main` is always present; `file` is '' for in-memory
// and temporary databases.
function databases() {
  return this.prepare('PRAGMA database_list').all().map((r) => ({ seq: r.seq, name: r.name, file: r.file || '' }));
}

// db.tableSchema(name): the CREATE statement SQLite recorded for a table, view,
// index or trigger, or null when the object has none (e.g. an internal one).
// Throws for a name that does not exist at all.
function tableSchema(name) {
  if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  const row = this.prepare('SELECT sql FROM sqlite_master WHERE name = ?').get(name);
  if (row === undefined) throw new TypeError(`No such schema object: ${name}`);
  return row.sql === undefined ? null : row.sql;
}

// db.dumpSchema(): every user CREATE statement (tables, indexes, views,
// triggers), in dependency-friendly order, as one SQL string terminated with
// ';'. Excludes SQLite's own objects and this package's internal tables — the
// same "user schema" the sync and introspection use. Handy for a schema
// snapshot or a diff; it is DDL only, no rows.
function dumpSchema() {
  const rows = this.prepare(`
    SELECT sql FROM sqlite_master
     WHERE sql IS NOT NULL
       AND name NOT LIKE 'sqlite_%'
       AND name NOT LIKE ?
     ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'view' THEN 2 ELSE 3 END, name`)
    .pluck().all(`${require('./dictionary').tables.prefix}%`);
  return rows.map((sql) => `${sql};`).join('\n');
}

// db.count(table, options?): number of rows in a table or view. `options.where`
// is SQL after WHERE, written by the caller (never from untrusted input);
// `options.params` are bound to it.
function count(table, options = {}) {
  const { sql, params } = whereClause(this, table, options);
  return Number(this.prepare(`SELECT count(*) AS n FROM ${require('./schema').quote(table)}${sql}`).pluck().get(...params));
}

// db.exists(table, options?): whether any row matches. Same options as count().
function exists(table, options = {}) {
  const { sql, params } = whereClause(this, table, options);
  const q = require('./schema').quote(table);
  return this.prepare(`SELECT EXISTS(SELECT 1 FROM ${q}${sql} LIMIT 1) AS e`).pluck().get(...params) === 1;
}

function whereClause(db, table, options) {
  const schema = require('./schema');
  if (typeof table !== 'string') throw new TypeError('Expected first argument to be a string');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  if (!schema.tableExists(db, table)) throw new TypeError(`No such table or view: ${table}`);
  const where = options.where === undefined ? '' : options.where;
  if (typeof where !== 'string') throw new TypeError('Expected the "where" option to be a string');
  const params = options.params === undefined ? [] : options.params;
  if (!Array.isArray(params)) throw new TypeError('Expected the "params" option to be an array');
  return { sql: where ? ` WHERE ${where}` : '', params };
}

module.exports = {
  tables, views, tableExists, columns, indexes, foreignKeys,
  databases, tableSchema, dumpSchema, count, exists
};
