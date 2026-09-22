'use strict';

// db.schemaDiff(target): the additive DDL needed to bring this database's user
// schema up to `target`, mixed onto Database.prototype by lib/database.js.
//
// `target` is another Database, an array of CREATE statements, or a string of
// `;`-separated CREATEs. The result is a list of safe, additive statements
// (CREATE TABLE for missing tables, ALTER TABLE ADD COLUMN for missing columns,
// CREATE INDEX for missing indexes) plus a summary. It never emits a DROP or a
// destructive ALTER: things this database has that the target lacks are listed
// under `summary.extra` but left alone, and a column whose type or nullability
// differs (which ALTER cannot reconcile) is listed under `summary.incompatible`
// and flips `additive` to false. `{ apply: true }` runs the statements in one
// transaction.
//
// An extension beyond the better-sqlite3 surface. It reads schema only; it
// reuses the same introspection (db.tables/columns/indexes/tableSchema) and the
// same "user object" definition (excludes sqlite_* and _better_sqlite_*).

const { requireOpen, kDb } = require('./util');
const { quote } = require('./schema');

// A comparable snapshot of one database's user schema.
function catalog(db) {
  const tables = db.tables();
  const columns = new Map();
  for (const t of tables) columns.set(t, new Map(db.columns(t).map((c) => [c.name, c])));
  const indexes = new Map();
  // Only explicit CREATE INDEX indexes (origin 'c'); implicit ones from UNIQUE
  // and PRIMARY KEY come with their table's DDL.
  for (const i of db.indexes()) {
    if (i.origin === 'c' && !i.name.startsWith('sqlite_')) indexes.set(i.name, i);
  }
  return { tables: new Set(tables), columns, indexes };
}

// The ADD COLUMN clause for a target column, or null when ALTER cannot add it
// (a NOT NULL column needs a default; a primary-key column cannot be added).
function addColumnClause(col) {
  if (col.primaryKey > 0) return null;
  if (col.notnull && (col.default === null || col.default === undefined)) return null;
  let ddl = quote(col.name);
  if (col.type) ddl += ` ${col.type}`;
  if (col.notnull) ddl += ' NOT NULL';
  if (col.default !== null && col.default !== undefined) ddl += ` DEFAULT ${col.default}`;
  return ddl;
}

function schemaDiff(target, options = {}) {
  requireOpen(this[kDb]);
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');

  // Resolve the target into a Database to introspect.
  let targetDb = null;
  let throwaway = false;
  if (typeof target === 'string' || Array.isArray(target)) {
    const Database = this.constructor;
    targetDb = new Database(':memory:');
    throwaway = true;
    if (typeof target === 'string') {
      if (target.trim()) targetDb.exec(target);
    } else {
      for (const stmt of target) {
        if (typeof stmt !== 'string') throw new TypeError('Each target statement must be a string');
        if (stmt.trim()) targetDb.exec(stmt);
      }
    }
  } else if (target && typeof target.tables === 'function' && typeof target.columns === 'function') {
    targetDb = target;
  } else {
    throw new TypeError('Expected target to be a Database, an array of CREATE statements, or a SQL string');
  }

  try {
    const here = catalog(this);
    const there = catalog(targetDb);

    const statements = [];
    const summary = {
      tablesToAdd: [], columnsToAdd: [], indexesToAdd: [],
      extra: { tables: [], columns: [], indexes: [] },
      incompatible: []
    };

    // Tables in target but not here → CREATE TABLE verbatim.
    for (const table of there.tables) {
      if (here.tables.has(table)) continue;
      const sql = targetDb.tableSchema(table);
      if (sql) { statements.push(`${sql};`); summary.tablesToAdd.push(table); }
    }
    // Tables here but not in target → extra (never dropped).
    for (const table of here.tables) if (!there.tables.has(table)) summary.extra.tables.push(table);

    // Columns, for tables present in both.
    for (const [table, targetCols] of there.columns) {
      if (!here.columns.has(table)) continue;   // handled as a whole-table add
      const hereCols = here.columns.get(table);
      for (const [name, col] of targetCols) {
        const mine = hereCols.get(name);
        if (!mine) {
          const clause = addColumnClause(col);
          if (clause) {
            statements.push(`ALTER TABLE ${quote(table)} ADD COLUMN ${clause};`);
            summary.columnsToAdd.push({ table, column: name });
          } else {
            summary.incompatible.push({ table, column: name, reason: 'cannot be added by ALTER (primary key, or NOT NULL without a default)' });
          }
          continue;
        }
        // Present in both: a differing type or nullability is not additive.
        if ((mine.type || '') !== (col.type || '') || Boolean(mine.notnull) !== Boolean(col.notnull)) {
          summary.incompatible.push({
            table, column: name,
            from: { type: mine.type, notnull: Boolean(mine.notnull) },
            to: { type: col.type, notnull: Boolean(col.notnull) }
          });
        }
      }
      for (const name of hereCols.keys()) if (!targetCols.has(name)) summary.extra.columns.push({ table, column: name });
    }

    // Indexes in target but not here → CREATE INDEX.
    for (const [name, idx] of there.indexes) {
      if (here.indexes.has(name)) continue;
      const sql = targetDb.tableSchema(name);
      const stmt = sql
        ? `${sql};`
        : `CREATE ${idx.unique ? 'UNIQUE ' : ''}INDEX ${quote(name)} ON ${quote(idx.table)} (${idx.columns.map((c) => (c == null ? c : quote(c))).join(', ')});`;
      statements.push(stmt);
      summary.indexesToAdd.push(name);
    }
    for (const name of here.indexes.keys()) if (!there.indexes.has(name)) summary.extra.indexes.push(name);

    const additive = summary.incompatible.length === 0;

    if (options.apply && statements.length) {
      const run = this.transaction(() => { for (const s of statements) this.exec(s); });
      run();
    }

    return { statements, summary, additive };
  } finally {
    if (throwaway) targetDb.close();
  }
}

module.exports = { schemaDiff };
