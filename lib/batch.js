'use strict';

// Bulk mutation helpers, mixed onto Database.prototype by lib/database.js and
// companions to db.insertMany() (lib/bulk.js):
//
//   db.updateMany(table, rows, options?)  -> { updated }
//   db.upsertMany(table, rows, options?)  -> { inserted }
//   db.deleteMany(table, keys, options?)  -> { deleted }
//
// Each runs in ONE transaction over the whole batch, so a failure partway
// through rolls everything back. All values are bound as parameters and all
// identifiers are quoted. Key/conflict columns default to the table's PRIMARY
// KEY (read from PRAGMA table_info); pass them explicitly for a table without
// one, or to key on a different unique column.
//
// Extensions beyond the better-sqlite3 surface.

const { requireOpen, kDb } = require('../adapters/util');
const schema = require('./schema');

// SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 999; stay clear of it.
const IN_CHUNK = 900;

function asColumns(value, what) {
  if (value === undefined) return null;
  const list = Array.isArray(value) ? value : [value];
  if (list.length === 0 || !list.every((c) => typeof c === 'string' && c)) {
    throw new TypeError(`Expected "${what}" to be a non-empty column name or array of names`);
  }
  return list;
}

// The PRIMARY KEY columns, ordered by their position in the key.
function primaryKey(db, table) {
  return schema.tableInfo(db, table)
    .filter((c) => c.pk > 0)
    .sort((a, b) => a.pk - b.pk)
    .map((c) => c.name);
}

function prelude(db, table, batch, options) {
  requireOpen(db[kDb]);
  if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a non-empty string');
  if (!Array.isArray(batch)) throw new TypeError('Expected second argument to be an array');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected the options to be an object');
  if (!schema.tableExists(db, table)) throw new TypeError(`No such table: ${table}`);
  return new Set(schema.tableInfo(db, table).map((c) => c.name));
}

function assertKnown(columns, known, table) {
  for (const c of columns) if (!known.has(c)) throw new TypeError(`No such column in ${table}: ${c}`);
}

function isPlainRow(row) {
  return row !== null && typeof row === 'object' && !Array.isArray(row);
}

// db.updateMany(table, rows, options?): apply one UPDATE per row, matched on the
// key column(s). Each row carries the key value(s) plus the columns to set.
// `options.keyColumns` (string | string[]) defaults to the primary key;
// `options.columns` limits which non-key columns are written (default: the
// non-key keys of the first row). Returns { updated }.
function updateMany(table, rows, options = {}) {
  const known = prelude(this, table, rows, options);
  if (rows.length === 0) return { updated: 0 };

  const keyColumns = asColumns(options.keyColumns, 'keyColumns') || primaryKey(this, table);
  if (keyColumns.length === 0) throw new TypeError(`Table ${table} has no primary key; pass options.keyColumns`);
  assertKnown(keyColumns, known, table);

  const keySet = new Set(keyColumns);
  let setColumns = asColumns(options.columns, 'columns');
  if (!setColumns) {
    const first = rows[0];
    if (!isPlainRow(first)) throw new TypeError('Each row must be a plain object');
    setColumns = Object.keys(first).filter((c) => !keySet.has(c));
  } else {
    setColumns = setColumns.filter((c) => !keySet.has(c));
  }
  if (setColumns.length === 0) throw new TypeError('No non-key columns to update');
  assertKnown(setColumns, known, table);

  const sql = `UPDATE ${schema.quote(table)} SET ${setColumns.map((c) => `${schema.quote(c)} = @${c}`).join(', ')} ` +
    `WHERE ${keyColumns.map((c) => `${schema.quote(c)} = @${c}`).join(' AND ')}`;
  const stmt = this.prepare(sql);

  let updated = 0;
  const run = this.transaction((batch) => {
    for (const row of batch) {
      if (!isPlainRow(row)) throw new TypeError('Each row must be a plain object');
      for (const k of keyColumns) {
        if (!Object.prototype.hasOwnProperty.call(row, k)) throw new TypeError(`Row is missing key column "${k}"`);
      }
      updated += stmt.run(row).changes;
    }
  });
  run(rows);
  return { updated };
}

// db.upsertMany(table, rows, options?): INSERT ... ON CONFLICT(conflict cols) DO
// UPDATE (or DO NOTHING with `mode: 'ignore'`). `options.conflictColumns`
// defaults to the primary key; `options.update` limits which columns are written
// on conflict (default: every inserted column that is not a conflict column);
// `options.columns` limits the inserted columns (default: the first row's keys).
// Returns { inserted } — SQLite counts both inserted and updated rows as
// changes, so this is the total rows affected.
function upsertMany(table, rows, options = {}) {
  const known = prelude(this, table, rows, options);
  const ignore = options.mode === 'ignore';
  if (options.mode !== undefined && options.mode !== 'ignore') {
    throw new TypeError('Expected the "mode" option to be "ignore" (or omitted)');
  }
  if (rows.length === 0) return { inserted: 0 };

  const conflictColumns = asColumns(options.conflictColumns, 'conflictColumns') || primaryKey(this, table);
  if (conflictColumns.length === 0) throw new TypeError(`Table ${table} has no primary key; pass options.conflictColumns`);
  assertKnown(conflictColumns, known, table);

  let columns = asColumns(options.columns, 'columns');
  if (!columns) {
    const first = rows[0];
    if (!isPlainRow(first)) throw new TypeError('Each row must be a plain object');
    columns = Object.keys(first);
  }
  assertKnown(columns, known, table);

  const conflictSet = new Set(conflictColumns);
  let updateColumns = asColumns(options.update, 'update');
  if (updateColumns) assertKnown(updateColumns, known, table);
  else updateColumns = columns.filter((c) => !conflictSet.has(c));

  const insert = `INSERT INTO ${schema.quote(table)} (${columns.map(schema.quote).join(', ')}) ` +
    `VALUES (${columns.map((c) => `@${c}`).join(', ')})`;
  const onConflict = `ON CONFLICT (${conflictColumns.map(schema.quote).join(', ')})`;
  const action = (ignore || updateColumns.length === 0)
    ? 'DO NOTHING'
    : `DO UPDATE SET ${updateColumns.map((c) => `${schema.quote(c)} = excluded.${schema.quote(c)}`).join(', ')}`;
  const stmt = this.prepare(`${insert} ${onConflict} ${action}`);

  let inserted = 0;
  const run = this.transaction((batch) => {
    for (const row of batch) {
      if (!isPlainRow(row)) throw new TypeError('Each row must be a plain object');
      inserted += stmt.run(row).changes;
    }
  });
  run(rows);
  return { inserted };
}

// db.deleteMany(table, keys, options?): delete many rows in one transaction.
// `keys` is either an array of scalar key values (deleted with a chunked
// `WHERE key IN (...)`, key column = `options.keyColumn` or the single-column
// primary key) or an array of criteria objects (each deleted by its own
// equality/IS NULL match). Returns { deleted }.
function deleteMany(table, keys, options = {}) {
  const known = prelude(this, table, keys, options);
  if (keys.length === 0) return { deleted: 0 };

  const criteriaMode = isPlainRow(keys[0]);
  if (criteriaMode) return deleteByCriteria(this, table, keys, known);

  const keyColumn = options.keyColumn === undefined ? null : options.keyColumn;
  if (keyColumn !== null && (typeof keyColumn !== 'string' || !keyColumn)) {
    throw new TypeError('Expected the "keyColumn" option to be a non-empty string');
  }
  let column = keyColumn;
  if (!column) {
    const pk = primaryKey(this, table);
    if (pk.length !== 1) throw new TypeError(`Table ${table} has no single-column primary key; pass options.keyColumn`);
    column = pk[0];
  }
  assertKnown([column], known, table);

  let deleted = 0;
  const run = this.transaction((values) => {
    for (let i = 0; i < values.length; i += IN_CHUNK) {
      const chunk = values.slice(i, i + IN_CHUNK);
      const sql = `DELETE FROM ${schema.quote(table)} WHERE ${schema.quote(column)} IN (${chunk.map(() => '?').join(', ')})`;
      deleted += this.prepare(sql).run(...chunk).changes;
    }
  });
  run(keys);
  return { deleted };
}

function deleteByCriteria(db, table, criteria, known) {
  let deleted = 0;
  const run = db.transaction((batch) => {
    for (const c of batch) {
      if (!isPlainRow(c)) throw new TypeError('Every criteria entry must be a plain object');
      const cols = Object.keys(c);
      if (cols.length === 0) throw new TypeError('A criteria object must have at least one column');
      assertKnown(cols, known, table);
      const clauses = [];
      const params = [];
      for (const col of cols) {
        if (c[col] === null) {
          clauses.push(`${schema.quote(col)} IS NULL`);
        } else {
          clauses.push(`${schema.quote(col)} = ?`);
          params.push(c[col]);
        }
      }
      const sql = `DELETE FROM ${schema.quote(table)} WHERE ${clauses.join(' AND ')}`;
      deleted += db.prepare(sql).run(...params).changes;
    }
  });
  run(criteria);
  return { deleted };
}

module.exports = { updateMany, upsertMany, deleteMany };
