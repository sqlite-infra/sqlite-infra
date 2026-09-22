'use strict';

// db.json(table, column): helpers for a JSON (TEXT) column, mixed onto
// Database.prototype by lib/database.js. A thin, typed layer over SQLite's JSON1
// functions (json_extract / json_set / json_patch / json_remove / json_each /
// json_valid), so a caller reads and writes into a document column by path
// without hand-writing those calls.
//
// A path is a JSON path string: '$' for the whole document, '$.a.b',
// '$.items[0]'. Values are always bound as parameters and identifiers are
// quoted, so nothing here is an injection vector. An extension beyond the
// better-sqlite3 surface.

const { quote, tableInfo, tableExists } = require('./schema');

function assertPath(path) {
  if (typeof path !== 'string' || path[0] !== '$') {
    throw new TypeError(`Expected a JSON path string starting with "$", got ${JSON.stringify(path)}`);
  }
  return path;
}

// json_type(...) names the SQL/JSON value at a path; turn the extracted text
// back into a JS value. A SQL NULL type (not the string 'null') means the path
// did not resolve.
function decode(value, type) {
  switch (type) {
    case null:
    case undefined:
      return undefined;               // path did not resolve
    case 'object':
    case 'array':
      return JSON.parse(value);
    case 'true':
      return true;
    case 'false':
      return false;
    case 'null':
      return null;
    default:                          // 'integer' | 'real' | 'text'
      return value;
  }
}

// A JSON value to bind, plus the SQL placeholder to wrap it in: objects and
// arrays go through json(?) so they are stored as JSON, not as a quoted string.
function bindable(value) {
  if (value !== null && typeof value === 'object') return { sql: 'json(?)', param: JSON.stringify(value) };
  return { sql: '?', param: value };
}

function json(table, column, options = {}) {
  if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a table name');
  if (typeof column !== 'string' || !column) throw new TypeError('Expected second argument to be a column name');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected third argument to be an options object');
  if (!tableExists(this, table)) throw new TypeError(`No such table: ${table}`);

  const info = tableInfo(this, table);
  if (!info.some((c) => c.name === column)) throw new TypeError(`No such column in ${table}: ${column}`);

  let primaryKey = options.primaryKey;
  if (primaryKey === undefined) {
    const pk = info.filter((c) => c.pk > 0);
    if (pk.length !== 1) throw new TypeError(`Table ${table} has no single-column primary key; pass options.primaryKey`);
    primaryKey = pk[0].name;
  } else if (typeof primaryKey !== 'string' || !info.some((c) => c.name === primaryKey)) {
    throw new TypeError(`No such primary-key column in ${table}: ${primaryKey}`);
  }

  const T = quote(table);
  const C = quote(column);
  const PK = quote(primaryKey);
  const db = this;

  return {
    table,
    column,
    primaryKey,

    // The value at `path` (whole document when omitted), or undefined for a
    // missing row or unresolved path.
    get(pk, path) {
      if (path === undefined) {
        const row = db.prepare(`SELECT ${C} AS v FROM ${T} WHERE ${PK} = ?`).get(pk);
        if (row === undefined || row.v === null || row.v === undefined) return row === undefined ? undefined : row.v;
        return JSON.parse(row.v);
      }
      assertPath(path);
      const row = db.prepare(
        `SELECT json_extract(${C}, ?) AS v, json_type(${C}, ?) AS t FROM ${T} WHERE ${PK} = ?`
      ).get(path, path, pk);
      if (row === undefined) return undefined;
      return decode(row.v, row.t);
    },

    // Set the value at `path`, creating intermediate structure. Returns RunResult.
    set(pk, path, value) {
      assertPath(path);
      const { sql, param } = bindable(value);
      return db.prepare(
        `UPDATE ${T} SET ${C} = json_set(COALESCE(${C}, '{}'), ?, ${sql}) WHERE ${PK} = ?`
      ).run(path, param, pk);
    },

    // Recursively merge `obj` into the document (json_patch). Returns RunResult.
    merge(pk, obj) {
      if (obj === null || typeof obj !== 'object') throw new TypeError('Expected an object to merge');
      return db.prepare(
        `UPDATE ${T} SET ${C} = json_patch(COALESCE(${C}, '{}'), json(?)) WHERE ${PK} = ?`
      ).run(JSON.stringify(obj), pk);
    },

    // Remove the value at `path`. Returns RunResult.
    remove(pk, path) {
      assertPath(path);
      return db.prepare(`UPDATE ${T} SET ${C} = json_remove(${C}, ?) WHERE ${PK} = ?`).run(path, pk);
    },

    // Rows whose document has `value` at `path`. Objects/arrays compare via json().
    where(path, value) {
      assertPath(path);
      const { sql, param } = bindable(value);
      return db.prepare(`SELECT * FROM ${T} WHERE json_extract(${C}, ?) = ${sql}`).all(path, param);
    },

    // The immediate children at `path` (whole document when omitted), as
    // [{ key, value, type, atom, path, fullkey }] with `value` parsed.
    each(pk, path) {
      // Placeholder order follows the SQL text: the json_each() path (if any)
      // comes before the WHERE key.
      let source = `json_each(t.${C})`;
      const args = [];
      if (path !== undefined) { assertPath(path); source = `json_each(t.${C}, ?)`; args.push(path); }
      args.push(pk);
      const rows = db.prepare(
        `SELECT je.key AS key, je.value AS value, je.type AS type, je.atom AS atom, je.path AS path, je.fullkey AS fullkey
           FROM ${T} t, ${source} je WHERE t.${PK} = ?`
      ).all(...args);
      return rows.map((r) => ({ ...r, value: decode(r.value, r.type) }));
    },

    // Whether the row's column holds valid JSON.
    valid(pk) {
      const row = db.prepare(`SELECT json_valid(${C}) AS v FROM ${T} WHERE ${PK} = ?`).get(pk);
      return row !== undefined && row.v === 1;
    }
  };
}

module.exports = { json };
