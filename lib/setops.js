'use strict';

// SQL set-operation composition, mixed onto Database.prototype by
// lib/database.js: db.union(...), db.unionAll(...), db.intersect(...),
// db.except(...). Each combines two or more SELECT sources into one query.
//
// A source is anything with a `.toSQL()` returning `{ sql, params }` (e.g. a
// db.from(...) query builder), a plain `{ sql, params }` object, or a raw SQL
// string. Each source's SQL is parenthesized and the sources are joined with
// the operator; their params are concatenated in source order. The combined
// query takes ORDER BY / LIMIT / OFFSET, and more set operations chain onto it
// left-to-right.
//
// An extension beyond the better-sqlite3 surface. Values are always bound; the
// only identifier this module emits is the ORDER BY column, which is quoted.

const { quote } = require('./schema');

const DIRECTIONS = new Set(['asc', 'desc']);

// Normalizes one source to { sql, params }.
function toPart(source) {
  if (source == null) throw new TypeError('A set-operation source cannot be null or undefined');
  if (typeof source === 'string') return { sql: source, params: [] };
  if (typeof source.toSQL === 'function') {
    const built = source.toSQL();
    return { sql: built.sql, params: built.params || [] };
  }
  if (typeof source.sql === 'string') return { sql: source.sql, params: Array.isArray(source.params) ? source.params : [] };
  throw new TypeError('Expected a set-operation source to be a query builder, a { sql, params } object, or a SQL string');
}

// Accepts (...sources) or (sourcesArray); returns a non-empty array of sources.
function collect(args) {
  const sources = args.length === 1 && Array.isArray(args[0]) ? args[0] : args;
  if (sources.length === 0) throw new TypeError('A set operation needs at least one source');
  return sources;
}

class SetQuery {
  constructor(db, operator, sources) {
    this._db = db;
    // parts[0] has no leading operator; the rest carry the operator before them.
    this._parts = collect(sources).map((s, i) => Object.assign(toPart(s), i === 0 ? {} : { op: operator }));
    this._orderBy = null;
    this._limit = null;
    this._offset = null;
  }

  _add(operator, args) {
    for (const s of collect(args)) this._parts.push(Object.assign(toPart(s), { op: operator }));
    return this;
  }

  union(...sources) { return this._add('UNION', sources); }
  unionAll(...sources) { return this._add('UNION ALL', sources); }
  intersect(...sources) { return this._add('INTERSECT', sources); }
  except(...sources) { return this._add('EXCEPT', sources); }

  orderBy(column, direction = 'asc') {
    if (typeof column !== 'string' || !column) throw new TypeError('Expected the order-by column to be a non-empty string');
    if (typeof direction !== 'string' || !DIRECTIONS.has(direction.toLowerCase())) {
      throw new TypeError("Expected the order-by direction to be 'asc' or 'desc'");
    }
    this._orderBy = { column, direction: direction.toLowerCase() };
    return this;
  }

  limit(n) {
    if (!Number.isInteger(n) || n < 0) throw new TypeError('Expected the limit to be a non-negative integer');
    this._limit = n;
    return this;
  }

  offset(n) {
    if (!Number.isInteger(n) || n < 0) throw new TypeError('Expected the offset to be a non-negative integer');
    this._offset = n;
    return this;
  }

  toSQL() {
    let sql = '';
    const params = [];
    // SQLite forbids parenthesizing a compound operand — `(SELECT ...) UNION ...`
    // is a syntax error — so each source is wrapped as a subquery in FROM, which
    // both makes it a valid simple SELECT and preserves any ORDER BY/LIMIT it
    // carries internally.
    this._parts.forEach((part, i) => {
      if (i > 0) sql += ` ${part.op} `;
      sql += `SELECT * FROM (${part.sql})`;
      params.push(...part.params);
    });
    if (this._orderBy) sql += ` ORDER BY ${quote(this._orderBy.column)} ${this._orderBy.direction.toUpperCase()}`;
    if (this._limit != null) sql += ` LIMIT ${this._limit}`;
    if (this._offset != null) sql += ` OFFSET ${this._offset}`;
    return { sql, params };
  }

  all() { const { sql, params } = this.toSQL(); return this._db.prepare(sql).all(...params); }
  get() { const { sql, params } = this.toSQL(); return this._db.prepare(sql).get(...params); }
  iterate() { const { sql, params } = this.toSQL(); return this._db.prepare(sql).iterate(...params); }
}

function union(...sources) { return new SetQuery(this, 'UNION', sources); }
function unionAll(...sources) { return new SetQuery(this, 'UNION ALL', sources); }
function intersect(...sources) { return new SetQuery(this, 'INTERSECT', sources); }
function except(...sources) { return new SetQuery(this, 'EXCEPT', sources); }

module.exports = { union, unionAll, intersect, except, SetQuery };
