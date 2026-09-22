'use strict';

// Common Table Expression (WITH) query builder, mixed onto Database.prototype
// by lib/database.js. Composes one or more CTEs and a main query into a single
// `WITH [RECURSIVE] name AS (body)[, ...] <mainQuery>` statement, executed
// through the database's public prepare()/all()/get()/iterate(), so the usual
// guards and row modes apply.
//
//   db.with('active', db.from('users').where({ active: true }))
//     .from('active').where('age > ?', 18).all();
//
//   db.withRecursive('cnt', 'SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5', ['x'])
//     .query('SELECT x FROM cnt').all();
//
// Safety mirrors lib/query.js: values ride through as bound parameters (CTE
// body params first, in declaration order, then the main query's), and every
// identifier — the CTE name and its optional column list — is escaped with
// schema.quote(). A CTE body / main query given as a raw string or { sql,
// params } is the caller's own SQL and is trusted as written.
//
// An extension beyond the better-sqlite3 surface.

const { quote } = require('./schema');

function assertName(name) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected the CTE name to be a non-empty string');
  return name;
}

function assertTable(table) {
  if (typeof table !== 'string' || !table) throw new TypeError('Expected a non-empty table name');
  return table;
}

// Normalises a CTE body or main query to { sql, params }. Accepts a raw SQL
// string, a { sql, params } object, or any builder with a toSQL() method
// (e.g. db.from(...) from lib/query.js).
function collect(source) {
  if (source == null) throw new TypeError('Expected a SQL string, a { sql, params } object, or a query builder');
  if (typeof source === 'string') return { sql: source, params: [] };
  if (typeof source.toSQL === 'function') {
    const r = source.toSQL();
    return { sql: r.sql, params: Array.isArray(r.params) ? r.params.slice() : [] };
  }
  if (typeof source.sql === 'string') {
    return { sql: source.sql, params: Array.isArray(source.params) ? source.params.slice() : [] };
  }
  throw new TypeError('A CTE source must be a SQL string, a { sql, params } object, or a builder with toSQL()');
}

class CteBuilder {
  constructor(db) {
    this.db = db;
    this._ctes = [];
    this._recursive = false;
  }

  // .with(name, source, columns?) — add a CTE. `columns` is an optional array
  // naming the CTE's output columns (required for a recursive CTE that refers
  // to its own columns by name).
  with(name, source, columns) {
    this._ctes.push(this._def(name, source, columns));
    return this;
  }

  withRecursive(name, source, columns) {
    this._recursive = true;
    this._ctes.push(this._def(name, source, columns));
    return this;
  }

  _def(name, source, columns) {
    assertName(name);
    if (columns !== undefined) {
      if (!Array.isArray(columns) || columns.length === 0) throw new TypeError('Expected the CTE columns to be a non-empty array');
    }
    return Object.assign({ name, columns: columns || null }, collect(source));
  }

  // The `WITH ... ` prefix and the params its bodies contribute, in order.
  _prefix() {
    if (this._ctes.length === 0) throw new TypeError('A CTE query needs at least one with()');
    const params = [];
    const parts = this._ctes.map((c) => {
      params.push(...c.params);
      const cols = c.columns ? ` (${c.columns.map(quote).join(', ')})` : '';
      return `${quote(c.name)}${cols} AS (${c.sql})`;
    });
    return { sql: `${this._recursive ? 'WITH RECURSIVE' : 'WITH'} ${parts.join(', ')} `, params };
  }

  // Main query given directly (raw SQL / { sql, params } / builder).
  query(source) {
    const main = collect(source);
    const pre = this._prefix();
    return new CteResult(this.db, pre.sql + main.sql, [...pre.params, ...main.params]);
  }

  // Main query built fluently over one of the CTEs (or any table), reusing the
  // SELECT builder from lib/query.js.
  from(table) {
    const { SelectBuilder } = require('./query');
    return new CteSelect(this, new SelectBuilder(this.db, assertTable(table)));
  }
}

// A composed CTE + raw main query.
class CteResult {
  constructor(db, sql, params) {
    this.db = db;
    this._sql = sql;
    this._params = params;
    this._pluck = false;
  }
  pluck(toggle = true) { this._pluck = toggle !== false; return this; }
  toSQL() { return { sql: this._sql, params: this._params.slice() }; }
  _stmt() { return this._pluck ? this.db.prepare(this._sql).pluck() : this.db.prepare(this._sql); }
  all() { return this._stmt().all(this._params); }
  get() { return this._stmt().get(this._params); }
  iterate() { return this._stmt().iterate(this._params); }
}

// A composed CTE + a fluent SELECT over it. Delegates the chainable SELECT
// methods to an inner SelectBuilder and prepends the WITH clause at execution.
class CteSelect {
  constructor(cte, inner) {
    this._cte = cte;
    this._inner = inner;
  }

  toSQL() {
    const pre = this._cte._prefix();
    const main = this._inner.toSQL();
    return { sql: pre.sql + main.sql, params: [...pre.params, ...main.params] };
  }

  _prepared() {
    const { sql, params } = this.toSQL();
    const stmt = this._inner._pluck ? this._cte.db.prepare(sql).pluck() : this._cte.db.prepare(sql);
    return { stmt, params };
  }

  all() { const { stmt, params } = this._prepared(); return stmt.all(params); }
  get() { const { stmt, params } = this._prepared(); return stmt.get(params); }
  iterate() { const { stmt, params } = this._prepared(); return stmt.iterate(params); }

  count() {
    // count over the composed query, wrapping it as a subquery so the CTEs and
    // any grouping/limit are respected.
    const { sql, params } = this.toSQL();
    return Number(this._cte.db.prepare(`SELECT count(*) AS c FROM (${sql})`).pluck().get(params));
  }
}

// Delegate every chainable SelectBuilder method to the inner builder.
for (const method of [
  'select', 'distinct', 'where', 'andWhere', 'orWhere', 'join', 'leftJoin',
  'groupBy', 'having', 'orderBy', 'limit', 'offset', 'pluck'
]) {
  CteSelect.prototype[method] = function delegate(...args) {
    this._inner[method](...args);
    return this;
  };
}

function withCte(name, source, columns) { return new CteBuilder(this).with(name, source, columns); }
function withRecursive(name, source, columns) { return new CteBuilder(this).withRecursive(name, source, columns); }

module.exports = { with: withCte, withRecursive };
module.exports.CteBuilder = CteBuilder;
module.exports.CteSelect = CteSelect;
module.exports.CteResult = CteResult;
