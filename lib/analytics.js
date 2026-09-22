'use strict';

// db.analytics(table): a small aggregation/analytics query builder, mixed onto
// Database.prototype by lib/database.js. It composes GROUP BY aggregations,
// time bucketing, window-function columns and data-driven pivots without the
// caller hand-writing the SQL, while keeping the library's safety rules: every
// value is a bound parameter and every identifier is quoted with schema.quote().
//
// Lazy: nothing runs until a terminal (.all()/.get()/.toSQL()). A pivot is the
// one exception that must read the database at build time, to discover the
// distinct values that become output columns.
//
// An extension beyond the better-sqlite3 surface; read-only.

const schema = require('./schema');
const { quote } = schema;

const AGG_FNS = new Set(['count', 'sum', 'avg', 'min', 'max', 'total']);
const INTERVALS = {
  hour: '%Y-%m-%d %H:00:00',
  day: '%Y-%m-%d',
  week: '%Y-%W',
  month: '%Y-%m',
  year: '%Y'
};
const MAX_PIVOT_COLUMNS = 100;

function assertAggFn(fn) {
  if (!AGG_FNS.has(fn)) throw new TypeError(`Unsupported aggregate function "${fn}"; expected one of: ${[...AGG_FNS].join(', ')}`);
  return fn;
}

// Renders one { col: value } object into a WHERE fragment plus its params:
// '=' for a scalar, IN (...) for an array, IS NULL for null/undefined.
function objectCondition(criteria) {
  const parts = [];
  const params = [];
  for (const [col, value] of Object.entries(criteria)) {
    if (value === null || value === undefined) {
      parts.push(`${quote(col)} IS NULL`);
    } else if (Array.isArray(value)) {
      if (value.length === 0) { parts.push('0'); continue; }   // IN () matches nothing
      parts.push(`${quote(col)} IN (${value.map(() => '?').join(', ')})`);
      params.push(...value);
    } else {
      parts.push(`${quote(col)} = ?`);
      params.push(value);
    }
  }
  return { sql: parts.length ? parts.join(' AND ') : '1', params };
}

class AnalyticsBuilder {
  constructor(db, table) {
    if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a non-empty string');
    if (!schema.tableExists(db, table)) throw new TypeError(`No such table or view: ${table}`);
    this._db = db;
    this._table = table;
    this._selects = [];       // { sql, params } fragments for the SELECT list
    this._groupBy = [];       // quoted expressions
    this._wheres = [];        // { connector, sql, params }
    this._havings = [];       // { connector, sql, params }
    this._orderBy = [];       // quoted "col" dir
    this._limit = null;
    this._pivot = null;       // { rowColumn, columnColumn, valueAgg }
  }

  // ---- filtering ----

  where(condition, ...params) { return this._where(this._wheres, 'AND', condition, params); }
  andWhere(condition, ...params) { return this._where(this._wheres, 'AND', condition, params); }
  orWhere(condition, ...params) { return this._where(this._wheres, 'OR', condition, params); }
  having(condition, ...params) { return this._where(this._havings, 'AND', condition, params); }

  _where(target, connector, condition, params) {
    if (typeof condition === 'string') {
      target.push({ connector, sql: condition, params });
    } else if (condition !== null && typeof condition === 'object') {
      const { sql, params: p } = objectCondition(condition);
      target.push({ connector, sql, params: p });
    } else {
      throw new TypeError('Expected a SQL string or a { column: value } object');
    }
    return this;
  }

  // ---- grouping and aggregation ----

  groupBy(...columns) {
    for (const c of columns) {
      if (typeof c !== 'string' || !c) throw new TypeError('groupBy() expects column name strings');
      this._groupBy.push(quote(c));
      this._selects.push({ sql: `${quote(c)}`, params: [] });
    }
    return this;
  }

  // spec: { alias: 'count' } or { alias: { fn, column, distinct } }.
  agg(spec) {
    if (spec === null || typeof spec !== 'object') throw new TypeError('agg() expects an object of { alias: fnSpec }');
    for (const [alias, def] of Object.entries(spec)) {
      let fn;
      let column;
      let distinct = false;
      if (typeof def === 'string') {
        fn = assertAggFn(def);
      } else if (def && typeof def === 'object') {
        fn = assertAggFn(def.fn);
        column = def.column;
        distinct = Boolean(def.distinct);
      } else {
        throw new TypeError(`agg alias "${alias}" must be a function name or { fn, column }`);
      }
      let inner;
      if (column === undefined || column === null) {
        if (fn !== 'count') throw new TypeError(`agg "${alias}": ${fn} requires a "column"`);
        inner = '*';
      } else {
        inner = `${distinct ? 'DISTINCT ' : ''}${quote(column)}`;
      }
      this._selects.push({ sql: `${fn}(${inner}) AS ${quote(alias)}`, params: [] });
    }
    return this;
  }

  // Bucket rows by a time column. unit: 'ms' (default) | 's' | 'iso'.
  timeBucket(column, interval, options = {}) {
    if (typeof column !== 'string' || !column) throw new TypeError('timeBucket() needs a column name');
    const format = INTERVALS[interval];
    if (!format) throw new TypeError(`Unsupported interval "${interval}"; expected one of: ${Object.keys(INTERVALS).join(', ')}`);
    const unit = options.unit === undefined ? 'ms' : options.unit;
    const as = options.as || 'bucket';
    let dt;
    if (unit === 'ms') dt = `datetime(${quote(column)} / 1000, 'unixepoch')`;
    else if (unit === 's') dt = `datetime(${quote(column)}, 'unixepoch')`;
    else if (unit === 'iso') dt = `datetime(${quote(column)})`;
    else throw new TypeError(`Unsupported unit "${unit}"; expected 'ms', 's' or 'iso'`);
    const expr = `strftime('${format}', ${dt})`;
    this._selects.push({ sql: `${expr} AS ${quote(as)}`, params: [] });
    this._groupBy.push(expr);
    return this;
  }

  // A window-function (or any) expression added to the SELECT list.
  window(expr, options = {}) {
    if (typeof expr !== 'string' || !expr) throw new TypeError('window() needs an expression string');
    if (!options.as) throw new TypeError('window() needs an { as } alias');
    this._selects.push({ sql: `${expr} AS ${quote(options.as)}`, params: [] });
    return this;
  }

  // ---- pivot ----

  pivot(rowColumn, columnColumn, valueAgg) {
    if (typeof rowColumn !== 'string' || !rowColumn) throw new TypeError('pivot() needs a rowColumn');
    if (typeof columnColumn !== 'string' || !columnColumn) throw new TypeError('pivot() needs a columnColumn');
    if (!valueAgg || typeof valueAgg !== 'object') throw new TypeError('pivot() needs a valueAgg { fn, column }');
    assertAggFn(valueAgg.fn);
    if (typeof valueAgg.column !== 'string' || !valueAgg.column) throw new TypeError('pivot() valueAgg needs a "column"');
    this._pivot = { rowColumn, columnColumn, valueAgg };
    return this;
  }

  // ---- ordering ----

  orderBy(column, direction = 'asc') {
    if (typeof column !== 'string' || !column) throw new TypeError('orderBy() needs a column');
    const dir = String(direction).toLowerCase();
    if (dir !== 'asc' && dir !== 'desc') throw new TypeError("orderBy() direction must be 'asc' or 'desc'");
    this._orderBy.push(`${quote(column)} ${dir.toUpperCase()}`);
    return this;
  }

  limit(n) {
    if (!Number.isInteger(n) || n < 0) throw new TypeError('limit() expects a non-negative integer');
    this._limit = n;
    return this;
  }

  // ---- build and run ----

  _whereClause(list, keyword) {
    if (list.length === 0) return { sql: '', params: [] };
    let sql = '';
    const params = [];
    list.forEach((c, i) => {
      sql += i === 0 ? '' : ` ${c.connector} `;
      sql += `(${c.sql})`;
      params.push(...c.params);
    });
    return { sql: ` ${keyword} ${sql}`, params };
  }

  toSQL() {
    return this._pivot ? this._buildPivot() : this._buildGrouped();
  }

  _buildGrouped() {
    const params = [];
    const cols = this._selects.length ? this._selects : [{ sql: '*', params: [] }];
    const selectSql = cols.map((c) => c.sql).join(', ');
    for (const c of cols) params.push(...c.params);

    let sql = `SELECT ${selectSql} FROM ${quote(this._table)}`;
    const where = this._whereClause(this._wheres, 'WHERE');
    sql += where.sql; params.push(...where.params);
    if (this._groupBy.length) sql += ` GROUP BY ${this._groupBy.join(', ')}`;
    const having = this._whereClause(this._havings, 'HAVING');
    sql += having.sql; params.push(...having.params);
    if (this._orderBy.length) sql += ` ORDER BY ${this._orderBy.join(', ')}`;
    if (this._limit !== null) sql += ` LIMIT ${this._limit}`;
    return { sql, params };
  }

  _buildPivot() {
    const { rowColumn, columnColumn, valueAgg } = this._pivot;
    // Discover the distinct values that become columns, honouring the filters.
    const where = this._whereClause(this._wheres, 'WHERE');
    const distinctSql = `SELECT DISTINCT ${quote(columnColumn)} AS v FROM ${quote(this._table)}${where.sql} ORDER BY v`;
    const values = this._db.prepare(distinctSql).pluck().all(...where.params);
    if (values.length > MAX_PIVOT_COLUMNS) {
      throw new RangeError(`pivot() would create ${values.length} columns (max ${MAX_PIVOT_COLUMNS}); filter columnColumn first`);
    }

    const params = [];
    const selectParts = [`${quote(rowColumn)}`];
    for (const v of values) {
      const alias = v === null ? 'null' : String(v);
      if (v === null) {
        selectParts.push(`${valueAgg.fn}(CASE WHEN ${quote(columnColumn)} IS NULL THEN ${quote(valueAgg.column)} END) AS ${quote(alias)}`);
      } else {
        selectParts.push(`${valueAgg.fn}(CASE WHEN ${quote(columnColumn)} = ? THEN ${quote(valueAgg.column)} END) AS ${quote(alias)}`);
        params.push(v);
      }
    }

    let sql = `SELECT ${selectParts.join(', ')} FROM ${quote(this._table)}`;
    sql += where.sql; params.push(...where.params);
    sql += ` GROUP BY ${quote(rowColumn)}`;
    const having = this._whereClause(this._havings, 'HAVING');
    sql += having.sql; params.push(...having.params);
    if (this._orderBy.length) sql += ` ORDER BY ${this._orderBy.join(', ')}`;
    else sql += ` ORDER BY ${quote(rowColumn)}`;
    if (this._limit !== null) sql += ` LIMIT ${this._limit}`;
    return { sql, params };
  }

  all() {
    const { sql, params } = this.toSQL();
    return this._db.prepare(sql).all(...params);
  }

  get() {
    const { sql, params } = this.toSQL();
    return this._db.prepare(sql).get(...params);
  }
}

function analytics(table) {
  return new AnalyticsBuilder(this, table);
}

module.exports = { analytics, AnalyticsBuilder };
