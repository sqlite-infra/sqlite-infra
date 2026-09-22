'use strict';

// Fluent SQL query builder, mixed onto Database.prototype by lib/database.js
// (the same way lib/introspect.js is). It composes SELECT / INSERT / UPDATE /
// DELETE from small chainable builders and executes through the database's
// public prepare()/run()/all()/get()/iterate(), so every existing guard,
// row-mode and the verbose hook still apply.
//
//   db.from('users').where({ org_id: 1 }).orderBy('name').all();
//   db.insertInto('users').values({ name: 'a', org_id: 1 }).returning('id').run();
//   db.updateTable('users').set({ name: 'b' }).where({ id: 1 }).run();
//   db.deleteFrom('users').where('id > ?', 100).run();
//
// Safety, without exception:
//   - every VALUE is bound as a parameter, never interpolated;
//   - every IDENTIFIER (table/column) is escaped with schema.quote();
//   - a raw SQL fragment (a where() string, a join ON, set() text, a column
//     { expr }) is the caller's own SQL and is trusted as written — pass
//     values through its `?` placeholders, never by string concatenation.
//
// This is an extension beyond the better-sqlite3 surface.

const { quote } = require('./schema');

// A dotted name is treated as qualified: 't.col' -> "t"."col". '*' passes
// through, so 'a.*' -> "a".*.
function quoteIdent(name) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected an identifier string');
  if (name === '*') return '*';
  return name.split('.').map((p) => (p === '*' ? '*' : quote(p))).join('.');
}

function isPlainObject(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function assertTable(table) {
  if (typeof table !== 'string' || !table) throw new TypeError('Expected a non-empty table name');
  return table;
}

function assertInt(n, what) {
  if (!Number.isInteger(n) || n < 0) throw new TypeError(`Expected ${what} to be a non-negative integer`);
  return n;
}

// Compiles one condition to { sql, params }. A string is a raw fragment with
// its own '?' placeholders; a plain object ANDs `col = ?` (or `IN (...)` for an
// array, `IS NULL` for null).
function compileCondition(cond, params) {
  if (typeof cond === 'string') return { sql: cond, params: params.slice() };
  if (isPlainObject(cond)) {
    const parts = [];
    const out = [];
    for (const [col, val] of Object.entries(cond)) {
      const q = quoteIdent(col);
      if (val === null) {
        parts.push(`${q} IS NULL`);
      } else if (Array.isArray(val)) {
        if (val.length === 0) { parts.push('0'); continue; }   // empty IN () is never true
        parts.push(`${q} IN (${val.map(() => '?').join(', ')})`);
        out.push(...val);
      } else {
        parts.push(`${q} = ?`);
        out.push(val);
      }
    }
    return { sql: parts.length ? parts.join(' AND ') : '1', params: out };
  }
  throw new TypeError('A condition must be a SQL string or a plain object');
}

function addWhere(state, connector, cond, params) {
  state.push(Object.assign({ connector }, compileCondition(cond, params)));
}

function renderWhere(state) {
  if (state.length === 0) return { sql: '', params: [] };
  const params = [];
  let sql = '';
  state.forEach((c, i) => {
    sql += `${i === 0 ? '' : ` ${c.connector} `}(${c.sql})`;
    params.push(...c.params);
  });
  return { sql: ` WHERE ${sql}`, params };
}

function renderReturning(cols) {
  return ` RETURNING ${cols.map((c) => (c === '*' ? '*' : quoteIdent(c))).join(', ')}`;
}

// ---- SELECT ---------------------------------------------------------------

class SelectBuilder {
  constructor(db, table) {
    this.db = db;
    this.table = table;
    this._columns = null;
    this._distinct = false;
    this._joins = [];
    this._where = [];
    this._groupBy = [];
    this._having = null;
    this._order = [];
    this._limit = null;
    this._offset = null;
    this._pluck = false;
  }

  select(...cols) { this._columns = cols.length ? cols : null; return this; }
  distinct() { this._distinct = true; return this; }
  where(cond, ...params) { addWhere(this._where, 'AND', cond, params); return this; }
  andWhere(cond, ...params) { addWhere(this._where, 'AND', cond, params); return this; }
  orWhere(cond, ...params) { addWhere(this._where, 'OR', cond, params); return this; }
  join(table, on, ...params) { this._joins.push({ kind: 'JOIN', table, on, params }); return this; }
  leftJoin(table, on, ...params) { this._joins.push({ kind: 'LEFT JOIN', table, on, params }); return this; }
  groupBy(...cols) { this._groupBy.push(...cols); return this; }
  having(cond, ...params) { this._having = { cond, params }; return this; }
  orderBy(col, dir = 'asc') {
    const d = String(dir).toLowerCase();
    if (d !== 'asc' && d !== 'desc') throw new TypeError(`orderBy direction must be 'asc' or 'desc', got ${dir}`);
    this._order.push(`${quoteIdent(col)} ${d.toUpperCase()}`);
    return this;
  }
  limit(n) { this._limit = assertInt(n, 'limit'); return this; }
  offset(n) { this._offset = assertInt(n, 'offset'); return this; }
  pluck(toggle = true) { this._pluck = toggle !== false; return this; }

  _columnsSql() {
    if (!this._columns) return '*';
    return this._columns.map((c) => {
      if (typeof c === 'string') return quoteIdent(c);
      if (c && typeof c === 'object' && typeof c.expr === 'string') {
        return c.as ? `${c.expr} AS ${quote(c.as)}` : c.expr;
      }
      throw new TypeError('A column must be a string or { expr, as }');
    }).join(', ');
  }

  _fromAndJoins(params) {
    let sql = ` FROM ${quoteIdent(this.table)}`;
    for (const j of this._joins) {
      sql += ` ${j.kind} ${quoteIdent(j.table)} ON ${j.on}`;
      params.push(...j.params);
    }
    return sql;
  }

  toSQL() {
    const params = [];
    let sql = `SELECT ${this._distinct ? 'DISTINCT ' : ''}${this._columnsSql()}`;
    sql += this._fromAndJoins(params);
    const w = renderWhere(this._where); sql += w.sql; params.push(...w.params);
    if (this._groupBy.length) sql += ` GROUP BY ${this._groupBy.map(quoteIdent).join(', ')}`;
    if (this._having) {
      const h = compileCondition(this._having.cond, this._having.params);
      sql += ` HAVING ${h.sql}`; params.push(...h.params);
    }
    if (this._order.length) sql += ` ORDER BY ${this._order.join(', ')}`;
    if (this._limit !== null) sql += ` LIMIT ${this._limit}`;
    if (this._offset !== null) sql += ` OFFSET ${this._offset}`;
    return { sql, params };
  }

  // cached: reuse a compiled statement from the connection's prepareCached()
  // LRU so a repeated query shape compiles once. The pluck mode is forced every
  // time because a cached Statement is shared and its mode persists across
  // callers (see db.prepareCached). Single-shot terminals (all/get/count) may
  // cache; iterate() may not — an iterator holds the statement open, and a
  // shared one can't back two live iterators, nor be left locked for the next
  // caller of the same SQL — so it prepares fresh each time.
  _prepared(cached) {
    const { sql, params } = this.toSQL();
    const base = cached ? this.db.prepareCached(sql) : this.db.prepare(sql);
    return { stmt: base.pluck(this._pluck), params };
  }

  all() { const { stmt, params } = this._prepared(true); return stmt.all(params); }
  get() { const { stmt, params } = this._prepared(true); return stmt.get(params); }
  iterate() { const { stmt, params } = this._prepared(false); return stmt.iterate(params); }

  count() {
    const params = [];
    let sql = `SELECT count(*) AS c${this._fromAndJoins(params)}`;
    const w = renderWhere(this._where); sql += w.sql; params.push(...w.params);
    if (this._groupBy.length) sql += ` GROUP BY ${this._groupBy.map(quoteIdent).join(', ')}`;
    return Number(this.db.prepareCached(sql).pluck().get(params));
  }
}

// ---- INSERT ---------------------------------------------------------------

class InsertBuilder {
  constructor(db, table) {
    this.db = db;
    this.table = table;
    this._rows = null;
    this._conflict = null;
    this._returning = null;
  }

  values(rows) {
    const arr = Array.isArray(rows) ? rows : [rows];
    if (arr.length === 0) throw new TypeError('values() requires at least one row');
    for (const r of arr) if (!isPlainObject(r)) throw new TypeError('Each row must be a plain object');
    this._rows = arr;
    return this;
  }

  onConflict(...cols) { this._conflict = { cols, action: { type: 'nothing' } }; return this; }
  doNothing() {
    if (!this._conflict) throw new TypeError('doNothing() must follow onConflict()');
    this._conflict.action = { type: 'nothing' };
    return this;
  }
  doUpdate(set) {
    if (!this._conflict) throw new TypeError('doUpdate() must follow onConflict()');
    if (!isPlainObject(set) || Object.keys(set).length === 0) throw new TypeError('doUpdate() requires a non-empty set object');
    this._conflict.action = { type: 'update', set };
    return this;
  }
  returning(...cols) { this._returning = cols.length ? cols : ['*']; return this; }

  _build(columns) {
    let sql = `INSERT INTO ${quoteIdent(this.table)} (${columns.map(quote).join(', ')})`
      + ` VALUES (${columns.map(() => '?').join(', ')})`;
    const extra = [];
    if (this._conflict) {
      const target = this._conflict.cols.length ? `(${this._conflict.cols.map(quote).join(', ')})` : '';
      if (this._conflict.action.type === 'nothing') {
        sql += ` ON CONFLICT${target} DO NOTHING`;
      } else {
        const cols = Object.keys(this._conflict.action.set);
        sql += ` ON CONFLICT${target} DO UPDATE SET ${cols.map((c) => `${quote(c)} = ?`).join(', ')}`;
        extra.push(...cols.map((c) => this._conflict.action.set[c]));
      }
    }
    if (this._returning) sql += renderReturning(this._returning);
    return { sql, extra };
  }

  run() {
    if (!this._rows) throw new TypeError('insertInto() requires values()');
    const columns = Object.keys(this._rows[0]);
    if (columns.length === 0) throw new TypeError('The first row has no columns to insert');
    const { sql, extra } = this._build(columns);
    const stmt = this.db.prepareCached(sql);
    const rowParams = (row) => [...columns.map((c) => row[c]), ...extra];

    if (this._returning) {
      const out = [];
      this.db.transaction(() => { for (const row of this._rows) out.push(...stmt.all(rowParams(row))); })();
      return out;
    }
    let changes = 0;
    let lastInsertRowid = 0;
    this.db.transaction(() => {
      for (const row of this._rows) {
        const r = stmt.run(rowParams(row));
        changes += r.changes;
        lastInsertRowid = r.lastInsertRowid;
      }
    })();
    return { changes, lastInsertRowid };
  }
}

// ---- UPDATE ---------------------------------------------------------------

class UpdateBuilder {
  constructor(db, table) {
    this.db = db;
    this.table = table;
    this._set = null;
    this._setParams = [];
    this._where = [];
    this._returning = null;
  }

  set(spec, ...params) {
    if (typeof spec === 'string') { this._set = { raw: spec }; this._setParams = params; }
    else if (isPlainObject(spec)) { this._set = { obj: spec }; }
    else throw new TypeError('set() requires a plain object or a SQL fragment string');
    return this;
  }
  where(cond, ...params) { addWhere(this._where, 'AND', cond, params); return this; }
  andWhere(cond, ...params) { addWhere(this._where, 'AND', cond, params); return this; }
  orWhere(cond, ...params) { addWhere(this._where, 'OR', cond, params); return this; }
  returning(...cols) { this._returning = cols.length ? cols : ['*']; return this; }

  toSQL() {
    if (!this._set) throw new TypeError('updateTable() requires set()');
    const params = [];
    let setSql;
    if (this._set.raw !== undefined) { setSql = this._set.raw; params.push(...this._setParams); }
    else {
      const cols = Object.keys(this._set.obj);
      if (cols.length === 0) throw new TypeError('set() object is empty');
      setSql = cols.map((c) => `${quote(c)} = ?`).join(', ');
      params.push(...cols.map((c) => this._set.obj[c]));
    }
    let sql = `UPDATE ${quoteIdent(this.table)} SET ${setSql}`;
    const w = renderWhere(this._where); sql += w.sql; params.push(...w.params);
    if (this._returning) sql += renderReturning(this._returning);
    return { sql, params };
  }

  run() {
    const { sql, params } = this.toSQL();
    const stmt = this.db.prepareCached(sql);
    return this._returning ? stmt.all(params) : stmt.run(params);
  }
}

// ---- DELETE ---------------------------------------------------------------

class DeleteBuilder {
  constructor(db, table) {
    this.db = db;
    this.table = table;
    this._where = [];
    this._returning = null;
  }

  where(cond, ...params) { addWhere(this._where, 'AND', cond, params); return this; }
  andWhere(cond, ...params) { addWhere(this._where, 'AND', cond, params); return this; }
  orWhere(cond, ...params) { addWhere(this._where, 'OR', cond, params); return this; }
  returning(...cols) { this._returning = cols.length ? cols : ['*']; return this; }

  toSQL() {
    const params = [];
    let sql = `DELETE FROM ${quoteIdent(this.table)}`;
    const w = renderWhere(this._where); sql += w.sql; params.push(...w.params);
    if (this._returning) sql += renderReturning(this._returning);
    return { sql, params };
  }

  run() {
    const { sql, params } = this.toSQL();
    const stmt = this.db.prepareCached(sql);
    return this._returning ? stmt.all(params) : stmt.run(params);
  }
}

// ---- mixin ----------------------------------------------------------------

function from(table) { return new SelectBuilder(this, assertTable(table)); }
function insertInto(table) { return new InsertBuilder(this, assertTable(table)); }
function updateTable(table) { return new UpdateBuilder(this, assertTable(table)); }
function deleteFrom(table) { return new DeleteBuilder(this, assertTable(table)); }

module.exports = { from, insertInto, updateTable, deleteFrom };
// Builder classes exposed for tests and for typing.
module.exports.SelectBuilder = SelectBuilder;
module.exports.InsertBuilder = InsertBuilder;
module.exports.UpdateBuilder = UpdateBuilder;
module.exports.DeleteBuilder = DeleteBuilder;
