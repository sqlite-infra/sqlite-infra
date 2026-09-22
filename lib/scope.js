'use strict';

// db.scoped(table, options): a reusable, chainable query surface with a
// soft-delete filter and named scopes, mixed onto Database.prototype by
// lib/database.js.
//
// A soft delete marks a row (a timestamp column) instead of removing it; every
// read then hides marked rows unless withDeleted()/onlyDeleted() asks otherwise.
// Named scopes are reusable filters (a criteria object, a { where, params }
// fragment, or a function that refines the query). The builder is immutable:
// each chain method returns a fresh query, so a base `db.scoped(...)` can be
// kept and branched from repeatedly.
//
// An extension beyond the better-sqlite3 surface. Self-contained: raw prepares
// and schema.quote(); values are always bound, identifiers always quoted.

const { requireOpen, kDb } = require('../adapters/util');
const { quote, tableExists, tableInfo } = require('./schema');

function primaryKeyOf(db, table) {
  const pks = tableInfo(db, table).filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk);
  return pks.length === 1 ? pks[0].name : null;
}

// Turns a { col: value } object into one ANDed { sql, params } fragment.
function criteriaFragment(criteria) {
  const parts = [];
  const params = [];
  for (const key of Object.keys(criteria)) {
    const value = criteria[key];
    if (value === null || value === undefined) {
      parts.push(`${quote(key)} IS NULL`);
    } else if (Array.isArray(value)) {
      if (value.length === 0) { parts.push('0'); continue; }   // IN () matches nothing
      parts.push(`${quote(key)} IN (${value.map(() => '?').join(', ')})`);
      params.push(...value);
    } else {
      parts.push(`${quote(key)} = ?`);
      params.push(value);
    }
  }
  return { sql: parts.length ? parts.join(' AND ') : '1', params };
}

class ScopedQuery {
  constructor(db, config, state) {
    this._db = db;
    this._config = config;   // { table, pk, softCol, scopes }
    // state is cloned on every chain step
    this._state = state || { conditions: [], deleted: 'active', order: [], limit: null, offset: null };
  }

  _clone(patch) {
    const s = this._state;
    const next = {
      conditions: s.conditions.slice(),
      deleted: s.deleted,
      order: s.order.slice(),
      limit: s.limit,
      offset: s.offset
    };
    Object.assign(next, patch);
    return new ScopedQuery(this._db, this._config, next);
  }

  where(criteria) {
    if (criteria === null || typeof criteria !== 'object') throw new TypeError('Expected criteria to be an object');
    const next = this._clone({});
    next._state.conditions = next._state.conditions.concat(criteriaFragment(criteria));
    return next;
  }

  scope(name, ...args) {
    const def = this._config.scopes[name];
    if (def === undefined) throw new TypeError(`No such scope: ${name}`);
    if (typeof def === 'function') {
      const out = def(this, ...args);
      if (!(out instanceof ScopedQuery)) throw new TypeError(`Scope "${name}" must return the query`);
      return out;
    }
    if (def && typeof def.where === 'string') {
      const next = this._clone({});
      next._state.conditions = next._state.conditions.concat({ sql: def.where, params: (def.params || []).slice() });
      return next;
    }
    if (def && typeof def === 'object') return this.where(def);
    throw new TypeError(`Scope "${name}" must be a criteria object, a { where, params } fragment, or a function`);
  }

  withDeleted() { return this._clone({ deleted: 'with' }); }
  onlyDeleted() { return this._clone({ deleted: 'only' }); }

  orderBy(column, direction = 'asc') {
    if (typeof column !== 'string') throw new TypeError('Expected the order column to be a string');
    const dir = String(direction).toLowerCase();
    if (dir !== 'asc' && dir !== 'desc') throw new TypeError('Expected the order direction to be "asc" or "desc"');
    return this._clone({ order: this._state.order.concat({ column, dir }) });
  }

  limit(n) {
    if (!Number.isInteger(n) || n < 0) throw new TypeError('Expected limit to be a non-negative integer');
    return this._clone({ limit: n });
  }

  offset(n) {
    if (!Number.isInteger(n) || n < 0) throw new TypeError('Expected offset to be a non-negative integer');
    return this._clone({ offset: n });
  }

  // ---- read helpers ----

  // The WHERE fragment for reads: conditions + the soft-delete filter.
  _readWhere(extra) {
    const parts = [];
    const params = [];
    const push = (frag) => { if (frag && frag.sql && frag.sql !== '1') { parts.push(`(${frag.sql})`); params.push(...frag.params); } else if (frag) { params.push(...frag.params); } };
    for (const c of this._state.conditions) push(c);
    if (extra) push(extra);
    const { softCol } = this._config;
    if (softCol) {
      if (this._state.deleted === 'active') parts.push(`${quote(softCol)} IS NULL`);
      else if (this._state.deleted === 'only') parts.push(`${quote(softCol)} IS NOT NULL`);
    }
    return { sql: parts.length ? ` WHERE ${parts.join(' AND ')}` : '', params };
  }

  _tail() {
    const s = this._state;
    let sql = '';
    if (s.order.length) sql += ` ORDER BY ${s.order.map((o) => `${quote(o.column)} ${o.dir.toUpperCase()}`).join(', ')}`;
    if (s.limit !== null) sql += ` LIMIT ${s.limit}`;
    if (s.offset !== null) sql += ` ${s.limit === null ? 'LIMIT -1 ' : ''}OFFSET ${s.offset}`;
    return sql;
  }

  all() {
    requireOpen(this._db[kDb]);
    const where = this._readWhere();
    const sql = `SELECT * FROM ${quote(this._config.table)}${where.sql}${this._tail()}`;
    return this._db.prepare(sql).all(...where.params);
  }

  first() {
    return this.limit(1).all()[0];
  }

  get(pk) {
    requireOpen(this._db[kDb]);
    if (!this._config.pk) throw new TypeError(`get(pk) needs a single-column primary key on ${this._config.table}`);
    const where = this._readWhere({ sql: `${quote(this._config.pk)} = ?`, params: [pk] });
    const sql = `SELECT * FROM ${quote(this._config.table)}${where.sql} LIMIT 1`;
    return this._db.prepare(sql).get(...where.params);
  }

  count() {
    requireOpen(this._db[kDb]);
    const where = this._readWhere();
    const sql = `SELECT count(*) AS n FROM ${quote(this._config.table)}${where.sql}`;
    return this._db.prepare(sql).pluck().get(...where.params);
  }

  pluck(column) {
    if (typeof column !== 'string') throw new TypeError('Expected the pluck column to be a string');
    const self = this;
    return {
      all() {
        requireOpen(self._db[kDb]);
        const where = self._readWhere();
        const sql = `SELECT ${quote(column)} FROM ${quote(self._config.table)}${where.sql}${self._tail()}`;
        return self._db.prepare(sql).pluck().all(...where.params);
      },
      get() {
        requireOpen(self._db[kDb]);
        const where = self._readWhere();
        const sql = `SELECT ${quote(column)} FROM ${quote(self._config.table)}${where.sql} LIMIT 1`;
        return self._db.prepare(sql).pluck().get(...where.params);
      }
    };
  }

  // ---- mutations ----

  // WHERE for a mutation: the target (a pk scalar or a criteria object) plus any
  // chained conditions/scopes. The soft-delete filter is NOT applied, so a
  // mutation can act on rows regardless of their deleted state.
  _mutationWhere(target) {
    const parts = [];
    const params = [];
    const push = (frag) => { if (frag) { parts.push(`(${frag.sql})`); params.push(...frag.params); } };
    for (const c of this._state.conditions) push(c);
    if (target !== undefined) {
      if (target !== null && typeof target === 'object' && !Array.isArray(target)) {
        push(criteriaFragment(target));
      } else {
        if (!this._config.pk) throw new TypeError(`A primary key is needed to target a row by value on ${this._config.table}`);
        parts.push(`${quote(this._config.pk)} = ?`);
        params.push(target);
      }
    }
    return { sql: parts.length ? ` WHERE ${parts.join(' AND ')}` : '', params };
  }

  softDelete(target) {
    requireOpen(this._db[kDb]);
    const { softCol } = this._config;
    if (!softCol) throw new TypeError(`softDelete() needs a "softDelete" column configured on ${this._config.table}`);
    const where = this._mutationWhere(target);
    // Only mark rows that are not already soft-deleted, so the count reflects
    // rows actually changed and a re-delete does not reset the timestamp.
    const notDeleted = `${quote(softCol)} IS NULL`;
    const clause = where.sql ? `${where.sql} AND ${notDeleted}` : ` WHERE ${notDeleted}`;
    const sql = `UPDATE ${quote(this._config.table)} SET ${quote(softCol)} = ?${clause}`;
    return this._db.prepare(sql).run(Date.now(), ...where.params).changes;
  }

  restore(target) {
    requireOpen(this._db[kDb]);
    const { softCol } = this._config;
    if (!softCol) throw new TypeError(`restore() needs a "softDelete" column configured on ${this._config.table}`);
    const where = this._mutationWhere(target);
    const sql = `UPDATE ${quote(this._config.table)} SET ${quote(softCol)} = NULL${where.sql}`;
    return this._db.prepare(sql).run(...where.params).changes;
  }

  hardDelete(target) {
    requireOpen(this._db[kDb]);
    const where = this._mutationWhere(target);
    const sql = `DELETE FROM ${quote(this._config.table)}${where.sql}`;
    return this._db.prepare(sql).run(...where.params).changes;
  }
}

function scoped(table, options = {}) {
  requireOpen(this[kDb]);
  if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a non-empty string');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  if (!tableExists(this, table)) throw new TypeError(`No such table or view: ${table}`);
  if (options.softDelete !== undefined && (typeof options.softDelete !== 'string' || !options.softDelete)) {
    throw new TypeError('Expected the "softDelete" option to be a column name');
  }
  const scopes = options.scopes || {};
  if (scopes === null || typeof scopes !== 'object') throw new TypeError('Expected the "scopes" option to be an object');
  const pk = options.primaryKey !== undefined ? options.primaryKey : primaryKeyOf(this, table);
  return new ScopedQuery(this, { table, pk, softCol: options.softDelete || null, scopes });
}

module.exports = { scoped, ScopedQuery };
