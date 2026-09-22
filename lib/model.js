'use strict';

// db.model(name, options?): a lightweight data-mapper / repository over one
// table, mixed onto Database.prototype by lib/database.js. It generates plain,
// parameterized SQL through the connection's own prepare()/transaction(), so
// the same guards, row modes and verbose hook apply; it is not an ORM and
// holds no session or identity map.
//
// A repository knows its table's column types (when told), so it transparently
// encodes values on the way in and decodes them on the way out: JSON columns
// as objects, booleans as 1/0, dates as ISO strings. It can keep created_at /
// updated_at timestamps, run a validator before writes, and fire lifecycle
// hooks.
//
// An extension beyond the better-sqlite3 surface.

const { requireOpen, kDb, isPlainObject } = require('./util');
const schema = require('./schema');

const TYPES = new Set(['text', 'integer', 'real', 'blob', 'json', 'boolean', 'date']);
const CREATED_AT = 'created_at';
const UPDATED_AT = 'updated_at';

// ---- value codecs ---------------------------------------------------------

function encode(type, value) {
  if (value === null || value === undefined) return null;
  switch (type) {
    case 'json': return JSON.stringify(value);
    case 'boolean': return value ? 1 : 0;
    case 'date': return value instanceof Date ? value.toISOString() : String(value);
    default: return value;
  }
}

function decode(type, value) {
  if (value === null || value === undefined) return value === undefined ? undefined : null;
  switch (type) {
    case 'json': return typeof value === 'string' ? JSON.parse(value) : value;
    case 'boolean': return Boolean(value);
    case 'date': return value instanceof Date ? value : new Date(value);
    default: return value;
  }
}

class Repository {
  constructor(db, table, options) {
    this.db = db;
    this.table = table;
    this.primaryKey = options.primaryKey || 'id';
    this.types = normalizeColumns(options.columns);
    this.timestamps = Boolean(options.timestamps);
    this.validate = options.validate || null;
    this.hooks = options.hooks || {};
    this._q = schema.quote(table);
    this._pkq = schema.quote(this.primaryKey);
    // Warm-up: the two statements whose SQL is fully fixed at construction
    // (keyed on the primary key) are compiled once, here, and held per
    // repository — see find()/delete(). model() is called during the
    // connection's set-up phase, so this moves scan()+compile off the hot
    // path. Every other method's SQL depends on the caller's criteria/columns,
    // so those go through db.prepareCached() to compile-once per distinct
    // shape instead. db is open (model() checks requireOpen), and the run-path
    // guards (requireOpen/requireNotBusy in Statement) still fire at call time.
    this._stmtFind = db.prepare(`SELECT * FROM ${this._q} WHERE ${this._pkq} = ?`);
    this._stmtDelete = db.prepare(`DELETE FROM ${this._q} WHERE ${this._pkq} = ?`);
  }

  // ---- encoding helpers ----

  _encodeRow(row) {
    const out = {};
    for (const key of Object.keys(row)) out[key] = encode(this.types[key], row[key]);
    return out;
  }

  _decodeRow(row) {
    if (row === undefined || row === null) return row;
    const out = {};
    for (const key of Object.keys(row)) out[key] = this.types[key] ? decode(this.types[key], row[key]) : row[key];
    return out;
  }

  _decodeAll(rows) { return rows.map((r) => this._decodeRow(r)); }

  _runValidate(row) {
    if (!this.validate) return;
    const result = this.validate(row);
    if (typeof result === 'string') throw validationError(result);
    if (Array.isArray(result) && result.length) throw validationError(result.join('; '));
  }

  _hook(name, ...args) {
    const fn = this.hooks[name];
    if (typeof fn === 'function') fn(...args);
  }

  // ---- WHERE building ----

  // criteria: { col: value }, where an array becomes IN (...) and null becomes
  // IS NULL. Returns { sql: ' WHERE ...' | '', params: [] }.
  _where(criteria) {
    if (criteria === undefined || criteria === null) return { sql: '', params: [] };
    if (!isPlainObject(criteria)) throw new TypeError('Expected criteria to be a plain object');
    const clauses = [];
    const params = [];
    for (const key of Object.keys(criteria)) {
      const col = schema.quote(key);
      const value = criteria[key];
      if (value === null) {
        clauses.push(`${col} IS NULL`);
      } else if (Array.isArray(value)) {
        if (value.length === 0) { clauses.push('0'); continue; }   // IN () matches nothing
        clauses.push(`${col} IN (${value.map(() => '?').join(', ')})`);
        for (const v of value) params.push(encode(this.types[key], v));
      } else {
        clauses.push(`${col} = ?`);
        params.push(encode(this.types[key], value));
      }
    }
    return { sql: clauses.length ? ` WHERE ${clauses.join(' AND ')}` : '', params };
  }

  // ---- reads ----

  find(pk) {
    const row = this._stmtFind.get(encode(this.types[this.primaryKey], pk));
    return row === undefined ? undefined : this._decodeRow(row);
  }

  findBy(criteria) { return this.first(criteria); }

  first(criteria) {
    const { sql, params } = this._where(criteria);
    const row = this.db.prepareCached(`SELECT * FROM ${this._q}${sql} LIMIT 1`).get(...params);
    return row === undefined ? undefined : this._decodeRow(row);
  }

  all(criteria) { return this.where(criteria); }

  where(criteria) {
    const { sql, params } = this._where(criteria);
    return this._decodeAll(this.db.prepareCached(`SELECT * FROM ${this._q}${sql}`).all(...params));
  }

  count(criteria) {
    const { sql, params } = this._where(criteria);
    return Number(this.db.prepareCached(`SELECT count(*) AS n FROM ${this._q}${sql}`).pluck().get(...params));
  }

  exists(criteria) {
    const { sql, params } = this._where(criteria);
    return this.db.prepareCached(`SELECT EXISTS(SELECT 1 FROM ${this._q}${sql} LIMIT 1) AS e`).pluck().get(...params) === 1;
  }

  paginate(options = {}) {
    const page = Math.max(1, Math.trunc(options.page || 1));
    const perPage = Math.max(1, Math.trunc(options.perPage || 20));
    const { sql, params } = this._where(options.where);
    const order = options.orderBy ? ` ORDER BY ${orderClause(options.orderBy)}` : '';
    const total = this.count(options.where);
    const rows = this._decodeAll(this.db.prepareCached(`SELECT * FROM ${this._q}${sql}${order} LIMIT ? OFFSET ?`)
      .all(...params, perPage, (page - 1) * perPage));
    return { rows, page, perPage, total, pages: Math.ceil(total / perPage) };
  }

  // ---- writes ----

  insert(row) {
    if (!isPlainObject(row)) throw new TypeError('Expected the row to be a plain object');
    const prepared = this._prepareWrite(row, true);
    this._runValidate(prepared);
    this._hook('beforeInsert', prepared);
    const encoded = this._encodeRow(prepared);
    const cols = Object.keys(encoded);
    const sql = `INSERT INTO ${this._q} (${cols.map(schema.quote).join(', ')}) VALUES (${cols.map((c) => `@${c}`).join(', ')}) RETURNING *`;
    const out = this._decodeRow(this.db.prepareCached(sql).get(encoded));
    this._hook('afterInsert', out);
    return out;
  }

  insertMany(rows) {
    if (!Array.isArray(rows)) throw new TypeError('Expected an array of rows');
    let inserted = 0;
    const run = this.db.transaction((batch) => { for (const r of batch) { this.insert(r); inserted += 1; } });
    run(rows);
    return { inserted };
  }

  update(pk, patch) {
    if (!isPlainObject(patch)) throw new TypeError('Expected the patch to be a plain object');
    const prepared = this._prepareWrite(patch, false);
    this._runValidate(Object.assign({ [this.primaryKey]: pk }, prepared));
    this._hook('beforeUpdate', prepared, pk);
    const cols = Object.keys(prepared);
    if (cols.length === 0) return this.find(pk);
    const encoded = this._encodeRow(prepared);
    const set = cols.map((c) => `${schema.quote(c)} = @${c}`).join(', ');
    const sql = `UPDATE ${this._q} SET ${set} WHERE ${schema.quote(this.primaryKey)} = @__pk RETURNING *`;
    const out = this.db.prepareCached(sql).get(Object.assign({ __pk: encode(this.types[this.primaryKey], pk) }, encoded));
    if (out === undefined) return undefined;
    const decoded = this._decodeRow(out);
    this._hook('afterUpdate', decoded);
    return decoded;
  }

  updateWhere(criteria, patch) {
    if (!isPlainObject(patch)) throw new TypeError('Expected the patch to be a plain object');
    const prepared = this._prepareWrite(patch, false);
    const cols = Object.keys(prepared);
    if (cols.length === 0) return 0;
    const encoded = this._encodeRow(prepared);
    const setParams = [];
    const set = cols.map((c) => { setParams.push(encoded[c]); return `${schema.quote(c)} = ?`; }).join(', ');
    const { sql, params } = this._where(criteria);
    return this.db.prepareCached(`UPDATE ${this._q} SET ${set}${sql}`).run(...setParams, ...params).changes;
  }

  upsert(row, options = {}) {
    if (!isPlainObject(row)) throw new TypeError('Expected the row to be a plain object');
    const prepared = this._prepareWrite(row, true);
    this._runValidate(prepared);
    const encoded = this._encodeRow(prepared);
    const cols = Object.keys(encoded);
    const conflict = options.onConflict ? [].concat(options.onConflict) : [this.primaryKey];
    const updates = cols.filter((c) => !conflict.includes(c) && c !== CREATED_AT);
    const setClause = updates.length
      ? updates.map((c) => `${schema.quote(c)} = excluded.${schema.quote(c)}`).join(', ')
      : `${schema.quote(conflict[0])} = ${schema.quote(conflict[0])}`;   // no-op update keeps RETURNING working
    const sql = `INSERT INTO ${this._q} (${cols.map(schema.quote).join(', ')}) VALUES (${cols.map((c) => `@${c}`).join(', ')}) ` +
      `ON CONFLICT (${conflict.map(schema.quote).join(', ')}) DO UPDATE SET ${setClause} RETURNING *`;
    return this._decodeRow(this.db.prepareCached(sql).get(encoded));
  }

  delete(pk) {
    this._hook('beforeDelete', pk);
    const changes = this._stmtDelete.run(encode(this.types[this.primaryKey], pk)).changes;
    if (changes > 0) this._hook('afterDelete', pk);
    return changes > 0;
  }

  deleteWhere(criteria) {
    const { sql, params } = this._where(criteria);
    return this.db.prepareCached(`DELETE FROM ${this._q}${sql}`).run(...params).changes;
  }

  // Applies timestamps to a copy of the caller's object. `creating` adds
  // created_at as well as updated_at.
  _prepareWrite(row, creating) {
    const copy = Object.assign({}, row);
    if (this.timestamps) {
      const now = Date.now();
      if (creating && copy[CREATED_AT] === undefined) copy[CREATED_AT] = now;
      copy[UPDATED_AT] = now;
    }
    return copy;
  }
}

function normalizeColumns(columns) {
  const types = {};
  if (columns === undefined) return types;
  if (!isPlainObject(columns)) throw new TypeError('Expected the "columns" option to be an object');
  for (const [name, type] of Object.entries(columns)) {
    if (!TYPES.has(type)) throw new TypeError(`Unknown column type for "${name}": ${type} (expected ${[...TYPES].join(', ')})`);
    types[name] = type;
  }
  return types;
}

function orderClause(orderBy) {
  const parts = Array.isArray(orderBy) ? orderBy : [orderBy];
  return parts.map((p) => {
    if (typeof p === 'string') return schema.quote(p);
    if (isPlainObject(p) && typeof p.column === 'string') {
      return `${schema.quote(p.column)}${String(p.direction).toLowerCase() === 'desc' ? ' DESC' : ' ASC'}`;
    }
    throw new TypeError('Expected orderBy to be a column name or { column, direction }');
  }).join(', ');
}

function validationError(message) {
  const err = new Error(message);
  err.name = 'ValidationError';
  return err;
}

// db.model(name, options?): build a Repository for a table.
function model(name, options = {}) {
  requireOpen(this[kDb]);
  if (typeof name !== 'string' || !name) throw new TypeError('Expected first argument to be a non-empty table name');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  if (!schema.tableExists(this, name)) throw new TypeError(`No such table: ${name}`);
  return new Repository(this, name, options);
}

// Only `model` is exported, so mixing this module onto Database.prototype adds
// exactly that one method. Repository is internal.
module.exports = { model };
