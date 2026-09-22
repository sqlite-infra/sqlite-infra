'use strict';

// db.loadRelated(rows, spec): batch eager-loader, mixed onto Database.prototype
// by lib/database.js. Given a set of parent rows and a relation (or an array of
// relations), it attaches the related rows with ONE query per relation — a
// single `... WHERE <foreignKey> IN (<distinct keys>)` — instead of one query
// per parent (the N+1 problem). Relations may nest, and nested levels are
// batched the same way across all children at that level.
//
// A relation is:
//   { as, type, table, localKey?, foreignKey, columns?, where?, params?, orderBy?, nested? }
//     as         property to attach the result under
//     type       'hasMany' (an array), 'hasOne' | 'belongsTo' (one row or null)
//     table      the related table
//     localKey   the key on the parent to match. Default 'id' for hasMany/hasOne;
//                for belongsTo it is the parent's foreign-key column (required).
//     foreignKey the key on the related table. For hasMany/hasOne the column that
//                points back at the parent (required); for belongsTo the related
//                table's own key (default 'id').
//     columns    optional projection (the join key is fetched even if omitted)
//     where      optional extra SQL fragment, ANDed in (caller-written)
//     params     values bound to `where`
//     orderBy    optional SQL fragment for hasMany ordering (caller-written)
//     nested     a relation or array applied recursively to the loaded children
//
// An extension beyond the better-sqlite3 surface. Values are bound; identifiers
// are quoted.

const { quote, tableExists } = require('./schema');

const TYPES = new Set(['hasMany', 'hasOne', 'belongsTo']);

function loadRelated(rows, spec) {
  if (!Array.isArray(rows)) throw new TypeError('Expected first argument to be an array of rows');
  const relations = Array.isArray(spec) ? spec : [spec];
  for (const relation of relations) applyRelation(this, rows, relation);
  return rows;
}

function applyRelation(db, rows, relation) {
  if (relation === null || typeof relation !== 'object') throw new TypeError('Each relation must be an object');
  const { as, type, table } = relation;
  if (typeof as !== 'string' || !as) throw new TypeError('A relation needs a non-empty "as"');
  if (!TYPES.has(type)) throw new TypeError(`A relation "type" must be one of: ${[...TYPES].join(', ')}`);
  if (typeof table !== 'string' || !table) throw new TypeError('A relation needs a "table"');
  if (!tableExists(db, table)) throw new TypeError(`No such table or view: ${table}`);

  const belongsTo = type === 'belongsTo';
  const localKey = relation.localKey || (belongsTo ? undefined : 'id');
  const foreignKey = relation.foreignKey || (belongsTo ? 'id' : undefined);
  if (typeof localKey !== 'string' || !localKey) throw new TypeError(`Relation "${as}" needs a "localKey"`);
  if (typeof foreignKey !== 'string' || !foreignKey) throw new TypeError(`Relation "${as}" needs a "foreignKey"`);

  const isMany = type === 'hasMany';
  const empty = () => (isMany ? [] : null);

  if (rows.length === 0) return;

  // Distinct, non-null parent keys, normalized so 1 and '1' and 1n match.
  const keyOf = (v) => (v === null || v === undefined ? null : String(v));
  const wanted = new Map();   // normalized key -> raw value (for binding)
  for (const row of rows) {
    const k = keyOf(row[localKey]);
    if (k !== null && !wanted.has(k)) wanted.set(k, row[localKey]);
  }

  if (wanted.size === 0) {
    for (const row of rows) row[as] = empty();
    return;
  }

  // Projection: always fetch the join key for grouping; if the caller projected
  // without it, drop it from the attached rows afterwards.
  let projection = '*';
  let dropJoinKey = false;
  if (relation.columns !== undefined) {
    if (!Array.isArray(relation.columns) || relation.columns.length === 0) {
      throw new TypeError(`Relation "${as}" "columns" must be a non-empty array`);
    }
    const cols = relation.columns.slice();
    if (!cols.includes(foreignKey)) { cols.push(foreignKey); dropJoinKey = true; }
    projection = cols.map(quote).join(', ');
  }

  const values = [...wanted.values()];
  const placeholders = values.map(() => '?').join(', ');
  let sql = `SELECT ${projection} FROM ${quote(table)} WHERE ${quote(foreignKey)} IN (${placeholders})`;
  const params = values.slice();

  if (relation.where !== undefined) {
    if (typeof relation.where !== 'string') throw new TypeError(`Relation "${as}" "where" must be a string`);
    sql += ` AND (${relation.where})`;
    if (relation.params !== undefined) {
      if (!Array.isArray(relation.params)) throw new TypeError(`Relation "${as}" "params" must be an array`);
      params.push(...relation.params);
    }
  }
  if (relation.orderBy !== undefined) {
    if (typeof relation.orderBy !== 'string') throw new TypeError(`Relation "${as}" "orderBy" must be a string`);
    sql += ` ORDER BY ${relation.orderBy}`;
  }

  const children = db.prepare(sql).all(...params);   // the single batched query

  const grouped = new Map();
  for (const child of children) {
    const k = keyOf(child[foreignKey]);
    if (dropJoinKey) delete child[foreignKey];
    let bucket = grouped.get(k);
    if (!bucket) grouped.set(k, (bucket = []));
    bucket.push(child);
  }

  for (const row of rows) {
    const bucket = grouped.get(keyOf(row[localKey]));
    row[as] = isMany ? (bucket || []) : (bucket ? bucket[0] : null);
  }

  if (relation.nested !== undefined) {
    // All loaded children across every parent, batched again one level down.
    loadRelated.call(db, children, relation.nested);
  }
}

module.exports = { loadRelated };
