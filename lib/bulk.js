'use strict';

// db.insertMany(table, rows, options?): insert an array of row objects in one
// transaction, mixed onto Database.prototype by lib/database.js. A convenience
// over hand-writing a prepared INSERT and a db.transaction() loop, which is one
// of the most common shapes in a service.
//
// The columns are the keys of the first row (or options.columns), and every
// row is bound by name, so a row missing a column raises the usual
// "Missing named parameter" error rather than silently inserting NULL. `mode`
// picks the conflict behaviour: 'insert' (default), 'replace' (INSERT OR
// REPLACE) or 'ignore' (INSERT OR IGNORE). Returns { inserted } — the number of
// rows the database actually wrote (below rows.length when 'ignore' skips
// conflicts). Atomic: any error rolls the whole batch back.
//
// An extension beyond the better-sqlite3 surface.

const { requireOpen, kDb } = require('../adapters/util');
const schema = require('./schema');

const MODES = { insert: 'INSERT', replace: 'INSERT OR REPLACE', ignore: 'INSERT OR IGNORE' };

function insertMany(table, rows, options = {}) {
  requireOpen(this[kDb]);
  if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a non-empty string');
  if (!Array.isArray(rows)) throw new TypeError('Expected second argument to be an array of row objects');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected third argument to be an options object');

  const verb = MODES[options.mode === undefined ? 'insert' : options.mode];
  if (!verb) throw new TypeError(`Expected the "mode" option to be "insert", "replace" or "ignore"`);
  if (!schema.tableExists(this, table)) throw new TypeError(`No such table: ${table}`);
  if (rows.length === 0) return { inserted: 0 };

  let columns = options.columns;
  if (columns === undefined) {
    const first = rows[0];
    if (first === null || typeof first !== 'object' || Array.isArray(first)) throw new TypeError('Each row must be a plain object');
    columns = Object.keys(first);
  } else if (!Array.isArray(columns) || columns.length === 0) {
    throw new TypeError('Expected the "columns" option to be a non-empty array');
  }
  if (columns.length === 0) throw new TypeError('The first row has no columns to insert');

  const known = new Set(schema.tableInfo(this, table).map((c) => c.name));
  for (const c of columns) if (!known.has(c)) throw new TypeError(`No such column in ${table}: ${c}`);

  const sql = `${verb} INTO ${schema.quote(table)} (${columns.map(schema.quote).join(', ')}) ` +
    `VALUES (${columns.map((c) => `@${c}`).join(', ')})`;
  const stmt = this.prepare(sql);

  let inserted = 0;
  const run = this.transaction((batch) => {
    for (const row of batch) inserted += stmt.run(row).changes;
  });
  run(rows);
  return { inserted };
}

module.exports = { insertMany };
