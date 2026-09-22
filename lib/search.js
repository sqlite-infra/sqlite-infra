'use strict';

// Full-text search, mixed onto Database.prototype by lib/database.js.
//
// This build of node:sqlite has no FTS5 module, so search is implemented as a
// JS-tokenized inverted index kept in a shadow table beside the data, rather
// than a virtual table. For each indexed row a set of (row_id, col, term) rows
// is stored; a query tokenizes the same way and scores rows by how many of its
// distinct terms they contain.
//
// Tokenization happens in JavaScript, which a SQLite trigger cannot call. So
// writes to the base table only record the affected rowid in a small "dirty"
// queue (pure-SQL triggers); search() and reindex() drain that queue and
// re-tokenize the changed rows in JS first. This is lazy consistency: the index
// is always correct as observed through search(), and the tokenizing work is
// deferred to the next read instead of charged to every write.
//
// Shadow tables (all under the dictionary's `_better_sqlite_` prefix, so the
// sync and db.tables() skip them), for a base table `docs`:
//   _better_sqlite_search_docs          (row_id, col, term)   the inverted index
//   _better_sqlite_search_docs_dirty    (row_id)              pending re-index queue
//   _better_sqlite_search_docs_config   (config)              the JSON index config
//
// Requires a rowid table (not WITHOUT ROWID), like the audit log.

const { quote, tableInfo, tableExists } = require('./schema');
const { tables } = require('./dictionary');

const DEFAULT_MIN_TOKEN = 2;

function baseName(table, options) {
  return options && options.into ? String(options.into) : `${tables.prefix}search_${table}`;
}

function tokenize(value, minToken, stopWords) {
  if (value === null || value === undefined) return [];
  const parts = String(value).toLowerCase().split(/[^a-z0-9_]+/);
  const out = new Set();
  for (const p of parts) {
    if (p.length >= minToken && !stopWords.has(p)) out.add(p);
  }
  return out;
}

function readConfig(db, base) {
  const cfgTable = `${base}_config`;
  if (!tableExists(db, cfgTable)) return null;
  const row = db.prepare(`SELECT config FROM ${quote(cfgTable)} LIMIT 1`).pluck().get();
  return row === undefined ? null : JSON.parse(row);
}

// Delete the index rows for one rowid and, when the row still exists, insert its
// fresh terms. Statements are passed in so a batch reuses them.
function indexRow(row, config, delStmt, insStmt) {
  delStmt.run(row.__rid);
  if (!row.__exists) return;
  const minToken = config.minToken;
  const stopWords = new Set(config.stopWords);
  for (const col of config.columns) {
    for (const term of tokenize(row[col], minToken, stopWords)) {
      insStmt.run(row.__rid, col, term);
    }
  }
}

// Rebuild the whole index from the base table and empty the dirty queue.
function rebuild(db, table, base, config) {
  const terms = quote(base);
  const dirty = quote(`${base}_dirty`);
  const cols = config.columns.map(quote).join(', ');
  const del = db.prepare(`DELETE FROM ${terms} WHERE row_id = ?`);
  const ins = db.prepare(`INSERT INTO ${terms} (row_id, col, term) VALUES (?, ?, ?)`);
  // Materialize the rows before writing: node:sqlite refuses writes on a
  // connection while one of its statements is still iterating.
  const rows = db.prepare(`SELECT rowid AS __rid, ${cols} FROM ${quote(table)}`).all();
  const run = db.transaction(() => {
    db.exec(`DELETE FROM ${terms}`);
    db.exec(`DELETE FROM ${dirty}`);
    for (const row of rows) {
      row.__exists = true;
      indexRow(row, config, del, ins);
    }
  });
  run();
}

// Drain the dirty queue: re-tokenize changed rows, drop terms for deleted ones.
function reconcile(db, table, base, config) {
  const dirty = quote(`${base}_dirty`);
  const ids = db.prepare(`SELECT row_id FROM ${dirty}`).pluck().all();
  if (ids.length === 0) return;
  const terms = quote(base);
  const del = db.prepare(`DELETE FROM ${terms} WHERE row_id = ?`);
  const ins = db.prepare(`INSERT INTO ${terms} (row_id, col, term) VALUES (?, ?, ?)`);
  const cols = config.columns.map(quote).join(', ');
  const fetch = db.prepare(`SELECT rowid AS __rid, ${cols} FROM ${quote(table)} WHERE rowid = ?`);
  const clear = db.prepare(`DELETE FROM ${dirty} WHERE row_id = ?`);
  const run = db.transaction(() => {
    for (const rid of ids) {
      const row = fetch.get(rid);
      if (row) { row.__exists = true; } else { /* deleted */ }
      indexRow(row || { __rid: rid, __exists: false }, config, del, ins);
      clear.run(rid);
    }
  });
  run();
}

function createTriggers(db, table, base) {
  const dirty = quote(`${base}_dirty`);
  const t = quote(table);
  dropTriggers(db, base);
  db.exec(`CREATE TRIGGER ${quote(`${base}_ai`)} AFTER INSERT ON ${t}
             BEGIN INSERT OR REPLACE INTO ${dirty}(row_id) VALUES (NEW.rowid); END`);
  db.exec(`CREATE TRIGGER ${quote(`${base}_au`)} AFTER UPDATE ON ${t}
             BEGIN INSERT OR REPLACE INTO ${dirty}(row_id) VALUES (OLD.rowid);
                   INSERT OR REPLACE INTO ${dirty}(row_id) VALUES (NEW.rowid); END`);
  db.exec(`CREATE TRIGGER ${quote(`${base}_ad`)} AFTER DELETE ON ${t}
             BEGIN INSERT OR REPLACE INTO ${dirty}(row_id) VALUES (OLD.rowid); END`);
}

function dropTriggers(db, base) {
  for (const suffix of ['_ai', '_au', '_ad']) {
    db.exec(`DROP TRIGGER IF EXISTS ${quote(`${base}${suffix}`)}`);
  }
}

// db.createSearchIndex(table, columns, options?): build an inverted-index over
// `columns` of `table` and keep it current through triggers. Returns the db.
// options: { into, minToken=2, stopWords=[] }.
function createSearchIndex(table, columns, options = {}) {
  if (typeof table !== 'string' || !table) throw new TypeError('Expected first argument to be a non-empty string');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected third argument to be an options object');
  if (!tableExists(this, table)) throw new TypeError(`No such table: ${table}`);
  const cols = Array.isArray(columns) ? columns.slice() : [columns];
  if (cols.length === 0 || cols.some((c) => typeof c !== 'string' || !c)) {
    throw new TypeError('Expected columns to be a non-empty string or array of strings');
  }
  const known = new Set(tableInfo(this, table).map((c) => c.name));
  for (const c of cols) if (!known.has(c)) throw new TypeError(`No such column in ${table}: ${c}`);

  const minToken = options.minToken === undefined ? DEFAULT_MIN_TOKEN : options.minToken;
  if (!Number.isInteger(minToken) || minToken < 1) throw new TypeError('Expected "minToken" to be a positive integer');
  const stopWords = (options.stopWords || []).map((w) => String(w).toLowerCase());

  const base = baseName(table, options);
  const config = { table, columns: cols, minToken, stopWords };

  this.exec(`CREATE TABLE IF NOT EXISTS ${quote(base)} (row_id INTEGER NOT NULL, col TEXT NOT NULL, term TEXT NOT NULL)`);
  this.exec(`CREATE INDEX IF NOT EXISTS ${quote(`${base}_term`)} ON ${quote(base)} (term)`);
  this.exec(`CREATE INDEX IF NOT EXISTS ${quote(`${base}_row`)} ON ${quote(base)} (row_id)`);
  this.exec(`CREATE TABLE IF NOT EXISTS ${quote(`${base}_dirty`)} (row_id INTEGER PRIMARY KEY)`);
  this.exec(`CREATE TABLE IF NOT EXISTS ${quote(`${base}_config`)} (config TEXT NOT NULL)`);
  this.exec(`DELETE FROM ${quote(`${base}_config`)}`);
  this.prepare(`INSERT INTO ${quote(`${base}_config`)} (config) VALUES (?)`).run(JSON.stringify(config));

  rebuild(this, table, base, config);
  createTriggers(this, table, base);
  return this;
}

// db.reindex(table, options?): rebuild the index from the base table.
function reindex(table, options = {}) {
  const base = baseName(table, options);
  const config = readConfig(this, base);
  if (!config) throw new TypeError(`No search index for table: ${table}`);
  rebuild(this, config.table, base, config);
  return this;
}

// db.search(table, query, options?): rows matching `query`, each with a `_score`
// (the number of distinct query terms it contains), best first. Reconciles
// pending writes first. options: { into, limit=50, prefix=true, match='all',
// columns }.
function search(table, query, options = {}) {
  if (typeof query !== 'string') throw new TypeError('Expected the query to be a string');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected the options to be an object');
  const base = baseName(table, options);
  const config = readConfig(this, base);
  if (!config) throw new TypeError(`No search index for table: ${table}`);
  reconcile(this, config.table, base, config);

  const minToken = config.minToken;
  const stopWords = new Set(config.stopWords);
  const terms = [...tokenize(query, minToken, stopWords)];
  if (terms.length === 0) return [];

  const usePrefix = options.prefix !== false;
  const match = options.match === undefined ? 'all' : options.match;
  if (match !== 'all' && match !== 'any') throw new TypeError('Expected "match" to be "all" or "any"');
  const limit = options.limit === undefined ? 50 : options.limit;
  if (!Number.isInteger(limit) || limit < 0) throw new TypeError('Expected "limit" to be a non-negative integer');

  let colFilter = '';
  const colParams = [];
  if (options.columns !== undefined) {
    const cols = Array.isArray(options.columns) ? options.columns : [options.columns];
    if (cols.length === 0) throw new TypeError('Expected "columns" to be a non-empty array');
    for (const c of cols) if (!config.columns.includes(c)) throw new TypeError(`Column not in the search index: ${c}`);
    colFilter = ` AND col IN (${cols.map(() => '?').join(', ')})`;
    colParams.push(...cols);
  }

  const idx = quote(base);
  const params = [];
  const unions = terms.map((term, i) => {
    const last = i === terms.length - 1;
    if (usePrefix && last) {
      params.push(i, `${term}%`, ...colParams);
      return `SELECT row_id, ? AS qi FROM ${idx} WHERE term LIKE ?${colFilter}`;
    }
    params.push(i, term, ...colParams);
    return `SELECT row_id, ? AS qi FROM ${idx} WHERE term = ?${colFilter}`;
  });

  const having = match === 'all' ? 'HAVING COUNT(DISTINCT qi) = ?' : '';
  if (match === 'all') params.push(terms.length);
  params.push(limit);

  const sql = `
    WITH matches AS (${unions.join(' UNION ALL ')}),
         scored AS (SELECT row_id, COUNT(DISTINCT qi) AS _score FROM matches GROUP BY row_id ${having})
    SELECT b.*, scored._score AS _score
      FROM scored JOIN ${quote(config.table)} b ON b.rowid = scored.row_id
     ORDER BY scored._score DESC, scored.row_id
     LIMIT ?`;
  return this.prepare(sql).all(...params);
}

// db.dropSearchIndex(table, options?): remove the index, its queue and triggers.
function dropSearchIndex(table, options = {}) {
  const base = baseName(table, options);
  dropTriggers(this, base);
  this.exec(`DROP TABLE IF EXISTS ${quote(base)}`);
  this.exec(`DROP TABLE IF EXISTS ${quote(`${base}_dirty`)}`);
  this.exec(`DROP TABLE IF EXISTS ${quote(`${base}_config`)}`);
  return this;
}

module.exports = { createSearchIndex, dropSearchIndex, search, reindex };
