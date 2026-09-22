'use strict';

// Streaming ETL pipeline, mixed onto Database.prototype by lib/database.js as
// db.pipeline(source). The data-processing core of the framework: pull rows
// from a query, an array, a file or any iterable, transform them lazily, and
// land them in a table or a file — all streamed, so a job that moves millions
// of rows never holds them all in memory.
//
//   db.pipeline({ query: 'SELECT * FROM raw' })
//     .filter((r) => r.amount > 0)
//     .map((r) => ({ id: r.id, cents: Math.round(r.amount * 100) }))
//     .batch(1000)                 // optional; .into() batches on its own too
//     .flatMap((rows) => rows)
//     .into('clean', { mode: 'replace' });   // -> { inserted }
//
// Transforms are lazy and chainable; nothing runs until a terminal
// (into/toArray/toFile/forEach/reduce/count) consumes the stream. Transform
// functions may be sync or async. A pipeline over a one-shot source (a live
// Statement iterator or a Readable) can be consumed once; one over an array,
// a { query } or a { file } re-reads its source each time a terminal runs.
//
// An extension beyond the better-sqlite3 surface. Self-contained: it uses only
// this.prepare / this.transaction and schema.quote.

const fs = require('fs');
const { quote, tableExists } = require('./schema');
const { requireOpen, kDb } = require('../adapters/util');

const INSERT_VERBS = { insert: 'INSERT', replace: 'INSERT OR REPLACE', ignore: 'INSERT OR IGNORE' };

function isThenable(x) { return x != null && typeof x.then === 'function'; }
function positiveInt(n, what) {
  if (!Number.isInteger(n) || n <= 0) throw new TypeError(`Expected ${what} to be a positive integer`);
  return n;
}
function nonNegativeInt(n, what) {
  if (!Number.isInteger(n) || n < 0) throw new TypeError(`Expected ${what} to be a non-negative integer`);
  return n;
}

// ---- source normalization -------------------------------------------------

// Returns a factory () => async iterator over the source's items.
function sourceFactory(db, source) {
  return async function* generate() {
    if (source == null) return;
    if (Array.isArray(source)) { yield* source; return; }
    // A prepared Statement (has iterate()).
    if (typeof source.iterate === 'function') { for (const row of source.iterate()) yield row; return; }
    if (typeof source === 'object' && typeof source.query === 'string') {
      const stmt = db.prepare(source.query);
      for (const row of stmt.iterate(...(source.params || []))) yield row;
      return;
    }
    if (typeof source === 'object' && typeof source.file === 'string') {
      yield* readFile(source.file, source.format);
      return;
    }
    if (typeof source[Symbol.asyncIterator] === 'function') { for await (const x of source) yield x; return; }
    if (typeof source !== 'string' && typeof source[Symbol.iterator] === 'function') { yield* source; return; }
    if (typeof source.next === 'function') { let r; while (!(r = source.next()).done) yield r.value; return; }
    throw new TypeError('Unsupported pipeline source: expected an array, iterable, Readable, Statement, { query } or { file }');
  };
}

async function* readFile(file, format) {
  const fmt = format || (file.toLowerCase().endsWith('.csv') ? 'csv' : 'ndjson');
  const stream = fs.createReadStream(file, { encoding: 'utf8' });
  if (fmt === 'csv') {
    let header = null;
    for await (const cells of parseCsv(stream)) {
      if (!header) { header = cells; continue; }
      const obj = {};
      for (let i = 0; i < header.length; i++) obj[header[i]] = cells[i] === undefined ? null : cells[i];
      yield obj;
    }
  } else if (fmt === 'ndjson') {
    let buf = '';
    for await (const chunk of stream) {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).replace(/\r$/, '');
        buf = buf.slice(nl + 1);
        if (line.trim()) yield JSON.parse(line);
      }
    }
    if (buf.trim()) yield JSON.parse(buf);
  } else {
    throw new TypeError(`Unknown file format: ${fmt}`);
  }
}

// Streaming RFC-4180 CSV tokenizer: yields arrays of string cells, one per row,
// handling quoted fields with embedded commas, quotes ("") and newlines, across
// chunk boundaries.
async function* parseCsv(chunks) {
  let field = '';
  let row = [];
  let inQuotes = false;
  let pendingQuote = false;   // saw a '"' inside quotes; next char decides
  let dirty = false;          // some content seen since the last emitted row
  const rows = [];

  const step = (c) => {
    if (pendingQuote) {
      pendingQuote = false;
      if (c === '"') { field += '"'; return; }   // escaped quote
      inQuotes = false;                          // closing quote; fall through
    }
    if (inQuotes) {
      if (c === '"') pendingQuote = true;
      else field += c;
      return;
    }
    if (c === '"') { inQuotes = true; dirty = true; }
    else if (c === ',') { row.push(field); field = ''; dirty = true; }
    else if (c === '\n') { row.push(field); field = ''; rows.push(row); row = []; dirty = false; }
    else if (c === '\r') { /* handled with the following \n */ }
    else { field += c; dirty = true; }
  };

  for await (const chunk of chunks) {
    const s = String(chunk);
    for (let i = 0; i < s.length; i++) step(s[i]);
    while (rows.length) yield rows.shift();
  }
  if (dirty || field !== '' || row.length) { row.push(field); yield row; }
}

// ---- CSV writing ----------------------------------------------------------

function csvCell(value) {
  if (value === null || value === undefined) return '';
  let s;
  if (value instanceof Date) s = value.toISOString();
  else if (typeof value === 'object' && !Buffer.isBuffer(value)) s = JSON.stringify(value);
  else if (Buffer.isBuffer(value)) s = value.toString('base64');
  else s = String(value);
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

// ---- Pipeline -------------------------------------------------------------

class Pipeline {
  constructor(db, gen) {
    this._db = db;
    this._gen = gen;   // () => async iterator of items
  }

  [Symbol.asyncIterator]() { return this._gen(); }

  _chain(transform) {
    const parent = this._gen;
    return new Pipeline(this._db, () => transform(parent()));
  }

  // ---- lazy transforms ----

  map(fn) {
    return this._chain(async function* mapped(src) {
      for await (const x of src) { const r = fn(x); yield isThenable(r) ? await r : r; }
    });
  }

  filter(fn) {
    return this._chain(async function* filtered(src) {
      for await (const x of src) { const keep = fn(x); if (isThenable(keep) ? await keep : keep) yield x; }
    });
  }

  flatMap(fn) {
    return this._chain(async function* flat(src) {
      for await (const x of src) {
        let r = fn(x);
        if (isThenable(r)) r = await r;
        if (r == null) continue;
        if (typeof r !== 'string' && typeof r[Symbol.iterator] === 'function') yield* r;
        else if (typeof r[Symbol.asyncIterator] === 'function') { for await (const y of r) yield y; }
        else yield r;
      }
    });
  }

  tap(fn) {
    return this._chain(async function* tapped(src) {
      for await (const x of src) { const r = fn(x); if (isThenable(r)) await r; yield x; }
    });
  }

  batch(n) {
    positiveInt(n, 'the batch size');
    return this._chain(async function* batched(src) {
      let b = [];
      for await (const x of src) { b.push(x); if (b.length >= n) { yield b; b = []; } }
      if (b.length) yield b;
    });
  }

  take(n) {
    nonNegativeInt(n, 'the count');
    return this._chain(async function* taken(src) {
      if (n === 0) return;
      let i = 0;
      for await (const x of src) { yield x; if (++i >= n) return; }
    });
  }

  drop(n) {
    nonNegativeInt(n, 'the count');
    return this._chain(async function* dropped(src) {
      let i = 0;
      for await (const x of src) { if (i++ < n) continue; yield x; }
    });
  }

  // ---- terminals ----

  async toArray() {
    const out = [];
    for await (const x of this) out.push(x);
    return out;
  }

  async forEach(fn) {
    for await (const x of this) { const r = fn(x); if (isThenable(r)) await r; }
  }

  async reduce(fn, initial) {
    let acc = initial;
    for await (const x of this) { const r = fn(acc, x); acc = isThenable(r) ? await r : r; }
    return acc;
  }

  async count() {
    let n = 0;
    for await (const _ of this) n += 1;   // eslint-disable-line no-unused-vars
    return n;
  }

  // Bulk-insert row objects into `table`, committing every `batchSize` rows.
  async into(table, options = {}) {
    const db = this._db;
    if (typeof table !== 'string' || !table) throw new TypeError('Expected a table name');
    if (options === null || typeof options !== 'object') throw new TypeError('Expected an options object');
    const verb = INSERT_VERBS[options.mode === undefined ? 'insert' : options.mode];
    if (!verb) throw new TypeError('Expected the "mode" option to be "insert", "replace" or "ignore"');
    if (!tableExists(db, table)) throw new TypeError(`No such table: ${table}`);
    const batchSize = options.batchSize === undefined ? 500 : positiveInt(options.batchSize, 'the batch size');
    let columns = options.columns;
    if (columns !== undefined && (!Array.isArray(columns) || columns.length === 0)) {
      throw new TypeError('Expected the "columns" option to be a non-empty array');
    }

    let stmt = null;
    let inserted = 0;
    let buffer = [];
    const flush = () => {
      if (!buffer.length) return;
      const rows = buffer;
      buffer = [];
      db.transaction(() => { for (const r of rows) inserted += stmt.run(r).changes; })();
    };

    for await (const row of this) {
      if (!stmt) {
        if (!columns) {
          if (row === null || typeof row !== 'object' || Array.isArray(row)) throw new TypeError('Each row must be a plain object');
          columns = Object.keys(row);
        }
        const sql = `${verb} INTO ${quote(table)} (${columns.map(quote).join(', ')}) ` +
          `VALUES (${columns.map((c) => `@${c}`).join(', ')})`;
        stmt = db.prepare(sql);
      }
      buffer.push(row);
      if (buffer.length >= batchSize) flush();
    }
    flush();
    return { inserted };
  }

  // Write items to a CSV or NDJSON file (streamed, backpressure-aware).
  async toFile(file, options = {}) {
    if (typeof file !== 'string' || !file) throw new TypeError('Expected a file path');
    const format = options.format || (file.toLowerCase().endsWith('.csv') ? 'csv' : 'ndjson');
    if (format !== 'csv' && format !== 'ndjson') throw new TypeError(`Unknown file format: ${format}`);
    const out = fs.createWriteStream(file);
    let error = null;
    out.on('error', (err) => { error = err; });
    const write = (s) => new Promise((resolve) => { if (out.write(s)) resolve(); else out.once('drain', resolve); });

    let rows = 0;
    try {
      if (format === 'csv') {
        let header = null;
        for await (const row of this) {
          if (error) throw error;
          if (!header) { header = Object.keys(row); await write(`${header.map(csvCell).join(',')}\n`); }
          await write(`${header.map((h) => csvCell(row[h])).join(',')}\n`);
          rows += 1;
        }
      } else {
        for await (const row of this) {
          if (error) throw error;
          await write(`${JSON.stringify(row)}\n`);
          rows += 1;
        }
      }
    } finally {
      await new Promise((resolve, reject) => out.end((err) => (err ? reject(err) : resolve())));
    }
    if (error) throw error;
    return { file, rows };
  }
}

// db.pipeline(source): start a pipeline over `source`.
function pipeline(source) {
  requireOpen(this[kDb]);
  return new Pipeline(this, sourceFactory(this, source));
}

module.exports = { pipeline, Pipeline };
