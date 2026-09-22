'use strict';

// Row export: one table (or view) as NDJSON for Athena / BigQuery and the
// like, with a schema.json sidecar. Keys, under the sync's `exports/` dir:
//
//   <dir><table>/dt=<YYYY-MM-DD>/<timestamp>.ndjson.gz
//   <dir><table>/schema.json
//
// Rows are read on a second, readonly connection to the same file, so a
// long iteration does not hold the caller's connection busy (an open
// iterator blocks writes and stalls the queue). Integers stay exact (a
// number when they fit, a decimal string beyond 2^53) and Buffers become
// base64; lib/importer.js reverses both.

const fs = require('fs');
const zlib = require('zlib');
const { isPlainObject, requireOpen } = require('../adapters/util');
const { quote, tableInfo, tableExists } = require('./schema');
const { Hasher, unlink, timestamp } = require('./staging');
const { tempPath } = require('./params');
const { keys: layout, contentTypes, contentEncodings } = require('./dictionary');

function encodeValue(value) {
  if (typeof value === 'bigint') {
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value.toString();
  }
  if (Buffer.isBuffer(value)) return value.toString('base64');
  if (value instanceof Uint8Array) return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('base64');
  return value;
}

function encodeRow(row) {
  const out = {};
  for (const key of Object.keys(row)) out[key] = encodeValue(row[key]);
  return out;
}

// Checks the options against the table; returns what the query needs.
function readOptions(db, table, options) {
  if (!isPlainObject(options)) throw new TypeError('Expected options to be an object');
  if (!tableExists(db, table)) throw new TypeError(`No such table or view: ${table}`);
  const info = tableInfo(db, table);
  let columns = info.map((c) => c.name);
  if (options.columns !== undefined) {
    if (!Array.isArray(options.columns) || options.columns.length === 0) throw new TypeError('Expected the "columns" option to be a non-empty array');
    for (const c of options.columns) if (!columns.includes(c)) throw new TypeError(`No such column in ${table}: ${c}`);
    columns = options.columns.slice();
  }
  const transform = options.transform;
  if (transform !== undefined && typeof transform !== 'function') throw new TypeError('Expected the "transform" option to be a function');
  const where = options.where === undefined ? '' : options.where;
  if (typeof where !== 'string') throw new TypeError('Expected the "where" option to be a string');
  const params = options.params === undefined ? [] : options.params;
  if (!Array.isArray(params)) throw new TypeError('Expected the "params" option to be an array');
  return { info, columns, transform, where, params, partition: options.partition !== false };
}

// Streams the query into `staged` (gzipped when asked); resolves { rows, sha256 }.
async function writeRows(reader, sql, params, transform, staged, gzip) {
  const hasher = new Hasher();
  const sink = fs.createWriteStream(staged);
  const gz = gzip ? zlib.createGzip() : null;
  const head = gz || hasher;
  if (gz) gz.pipe(hasher);
  hasher.pipe(sink);
  const finished = new Promise((resolve, reject) => {
    sink.on('finish', resolve);
    sink.on('error', reject);
    hasher.on('error', reject);
    if (gz) gz.on('error', reject);
  });

  // safeIntegers so 64-bit values survive; encodeValue turns the ones that
  // fit back into numbers.
  let rows = 0;
  const stmt = reader.prepare(sql).safeIntegers(true);
  for (const row of stmt.iterate(...params)) {
    let out = encodeRow(row);
    if (transform) {
      out = transform(out);
      if (out === null || out === undefined) continue;
    }
    rows += 1;
    if (!head.write(`${JSON.stringify(out)}\n`)) {
      await new Promise((resolve) => head.once('drain', resolve));
    }
  }
  head.end();
  await finished;
  return { rows, sha256: hasher.digest() };
}

// Exports `table` from `db` to `store` under `dir` (ends in '/'). Options:
//   where     SQL after WHERE, written by the caller (never from user input)
//   params    bound to `where`
//   columns   subset of columns; default all
//   transform row => row | null; null drops the row
//   partition 'dt' (default, by export date) | false
// Resolves { key, schemaKey, table, rows, size, sha256, at, gzip }.
async function exportRows(db, table, options, { store, dir, gzip }) {
  requireOpen(db);
  const { info, columns, transform, where, params, partition } = readOptions(db, table, options);
  const at = new Date();
  const sql = `SELECT ${columns.map(quote).join(', ')} FROM ${quote(table)}${where ? ` WHERE ${where}` : ''}`;

  // A second connection so the iteration does not hold the caller's
  // connection busy. In-memory databases have nothing to reopen.
  const Database = db.constructor;
  const reader = db.memory || db.name.startsWith('file:') ? db : new Database(db.name, { readonly: true });
  const staged = tempPath(db, 'rows');
  try {
    const { rows, sha256 } = await writeRows(reader, sql, params, transform, staged, gzip);
    const { size } = await fs.promises.stat(staged);

    const tableDir = `${dir}${table}/`;
    const day = partition ? `${layout.partition}${at.toISOString().slice(0, 10)}/` : '';
    const key = `${tableDir}${day}${timestamp(at)}${layout.extensions.ndjson}${gzip ? layout.extensions.gzip : ''}`;
    await store.put(key, fs.createReadStream(staged), {
      size,
      sha256,
      contentType: contentTypes.ndjson,
      contentEncoding: gzip ? contentEncodings.gzip : undefined
    });

    const schemaKey = `${tableDir}${layout.schema}`;
    const schema = {
      version: 1,
      table,
      columns: info.filter((c) => columns.includes(c.name)).map((c) => ({
        name: c.name, type: c.type || null, notnull: Boolean(c.notnull), pk: c.pk
      })),
      encoding: { INTEGER: 'number or decimal string when beyond 2^53', BLOB: 'base64' },
      updatedAt: at.toISOString()
    };
    await store.put(schemaKey, Buffer.from(JSON.stringify(schema, null, 2)), { contentType: contentTypes.json });

    return { key, schemaKey, table, rows, size, sha256, at: at.toISOString(), gzip };
  } finally {
    if (reader !== db) reader.close();
    await unlink(staged);
  }
}

module.exports = { exportRows };
