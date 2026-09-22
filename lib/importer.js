'use strict';

// Importer: bring data from an object store into a database.
//
//   // A whole database, from the sync's layout: latest snapshot plus
//   // every changeset of its generation, verified, written to `to`.
//   await Database.Importer.restore({ store, prefix: 'sqlite', dbId, to: 'data/app.db' });
//
//   // Rows, from an NDJSON or CSV object, into an existing table.
//   await Database.Importer.load(db, { store, key: 'exports/orders/dt=2026-09-14/x.ndjson.gz', table: 'orders' });
//
// Everything that arrives from a bucket is treated as untrusted input:
//
//   - A snapshot is accepted only if its SHA-256 matches the manifest, and a
//     changeset only if it matches the hash the sync stored in the
//     object's metadata. A store that returns no metadata fails the restore
//     rather than skipping the check (`verify: false` opts out, loudly).
//   - A changeset is executable in the sense that matters: it mutates the
//     database. Conflicts abort by default; the caller chooses 'replace' or
//     'omit' or supplies a function, and nothing is applied to the target
//     path until every changeset has been applied to a temp copy.
//   - Rows are validated against the table's declared columns before the
//     first write. An unknown column is a TypeError, not a silent drop.
//     Line length and object size are capped.
//
// load() is resumable. Progress is kept in `_better_sqlite_import`, updated
// in the same transaction as each batch, so a crash mid-file and a rerun of
// the same key continue at the first uncommitted line. Values round-trip
// from lib/sync.js: base64 strings become Buffers for BLOB columns,
// decimal strings become integers for INTEGER columns when they fit.

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const readline = require('readline');
const { pipeline } = require('stream/promises');
const { Transform, PassThrough } = require('stream');
const sqlite = require('./backend');
const { kDb } = require('./util');
const { assertObjectStore } = require('./interfaces');
const { unpack } = require('./changeset');
const { tables, defaults, importer: NAMES, keys: layout } = require('./dictionary');

const IMPORTER = tables.importer;

const DEFAULTS = defaults.importer;

// '"a", "b" or "c"' for an error message listing the accepted values.
function oneOf(values) {
  const quoted = Object.values(values).map((v) => `"${v}"`);
  return `${quoted.slice(0, -1).join(', ')} or ${quoted[quoted.length - 1]}`;
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype;
}

function quote(ident) {
  return `"${String(ident).replace(/"/g, '""')}"`;
}

class Hasher extends Transform {
  constructor() { super(); this.hash = crypto.createHash('sha256'); this.size = 0; }
  _transform(chunk, _enc, cb) { this.hash.update(chunk); this.size += chunk.length; cb(null, chunk); }
  digest() { return this.hash.digest('hex'); }
}

async function readAll(stream, limit) {
  const chunks = [];
  let size = 0;
  for await (const chunk of stream) {
    size += chunk.length;
    if (limit && size > limit) throw new RangeError(`Object exceeds ${limit} bytes`);
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function verifyFailure(what, expected, actual) {
  const err = new Error(`${what}: SHA-256 mismatch (manifest ${expected}, downloaded ${actual})`);
  err.code = NAMES.errors.integrity;
  return err;
}

// ---- restore ------------------------------------------------------------------

const CONFLICT = {
  [NAMES.onConflict.abort]: 'SQLITE_CHANGESET_ABORT',
  [NAMES.onConflict.replace]: 'SQLITE_CHANGESET_REPLACE',
  [NAMES.onConflict.omit]: 'SQLITE_CHANGESET_OMIT'
};

function conflictHandler(onConflict) {
  if (onConflict === undefined || onConflict === null) onConflict = NAMES.onConflict.abort;
  if (typeof onConflict === 'function') return onConflict;
  if (typeof onConflict === 'string' && CONFLICT[onConflict]) {
    const code = sqlite.constants[CONFLICT[onConflict]];
    return () => code;
  }
  throw new TypeError(`Expected "onConflict" to be ${Object.values(NAMES.onConflict).map((v) => `"${v}"`).join(', ')} or a function`);
}

// node:sqlite took a constant for onConflict in early releases and a
// function later. Try the function form first.
function applyChangeset(backend, bytes, onConflict) {
  try {
    return backend.applyChangeset(bytes, { onConflict });
  } catch (err) {
    if (err && err.code === 'ERR_INVALID_ARG_TYPE' && /onConflict/.test(err.message)) {
      return backend.applyChangeset(bytes, { onConflict: onConflict() });
    }
    throw err;
  }
}

// Restores the newest generation of `dbId` under `prefix` to the file `to`.
// Options:
//   store, prefix, dbId       where the sync wrote (dbId: the hash from the manifest key)
//   to                        destination path; replaced atomically on success
//   onConflict                'abort' (default) | 'replace' | 'omit' | (type) => constant
//   upToSeq                   stop after this changeset (point-in-time)
//   verify                    default true; false skips hash checks (emits a warning)
//   Database                  the Database class to open the copy with (set by index.js)
// Resolves { generation, seq, snapshotKey, applied: [keys], to }.
async function restore(options) {
  if (!isPlainObject(options)) throw new TypeError('Expected an options object');
  const store = assertObjectStore(options.store, 'store');
  const Database = options.Database || require('./database');
  if (typeof options.to !== 'string' || !options.to) throw new TypeError('Expected the "to" option to be a path');
  if (typeof options.dbId !== 'string' || !options.dbId) throw new TypeError('Expected the "dbId" option (the database hash in the manifest key)');
  const prefix = options.prefix === undefined ? '' : String(options.prefix).replace(/\/+$/, '');
  const base = prefix ? `${prefix}/${options.dbId}` : options.dbId;
  const onConflict = conflictHandler(options.onConflict);
  const verify = options.verify !== false;
  if (!verify) process.emitWarning('Importer.restore called with verify: false; downloaded objects are not checked', 'BetterSqliteImporterWarning');
  const upToSeq = options.upToSeq === undefined ? Infinity : options.upToSeq;
  if (upToSeq !== Infinity && (!Number.isInteger(upToSeq) || upToSeq < 0)) throw new TypeError('Expected "upToSeq" to be a non-negative integer');

  const manifestBytes = await readAll(await store.get(`${base}/${layout.manifest}`), DEFAULTS.maxManifestBytes);
  const manifest = JSON.parse(manifestBytes.toString('utf8'));
  if (!manifest || manifest.version !== 1 || !manifest.snapshot || typeof manifest.snapshot.key !== 'string') {
    throw new TypeError('Manifest has no snapshot to restore from');
  }
  const { snapshot, generation } = manifest;

  await fs.promises.access(path.dirname(path.resolve(options.to)));
  const tmp = `${options.to}.restore-${crypto.randomBytes(6).toString('hex')}`;
  try {
    // Snapshot: download, hash the stored bytes, decompress to the temp path.
    const hasher = new Hasher();
    const steps = [await store.get(snapshot.key), hasher];
    if (snapshot.gzip !== false && /\.gz$/.test(snapshot.key)) steps.push(zlib.createGunzip());
    steps.push(fs.createWriteStream(tmp));
    await pipeline(steps);
    if (verify) {
      if (typeof snapshot.sha256 !== 'string') throw new TypeError('Manifest snapshot has no sha256');
      const actual = hasher.digest();
      if (actual !== snapshot.sha256) throw verifyFailure(snapshot.key, snapshot.sha256, actual);
    }

    // Changesets of this generation, in sequence order.
    const listed = await store.list(`${base}/${layout.changesets}${generation}/`);
    const entries = listed
      .map((o) => ({ key: o.key, seq: Number((o.key.match(/(\d+)\.changeset$/) || [])[1]) }))
      .filter((o) => Number.isInteger(o.seq) && o.seq <= upToSeq)
      .sort((a, b) => a.seq - b.seq);

    const applied = [];
    let seq = 0;
    if (entries.length > 0) {
      const db = new Database(tmp);
      try {
        const backend = db[kDb].backend;
        if (typeof backend.applyChangeset !== 'function') throw new TypeError(`applyChangeset is not supported by node:sqlite on ${process.version}`);
        for (const entry of entries) {
          const bytes = await readAll(await store.get(entry.key), DEFAULTS.maxObjectBytes);
          if (verify) {
            const head = await store.head(entry.key);
            const expected = head && head.meta && head.meta.sha256;
            if (typeof expected !== 'string') {
              const err = new Error(`${entry.key}: store returned no sha256 metadata; refusing to apply an unverified changeset`);
              err.code = NAMES.errors.integrity;
              throw err;
            }
            const actual = crypto.createHash('sha256').update(bytes).digest('hex');
            if (actual !== expected) throw verifyFailure(entry.key, expected, actual);
          }
          const parts = unpack(bytes);
          db.transaction(() => {
            for (const part of parts) {
              const ok = applyChangeset(backend, part, onConflict);
              if (ok === false) throw Object.assign(new Error(`${entry.key}: changeset aborted on conflict`), { code: 'SQLITE_ABORT' });
            }
          }).immediate();
          applied.push(entry.key);
          seq = entry.seq;
        }
      } finally {
        db.close();
      }
    }

    await fs.promises.rename(tmp, options.to);
    return { generation, seq, snapshotKey: snapshot.key, applied, to: options.to };
  } catch (err) {
    await fs.promises.rm(tmp, { force: true }).catch(() => {});
    throw err;
  }
}

// ---- load -------------------------------------------------------------------

// Minimal RFC 4180 parser for one logical record. Returns the fields, or
// null when the record has an unterminated quote (caller appends the next line).
function parseCsvRecord(text) {
  const fields = [];
  let field = '';
  let quoted = false;
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        quoted = false; i += 1; continue;
      }
      field += ch; i += 1; continue;
    }
    if (ch === '"') { quoted = true; i += 1; continue; }
    if (ch === ',') { fields.push(field); field = ''; i += 1; continue; }
    field += ch; i += 1;
  }
  if (quoted) return null;
  fields.push(field);
  return fields;
}

function affinity(declared) {
  const t = String(declared || '').toUpperCase();
  if (t.includes('INT')) return 'INTEGER';
  if (t.includes('CHAR') || t.includes('CLOB') || t.includes('TEXT')) return 'TEXT';
  if (t === '' || t.includes('BLOB')) return 'BLOB';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB')) return 'REAL';
  return 'NUMERIC';
}

const BASE64 = /^[A-Za-z0-9+/]*={0,2}$/;

// Reverses lib/sync.js's encoding for one value given its column.
function decodeValue(value, column, fromCsv) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (typeof value === 'number' || typeof value === 'bigint') return value;
  if (Buffer.isBuffer(value)) return value;
  if (typeof value !== 'string') {
    // nested JSON: keep as text
    return JSON.stringify(value);
  }
  switch (column.affinity) {
    case 'BLOB':
      if (column.declared && value.length % 4 === 0 && BASE64.test(value)) return Buffer.from(value, 'base64');
      return value;
    case 'INTEGER':
      if (/^-?\d+$/.test(value)) {
        const big = BigInt(value);
        return big >= BigInt(Number.MIN_SAFE_INTEGER) && big <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(big) : big;
      }
      return fromCsv && /^-?\d*\.\d+$/.test(value) ? Number(value) : value;
    case 'REAL':
    case 'NUMERIC':
      return fromCsv && /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/.test(value) ? Number(value) : value;
    default:
      return value;
  }
}

// Loads rows from one object into `table`. Options:
//   store, key           what to read; `.gz` keys are gunzipped
//   table                target table (must exist)
//   format               'ndjson' (default; also for .jsonl/.ndjson keys) | 'csv'
//   mode                 'insert' (default) | 'replace' | 'upsert'
//   batch                rows per transaction (500)
//   columns              CSV without a header row: the column names in order
//   transform            row => row | null, before validation
//   resume               default true: continue where a previous run of this key stopped
//   nullValue            CSV only: field text meaning NULL (default '')
// Resolves { rows, skipped, lines, done }.
async function load(db, options) {
  if (!db || typeof db !== 'object' || !db[kDb]) throw new TypeError('Expected first argument to be a Database');
  if (!isPlainObject(options)) throw new TypeError('Expected second argument to be an options object');
  if (db.readonly) throw new TypeError('Cannot load into a readonly database');
  const store = assertObjectStore(options.store, 'store');
  if (typeof options.key !== 'string' || !options.key) throw new TypeError('Expected the "key" option to be a string');
  if (typeof options.table !== 'string' || !options.table) throw new TypeError('Expected the "table" option to be a string');
  const format = options.format || (/\.csv(\.gz)?$/.test(options.key) ? NAMES.formats.csv : NAMES.formats.ndjson);
  if (!Object.values(NAMES.formats).includes(format)) throw new TypeError(`Expected "format" to be ${oneOf(NAMES.formats)}`);
  const mode = options.mode || NAMES.modes.insert;
  if (!Object.values(NAMES.modes).includes(mode)) throw new TypeError(`Expected "mode" to be ${oneOf(NAMES.modes)}`);
  const batch = options.batch === undefined ? DEFAULTS.batch : options.batch;
  if (!Number.isInteger(batch) || batch <= 0) throw new TypeError('Expected the "batch" option to be a positive integer');
  const transform = options.transform;
  if (transform !== undefined && typeof transform !== 'function') throw new TypeError('Expected the "transform" option to be a function');
  const resume = options.resume !== false;
  const nullValue = options.nullValue === undefined ? '' : options.nullValue;
  const maxLineBytes = options.maxLineBytes || DEFAULTS.maxLineBytes;

  const info = db.prepare(`PRAGMA table_info(${quote(options.table)})`).all();
  if (info.length === 0) throw new TypeError(`No such table: ${options.table}`);
  const columns = new Map(info.map((c) => [c.name, { name: c.name, declared: c.type, affinity: affinity(c.type), pk: c.pk }]));
  const pkColumns = info.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk).map((c) => c.name);
  if (mode === NAMES.modes.upsert && pkColumns.length === 0) throw new TypeError(`upsert needs a PRIMARY KEY on ${options.table}`);

  db.exec(`
    CREATE TABLE IF NOT EXISTS ${IMPORTER} (
      key        TEXT PRIMARY KEY,
      "table"    TEXT NOT NULL,
      lines      INTEGER NOT NULL DEFAULT 0,
      rows       INTEGER NOT NULL DEFAULT 0,
      done_at    INTEGER,
      updated_at INTEGER NOT NULL
    ) WITHOUT ROWID`);
  const progress = db.prepare(`SELECT lines, rows, done_at FROM ${IMPORTER} WHERE key = ?`).get(options.key);
  if (resume && progress && progress.done_at !== null) {
    return { rows: 0, skipped: progress.lines, lines: progress.lines, done: true, resumed: true };
  }
  const skipLines = resume && progress ? progress.lines : 0;
  const saveProgress = db.prepare(`
    INSERT INTO ${IMPORTER} (key, "table", lines, rows, done_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT (key) DO UPDATE SET lines = excluded.lines, rows = excluded.rows, done_at = excluded.done_at, updated_at = excluded.updated_at`);

  // One prepared statement per distinct column set, since NDJSON rows may
  // omit columns. Column names were validated against table_info first.
  const statements = new Map();
  const statementFor = (names) => {
    const sig = names.join(' ');
    let stmt = statements.get(sig);
    if (stmt) return stmt;
    const cols = names.map(quote).join(', ');
    const marks = names.map(() => '?').join(', ');
    let sql;
    if (mode === NAMES.modes.replace) sql = `INSERT OR REPLACE INTO ${quote(options.table)} (${cols}) VALUES (${marks})`;
    else if (mode === NAMES.modes.upsert) {
      for (const pk of pkColumns) if (!names.includes(pk)) throw new TypeError(`upsert row is missing primary key column ${pk}`);
      const updates = names.filter((n) => !pkColumns.includes(n)).map((n) => `${quote(n)} = excluded.${quote(n)}`);
      const conflict = `ON CONFLICT (${pkColumns.map(quote).join(', ')}) DO ${updates.length ? `UPDATE SET ${updates.join(', ')}` : 'NOTHING'}`;
      sql = `INSERT INTO ${quote(options.table)} (${cols}) VALUES (${marks}) ${conflict}`;
    } else sql = `INSERT INTO ${quote(options.table)} (${cols}) VALUES (${marks})`;
    stmt = db.prepare(sql);
    statements.set(sig, stmt);
    return stmt;
  };

  const validate = (row, line) => {
    if (!isPlainObject(row)) throw new TypeError(`${options.key}:${line}: expected an object`);
    const names = Object.keys(row);
    for (const name of names) {
      if (!columns.has(name)) throw new TypeError(`${options.key}:${line}: unknown column "${name}" for table ${options.table}`);
    }
    return names;
  };

  const commit = db.transaction((pending, lines, rows, done) => {
    for (const { names, values } of pending) statementFor(names).run(...values);
    saveProgress.run(options.key, options.table, lines, rows, done ? Date.now() : null, Date.now());
  });

  let stream = await store.get(options.key);
  if (/\.gz$/.test(options.key)) {
    const gunzip = zlib.createGunzip();
    const out = new PassThrough();
    pipeline(stream, gunzip, out).catch((err) => out.destroy(err));
    stream = out;
  }
  const lines = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNo = 0;
  let rows = progress && resume ? progress.rows : 0;
  let skipped = 0;
  let csvHeader = options.columns ? options.columns.slice() : null;
  let csvCarry = null;
  let pending = [];

  const flush = (done) => {
    if (pending.length === 0 && !done) return;
    commit.immediate(pending, lineNo, rows, done);
    pending = [];
  };

  for await (const raw of lines) {
    if (raw.length > maxLineBytes) throw new RangeError(`${options.key}:${lineNo + 1}: line exceeds ${maxLineBytes} bytes`);
    let record;
    if (format === NAMES.formats.csv) {
      const text = csvCarry === null ? raw : `${csvCarry}\n${raw}`;
      const fields = parseCsvRecord(text);
      if (fields === null) { csvCarry = text; continue; }
      csvCarry = null;
      lineNo += 1;
      if (csvHeader === null) { csvHeader = fields; continue; }
      if (lineNo <= skipLines) { skipped += 1; continue; }
      if (fields.length === 1 && fields[0] === '' && text === '') continue;
      record = {};
      for (let i = 0; i < csvHeader.length; i += 1) {
        const value = fields[i];
        record[csvHeader[i]] = value === undefined || value === nullValue ? null : value;
      }
    } else {
      lineNo += 1;
      if (lineNo <= skipLines) { skipped += 1; continue; }
      const text = raw.trim();
      if (text === '') continue;
      try { record = JSON.parse(text); } catch (err) { throw new TypeError(`${options.key}:${lineNo}: invalid JSON: ${err.message}`); }
    }

    if (transform) {
      record = transform(record, lineNo);
      if (record === null || record === undefined) continue;
    }
    const names = validate(record, lineNo);
    if (mode === NAMES.modes.upsert) statementFor(names); // fail on a missing PK before writing anything
    const values = names.map((n) => decodeValue(record[n], columns.get(n), format === NAMES.formats.csv));
    pending.push({ names, values });
    rows += 1;
    if (pending.length >= batch) flush(false);
  }
  if (csvCarry !== null) throw new TypeError(`${options.key}: unterminated quoted field at end of file`);
  flush(true);
  return { rows: rows - (progress && resume ? progress.rows : 0), skipped, lines: lineNo, done: true };
}

module.exports = { restore, load, TABLE: IMPORTER, parseCsvRecord, decodeValue, affinity };
