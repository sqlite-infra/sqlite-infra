'use strict';

// Sync: ship a database to an object store, in three shapes.
//
//   const out = new Database.Sync(db, { store: new ObjectStore({ bucket }), prefix: 'sqlite' });
//   await out.snapshot();                 // whole database, consistent, gzipped
//   await out.changeset();                // everything this connection wrote since the last call
//   await out.exportRows('orders', {      // one table as NDJSON for Athena / BigQuery
//     where: 'created_at >= ?', params: [since]
//   });
//   out.start();                          // changeset every `everyMs`, snapshot every `snapshotEvery`
//
// Keys, under `prefix` (the database is named by a stable hash of its name,
// never by its path; see lib/identity.js):
//
//   <prefix>/<dbId>/manifest.json
//   <prefix>/<dbId>/snapshots/<generation>/<timestamp>.sqlite.gz
//   <prefix>/<dbId>/changesets/<generation>/<seq, 12 digits>.changeset
//   <prefix>/<dbId>/exports/<table>/dt=<YYYY-MM-DD>/<timestamp>.ndjson.gz
//   <prefix>/<dbId>/exports/<table>/schema.json
//
// A generation is one snapshot plus the changesets taken after it; restore
// (lib/importer.js) downloads the snapshot and applies the changesets in
// sequence order. A new snapshot starts a new generation.
//
// Snapshots use db.backup() when the engine has it and `VACUUM INTO`
// otherwise; both produce a consistent copy while writers continue. The
// copy is gzipped to a temp file first (lib/staging.js) so its size and
// SHA-256 are known before upload.
//
// Changesets come from node:sqlite's session extension, one session per
// user table. One session for the whole schema would also record this
// package's own tables (the queue, the consumer, and this module's pending
// queue, whose rows are the changesets themselves), so a whole-schema
// changeset would carry its own history and grow on itself. Per-table
// sessions keep the bookkeeping out. The parts are framed by lib/changeset.js.
// Two limits are the extension's own, not this module's: a session records
// only tables with an explicit PRIMARY KEY, and only writes made through
// this connection. A file shared by several writers needs snapshots, or one
// sync per writing process each producing its own stream. Schema
// changes are not in a changeset either; the sync notices them
// (`schemaChanged`) and start() takes a snapshot instead.
//
// changeset() is durable the way the queue is: the bytes are moved into a
// pending table in the same transaction that resets the sessions, then
// uploaded from there and deleted on success. A crash between the two loses
// nothing; the next Sync on this file uploads what is pending.
//
// Row exports are lib/export.js; this class only serializes them against
// the other two operations and emits the result.
//
// Nothing here is switched on by the environment. The store is an object
// the host constructed; there is no default bucket, prefix or endpoint.

const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { databaseId } = require('./identity');
const { kDb, isPlainObject, positiveInt, requireOpen, unsupported } = require('./util');
const { assertObjectStore } = require('./interfaces');
const { pack } = require('./changeset');
const { userTables, hasPrimaryKey, schemaHash } = require('./schema');
const { stage, unlink, timestamp } = require('./staging');
const { tempPath } = require('./params');
const { exportRows } = require('./export');
const { tables, defaults, events, keys: layout, contentTypes, contentEncodings, store: storeNames } = require('./dictionary');

const SYNC = tables.syncState;
const PENDING = tables.syncPending;

const DEFAULTS = defaults.sync;

function libVersion() {
  try { return require('../package.json').version; } catch { return 'unknown'; }
}

function padSeq(seq) {
  return String(seq).padStart(12, '0');
}

class Sync extends EventEmitter {
  constructor(db, options) {
    super();
    if (!db || typeof db !== 'object' || !db[kDb]) throw new TypeError('Expected first argument to be a Database');
    if (!isPlainObject(options)) throw new TypeError('Expected second argument to be an options object');
    if (db.readonly) throw new TypeError('Cannot attach a Sync to a readonly database');

    this.db = db;
    this.store = assertObjectStore(options.store, 'store');
    const prefix = options.prefix === undefined ? '' : options.prefix;
    if (typeof prefix !== 'string') throw new TypeError('Expected the "prefix" option to be a string');
    this.gzip = 'gzip' in options ? Boolean(options.gzip) : DEFAULTS.gzip;
    this.everyMs = positiveInt(options, 'everyMs', null);
    this.snapshotEvery = positiveInt(options, 'snapshotEvery', null);
    this.leaseMs = positiveInt(options, 'leaseMs', DEFAULTS.leaseMs);

    const hasSessions = typeof db[kDb].backend.createSession === 'function';
    if (options.changesets === true && !hasSessions) throw unsupported('changesets (createSession)');
    this.changesets = 'changesets' in options ? Boolean(options.changesets) : hasSessions;

    this.dbId = databaseId(db.name, db.memory);
    this.base = prefix ? `${prefix.replace(/\/+$/, '')}/${this.dbId}` : this.dbId;
    this.owner = `${os.hostname()}:${process.pid}:${crypto.randomBytes(4).toString('hex')}`;
    this.sessions = new Map();     // table -> Session
    this.schema = null;            // hash when sessions were (re)opened
    this.timer = null;
    this.running = false;
    this.busy = null;              // promise of the operation in progress

    db.exec(`
      CREATE TABLE IF NOT EXISTS ${SYNC} (
        id           INTEGER PRIMARY KEY CHECK (id = 1),
        generation   INTEGER NOT NULL DEFAULT 0,
        seq          INTEGER NOT NULL DEFAULT 0,
        locked_until INTEGER,
        lock_owner   TEXT,
        updated_at   INTEGER
      );
      INSERT OR IGNORE INTO ${SYNC} (id, generation, seq) VALUES (1, 0, 0);
      CREATE TABLE IF NOT EXISTS ${PENDING} (
        generation INTEGER NOT NULL,
        seq        INTEGER NOT NULL,
        bytes      BLOB    NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (generation, seq)
      );
    `);

    this.stmts = {
      state: db.prepare(`SELECT generation, seq, locked_until, lock_owner FROM ${SYNC} WHERE id = 1`),
      countPending: db.prepare(`SELECT count(*) AS n FROM ${PENDING}`).pluck(),
      bumpSeq: db.prepare(`UPDATE ${SYNC} SET seq = seq + 1, updated_at = ? WHERE id = 1 RETURNING generation, seq`),
      bumpGeneration: db.prepare(`UPDATE ${SYNC} SET generation = ?, seq = 0, updated_at = ? WHERE id = 1`),
      insertPending: db.prepare(`INSERT INTO ${PENDING} (generation, seq, bytes, created_at) VALUES (?, ?, ?, ?)`),
      listPending: db.prepare(`SELECT generation, seq, bytes FROM ${PENDING} ORDER BY generation, seq`),
      deletePending: db.prepare(`DELETE FROM ${PENDING} WHERE generation = ? AND seq = ?`),
      dropOldPending: db.prepare(`DELETE FROM ${PENDING} WHERE generation < ?`),
      lease: db.prepare(`
        UPDATE ${SYNC} SET locked_until = ?, lock_owner = ?
         WHERE id = 1 AND (locked_until IS NULL OR locked_until <= ? OR lock_owner = ?)
        RETURNING generation`),
      release: db.prepare(`UPDATE ${SYNC} SET locked_until = NULL, lock_owner = NULL WHERE id = 1 AND lock_owner = ?`)
    };

    if (this.changesets) this._openSessions();
    if (options.autoStart === true) this.start();
  }

  // ---- state ------------------------------------------------------------------

  get generation() { return this.stmts.state.get().generation; }
  get seq() { return this.stmts.state.get().seq; }
  pending() { return this.stmts.countPending.get(); }

  // True when a table was created, dropped or altered since the sessions
  // were opened: the next changeset would miss it, so take a snapshot.
  get schemaChanged() {
    return this.changesets && schemaHash(this.db) !== this.schema;
  }

  // Tables the session extension cannot record (no explicit PRIMARY KEY).
  unsupportedTables() {
    return userTables(this.db).filter((t) => !hasPrimaryKey(this.db, t));
  }

  keys() {
    return {
      manifest: `${this.base}/${layout.manifest}`,
      snapshots: `${this.base}/${layout.snapshots}`,
      changesets: `${this.base}/${layout.changesets}`,
      exports: `${this.base}/${layout.exports}`
    };
  }

  // ---- snapshot -------------------------------------------------------------

  // A consistent copy of the whole database, gzipped, as a new generation.
  // Resolves { key, generation, size, sha256, at }.
  snapshot() {
    return this._exclusive(async () => {
      const db = this.db;
      requireOpen(db);
      const at = new Date();
      const copy = tempPath(db, 'snapshot');
      const staged = `${copy}.staged`;
      try {
        if (db.constructor.engine && db.constructor.engine.backup) {
          await db.backup(copy);
        } else {
          db.exec(`VACUUM INTO '${copy.replace(/'/g, "''")}'`);
        }
        // Boundary: everything written before this point is in the copy;
        // everything after goes to the sessions just reset. The final
        // delta of the current generation is kept so that generation stays
        // complete even if this upload fails.
        const newGeneration = this.stmts.state.get().generation + 1;
        if (this.changesets) {
          this._cutChangeset(false);
          // Reopen even when nothing was cut, so tables created since the
          // sessions were opened are recorded from here on.
          this._openSessions();
        }

        const { size, sha256 } = await stage(copy, staged, this.gzip);
        const key = `${this.keys().snapshots}${newGeneration}/${timestamp(at)}${layout.extensions.sqlite}${this.gzip ? layout.extensions.gzip : ''}`;
        await this.store.put(key, fs.createReadStream(staged), {
          size,
          sha256,
          contentType: this.gzip ? contentTypes.gzip : contentTypes.sqlite,
          contentEncoding: this.gzip ? contentEncodings.gzip : undefined
        });

        db.transaction(() => {
          this.stmts.bumpGeneration.run(newGeneration, Date.now());
          this.stmts.dropOldPending.run(newGeneration);
        }).immediate();

        const info = { key, generation: newGeneration, size, sha256, at: at.toISOString(), gzip: this.gzip };
        await this._writeManifest({ snapshot: info });
        this.emit(events.sync.snapshot, info);
        return info;
      } finally {
        await unlink(copy);
        await unlink(staged);
      }
    });
  }

  // ---- changesets ---------------------------------------------------------------

  // Moves this connection's writes since the last cut into the pending
  // table and uploads everything pending. Resolves the info of the newest
  // upload, or null when nothing changed and nothing was pending.
  changeset() {
    if (!this.changesets) return Promise.reject(unsupported('changesets (createSession)'));
    return this._exclusive(async () => {
      requireOpen(this.db);
      this._cutChangeset(true);
      return this._uploadPending();
    });
  }

  // Synchronous: read every session, frame the parts, store them as the next
  // seq of the current generation, reopen the sessions. All in one
  // transaction so a crash leaves either the old sessions' content or the
  // pending row, never neither.
  _cutChangeset(noteSchema) {
    const parts = [];
    for (const session of this.sessions.values()) {
      const bytes = session.changeset();
      if (bytes && bytes.length > 0) parts.push(bytes);
    }
    if (parts.length === 0) {
      if (noteSchema && this.schemaChanged) this.emit(events.sync.schemaChanged);
      return null;
    }
    const packed = pack(parts);
    const row = this.db.transaction(() => {
      const { generation, seq } = this.stmts.bumpSeq.get(Date.now());
      this.stmts.insertPending.run(generation, seq, packed, Date.now());
      return { generation, seq };
    }).immediate();
    this._openSessions();
    return row;
  }

  async _uploadPending() {
    let last = null;
    for (const row of this.stmts.listPending.all()) {
      const bytes = Buffer.isBuffer(row.bytes) ? row.bytes : Buffer.from(row.bytes);
      const sha256 = crypto.createHash('sha256').update(bytes).digest('hex');
      const key = `${this.keys().changesets}${row.generation}/${padSeq(row.seq)}${layout.extensions.changeset}`;
      await this.store.put(key, bytes, { size: bytes.length, sha256, contentType: contentTypes.binary });
      if (!this.db.open) return last;
      this.stmts.deletePending.run(row.generation, row.seq);
      last = { key, generation: row.generation, seq: row.seq, size: bytes.length, sha256 };
      this.emit(events.sync.changeset, last);
    }
    if (last) await this._writeManifest({});
    return last;
  }

  _openSessions() {
    this._closeSessions();
    const backend = this.db[kDb].backend;
    for (const table of userTables(this.db)) {
      if (hasPrimaryKey(this.db, table)) this.sessions.set(table, backend.createSession({ table }));
    }
    this.schema = schemaHash(this.db);
  }

  _closeSessions() {
    for (const session of this.sessions.values()) {
      try { session.close(); } catch { /* already closed */ }
    }
    this.sessions.clear();
  }

  // ---- row export -----------------------------------------------------------

  // Streams one table (or view) as NDJSON; see lib/export.js for the options.
  // Resolves { key, schemaKey, table, rows, size, sha256, at, gzip }.
  exportRows(table, options = {}) {
    if (typeof table !== 'string' || !table) throw new TypeError('Expected table to be a non-empty string');
    if (!isPlainObject(options)) throw new TypeError('Expected options to be an object');
    const target = { store: this.store, dir: this.keys().exports, gzip: this.gzip };
    return this._exclusive(async () => {
      const result = await exportRows(this.db, table, options, target);
      this.emit(events.sync.export, result);
      return result;
    });
  }

  // ---- manifest ---------------------------------------------------------------

  async _readManifest() {
    let stream;
    try {
      stream = await this.store.get(this.keys().manifest);
    } catch (err) {
      if (err && err.code === storeNames.errors.notFound) return null;
      throw err;
    }
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    try { return JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { return null; }
  }

  async _writeManifest(patch) {
    const previous = (await this._readManifest()) || {};
    const state = this.stmts.state.get();
    const manifest = Object.assign({}, previous, {
      version: 1,
      dbId: this.dbId,
      generation: state.generation,
      seq: state.seq,
      updatedAt: new Date().toISOString(),
      lib: libVersion()
    }, patch);
    if (!manifest.snapshot) manifest.snapshot = null;
    await this.store.put(this.keys().manifest, Buffer.from(JSON.stringify(manifest, null, 2)), { contentType: contentTypes.json });
    return manifest;
  }

  manifest() { return this._readManifest(); }

  // ---- scheduling ---------------------------------------------------------------

  // Every `everyMs`: a changeset, or a snapshot when the schema changed,
  // when `snapshotEvery` changesets have gone out, when no snapshot exists
  // yet, or always when changesets are off. Only one process on a file runs
  // a tick at a time (lease in the sync table). The timer is ref'd: a
  // sync that is started is expected to keep running. Tick failures
  // are emitted as 'error'; attach a listener, or Node throws them.
  start() {
    if (this.running) return this;
    if (!this.everyMs) throw new TypeError('start() needs the "everyMs" option');
    this.running = true;
    this.timer = setInterval(() => { this._tick().catch((err) => this.emit(events.sync.error, err)); }, this.everyMs);
    return this;
  }

  stop() {
    this.running = false;
    if (this.timer) { clearInterval(this.timer); this.timer = null; }
    return this;
  }

  // Waits for the operation in progress, then stops.
  async drain() {
    this.stop();
    if (this.busy) await this.busy.catch(() => {});
    this._release();
  }

  async _tick() {
    if (!this.running || !this.db.open || this.busy) return;
    const now = Date.now();
    if (!this.stmts.lease.get(now + this.leaseMs, this.owner, now, this.owner)) return;
    try {
      const state = this.stmts.state.get();
      const needSnapshot = state.generation === 0
        || this.schemaChanged
        || (this.snapshotEvery && state.seq >= this.snapshotEvery)
        || !this.changesets;
      if (needSnapshot) await this.snapshot();
      else await this.changeset();
    } finally {
      this._release();
    }
  }

  _release() {
    if (this.db.open) this.stmts.release.run(this.owner);
  }

  // Serializes snapshot / changeset / exportRows: they share the sessions,
  // the sync row and the temp directory.
  _exclusive(fn) {
    const run = (this.busy || Promise.resolve()).catch(() => {}).then(fn);
    this.busy = run;
    return run.finally(() => { if (this.busy === run) this.busy = null; });
  }

  close() {
    this.stop();
    this._closeSessions();
  }
}

Sync.TABLES = Object.freeze({ SYNC, PENDING });
module.exports = Sync;
