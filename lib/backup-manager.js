'use strict';

// BackupManager: scheduled backups of a database to an ObjectStore, plus
// retention. A thin scheduler on top of Database.Sync (lib/sync.js) — Sync owns
// the actual snapshot/changeset upload and the store layout; this class decides
// WHEN to back up, keeps at most `retain` snapshot generations in the store,
// and reports what it did.
//
//   const mgr = new Database.BackupManager(db, {
//     store: new ObjectStore({ bucket }),   // any ObjectStore
//     everyMs: 3_600_000,                    // hourly
//     snapshotEvery: 24,                     // a fresh snapshot every 24 changesets
//     retain: 7,                             // keep the newest 7 generations
//   });
//   mgr.on('backup', (info) => log(info));
//
// On each tick it takes a changeset (when the engine has sessions and
// changesets are on) or a snapshot (the first backup, after a schema change,
// every `snapshotEvery` changesets, or always when changesets are off), then
// prunes old generations. Only one backup runs at a time: a tick that lands
// while one is still in flight is skipped. Like Sync's own scheduler the timer
// is unref'd, so a manager never keeps a process alive on its own, and a
// failure inside a tick is emitted as 'error' rather than thrown into the timer.

const { EventEmitter } = require('events');
const { kDb, isPlainObject, positiveInt } = require('./util');
const Sync = require('./sync');

const DEFAULT_EVERY_MS = 60 * 60 * 1000;   // hourly

// The generation directory segment of a snapshot/changeset key, or null. Keys
// look like `<prefix>/snapshots/<generation>/<timestamp>...`.
function generationOf(key, prefix) {
  if (typeof key !== 'string' || !key.startsWith(prefix)) return null;
  const segment = key.slice(prefix.length).split('/')[0];
  const n = Number(segment);
  return Number.isInteger(n) && n > 0 ? n : null;
}

class BackupManager extends EventEmitter {
  constructor(db, options = {}) {
    super();
    if (!db || typeof db !== 'object' || !db[kDb]) throw new TypeError('Expected first argument to be a Database');
    if (!isPlainObject(options)) throw new TypeError('Expected second argument to be an options object');
    if (options.store == null) throw new TypeError('BackupManager needs a "store"');

    this.db = db;
    this.everyMs = positiveInt(options, 'everyMs', DEFAULT_EVERY_MS);
    this.snapshotEvery = positiveInt(options, 'snapshotEvery', null);
    this.retain = positiveInt(options, 'retain', null);

    // The Sync does the work; this manager schedules it. Do not hand Sync
    // everyMs/autoStart — its own timer would double up with ours.
    this.sync = new Sync(db, {
      store: options.store,
      prefix: options.prefix,
      gzip: options.gzip,
      changesets: options.changesets
    });
    this.store = this.sync.store;

    this.timer = null;
    this.running = false;
    this.busy = null;      // promise of the backup in progress, or null

    if (options.autoStart !== false) this.start();
  }

  get generation() { return this.sync.generation; }
  get seq() { return this.sync.seq; }

  // ---- lifecycle ------------------------------------------------------------

  start() {
    if (this.running) return this;
    this.running = true;
    this.timer = setInterval(() => { this._tick(); }, this.everyMs);
    // Never hold the event loop open just to run backups.
    this.timer.unref();
    return this;
  }

  stop() {
    this.running = false;
    if (this.timer) { clearInterval(this.timer); this.timer = null; }
    return this;
  }

  // Waits for the backup in progress (if any), then stops.
  async drain() {
    this.stop();
    if (this.busy) await this.busy.catch(() => {});
  }

  close() {
    this.stop();
    this.sync.close();
  }

  // ---- backups --------------------------------------------------------------

  // Back up now. `{ snapshot: true }` forces a full snapshot; otherwise it
  // takes a changeset when the engine supports them (a snapshot for the first
  // backup, after a schema change, or when changesets are off). Resolves the
  // upload info, or null when a changeset found nothing to send. Serialized
  // against any other backup.
  backupNow(options = {}) {
    if (!isPlainObject(options)) throw new TypeError('Expected first argument to be an options object');
    return this._serialize(() => this._doBackup(Boolean(options.snapshot)));
  }

  // Delete snapshot generations beyond `retain`, newest kept. Returns the
  // number of generations removed (0 when `retain` is unset or nothing is old
  // enough). Also removes the changesets belonging to the dropped generations.
  async prune() {
    if (!this.retain) return 0;
    const keys = this.sync.keys();
    const snapshots = await this.store.list(keys.snapshots);
    const generations = new Set();
    for (const object of snapshots) {
      const g = generationOf(object.key, keys.snapshots);
      if (g !== null) generations.add(g);
    }
    const ordered = [...generations].sort((a, b) => b - a);
    const keep = new Set(ordered.slice(0, this.retain));
    const drop = ordered.slice(this.retain);
    if (drop.length === 0) return 0;

    for (const object of snapshots) {
      const g = generationOf(object.key, keys.snapshots);
      if (g !== null && !keep.has(g)) await this.store.delete(object.key);
    }
    const changesets = await this.store.list(keys.changesets);
    for (const object of changesets) {
      const g = generationOf(object.key, keys.changesets);
      if (g !== null && !keep.has(g)) await this.store.delete(object.key);
    }
    this.emit('pruned', drop.length);
    return drop.length;
  }

  // The latest manifest recorded in the store (or null).
  manifest() { return this.sync.manifest(); }

  // ---- internals ------------------------------------------------------------

  async _doBackup(forceSnapshot) {
    const info = (forceSnapshot || this._needSnapshot())
      ? await this._snapshot()
      : await this.sync.changeset();     // may resolve null
    if (info) this.emit('backup', info);
    return info;
  }

  async _snapshot() {
    const info = await this.sync.snapshot();
    this.emit('snapshot', info);
    return info;
  }

  _needSnapshot() {
    if (!this.sync.changesets) return true;         // no changeset support → always snapshot
    if (this.sync.generation === 0) return true;    // nothing uploaded yet
    if (this.sync.schemaChanged) return true;       // a changeset would miss the new schema
    if (this.snapshotEvery && this.sync.seq >= this.snapshotEvery) return true;
    return false;
  }

  // Runs backups one at a time. A queued call waits for the one before it.
  _serialize(fn) {
    const run = (this.busy || Promise.resolve()).catch(() => {}).then(fn);
    this.busy = run;
    return run.finally(() => { if (this.busy === run) this.busy = null; });
  }

  // The scheduled path: skip if not running, closed, or a backup is in flight;
  // back up, then prune; a failure becomes an 'error' event, never a throw.
  _tick() {
    if (!this.running || !this.db.open || this.busy) return Promise.resolve();
    const run = this._serialize(async () => {
      await this._doBackup(false);
      if (this.retain) await this.prune();
    });
    return run.catch((err) => this.emit('error', err));
  }
}

module.exports = { BackupManager };
