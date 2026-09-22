'use strict';

// Database: a synchronous SQLite connection with better-sqlite3's API, built
// on Node's bundled node:sqlite instead of a compiled addon.
//
//   const Database = require('better-sqlite3');
//   const db = new Database('data/app.db');      // or ':memory:', or a Buffer
//   db.pragma('journal_mode = WAL');
//   db.exec('CREATE TABLE IF NOT EXISTS ...');
//   const stmt = db.prepare('INSERT INTO t (a) VALUES (@a)');
//   const { lastInsertRowid, changes } = stmt.run({ a: 1 });
//   db.transaction((rows) => { for (const r of rows) stmt.run(r); })(rows);
//
// Options (same names and defaults as better-sqlite3):
//   readonly       open read-only (false)
//   fileMustExist  throw instead of creating a missing file (false)
//   timeout        ms to wait on a locked database (5000)
//   verbose        function called with every SQL string executed (null)
//
// Everything the engine underneath exposes is wired: user-defined functions
// and aggregates, online backup, serialize/deserialize, extension loading.
// Features node:sqlite does not offer on the running Node version throw a
// TypeError that says so and light up automatically once the engine grows
// them. Virtual tables (db.table()) have no node:sqlite counterpart.
//
// Like better-sqlite3, the connection refuses operations that are unsafe
// while a statement is iterating or a user-defined function is running
// (close(), exec(), writes, registering functions). node:sqlite itself does
// not check, and some of those sequences crash the process. Unlike
// better-sqlite3 there is no unsafeMode() escape hatch: the method exists so
// code written for better-sqlite3 still loads, but the guards stay on.

const fs = require('fs');
const path = require('path');
const sqlite = require('./backend');
const SqliteError = require('./sqlite-error');
const { Statement, kPrivate } = require('./statement');
const { translate, convert } = require('./util');
const { scan } = require('./params');
const { defaults } = require('./dictionary');
const {
  kDb, getBooleanOption, unsupported, busyWrap, getLength,
  requireOpen, requireNotBusy, requireNoIterators
} = require('./util');

const { DatabaseSync, StatementSync } = sqlite;

const DEFAULT_CACHE_SIZE = -16000;

// db.prepareCached() keeps at most this many compiled statements per
// connection, evicting the least-recently-used when full.
const PREPARED_CACHE_MAX = 100;

// Engine capabilities, by feature detection rather than version comparison.
const engine = {
  isTransaction: 'isTransaction' in DatabaseSync.prototype,      // Node 22.16+, 24+
  aggregate: typeof DatabaseSync.prototype.aggregate === 'function',
  serialize: typeof DatabaseSync.prototype.serialize === 'function',
  deserialize: typeof DatabaseSync.prototype.deserialize === 'function',
  defensive: typeof DatabaseSync.prototype.enableDefensive === 'function',  // Node 24.12+
  arrays: typeof StatementSync.prototype.setReturnArrays === 'function',    // Node 22.16+, 24+
  backup: typeof sqlite.backup === 'function',
  sessions: typeof DatabaseSync.prototype.createSession === 'function'   // Node 22.12+, 23.3+
};

function Database(filenameGiven, options) {
  if (new.target == null) {
    return new Database(filenameGiven, options);
  }

  let buffer = null;
  if (Buffer.isBuffer(filenameGiven)) {
    buffer = filenameGiven;
    filenameGiven = ':memory:';
  }
  if (filenameGiven == null) filenameGiven = '';
  if (options == null) options = {};

  if (typeof filenameGiven !== 'string') throw new TypeError('Expected first argument to be a string');
  if (typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  if ('readOnly' in options) throw new TypeError('Misspelled option "readOnly" should be "readonly"');
  if ('memory' in options) throw new TypeError('Option "memory" was removed in v7.0.0 (use ":memory:" filename instead)');

  const filename = filenameGiven.trim();
  const anonymous = filename === '' || filename === ':memory:';
  const readonly = getBooleanOption(options, 'readonly');
  const fileMustExist = getBooleanOption(options, 'fileMustExist');
  const timeout = 'timeout' in options ? options.timeout : defaults.database.timeoutMs;
  const verbose = 'verbose' in options ? options.verbose : null;

  if (readonly && anonymous && !buffer) throw new TypeError('In-memory/temporary databases cannot be readonly');
  if (!Number.isInteger(timeout) || timeout < 0) throw new TypeError('Expected the "timeout" option to be a positive integer');
  if (timeout > 0x7fffffff) throw new RangeError('Option "timeout" cannot be greater than 2147483647');
  if (verbose != null && typeof verbose !== 'function') throw new TypeError('Expected the "verbose" option to be a function');
  if (buffer && !engine.deserialize) throw unsupported('Opening a database from a Buffer (deserialize)');

  if (!anonymous && !filename.startsWith('file:')) {
    if (!fs.existsSync(path.dirname(filename))) {
      throw new TypeError('Cannot open database because the directory does not exist');
    }
    if (fileMustExist && !fs.existsSync(filename)) {
      throw new SqliteError('unable to open database file', 'SQLITE_CANTOPEN');
    }
  }

  // allowExtension only permits enableLoadExtension() later; loading stays off
  // until loadExtension() turns it on for the duration of one load.
  const backend = translate(() => new DatabaseSync(anonymous ? ':memory:' : filename, {
    readOnly: readonly && !buffer,
    allowExtension: true
  }));
  if (buffer) {
    // deserialize() always yields a writable database; query_only makes
    // writes fail with SQLITE_READONLY the way a read-only open would.
    translate(() => backend.deserialize(buffer));
    if (readonly) translate(() => backend.exec('PRAGMA query_only = 1'));
  }

  const state = {
    backend,
    open: true,
    verbose,
    safeIntegers: false,
    busy: false,        // inside a user-defined function/aggregate callback
    iterators: 0,       // Statement iterators not yet finished
    probe: null,        // BEGIN/COMMIT pair for inTransaction on old engines
    changes: null,      // SELECT total_changes(), for Statement#run()
    totalChanges() {
      if (!this.changes) this.changes = this.backend.prepare('SELECT total_changes() AS c');
      return this.changes.get().c;
    }
  };
  Object.defineProperties(this, {
    [kDb]: { value: state },
    name: { value: filenameGiven, enumerable: true },
    readonly: { value: readonly, enumerable: true },
    memory: { value: anonymous, enumerable: true }
  });

  // better-sqlite3's build defaults cache_size to -16000 (16 MB) where stock
  // SQLite uses -2000. Setting it reads the schema, which another connection
  // holding an exclusive lock would block; with busy_timeout still 0 that
  // fails at once instead of waiting, and the stock size is kept.
  try {
    translate(() => backend.exec(`PRAGMA cache_size = ${DEFAULT_CACHE_SIZE}`));
  } catch (err) {
    if (err.code !== 'SQLITE_BUSY') throw err;
  }
  // better-sqlite3 applies its busy timeout on open; SQLite's default is 0.
  translate(() => backend.exec(`PRAGMA busy_timeout = ${timeout}`));
  // SQLITE_DBCONFIG_DEFENSIVE, which better-sqlite3 turns on by default.
  // Node 24.12+ exposes it; earlier engines have no equivalent. It stays on
  // for the life of the connection (see unsafeMode()).
  if (engine.defensive) backend.enableDefensive(true);
}

Object.defineProperties(Database.prototype, {
  open: {
    get() { return this[kDb].open; },
    enumerable: true
  },
  inTransaction: {
    get() {
      const state = this[kDb];
      if (!state.open) return false;
      if (engine.isTransaction) return state.backend.isTransaction;
      // Older node:sqlite has no sqlite3_get_autocommit() equivalent, so ask
      // SQLite directly: a BEGIN inside a transaction is refused, and a BEGIN
      // that succeeds is committed at once, which takes no lock and writes
      // nothing.
      if (!state.probe) {
        state.probe = { begin: state.backend.prepare('BEGIN'), commit: state.backend.prepare('COMMIT') };
      }
      try {
        state.probe.begin.run();
      } catch (err) {
        if (err && err.code === 'ERR_SQLITE_ERROR' && /within a transaction/.test(err.message)) return true;
        throw convert(err);
      }
      state.probe.commit.run();
      return false;
    },
    enumerable: true
  }
});

Database.prototype.prepare = function prepare(sql) {
  return this._prepare(sql, {});
};

// db.prepareCached(sql): like prepare(), but the compiled Statement is cached
// per connection and returned again for the same SQL, so a hot statement is
// compiled once. The cache is an LRU capped at PREPARED_CACHE_MAX and is
// cleared by close() and by clearPreparedCache(). The same Statement object is
// returned each time, so its row-mode toggles (pluck/raw/expand/safeIntegers)
// and any bind() persist across calls — do not cache a statement you intend to
// bind() permanently unless every caller wants that binding. An extension
// beyond the better-sqlite3 surface.
Database.prototype.prepareCached = function prepareCached(sql) {
  if (typeof sql !== 'string') throw new TypeError('Expected first argument to be a string');
  const state = this[kDb];
  requireOpen(state);
  const cache = state.prepared || (state.prepared = new Map());
  const hit = cache.get(sql);
  if (hit !== undefined) {
    cache.delete(sql);      // re-insert so it counts as most-recently-used
    cache.set(sql, hit);
    return hit;
  }
  const stmt = this._prepare(sql, {});
  cache.set(sql, stmt);
  if (cache.size > PREPARED_CACHE_MAX) cache.delete(cache.keys().next().value);
  return stmt;
};

// db.clearPreparedCache(): drop every statement prepareCached() is holding.
// Returns the database.
Database.prototype.clearPreparedCache = function clearPreparedCache() {
  const state = this[kDb];
  if (state.prepared) state.prepared.clear();
  return this;
};

Database.prototype._prepare = function _prepare(sql, options) {
  if (typeof sql !== 'string') throw new TypeError('Expected first argument to be a string');
  const state = this[kDb];
  requireOpen(state);
  requireNotBusy(state);
  const params = scan(sql);
  if (params.empty) throw new RangeError('The supplied SQL string contains no statements');
  if (params.multiple) throw new RangeError('The supplied SQL string contains more than one statement');
  const backend = translate(() => state.backend.prepare(sql));
  const stmt = new Statement(kPrivate, this, state, backend, sql, params, options);
  if (state.safeIntegers) stmt.safeIntegers(true);
  return stmt;
};

// Unwrapped node:sqlite statement and a guarded way to run it, for the
// transaction controller.
Database.prototype._raw = function _raw(sql) {
  const state = this[kDb];
  requireOpen(state);
  return translate(() => state.backend.prepare(sql));
};

Database.prototype._run = function _run(rawStatement) {
  const state = this[kDb];
  requireOpen(state);
  requireNotBusy(state);
  requireNoIterators(state);
  return translate(() => rawStatement.run());
};

Database.prototype.exec = function exec(sql) {
  if (typeof sql !== 'string') throw new TypeError('Expected first argument to be a string');
  const state = this[kDb];
  requireOpen(state);
  requireNotBusy(state);
  requireNoIterators(state);
  if (state.verbose) state.verbose.call(undefined, sql);
  translate(() => state.backend.exec(sql));
  return this;
};

Database.prototype.pragma = function pragma(source, options) {
  if (options == null) options = {};
  if (typeof source !== 'string') throw new TypeError('Expected first argument to be a string');
  if (typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  const simple = getBooleanOption(options, 'simple');
  const state = this[kDb];
  requireOpen(state);
  requireNotBusy(state);
  requireNoIterators(state);

  const stmt = this._prepare(`PRAGMA ${source}`, { pragma: true });
  if (!stmt.reader) {
    // e.g. PRAGMA foreign_keys = ON: executes but yields no rows.
    stmt.run();
    return simple ? undefined : [];
  }
  return simple ? stmt.pluck().get() : stmt.all();
};

// db.explain(sql): the bytecode program as rows of { addr, opcode, p1..p5,
// comment }, or the query plan for 'QUERY PLAN ...'. Parameters stay unbound.
Database.prototype.explain = function explain(source) {
  if (typeof source !== 'string') throw new TypeError('Expected first argument to be a string');
  return this._prepare(`EXPLAIN ${source}`, { explain: true }).all();
};

Database.prototype.transaction = require('./transaction');

// db.function(name, [options], fn): register a scalar SQL function.
// Options: varargs, deterministic, directOnly, safeIntegers (BigInt args).
Database.prototype.function = function fn(name, options, callback) {
  if (options == null) options = {};
  if (typeof options === 'function') { callback = options; options = {}; }
  if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  if (typeof callback !== 'function') throw new TypeError('Expected last argument to be a function');
  if (typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');
  if (!name) throw new TypeError('User-defined function name cannot be an empty string');

  const state = this[kDb];
  const safeIntegers = 'safeIntegers' in options ? getBooleanOption(options, 'safeIntegers') : state.safeIntegers;
  const deterministic = getBooleanOption(options, 'deterministic');
  const directOnly = getBooleanOption(options, 'directOnly');
  const varargs = getBooleanOption(options, 'varargs');
  let argCount = 0;
  if (!varargs) {
    argCount = getLength(callback);
    if (argCount > 100) throw new RangeError('User-defined functions cannot have more than 100 arguments');
  }

  requireOpen(state);
  requireNotBusy(state);
  requireNoIterators(state);
  const config = { varargs, deterministic, directOnly, useBigIntArguments: safeIntegers };
  translate(() => state.backend.function(name, config, busyWrap(state, callback, argCount)));
  return this;
};

// db.aggregate(name, { start, step, inverse, result, ... }): register an
// aggregate (and, with `inverse`, window) function. Semantics follow
// better-sqlite3: `start` may be a value or a factory; step()/inverse()
// returning undefined leave the accumulator unchanged; the argument count is
// the greater of step.length and inverse.length, minus the accumulator.
Database.prototype.aggregate = function aggregate(name, options) {
  if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  if (typeof options !== 'object' || options === null) throw new TypeError('Expected second argument to be an options object');
  if (!name) throw new TypeError('User-defined function name cannot be an empty string');

  const start = 'start' in options ? options.start : null;
  const step = getFunctionOption(options, 'step', true);
  const inverse = getFunctionOption(options, 'inverse', false);
  const result = getFunctionOption(options, 'result', false);
  const state = this[kDb];
  const safeIntegers = 'safeIntegers' in options ? getBooleanOption(options, 'safeIntegers') : state.safeIntegers;
  const deterministic = getBooleanOption(options, 'deterministic');
  const directOnly = getBooleanOption(options, 'directOnly');
  const varargs = getBooleanOption(options, 'varargs');

  let length = 1;
  if (!varargs) {
    length = Math.max(getLength(step), inverse ? getLength(inverse) : 0);
    if (length - 1 > 100) throw new RangeError('User-defined functions cannot have more than 100 arguments');
  }

  requireOpen(state);
  requireNotBusy(state);
  requireNoIterators(state);
  if (!engine.aggregate) throw unsupported('aggregate()');

  const keep = (fn) => function stepOrInverse(accumulator, ...args) {
    const value = fn.call(this, accumulator, ...args);
    return value === undefined ? accumulator : value;
  };
  const config = {
    start: typeof start === 'function' ? busyWrap(state, start) : () => start,
    step: busyWrap(state, keep(step), length),
    varargs,
    deterministic,
    directOnly,
    useBigIntArguments: safeIntegers
  };
  if (inverse) config.inverse = busyWrap(state, keep(inverse), length);
  if (result) config.result = busyWrap(state, result);
  translate(() => state.backend.aggregate(name, config));
  return this;
};

function getFunctionOption(options, key, required) {
  const value = key in options ? options[key] : null;
  if (typeof value === 'function') return value;
  if (value != null) throw new TypeError(`Expected the "${key}" option to be a function`);
  if (required) throw new TypeError(`Missing required option "${key}"`);
  return null;
}

// db.table(): virtual tables have no node:sqlite counterpart.
Database.prototype.table = function table(name) {
  if (typeof name !== 'string') throw new TypeError('Expected first argument to be a string');
  if (!name) throw new TypeError('Virtual table module name cannot be an empty string');
  const state = this[kDb];
  requireOpen(state);
  requireNotBusy(state);
  requireNoIterators(state);
  throw unsupported('table() (virtual tables)');
};

// db.loadExtension(path): load a SQLite extension. Extension loading is
// enabled only for the duration of the call.
Database.prototype.loadExtension = function loadExtension(extension) {
  if (typeof extension !== 'string') throw new TypeError('Expected first argument to be a string');
  const state = this[kDb];
  requireOpen(state);
  requireNotBusy(state);
  requireNoIterators(state);
  translate(() => state.backend.enableLoadExtension(true));
  try {
    translate(() => state.backend.loadExtension(extension));
  } finally {
    state.backend.enableLoadExtension(false);
  }
  return this;
};

// db.backup(destination, { attached, progress }): online backup to a file.
// Resolves to { totalPages, remainingPages: 0 }. `progress` receives
// { totalPages, remainingPages } before the first step and after each step
// that leaves pages to copy. If `progress` throws, or returns something other
// than a number or undefined, the promise rejects with that error and a
// destination file that did not exist before is removed; a failure raised
// after the copy has started is delivered once the copy completes, because
// node:sqlite cannot abort a backup midway.
Database.prototype.backup = async function backup(filename, options) {
  if (options == null) options = {};
  if (typeof filename !== 'string') throw new TypeError('Expected first argument to be a string');
  if (typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');

  filename = filename.trim();
  const attached = 'attached' in options ? options.attached : 'main';
  const progress = 'progress' in options ? options.progress : null;

  if (!filename) throw new TypeError('Backup filename cannot be an empty string');
  if (filename === ':memory:') throw new TypeError('Invalid backup filename ":memory:"');
  if (typeof attached !== 'string') throw new TypeError('Expected the "attached" option to be a string');
  if (!attached) throw new TypeError('The "attached" option cannot be an empty string');
  if (progress != null && typeof progress !== 'function') throw new TypeError('Expected the "progress" option to be a function');
  if (!engine.backup) throw unsupported('backup()');

  const state = this[kDb];
  requireOpen(state);
  await fs.promises.access(path.dirname(filename)).catch(() => {
    throw new TypeError('Cannot save backup because the directory does not exist');
  });
  const isNewFile = await fs.promises.access(filename).then(() => false, () => true);

  // better-sqlite3 starts with a zero-page step and reports it, so the first
  // progress call is { totalPages, remainingPages: totalPages }; a number
  // returned from it sets the pages per step (default 100). Later returns
  // cannot change the rate: node:sqlite fixes it when the copy starts.
  let rate = 100;
  let failure = null;
  const report = (info) => {
    if (!progress || failure) return;
    try {
      const ret = progress.call(undefined, info);
      if (ret === undefined) return;
      if (typeof ret !== 'number' || ret !== ret) {
        throw new TypeError('Expected progress callback to return a number or undefined');
      }
      rate = Math.max(1, Math.min(0x7fffffff, Math.round(ret)));
    } catch (err) {
      failure = err;
    }
  };
  if (progress) {
    const totalPages = translate(() => state.backend.prepare(`PRAGMA "${attached.replace(/"/g, '""')}".page_count`).get()).page_count;
    report({ totalPages: Number(totalPages), remainingPages: Number(totalPages) });
    if (failure) throw failure;
  }
  const config = { source: attached, target: 'main', rate, progress: report };
  try {
    const totalPages = await sqlite.backup(state.backend, filename, config);
    if (failure) throw failure;
    return { totalPages, remainingPages: 0 };
  } catch (err) {
    if (isNewFile) await fs.promises.rm(filename, { force: true }).catch(() => {});
    throw convert(err);
  }
};

// db.serialize({ attached }): the database as a Buffer. Needs
// DatabaseSync#serialize (Node 24+); older engines throw.
Database.prototype.serialize = function serialize(options) {
  if (options == null) options = {};
  if (typeof options !== 'object') throw new TypeError('Expected first argument to be an options object');
  const attached = 'attached' in options ? options.attached : 'main';
  if (typeof attached !== 'string') throw new TypeError('Expected the "attached" option to be a string');
  if (!attached) throw new TypeError('The "attached" option cannot be an empty string');
  const state = this[kDb];
  requireOpen(state);
  requireNotBusy(state);
  if (!engine.serialize) throw unsupported('serialize()');
  const bytes = translate(() => state.backend.serialize(attached));
  return Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
};

// db.defaultSafeIntegers([toggle]): make new statements return INTEGERs as
// BigInt, and pass BigInt arguments to user functions, unless overridden.
Database.prototype.defaultSafeIntegers = function defaultSafeIntegers(toggle = true) {
  if (typeof toggle !== 'boolean') throw new TypeError('Expected first argument to be a boolean');
  this[kDb].safeIntegers = toggle;
  return this;
};

// db.unsafeMode([toggle]): accepted for API parity with better-sqlite3 and
// deliberately a no-op. Upstream, this lifts the iterator guard (so exec(),
// pragma() and run() may interleave with an open iterator) and SQLite's
// defensive flag (journal_mode = OFF, writable_schema = ON, ...). On
// node:sqlite the guarded sequences are the ones that crash the process, and
// the defensive flag is what keeps a stray pragma from corrupting a database
// file, so neither is negotiable here. Code that relies on unsafe mode fails
// loudly at the guarded call with the usual TypeError instead of running.
Database.prototype.unsafeMode = function unsafeMode(toggle = true) {
  if (typeof toggle !== 'boolean') throw new TypeError('Expected first argument to be a boolean');
  return this;
};

Database.prototype.close = function close() {
  const state = this[kDb];
  if (state.open) {
    requireNotBusy(state);
    requireNoIterators(state);
    state.open = false;
    state.probe = null;
    state.changes = null;
    if (state.prepared) state.prepared.clear();
    translate(() => state.backend.close());
  }
  return this;
};

Database.prototype[Symbol.for('nodejs.util.inspect.custom')] = function inspect() {
  return `Database {\n  name: ${JSON.stringify(this.name)},\n  open: ${this.open},\n  readonly: ${this.readonly},\n  memory: ${this.memory}\n}`;
};

// Extensions beyond the better-sqlite3 surface, kept in their own modules and
// mixed onto the prototype here: read-only schema introspection (lib/introspect),
// operational/maintenance helpers (lib/maintenance), and the migration runner
// (lib/migrate). They use only the public methods above, so the same guards
// apply as to any other call.
// Framework layer, kept in their own modules and mixed onto the prototype here.
// query and pipeline also export their builder/Pipeline classes, so only the
// intended prototype methods are picked from them (the classes go on the
// constructor below).
const migrateModule = require('./migrate');
const query = require('./query');
const pipelineModule = require('./pipeline');
Object.assign(
  Database.prototype,
  require('./introspect'),
  require('./maintenance'),
  require('./bulk'),
  require('./batch'),
  require('./schema-builder'),
  require('./model'),
  require('./relations'),
  require('./diff'),
  require('./views'),
  require('./audit'),
  require('./cache'),
  require('./search'),
  require('./observe'),
  require('./lock'),
  require('./json'),
  require('./retry'),
  { scoped: require('./scope').scoped },
  { timeseries: require('./timeseries').timeseries },
  { kv: require('./kv').kv },
  { generateTypes: require('./typegen').generateTypes },
  { partitioned: require('./partition').partitioned },
  { from: query.from, insertInto: query.insertInto, updateTable: query.updateTable, deleteFrom: query.deleteFrom },
  { analytics: require('./analytics').analytics },
  { pipeline: pipelineModule.pipeline },
  { with: require('./cte').with, withRecursive: require('./cte').withRecursive },
  (({ union, unionAll, intersect, except }) => ({ union, unionAll, intersect, except }))(require('./setops')),
  { migrate: migrateModule.migrate, rollback: migrateModule.rollback }
);
// Static helper: load a directory of migration modules for migrate()/rollback().
Database.loadMigrations = migrateModule.loadMigrations;
// Connection pool (WAL): one writer plus read-only readers over one file.
const poolModule = require('./pool');
Database.Pool = poolModule.Pool;
Database.pool = poolModule.createPool;
// The streaming ETL pipeline class, for `new Database.Pipeline(...)` style use.
Database.Pipeline = pipelineModule.Pipeline;
// Job scheduler over a Queue-like sink.
Database.Scheduler = require('./scheduler').Scheduler;
// Safe SQL composition DSL (tagged template + expression helpers).
const exprModule = require('./expr');
Database.sql = exprModule.sql;
Database.expr = exprModule.expr;
Database.Fragment = exprModule.Fragment;
// Standalone row/schema validator (usable in pipelines, ingest, models).
const validateModule = require('./validate');
Database.validator = validateModule.validator;
Database.ValidationError = validateModule.ValidationError;
// Scheduled snapshot/changeset backups to an ObjectStore, with pruning.
Database.BackupManager = require('./backup-manager').BackupManager;
// Generate TypeScript row interfaces from a database's schema.
Database.generateTypes = require('./typegen').forDatabase;

// What the running engine supports; useful for callers that branch on it.
Database.engine = Object.freeze({ ...engine });

module.exports = Database;
