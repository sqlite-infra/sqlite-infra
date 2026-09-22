'use strict';

// Operational and maintenance helpers, mixed onto Database.prototype by
// lib/database.js. Each is a thin, validated wrapper over the SQL statement or
// PRAGMA an operator would otherwise hand-write, so a service can checkpoint,
// vacuum, analyze and integrity-check its database through named methods that
// return shaped results instead of raw pragma rows.
//
// Extensions beyond the better-sqlite3 surface. They go through the connection
// the same way every other write does, so the iterator/busy guards apply and a
// verbose logger sees them.

const { quote } = require('./schema');

const CHECKPOINT_MODES = { passive: 'PASSIVE', full: 'FULL', restart: 'RESTART', truncate: 'TRUNCATE' };

// db.checkpoint(mode?): run a WAL checkpoint. `mode` is 'passive' (default),
// 'full', 'restart' or 'truncate'. Returns { busy, log, checkpointed }: `busy`
// is 1 when the checkpoint could not finish because of concurrent readers or
// writers, `log` the total frames in the WAL, `checkpointed` those moved back
// into the database. On a non-WAL database SQLite reports { busy: 0, log: -1,
// checkpointed: -1 }.
function checkpoint(mode = 'passive') {
  if (typeof mode !== 'string') throw new TypeError('Expected the checkpoint mode to be a string');
  const flag = CHECKPOINT_MODES[mode.toLowerCase()];
  if (!flag) throw new TypeError(`Expected the checkpoint mode to be one of: ${Object.keys(CHECKPOINT_MODES).join(', ')}`);
  const [row] = this.pragma(`wal_checkpoint(${flag})`);
  return { busy: row.busy, log: row.log, checkpointed: row.checkpointed };
}

// db.vacuum(into?): rebuild the database, defragmenting it and reclaiming free
// pages. With `into` (a file path) writes a fresh, compacted copy there with
// VACUUM INTO and leaves the live database untouched. Returns the database.
function vacuum(into) {
  if (into === undefined) {
    this.exec('VACUUM');
  } else {
    if (typeof into !== 'string' || !into) throw new TypeError('Expected the "into" argument to be a non-empty string');
    this.exec(`VACUUM INTO '${into.replace(/'/g, "''")}'`);
  }
  return this;
}

// db.analyze(target?): gather query-planner statistics for the whole database,
// or for one table or index when `target` is given. Returns the database.
function analyze(target) {
  if (target === undefined) {
    this.exec('ANALYZE');
  } else {
    if (typeof target !== 'string' || !target) throw new TypeError('Expected the "target" argument to be a non-empty string');
    this.exec(`ANALYZE ${quote(target)}`);
  }
  return this;
}

// db.optimize(): run PRAGMA optimize, the maintenance step SQLite recommends
// before closing a long-lived connection. Returns the database.
function optimize() {
  this.pragma('optimize');
  return this;
}

// db.integrityCheck(maxErrors?): a full integrity check (PRAGMA integrity_check).
// Returns an array of problem descriptions; the single-element array whose only
// entry is SQLite's health sentinel means no problems were found. `maxErrors`
// (default 100) caps how many problems SQLite reports.
function integrityCheck(maxErrors = 100) {
  return checkList.call(this, 'integrity_check', maxErrors);
}

// db.quickCheck(maxErrors?): PRAGMA quick_check — like integrityCheck but skips
// the (slower) index-consistency checks. Same return shape.
function quickCheck(maxErrors = 100) {
  return checkList.call(this, 'quick_check', maxErrors);
}

function checkList(pragma, maxErrors) {
  if (!Number.isInteger(maxErrors) || maxErrors <= 0) throw new TypeError('Expected the "maxErrors" argument to be a positive integer');
  return this.pragma(`${pragma}(${maxErrors})`).map((row) => row[pragma]);
}

// db.foreignKeyCheck(table?): PRAGMA foreign_key_check for the whole database,
// or one table. Returns [{ table, rowid, parent, foreignKey }], one per
// violation (empty when the foreign keys are consistent). `parent` is the
// referenced table; `foreignKey` is the index of the failing key in that
// table's foreign-key list (as PRAGMA foreign_key_list reports it).
function foreignKeyCheck(table) {
  const sql = table === undefined ? 'foreign_key_check' : `foreign_key_check(${quote(assertName(table))})`;
  return this.pragma(sql).map((row) => ({
    table: row.table,
    rowid: row.rowid,
    parent: row.parent,
    foreignKey: row.fkid
  }));
}

// db.userVersion([value]): read PRAGMA user_version, or set it when `value`
// (a non-negative 32-bit integer) is given. The user_version is a slot SQLite
// never touches — a natural place for a schema-version number. Returns the
// number when reading and the database when writing.
function userVersion(value) {
  if (value === undefined) {
    return this.pragma('user_version', { simple: true });
  }
  if (!Number.isInteger(value) || value < 0 || value > 0x7fffffff) {
    throw new TypeError('Expected the user_version to be a non-negative 32-bit integer');
  }
  this.pragma(`user_version = ${value}`);
  return this;
}

// db.attach(file, schema): ATTACH a database file under the schema name
// `schema`, so its tables are reachable as `schema.table`. The file path is
// bound as a parameter; the schema name is a quoted identifier. Returns the
// database.
function attach(file, schema) {
  if (typeof file !== 'string' || !file) throw new TypeError('Expected the "file" argument to be a non-empty string');
  this.prepare(`ATTACH DATABASE ? AS ${quote(assertName(schema))}`).run(file);
  return this;
}

// db.detach(schema): DETACH a database attached with attach(). Returns the
// database.
function detach(schema) {
  this.exec(`DETACH DATABASE ${quote(assertName(schema))}`);
  return this;
}

const JOURNAL_MODES = new Set(['delete', 'truncate', 'persist', 'memory', 'wal', 'off']);

// db.journalMode([mode]): read the journal mode, or set it to one of 'delete',
// 'truncate', 'persist', 'memory', 'wal', 'off'. Returns the current mode
// (lower-case) when reading and the database when setting.
function journalMode(mode) {
  if (mode === undefined) return this.pragma('journal_mode', { simple: true });
  if (typeof mode !== 'string' || !JOURNAL_MODES.has(mode.toLowerCase())) {
    throw new TypeError(`Expected the journal mode to be one of: ${[...JOURNAL_MODES].join(', ')}`);
  }
  this.pragma(`journal_mode = ${mode}`);
  return this;
}

// db.foreignKeysEnabled([enabled]): read whether foreign-key enforcement is on,
// or set it. Returns a boolean when reading and the database when setting.
// (Named to avoid colliding with db.foreignKeys(table), which lists a table's
// declared foreign keys.)
function foreignKeysEnabled(enabled) {
  if (enabled === undefined) return this.pragma('foreign_keys', { simple: true }) === 1;
  if (typeof enabled !== 'boolean') throw new TypeError('Expected first argument to be a boolean');
  this.pragma(`foreign_keys = ${enabled ? 'ON' : 'OFF'}`);
  return this;
}

// db.walAutocheckpoint([pages]): read the WAL auto-checkpoint threshold in
// pages, or set it (0 disables automatic checkpoints). Returns the number when
// reading and the database when setting.
function walAutocheckpoint(pages) {
  if (pages === undefined) return this.pragma('wal_autocheckpoint', { simple: true });
  if (!Number.isInteger(pages) || pages < 0) throw new TypeError('Expected the page count to be a non-negative integer');
  this.pragma(`wal_autocheckpoint = ${pages}`);
  return this;
}

function assertName(name) {
  if (typeof name !== 'string' || !name) throw new TypeError('Expected the name to be a non-empty string');
  return name;   // quote() escapes it for use as a SQL identifier
}

module.exports = {
  checkpoint, vacuum, analyze, optimize, integrityCheck, quickCheck, foreignKeyCheck, userVersion,
  attach, detach, journalMode, foreignKeysEnabled, walAutocheckpoint
};
