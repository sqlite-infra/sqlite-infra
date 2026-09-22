'use strict';

// better-sqlite3: the better-sqlite3 API (Database, Statement, transactions,
// SqliteError codes) implemented on Node's built-in node:sqlite module.
// No native addon, no build step.

// V8 compile cache (Node 22.8+): persist the bytecode compilation of this
// package's ~70 modules to disk so subsequent process starts skip parse+compile.
// Input-independent and safe to run at require() time. Process-global — it also
// benefits the host's other modules; opt out with BETTER_SQLITE_COMPILE_CACHE=off.
// Wrapped so an unsupported runtime or a read-only cache dir can never affect
// importing this package.
if (process.env.BETTER_SQLITE_COMPILE_CACHE !== 'off') {
  try { require('node:module').enableCompileCache(); } catch { /* older Node or no writable cache dir */ }
}

const Database = require('./lib/database');
Database.SqliteError = require('./lib/sqlite-error');
Database.Queue = require('./lib/queue');
Database.Consumer = require('./lib/consumer');
Database.Sync = require('./lib/sync');
Database.interfaces = require('./lib/interfaces');
const importer = require('./lib/importer');
Database.Importer = {
  restore: (options) => importer.restore(Object.assign({ Database }, options)),
  load: importer.load,
  TABLE: importer.TABLE
};
Database.default = Database;

// Advisory, once per process: warn on stderr if a second SQLite driver is
// present in the host project. Deferred so require() returns immediately, and
// wrapped so a scan problem can never affect importing this package. Silence
// with BETTER_SQLITE_COLLISION=off. See lib/collision.js.
setImmediate(() => {
  try {
    require('./lib/collision').report();
  } catch (err) { /* never let the advisory affect the host */ }
});

module.exports = Database;
