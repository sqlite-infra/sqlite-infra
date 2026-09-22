'use strict';

// Error thrown for anything SQLite itself rejects. `code` is the SQLite result
// code name (e.g. 'SQLITE_CONSTRAINT_UNIQUE'), the same names better-sqlite3
// uses, so callers keep switching on `err.code`.
class SqliteError extends Error {
  constructor(message, code) {
    if (typeof code !== 'string') {
      throw new TypeError('Expected second argument to be a string');
    }
    super('' + message);
    this.code = code;
    if (typeof Error.captureStackTrace === 'function') {
      Error.captureStackTrace(this, SqliteError);
    }
  }
}

Object.defineProperty(SqliteError.prototype, 'name', {
  value: 'SqliteError',
  writable: true,
  enumerable: false,
  configurable: true
});

module.exports = SqliteError;
