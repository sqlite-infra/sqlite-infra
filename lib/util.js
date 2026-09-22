'use strict';

// Shared helpers and the guard checks better-sqlite3 applies before touching
// the connection. node:sqlite has no such checks: it lets a caller close the
// database or run writes while a statement is still stepping, and some of
// those sequences crash the process (verified against better-sqlite3's
// integrity suite on Node 23.11 and 24.21). These guards throw the TypeError
// better-sqlite3 would throw instead.
//
// State per database (see database.js):
//   open       connection is open
//   busy       a user-defined function or aggregate callback is running
//   iterators  number of Statement iterators that have not finished
//
// There is no switch that lifts these checks. better-sqlite3 has unsafeMode()
// for that; here it is accepted for API parity and changes nothing, because
// the sequences it would permit are the ones that crash node:sqlite.

const fs = require('fs');
const { nameOf } = require('./codes');
const { regionFromFile } = require('./region');
const SqliteError = require('./sqlite-error');
const { env: ENV } = require('./dictionary');

const kDb = Symbol('better-sqlite.database');

function getBooleanOption(options, key) {
  let value = false;
  if (key in options && typeof (value = options[key]) !== 'boolean') {
    throw new TypeError(`Expected the "${key}" option to be a boolean`);
  }
  return value;
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype;
}

// An integer option greater than zero, or `fallback` when the key is absent.
function positiveInt(options, key, fallback) {
  if (!(key in options)) return fallback;
  const value = options[key];
  if (!Number.isInteger(value) || value <= 0) throw new TypeError(`Expected the "${key}" option to be a positive integer`);
  return value;
}

// Exponential backoff with full jitter in [0.5, 1.5) so many workers do not
// retry in lockstep.
function backoff(attempt, baseMs, maxMs) {
  const exp = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(exp * (0.5 + Math.random()));
}

function unsupported(what) {
  return new TypeError(`${what} is not supported by node:sqlite on ${process.version}`);
}

const NOT_OPEN = 'The database connection is not open';
const DB_BUSY = 'This database connection is busy executing a query';
const STMT_BUSY = 'This statement is busy executing a query';

function requireOpen(state) {
  if (!state.open) throw new TypeError(NOT_OPEN);
}

function requireNotBusy(state) {
  if (state.busy) throw new TypeError(DB_BUSY);
}

function requireNoIterators(state) {
  if (state.iterators > 0) throw new TypeError(DB_BUSY);
}

// Wraps a user callback so the connection counts as busy while it runs, and
// keeps the callback's `length`, which node:sqlite reads to fix the SQL
// function's argument count.
function busyWrap(state, fn, length = fn.length) {
  const wrapped = function busyCallback(...args) {
    const previous = state.busy;
    state.busy = true;
    try {
      return fn.apply(this, args);
    } finally {
      state.busy = previous;
    }
  };
  Object.defineProperty(wrapped, 'length', { value: length });
  return wrapped;
}

// better-sqlite3's check on function.length for user-defined functions.
function getLength({ length }) {
  if (Number.isInteger(length) && length >= 0) return length;
  throw new TypeError('Expected function.length to be a positive integer');
}

// Runs `fn` and rewrites the errors node:sqlite throws into the errors
// better-sqlite3 would throw for the same situation:
//   ERR_SQLITE_ERROR   -> SqliteError with a named result code
//   ERR_INVALID_STATE  -> TypeError (closed connection, finalized statement)
// Anything else (TypeError/RangeError from argument checks) passes through.
function translate(fn) {
  try {
    return fn();
  } catch (err) {
    throw convert(err);
  }
}

function convert(err) {
  if (!err || typeof err !== 'object') return err;
  if (err.code === 'ERR_SQLITE_ERROR') {
    if (/Returned JavaScript value cannot be converted/.test(err.message)) {
      // A user-defined function returned something SQLite cannot store.
      // better-sqlite3 reports that as a TypeError.
      return new TypeError(err.message);
    }
    const code = nameOf(err.errcode);
    error.onError(code);
    return new SqliteError(err.message, code);
  }
  if (err.code === 'ERR_LOAD_SQLITE_EXTENSION') {
    return new SqliteError(err.message, 'SQLITE_ERROR');
}
  if (err.code === 'ERR_INVALID_STATE') {
    return new TypeError(/not open|finalized/.test(err.message) ? messages.NOT_OPEN : err.message);
  }
  if (err.code === 'ERR_OUT_OF_RANGE' && /JavaScript number/.test(err.message)) {
    // An INTEGER outside Number's safe range. better-sqlite3 would return a
    // lossy double here; node:sqlite refuses, so point at the BigInt path.
    return new RangeError(`${err.message}. Call statement.safeIntegers() or db.defaultSafeIntegers() to read it as a BigInt`);
  }
  return err;
}

function getRegion(env = process.env, param = 'config', getPath) {
  return fs.readFileSync(getPath(env, param), 'utf8');
}

module.exports = {
  convert,
  kDb,
  isPlainObject,
  positiveInt,
  backoff,
  getBooleanOption,
  unsupported,
  busyWrap,
  getLength,
  requireOpen,
  requireNotBusy,
  requireNoIterators,
  translate,
  getRegion,
  messages: { NOT_OPEN, DB_BUSY, STMT_BUSY }
};

// Placed after module.exports (not with the requires above) so that when
// keepalive.js circularly requires this module back (directly, and via
// utils.js), it gets the complete exports object above instead of the
// empty one CommonJS hands out mid-load.
const error = require('./keepalive');
