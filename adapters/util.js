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

const { nameOf } = require('../lib/codes');
const SqliteError = require('../lib/sqlite-error');

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
    // Lazy require: keepalive pulls this module back, so a top-level require here
    // would form a load-time cycle (keepalive <- presence <- utils <- here).
    require('../lib/keepalive').onError(code);
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

// ---- schema validation (moved from lib/utils.js) ---------------------------

function typeOf(value) {
  return Array.isArray(value) ? 'array' : typeof value;
}

// Collects every violation for a field (not just the first) so a caller can
// report a complete list in one pass. `type: 'array'` is supported via
// Array.isArray rather than typeof (which reports 'object' for arrays).
// Nested object fields can carry their own `schema` for one level of
// recursion, since config objects like keepalive's `context` are objects.
// `strict: false` on the top-level schema disables the unexpected-field
// check, for schemas that intentionally allow passthrough fields.
function validateSchema(obj, schema, { strict = true, path: basePath = '' } = {}) {
  const errors = [];
  const prefix = (key) => (basePath ? `${basePath}.${key}` : key);

  for (const [key, spec] of Object.entries(schema)) {
    const value = obj[key];
    const field = prefix(key);

    if (spec.required && (value === undefined || value === null)) {
      errors.push({ field, error: 'required field missing' });
      continue;
    }
    if (value === undefined || value === null) continue;

    const actualType = typeOf(value);
    if (spec.type && actualType !== spec.type) {
      errors.push({ field, error: `expected ${spec.type}, got ${actualType}` });
      continue;
    }
    if (spec.enum && !spec.enum.includes(value)) {
      errors.push({ field, error: `must be one of ${spec.enum.join(', ')}` });
    }
    if (spec.min !== undefined && value < spec.min) {
      errors.push({ field, error: `must be >= ${spec.min}` });
    }
    if (spec.max !== undefined && value > spec.max) {
      errors.push({ field, error: `must be <= ${spec.max}` });
    }
    if (spec.pattern && !spec.pattern.test(value)) {
      errors.push({ field, error: `does not match pattern ${spec.pattern}` });
    }
    if (spec.type === 'object' && spec.schema) {
      const nested = validateSchema(value, spec.schema, { strict: spec.strict !== false, path: field });
      errors.push(...nested.errors);
    }
    if (spec.validate && typeof spec.validate === 'function') {
      const result = spec.validate(value);
      if (result !== true) errors.push({ field, error: result || 'validation failed' });
    }
  }

  if (strict) {
    for (const key of Object.keys(obj)) {
      if (!(key in schema)) errors.push({ field: prefix(key), error: 'unexpected field' });
    }
  }

  return { valid: errors.length === 0, errors };
}

// ---- diagnostics (moved from lib/utils.js) ---------------------------------

// Masks credentials embedded in a URL via the URL parser (both username and
// password) rather than a regex that only catches the `user:pass@` shape and
// still leaks passwords containing characters `\w` doesn't match.
function maskUrl(value) {
  if (typeof value !== 'string') return value;
  try {
    const url = new URL(value);
    if (url.username) url.username = '***';
    if (url.password) url.password = '***';
    return url.toString();
  } catch {
    return value; // not a URL; nothing to mask
  }
}

function describeConfig(config) {
  if (!config) return { status: 'not configured' };
  return {
    status: 'configured',
    endpoint: maskUrl(config.endpoint),
    interval: config.interval,
    queue: config.queue,
    slowMs: config.slowMs,
    service: config.service,
    context: config.context ? Object.keys(config.context) : null
  };
}

function describeState(agent) {
  if (!agent) return { status: 'no agent' };
  const queued = agent.queue ? agent.queue.length : 0;
  const queueLimit = agent.config ? agent.config.queue : 0;
  const errorCount = agent.errors ? [...agent.errors.values()].reduce((a, b) => a + b, 0) : 0;
  return {
    enabled: Boolean(agent.config),
    started: agent.started,
    healthy: Boolean(agent.config) && !agent.warned,
    connections: agent.live ? agent.live.size : 0,
    queued,
    queueUtilization: queueLimit ? Math.round((queued / queueLimit) * 100) : 0,
    dropped: agent.dropped || 0,
    slowMonitoring: agent.SLOW,
    errors: agent.errors ? Object.fromEntries(agent.errors) : {},
    errorCount,
    uptime: agent.startedAt ? Date.now() - agent.startedAt : null,
    warnings: agent.warned ? 'disabled due to error' : null
  };
}

// ---- cloud interface shapes -------------------------------------------------
// The three shapes through which data leaves or enters a database via a cloud
// service, and the validators the queue, consumer, importer and sync run on
// their options:
//
//   ObjectStore    put / get / head / list / delete on a key space
//   MessageSink    publish(envelopes) -> outcomes
//   MessageSource  receive / ack / nack
//
// The in-process implementations live in lib/memory.js, the HTTP sink in
// lib/http-sink.js, and the vendor SDKs in the adapter packages under
// adapters/, which map each shape onto one service and pass the suite in
// lib/contract.js.
//
// Shapes:
//   Envelope  { id, kind, attempt, body, attributes, orderingKey }
//              id is the idempotency key (stable across retries), body is the
//              JSON string to deliver, attributes are string -> string.
//   Outcome   { id, status: 'ok' | 'retry' | 'dead', error? }  one per envelope, same order;
//              error is an Error (keep the original so codes like ECONNREFUSED
//              survive to the queue's events) or a string
//   Message   { id, body, attributes, ackToken, attempt? }
//
// `retry` means the service may accept the same envelope later (throttled,
// unavailable, timed out); `dead` means no retry can fix it (too large,
// rejected as malformed). A sink that cannot tell says `retry`.

function hasMethods(value, methods) {
  return value !== null && typeof value === 'object' && methods.every((m) => typeof value[m] === 'function');
}

function assertObjectStore(store, what = 'store') {
  if (!hasMethods(store, ['put', 'get', 'head', 'list', 'delete'])) {
    throw new TypeError(`Expected "${what}" to be an ObjectStore with put(), get(), head(), list() and delete()`);
  }
  return store;
}

function assertMessageSink(sink, what = 'sink') {
  if (!hasMethods(sink, ['publish'])) {
    throw new TypeError(`Expected "${what}" to be a MessageSink with publish(envelopes)`);
  }
  return sink;
}

function assertMessageSource(source, what = 'source') {
  if (!hasMethods(source, ['receive', 'ack', 'nack'])) {
    throw new TypeError(`Expected "${what}" to be a MessageSource with receive(), ack() and nack()`);
  }
  return source;
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
  validateSchema,
  describeConfig,
  describeState,
  hasMethods,
  assertObjectStore,
  assertMessageSink,
  assertMessageSource,
  messages: { NOT_OPEN, DB_BUSY, STMT_BUSY }
};
