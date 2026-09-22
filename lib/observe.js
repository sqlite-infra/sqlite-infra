'use strict';

// Per-connection query observability, mixed onto Database.prototype by
// lib/database.js. observe() turns on timing for one connection: it installs an
// own-property `prepare` on that instance which delegates to the prototype
// prepare and then wraps the returned Statement's run()/get()/all() to measure
// elapsed time and accumulate per-SQL stats. iterate() is not timed (a lazy
// cursor has no single elapsed span). All state lives in a module-level
// WeakMap<db, state>, so nothing in database.js changes and a collected
// connection's stats are released with it.
//
// Timing never affects correctness: a slow/query callback that throws is
// swallowed, and a query error propagates unchanged. When a connection is not
// observing, the wrapped methods short-circuit to the original with a single
// boolean check, so leaving observe() off (or calling unobserve()) restores
// near-zero overhead.

const states = new WeakMap();

function stateOf(db) {
  let state = states.get(db);
  if (!state) {
    state = { observing: false, options: {}, stats: new Map(), wrapped: new WeakSet() };
    states.set(db, state);
  }
  return state;
}

function record(state, sql, ms, rows) {
  let entry = state.stats.get(sql);
  if (!entry) { entry = { count: 0, totalMs: 0, maxMs: 0, rows: 0 }; state.stats.set(sql, entry); }
  entry.count += 1;
  entry.totalMs += ms;
  if (ms > entry.maxMs) entry.maxMs = ms;
  entry.rows += rows;
}

function fire(fn, sql, ms) {
  if (typeof fn !== 'function') return;
  try { fn(sql, ms); } catch { /* an observer callback must never break a query */ }
}

// Rows "produced" by one call, for the stats: the row array length for all(),
// 1/0 for a get() hit/miss, 0 for run() (it changes rows, it does not read them).
function rowsOf(method, result) {
  if (method === 'all') return Array.isArray(result) ? result.length : 0;
  if (method === 'get') return result === undefined ? 0 : 1;
  return 0;
}

function wrapStatement(stmt, state) {
  if (state.wrapped.has(stmt)) return stmt;
  state.wrapped.add(stmt);
  const sql = stmt.source;
  for (const method of ['run', 'get', 'all']) {
    const original = stmt[method].bind(stmt);
    Object.defineProperty(stmt, method, {
      configurable: true,
      writable: true,
      value: function observed(...args) {
        if (!state.observing) return original(...args);
        const start = performance.now();
        const result = original(...args);          // a query error propagates unchanged
        const ms = performance.now() - start;
        record(state, sql, ms, rowsOf(method, result));
        const opts = state.options;
        fire(opts.onQuery, sql, ms);
        if (opts.slowMs !== undefined && ms >= opts.slowMs) fire(opts.onSlow, sql, ms);
        return result;
      }
    });
  }
  return stmt;
}

// db.observe({ slowMs?, onSlow?, onQuery? }): start timing this connection's
// prepared statements. Returns the database.
function observe(options = {}) {
  if (options === null || typeof options !== 'object') throw new TypeError('Expected first argument to be an options object');
  const db = this;
  const state = stateOf(db);
  state.observing = true;
  state.options = options;
  if (!Object.prototype.hasOwnProperty.call(db, 'prepare')) {
    Object.defineProperty(db, 'prepare', {
      configurable: true,
      writable: true,
      value: function observedPrepare(sql) {
        const stmt = Object.getPrototypeOf(db).prepare.call(db, sql);
        if (state.observing) wrapStatement(stmt, state);
        return stmt;
      }
    });
  }
  return db;
}

// db.unobserve(): stop timing and restore the prototype prepare. Collected
// stats are kept (read them with queryStats()). Returns the database.
function unobserve() {
  const state = stateOf(this);
  state.observing = false;
  if (Object.prototype.hasOwnProperty.call(this, 'prepare')) delete this.prepare;
  return this;
}

// db.queryStats(): [{ sql, count, totalMs, avgMs, maxMs, rows }], busiest
// (highest totalMs) first.
function queryStats() {
  const state = stateOf(this);
  const out = [];
  for (const [sql, e] of state.stats) {
    out.push({ sql, count: e.count, totalMs: e.totalMs, avgMs: e.count ? e.totalMs / e.count : 0, maxMs: e.maxMs, rows: e.rows });
  }
  out.sort((a, b) => b.totalMs - a.totalMs);
  return out;
}

// db.resetStats(): clear collected stats; leaves observing on/off unchanged.
function resetStats() {
  stateOf(this).stats = new Map();
  return this;
}

module.exports = { observe, unobserve, queryStats, resetStats };
