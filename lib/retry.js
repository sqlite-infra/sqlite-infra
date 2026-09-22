'use strict';

// db.retryTransaction(fn, options?): run fn inside a transaction, retrying the
// whole transaction with exponential backoff when it fails because the database
// was busy or locked. Mixed onto Database.prototype by lib/database.js.
//
// node:sqlite is synchronous, so the backoff between attempts is a synchronous
// sleep (Atomics.wait on a private buffer). Only SQLITE_BUSY / SQLITE_LOCKED are
// retried; any other error is rethrown at once. An extension beyond the
// better-sqlite3 surface, for services with several writers on one file.

const { backoff } = require('../adapters/util');

const MODES = ['deferred', 'immediate', 'exclusive'];

// A private, never-notified buffer: Atomics.wait blocks the thread for the
// timeout and returns 'timed-out'.
const SLEEP_BUFFER = new Int32Array(new SharedArrayBuffer(4));
function sleep(ms) {
  if (ms > 0) Atomics.wait(SLEEP_BUFFER, 0, 0, ms);
}

function isBusy(err) {
  return !!err && (
    err.code === 'SQLITE_BUSY' ||
    err.code === 'SQLITE_LOCKED' ||
    /database is locked|SQLITE_BUSY/i.test(err.message || '')
  );
}

function retryTransaction(fn, options = {}) {
  if (typeof fn !== 'function') throw new TypeError('Expected first argument to be a function');
  if (options === null || typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');

  const retries = options.retries === undefined ? 5 : options.retries;
  if (!Number.isInteger(retries) || retries < 0) throw new TypeError('Expected the "retries" option to be a non-negative integer');
  const baseMs = options.baseMs === undefined ? 25 : options.baseMs;
  const maxMs = options.maxMs === undefined ? 500 : options.maxMs;
  const mode = options.mode === undefined ? 'deferred' : options.mode;
  if (!MODES.includes(mode)) throw new TypeError('Expected the "mode" option to be "deferred", "immediate" or "exclusive"');

  const tx = this.transaction(fn);
  const runner = mode === 'immediate' ? tx.immediate : mode === 'exclusive' ? tx.exclusive : tx.deferred;

  let attempt = 0;
  for (;;) {
    attempt += 1;
    try {
      return runner();
    } catch (err) {
      // attempt counts from 1; total attempts allowed is retries + 1.
      if (!isBusy(err) || attempt > retries) throw err;
      sleep(backoff(attempt, baseMs, maxMs));
    }
  }
}

module.exports = { retryTransaction };
