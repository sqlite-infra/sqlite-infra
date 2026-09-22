'use strict';

// Tests for db.retryTransaction (lib/retry.js): the busy-retry transaction
// wrapper. Backoff is kept tiny so the suite is fast.

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/retry'));

function seeded() {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
  return db;
}

const busy = () => Object.assign(new Error('database is locked'), { code: 'SQLITE_BUSY' });

test('runs fn in a transaction, returns its value, and commits', () => {
  const db = seeded();
  const result = db.retryTransaction(() => {
    db.prepare('INSERT INTO t (id, v) VALUES (1, ?)').run('a');
    return 42;
  }, { baseMs: 1, maxMs: 2 });
  assert.equal(result, 42);
  assert.equal(db.prepare('SELECT v FROM t WHERE id = 1').pluck().get(), 'a');
  db.close();
});

test('a non-busy error is not retried and is rethrown once', () => {
  const db = seeded();
  let calls = 0;
  assert.throws(() => db.retryTransaction(() => { calls += 1; throw new Error('boom'); }, { baseMs: 1, maxMs: 2 }), /boom/);
  assert.equal(calls, 1, 'fn ran exactly once');
  db.close();
});

test('a busy error is retried until it succeeds', () => {
  const db = seeded();
  let calls = 0;
  const result = db.retryTransaction(() => {
    calls += 1;
    if (calls < 3) throw busy();          // fail the first two attempts
    db.prepare('INSERT INTO t (id, v) VALUES (2, ?)').run('ok');
    return 'done';
  }, { retries: 5, baseMs: 1, maxMs: 2 });
  assert.equal(result, 'done');
  assert.equal(calls, 3);
  assert.equal(db.prepare('SELECT v FROM t WHERE id = 2').pluck().get(), 'ok');
  db.close();
});

test('exhausting retries rethrows the busy error after retries+1 attempts', () => {
  const db = seeded();
  let calls = 0;
  assert.throws(() => db.retryTransaction(() => { calls += 1; throw busy(); }, { retries: 3, baseMs: 1, maxMs: 2 }),
    (err) => err.code === 'SQLITE_BUSY');
  assert.equal(calls, 4, 'retries (3) + 1 initial attempt');
  db.close();
});

test('mode selects the BEGIN flavour and still works', () => {
  const db = seeded();
  for (const mode of ['deferred', 'immediate', 'exclusive']) {
    const n = db.retryTransaction(() => {
      db.prepare('INSERT INTO t (v) VALUES (?)').run(mode);
      return db.prepare('SELECT count(*) FROM t').pluck().get();
    }, { mode, baseMs: 1, maxMs: 2 });
    assert.ok(n >= 1);
  }
  db.close();
});

test('validates its arguments', () => {
  const db = seeded();
  assert.throws(() => db.retryTransaction('nope'), TypeError);
  assert.throws(() => db.retryTransaction(() => {}, { retries: -1 }), /"retries"/);
  assert.throws(() => db.retryTransaction(() => {}, { mode: 'bogus' }), /"mode"/);
  db.close();
});
