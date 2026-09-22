'use strict';

// Tests for lib/kv.js — the SQLite-backed key-value store (db.kv()).

const test = require('node:test');
const assert = require('node:assert/strict');

const Database = require('../index.js');
Object.assign(Database.prototype, require('../lib/kv'));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test('set/get round-trips every value type exactly', () => {
  const db = new Database(':memory:');
  const kv = db.kv();
  const cases = {
    str: 'hello',
    num: 42,
    float: 3.14,
    zero: 0,
    boolT: true,
    boolF: false,
    nul: null,
    obj: { a: 1, b: [2, 3], c: { d: 'e' } },
    arr: [1, 'two', false, null, { x: 1 }]
  };
  for (const [k, v] of Object.entries(cases)) kv.set(k, v);
  for (const [k, v] of Object.entries(cases)) assert.deepEqual(kv.get(k), v, k);
  // Types are preserved, not stringified.
  assert.equal(typeof kv.get('num'), 'number');
  assert.equal(typeof kv.get('boolT'), 'boolean');
  assert.equal(kv.get('nul'), null);
  assert.equal(kv.get('missing'), undefined);
  db.close();
});

test('has, delete and chainable set', () => {
  const db = new Database(':memory:');
  const kv = db.kv();
  assert.equal(kv.set('a', 1).set('b', 2), kv);   // chainable
  assert.equal(kv.has('a'), true);
  assert.equal(kv.has('nope'), false);
  assert.equal(kv.delete('a'), true);
  assert.equal(kv.delete('a'), false);
  assert.equal(kv.has('a'), false);
  db.close();
});

test('keys/entries/values are sorted and namespace-scoped', () => {
  const db = new Database(':memory:');
  const a = db.kv('a');
  const b = db.kv('b');
  a.set('z', 1).set('x', 2).set('y', 3);
  b.set('only', 99);
  assert.deepEqual(a.keys(), ['x', 'y', 'z']);
  assert.deepEqual(a.values(), [2, 3, 1]);
  assert.deepEqual(a.entries(), [['x', 2], ['y', 3], ['z', 1]]);
  assert.deepEqual(b.keys(), ['only']);
  assert.equal(a.get('only'), undefined, 'namespaces are isolated');
  assert.equal(a.namespace, 'a');
  db.close();
});

test('incr initializes, increments, and rejects non-numbers', () => {
  const db = new Database(':memory:');
  const kv = db.kv();
  assert.equal(kv.incr('n'), 1);
  assert.equal(kv.incr('n'), 2);
  assert.equal(kv.incr('n', 5), 7);
  assert.equal(kv.incr('n', -3), 4);
  kv.set('s', 'text');
  assert.throws(() => kv.incr('s'), /not a number/);
  db.close();
});

test('TTL entries expire lazily', async () => {
  const db = new Database(':memory:');
  const kv = db.kv();
  kv.set('temp', 'x', { ttlMs: 10 });
  kv.set('perm', 'y');
  assert.equal(kv.get('temp'), 'x');
  assert.equal(kv.size(), 2);
  await sleep(20);
  assert.equal(kv.get('temp'), undefined);
  assert.equal(kv.has('temp'), false);
  assert.equal(kv.size(), 1);
  assert.deepEqual(kv.keys(), ['perm']);
  db.close();
});

test('getSet computes once, then serves the cached value', () => {
  const db = new Database(':memory:');
  const kv = db.kv();
  let calls = 0;
  const produce = () => { calls += 1; return { n: calls }; };
  assert.deepEqual(kv.getSet('k', produce), { n: 1 });
  assert.deepEqual(kv.getSet('k', produce), { n: 1 });
  assert.equal(calls, 1);
  db.close();
});

test('getSet re-computes once a TTL entry has expired', async () => {
  const db = new Database(':memory:');
  const kv = db.kv();
  let calls = 0;
  const produce = () => { calls += 1; return calls; };
  assert.equal(kv.getSet('k', produce, { ttlMs: 10 }), 1);
  await sleep(20);
  assert.equal(kv.getSet('k', produce, { ttlMs: 10 }), 2);
  assert.equal(calls, 2);
  db.close();
});

test('clear and size are scoped to the namespace', () => {
  const db = new Database(':memory:');
  const a = db.kv('a');
  const b = db.kv('b');
  a.set('1', 1).set('2', 2).set('3', 3);
  b.set('x', 1);
  assert.equal(a.size(), 3);
  assert.equal(a.clear(), 3);
  assert.equal(a.size(), 0);
  assert.equal(b.size(), 1, 'other namespace untouched');
  db.close();
});

test('namespace is validated and undefined cannot be stored', () => {
  const db = new Database(':memory:');
  assert.throws(() => db.kv(''), TypeError);
  assert.throws(() => db.kv(5), TypeError);
  assert.throws(() => db.kv().set('u', undefined), /undefined/);
  db.close();
});
